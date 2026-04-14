"""
Cliente HTTP base para la API de Mercado Libre.
Maneja automáticamente:
  - Inyección del Bearer token
  - Rate limiting (respeta los límites de ML)
  - Retry con backoff en errores 429 / 5xx
  - Paginación automática
"""

import time
import logging
from pathlib import Path
from typing import Optional, Generator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import ML_API_BASE, REQUEST_DELAY_SECONDS
from auth.ml_auth import get_valid_access_token

logger = logging.getLogger(__name__)


class MLClient:
    """
    Cliente reutilizable para la API de Mercado Libre.

    Uso:
        client = MLClient()
        data = client.get("/users/me")
        items = client.get("/users/{user_id}/items/search", params={"limit": 50})
    """

    def __init__(self):
        self.base_url = ML_API_BASE
        self.session  = self._build_session()
        self._last_request_time = 0.0

    # ─── Construcción de sesión con retry automático ──────────────

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session

    # ─── Rate limiting ────────────────────────────────────────────

    def _rate_limit(self):
        """Espera lo necesario para respetar el rate limit configurado."""
        elapsed = time.time() - self._last_request_time
        wait    = REQUEST_DELAY_SECONDS - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_time = time.time()

    # ─── Request base ─────────────────────────────────────────────

    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Ejecuta un request autenticado."""
        self._rate_limit()

        token = get_valid_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
            **kwargs.pop("headers", {}),
        }

        url = f"{self.base_url}{endpoint}"
        resp = self.session.request(method, url, headers=headers, **kwargs)

        # Log útil para debugging
        logger.debug(f"{method} {endpoint} → {resp.status_code}")

        # Manejo de errores específicos de ML
        if resp.status_code == 401:
            raise PermissionError("Token inválido o expirado. Volvé a autorizarte.")
        if resp.status_code == 403:
            raise PermissionError(f"Sin permiso para acceder a: {endpoint}")
        if resp.status_code == 404:
            raise ValueError(f"Recurso no encontrado: {endpoint}")

        resp.raise_for_status()
        return resp.json()

    def get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, json: Optional[dict] = None) -> dict:
        return self._request("POST", endpoint, json=json)

    # ─── Paginación automática ────────────────────────────────────

    def get_all_pages(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        results_key: str = "results",
        limit: int = 50,
        max_items: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """
        Itera automáticamente sobre todas las páginas de un endpoint paginado.

        Yields cada item individualmente.

        Ejemplo:
            for item in client.get_all_pages("/users/123/items/search"):
                print(item)
        """
        params = params or {}
        params["limit"]  = limit
        params["offset"] = 0

        total_fetched = 0

        while True:
            data  = self.get(endpoint, params=params)
            items = data.get(results_key, [])

            if not items:
                break

            for item in items:
                yield item
                total_fetched += 1
                if max_items and total_fetched >= max_items:
                    return

            # Verificar si hay más páginas
            paging = data.get("paging", {})
            total  = paging.get("total", 0)

            params["offset"] += limit

            if params["offset"] >= min(total, 9950):
                break

    # ─── Endpoints de conveniencia ────────────────────────────────

    def get_my_user(self) -> dict:
        """Retorna los datos del usuario autenticado."""
        return self.get("/users/me")

    def get_user(self, user_id: str) -> dict:
        """Retorna datos públicos de cualquier usuario (para competencia)."""
        return self.get(f"/users/{user_id}")

    def get_item(self, item_id: str) -> dict:
        """Retorna datos de una publicación."""
        return self.get(f"/items/{item_id}")

    def get_items_bulk(self, item_ids: list[str]) -> list[dict]:
        """
        Trae hasta 20 items en una sola llamada (más eficiente que uno por uno).
        ML permite máximo 20 IDs por request.
        """
        results = []
        for i in range(0, len(item_ids), 20):
            chunk = item_ids[i:i+20]
            ids_str = ",".join(chunk)
            data = self.get("/items", params={"ids": ids_str})
            for entry in data:
                if entry.get("code") == 200:
                    results.append(entry["body"])
        return results
