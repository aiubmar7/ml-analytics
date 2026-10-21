"""
Autenticación con Mercado Libre usando OAuth2.

Los tokens se guardan en Dropbox (tokens/ml_tokens.json) y se renuevan
solos: cada vez que se refresca el access_token se persiste de vuelta el
refresh_token que devuelve ML (que puede rotar). Así el token nunca queda
viejo y NO hay que re-autorizar a mano cada ~6 meses.

Orden de búsqueda del refresh_token:
  1. Dropbox  -> tokens/ml_tokens.json (fuente de verdad, se auto-actualiza)
  2. Streamlit Secret ML_REFRESH_TOKEN (solo para el primer arranque / bootstrap)
  3. Archivo local .tokens.json (desarrollo)
"""

import os
import json
import time
import logging
import webbrowser
from pathlib import Path
from urllib.parse import urlencode

import requests

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import (
    ML_APP_ID, ML_SECRET_KEY, ML_REDIRECT_URI,
    ML_AUTH_URL, ML_TOKEN_URL, TOKENS_LOCAL_PATH,
)

logger = logging.getLogger(__name__)

# Margen (segundos) antes del vencimiento para disparar el refresh.
_REFRESH_MARGIN = 600

# Cache en memoria dentro del proceso: evita pegarle a Dropbox y a ML en
# cada rerun de Streamlit. Se renueva solo cuando el access_token vence.
_mem_tokens = None


# ─── Persistencia ─────────────────────────────────────────────────

def _dropbox():
    from storage.dropbox_client import DropboxClient
    return DropboxClient()


def _load_tokens_dropbox() -> dict:
    try:
        return _dropbox().restore_ml_tokens() or {}
    except Exception as e:
        logger.warning(f"No se pudo leer tokens desde Dropbox: {e}")
        return {}


def _save_tokens_dropbox(tokens: dict) -> None:
    try:
        _dropbox().backup_ml_tokens(tokens)
    except Exception as e:
        logger.warning(f"No se pudo guardar tokens en Dropbox: {e}")


def _load_tokens() -> dict:
    path = Path(TOKENS_LOCAL_PATH)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}


def _save_tokens(tokens: dict) -> None:
    try:
        Path(TOKENS_LOCAL_PATH).write_text(json.dumps(tokens, indent=2))
    except Exception:
        pass  # En Streamlit Cloud el filesystem es efímero / de solo lectura.


def _store_everywhere(tokens: dict) -> None:
    """Guarda el bundle de tokens en memoria + Dropbox + archivo local."""
    global _mem_tokens
    _mem_tokens = tokens
    _save_tokens_dropbox(tokens)
    _save_tokens(tokens)


def _bootstrap_refresh_token() -> str:
    """
    Refresh token para el PRIMER arranque (cuando Dropbox todavía no tiene
    el bundle). Primero el Secret de Streamlit, después .env / archivo local.
    """
    try:
        import streamlit as st
        rt = st.secrets.get("ML_REFRESH_TOKEN")
        if rt:
            return rt
    except Exception:
        pass
    return os.getenv("ML_REFRESH_TOKEN", "") or _load_tokens().get("refresh_token", "")


# ─── OAuth ────────────────────────────────────────────────────────

def get_auth_url() -> str:
    params = {
        "response_type": "code",
        "client_id": ML_APP_ID,
        "redirect_uri": ML_REDIRECT_URI,
    }
    return f"{ML_AUTH_URL}?{urlencode(params)}"


def exchange_code_for_tokens(code: str) -> dict:
    payload = {
        "grant_type":    "authorization_code",
        "client_id":     ML_APP_ID,
        "client_secret": ML_SECRET_KEY,
        "code":          code,
        "redirect_uri":  ML_REDIRECT_URI,
    }
    resp = requests.post(ML_TOKEN_URL, data=payload, timeout=(10, 20))
    resp.raise_for_status()
    tokens = resp.json()
    tokens["obtained_at"] = time.time()
    _store_everywhere(tokens)
    print("✅ Tokens obtenidos y guardados correctamente.")
    return tokens


def refresh_access_token(refresh_token: str) -> dict:
    payload = {
        "grant_type":    "refresh_token",
        "client_id":     ML_APP_ID,
        "client_secret": ML_SECRET_KEY,
        "refresh_token": refresh_token,
    }
    resp = requests.post(ML_TOKEN_URL, data=payload, timeout=(10, 20))
    resp.raise_for_status()
    tokens = resp.json()
    tokens["obtained_at"] = time.time()
    # ML puede NO devolver un refresh_token nuevo: en ese caso conservamos
    # el que ya teníamos para no cortar la cadena de renovación.
    if not tokens.get("refresh_token"):
        tokens["refresh_token"] = refresh_token
    _store_everywhere(tokens)
    return tokens


def _is_fresh(tokens: dict) -> bool:
    if not tokens or not tokens.get("access_token"):
        return False
    obtained_at = tokens.get("obtained_at", 0)
    expires_in  = tokens.get("expires_in", 21600)
    return (time.time() - obtained_at) < (expires_in - _REFRESH_MARGIN)


def get_valid_access_token() -> str:
    """
    Retorna un access_token vigente, renovándose y persistiéndose solo.

    No vuelve a pegarle a ML mientras el access_token siga vigente (lo
    cachea ~6 h), y cuando lo renueva guarda el refresh_token nuevo en
    Dropbox, de modo que la cadena nunca se corta.
    """
    global _mem_tokens

    # 1. Cache en memoria del proceso.
    if _is_fresh(_mem_tokens):
        return _mem_tokens["access_token"]

    # 2. Bundle persistido en Dropbox (fuente de verdad entre reinicios).
    tokens = _load_tokens_dropbox()
    if _is_fresh(tokens):
        _mem_tokens = tokens
        return tokens["access_token"]

    # 3. Refresh token a usar: el de Dropbox si existe, si no el de bootstrap.
    refresh_token = (tokens or {}).get("refresh_token") or _bootstrap_refresh_token()
    if not refresh_token:
        raise RuntimeError(
            "No hay refresh token disponible. Re-autorizá ejecutando "
            "`python auth/ml_auth.py`, o cargá ML_REFRESH_TOKEN en los Secrets."
        )

    # 4. Refrescar y persistir (auto-renovación).
    tokens = refresh_access_token(refresh_token)
    return tokens["access_token"]


def authorize_interactive():
    print("\n🔐 Autorización de Mercado Libre")
    print("=" * 40)

    tokens = _load_tokens()
    if tokens:
        print("✅ Ya tenés tokens guardados.")
        print(f"   User ID: {tokens.get('user_id', 'desconocido')}")
        renovar = input("¿Querés renovarlos de todas formas? (s/N): ").strip().lower()
        if renovar != "s":
            return tokens

    url = get_auth_url()
    print(f"\n1. Abriendo navegador para autorizar la app...")
    print(f"   URL: {url}\n")

    try:
        webbrowser.open(url)
    except Exception:
        print("   (No se pudo abrir el navegador automáticamente)")

    print("2. Después de autorizar, ML te redirige a una URL como:")
    print("   https://www.google.com?code=TU_CODIGO_AQUI\n")

    code = input("3. Pegá el valor del parámetro 'code' aquí: ").strip()

    if not code:
        print("❌ No ingresaste ningún código.")
        return None

    tokens = exchange_code_for_tokens(code)
    print(f"\n✅ ¡Autorización exitosa!")
    print(f"   User ID: {tokens.get('user_id')}")
    print(f"   Token válido por: {tokens.get('expires_in', 0) // 3600} horas")
    return tokens


if __name__ == "__main__":
    authorize_interactive()
