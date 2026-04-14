"""
Cliente de Dropbox para persistir datos del proyecto.
Guarda y lee archivos Parquet (eficiente) y JSON (tokens, configs).

Estructura en Dropbox:
  /ml_analytics/
  ├── tokens/
  │   └── ml_tokens.json          ← backup de tokens ML
  ├── data/
  │   ├── my_sales/
  │   │   └── sales_YYYY-MM.parquet
  │   ├── competition/
  │   │   └── {seller_id}_YYYY-MM.parquet
  │   ├── categories/
  │   │   └── {category_id}_YYYY-MM.parquet
  │   └── keywords/
  │       └── {keyword}_YYYY-MM.parquet
  └── logs/
      └── sync_log.json
"""

import io
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

import dropbox
from dropbox.exceptions import ApiError, AuthError
from dropbox.files import WriteMode
import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import (
    DROPBOX_APP_KEY, DROPBOX_APP_SECRET,
    DROPBOX_ACCESS_TOKEN, DROPBOX_REFRESH_TOKEN,
    DROPBOX_BASE_PATH
)

logger = logging.getLogger(__name__)


class DropboxClient:
    """
    Cliente Dropbox para leer/escribir datos del proyecto.

    Uso:
        dbx = DropboxClient()
        dbx.save_dataframe(df, "data/my_sales/sales_2024-01.parquet")
        df = dbx.load_dataframe("data/my_sales/sales_2024-01.parquet")
    """

    def __init__(self):
        self.dbx = self._connect()

    def _connect(self) -> dropbox.Dropbox:
        """Conecta a Dropbox con refresh token (no vence) o access token."""
        try:
            # Leer credenciales desde Streamlit Secrets o .env
            try:
                import streamlit as st
                refresh_token = st.secrets.get("DROPBOX_REFRESH_TOKEN") or DROPBOX_REFRESH_TOKEN
                app_key       = st.secrets.get("DROPBOX_APP_KEY") or DROPBOX_APP_KEY
                app_secret    = st.secrets.get("DROPBOX_APP_SECRET") or DROPBOX_APP_SECRET
                access_token  = st.secrets.get("DROPBOX_ACCESS_TOKEN") or DROPBOX_ACCESS_TOKEN
            except Exception:
                refresh_token = DROPBOX_REFRESH_TOKEN
                app_key       = DROPBOX_APP_KEY
                app_secret    = DROPBOX_APP_SECRET
                access_token  = DROPBOX_ACCESS_TOKEN

            if refresh_token and app_key and app_secret:
                # Conexión con refresh token — recomendado, no vence
                dbx = dropbox.Dropbox(
                    app_key=app_key,
                    app_secret=app_secret,
                    oauth2_refresh_token=refresh_token,
                )
            elif access_token:
                # Conexión con access token — vence en ~4hs
                dbx = dropbox.Dropbox(access_token)
            else:
                raise ValueError("No hay credenciales de Dropbox configuradas")

            # Verificar conexión
            account = dbx.users_get_current_account()
            logger.info(f"✅ Dropbox conectado como: {account.name.display_name}")
            return dbx

        except AuthError as e:
            raise ConnectionError(f"Error de autenticación en Dropbox: {e}")

    def _full_path(self, relative_path: str) -> str:
        """Construye la ruta completa en Dropbox."""
        return f"{DROPBOX_BASE_PATH}/{relative_path.lstrip('/')}"

    # ─── DataFrames (Parquet) ─────────────────────────────────────

    def save_dataframe(self, df: pd.DataFrame, relative_path: str) -> None:
        """Guarda un DataFrame como Parquet en Dropbox."""
        full_path = self._full_path(relative_path)

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        self.dbx.files_upload(
            buffer.read(),
            full_path,
            mode=WriteMode.overwrite,
        )
        logger.info(f"💾 Guardado: {full_path} ({len(df)} filas)")

    def load_dataframe(self, relative_path: str) -> Optional[pd.DataFrame]:
        """Carga un DataFrame desde Parquet en Dropbox. Retorna None si no existe."""
        full_path = self._full_path(relative_path)
        try:
            _, response = self.dbx.files_download(full_path)
            buffer = io.BytesIO(response.content)
            df = pd.read_parquet(buffer, engine="pyarrow")
            logger.info(f"📂 Cargado: {full_path} ({len(df)} filas)")
            return df
        except ApiError as e:
            if "not_found" in str(e):
                return None
            raise

    def append_dataframe(self, df_new: pd.DataFrame, relative_path: str) -> pd.DataFrame:
        """
        Agrega filas a un Parquet existente (o lo crea si no existe).
        Elimina duplicados por 'id' si la columna existe.
        """
        df_existing = self.load_dataframe(relative_path)

        if df_existing is not None:
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            if "id" in df_combined.columns:
                df_combined = df_combined.drop_duplicates(subset=["id"], keep="last")
        else:
            df_combined = df_new

        self.save_dataframe(df_combined, relative_path)
        return df_combined

    # ─── JSON (tokens, configs, logs) ────────────────────────────

    def save_json(self, data: dict, relative_path: str) -> None:
        """Guarda un dict como JSON en Dropbox."""
        full_path = self._full_path(relative_path)
        content   = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        self.dbx.files_upload(content, full_path, mode=WriteMode.overwrite)

    def load_json(self, relative_path: str) -> Optional[dict]:
        """Carga un JSON desde Dropbox. Retorna None si no existe."""
        full_path = self._full_path(relative_path)
        try:
            _, response = self.dbx.files_download(full_path)
            return json.loads(response.content)
        except ApiError as e:
            if "not_found" in str(e):
                return None
            raise

    # ─── Listado de archivos ──────────────────────────────────────

    def list_files(self, relative_folder: str) -> list[str]:
        """Lista archivos en una carpeta de Dropbox."""
        full_path = self._full_path(relative_folder)
        try:
            result = self.dbx.files_list_folder(full_path)
            return [entry.name for entry in result.entries]
        except ApiError as e:
            if "not_found" in str(e):
                return []
            raise

    # ─── Backup de tokens ML ─────────────────────────────────────

    def backup_ml_tokens(self, tokens: dict) -> None:
        """Guarda backup de tokens ML en Dropbox."""
        tokens["backup_at"] = datetime.now().isoformat()
        self.save_json(tokens, "tokens/ml_tokens.json")
        logger.info("🔐 Tokens ML respaldados en Dropbox.")

    def restore_ml_tokens(self) -> Optional[dict]:
        """Restaura tokens ML desde Dropbox (útil si perdés el archivo local)."""
        return self.load_json("tokens/ml_tokens.json")

    # ─── Log de sincronización ────────────────────────────────────

    def log_sync(self, module: str, status: str, details: dict = None) -> None:
        """Registra una sincronización en el log."""
        log = self.load_json("logs/sync_log.json") or {"syncs": []}
        log["syncs"].append({
            "timestamp": datetime.now().isoformat(),
            "module":    module,
            "status":    status,
            **(details or {}),
        })
        # Mantener solo los últimos 500 registros
        log["syncs"] = log["syncs"][-500:]
        self.save_json(log, "logs/sync_log.json")
