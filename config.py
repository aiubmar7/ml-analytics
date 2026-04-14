"""
Cliente de Dropbox para persistir datos del proyecto.
Guarda y lee archivos Parquet (eficiente) y JSON (tokens, configs).

Estructura en Dropbox:
  /ml_analytics/
  ├── tokens/
  │   └── ml_tokens.json
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

    def __init__(self):
        self.dbx = self._connect()

    def _connect(self) -> dropbox.Dropbox:
        """Conecta a Dropbox con refresh token (no vence) o access token."""
        try:
            # Leer desde Streamlit Secrets si está disponible
            refresh_token = DROPBOX_REFRESH_TOKEN
            app_key = DROPBOX_APP_KEY
            app_secret = DROPBOX_APP_SECRET
            access_token = DROPBOX_ACCESS_TOKEN

            try:
                import streamlit as st
                if hasattr(st, 'secrets'):
                    if "DROPBOX_REFRESH_TOKEN" in st.secrets:
                        refresh_token = st.secrets["DROPBOX_REFRESH_TOKEN"]
                    if "DROPBOX_APP_KEY" in st.secrets:
                        app_key = st.secrets["DROPBOX_APP_KEY"]
                    if "DROPBOX_APP_SECRET" in st.secrets:
                        app_secret = st.secrets["DROPBOX_APP_SECRET"]
                    if "DROPBOX_ACCESS_TOKEN" in st.secrets:
                        access_token = st.secrets["DROPBOX_ACCESS_TOKEN"]
            except Exception:
                pass

            if refresh_token and app_key and app_secret:
                dbx = dropbox.Dropbox(
                    app_key=app_key,
                    app_secret=app_secret,
                    oauth2_refresh_token=refresh_token,
                )
            elif access_token:
                dbx = dropbox.Dropbox(access_token)
            else:
                raise ValueError("No hay credenciales de Dropbox configuradas")

            account = dbx.users_get_current_account()
            logger.info(f"Dropbox conectado como: {account.name.display_name}")
            return dbx

        except AuthError as e:
            raise ConnectionError(f"Error de autenticación en Dropbox: {e}")

    def _full_path(self, relative_path: str) -> str:
        return f"{DROPBOX_BASE_PATH}/{relative_path.lstrip('/')}"

    def save_dataframe(self, df: pd.DataFrame, relative_path: str) -> None:
        full_path = self._full_path(relative_path)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        self.dbx.files_upload(buffer.read(), full_path, mode=WriteMode.overwrite)
        logger.info(f"Guardado: {full_path} ({len(df)} filas)")

    def load_dataframe(self, relative_path: str) -> Optional[pd.DataFrame]:
        full_path = self._full_path(relative_path)
        try:
            _, response = self.dbx.files_download(full_path)
            buffer = io.BytesIO(response.content)
            return pd.read_parquet(buffer, engine="pyarrow")
        except ApiError as e:
            if "not_found" in str(e):
                return None
            raise

    def append_dataframe(self, df_new: pd.DataFrame, relative_path: str) -> pd.DataFrame:
        df_existing = self.load_dataframe(relative_path)
        if df_existing is not None:
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            if "id" in df_combined.columns:
                df_combined = df_combined.drop_duplicates(subset=["id"], keep="last")
        else:
            df_combined = df_new
        self.save_dataframe(df_combined, relative_path)
        return df_combined

    def save_json(self, data: dict, relative_path: str) -> None:
        full_path = self._full_path(relative_path)
        content = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        self.dbx.files_upload(content, full_path, mode=WriteMode.overwrite)

    def load_json(self, relative_path: str) -> Optional[dict]:
        full_path = self._full_path(relative_path)
        try:
            _, response = self.dbx.files_download(full_path)
            return json.loads(response.content)
        except ApiError as e:
            if "not_found" in str(e):
                return None
            raise

    def list_files(self, relative_folder: str) -> list[str]:
        full_path = self._full_path(relative_folder)
        try:
            result = self.dbx.files_list_folder(full_path)
            return [entry.name for entry in result.entries]
        except ApiError as e:
            if "not_found" in str(e):
                return []
            raise

    def backup_ml_tokens(self, tokens: dict) -> None:
        tokens["backup_at"] = datetime.now().isoformat()
        self.save_json(tokens, "tokens/ml_tokens.json")

    def restore_ml_tokens(self) -> Optional[dict]:
        return self.load_json("tokens/ml_tokens.json")

    def log_sync(self, module: str, status: str, details: dict = None) -> None:
        log = self.load_json("logs/sync_log.json") or {"syncs": []}
        log["syncs"].append({
            "timestamp": datetime.now().isoformat(),
            "module": module,
            "status": status,
            **(details or {}),
        })
        log["syncs"] = log["syncs"][-500:]
        self.save_json(log, "logs/sync_log.json")
