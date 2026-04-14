"""
Configuración central del proyecto.
Lee credenciales desde .env (local) o Streamlit Secrets (nube).
"""

import os

# Si estamos en local, cargar .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def _get(key, default=None):
    # Intentar desde Streamlit Secrets
    try:
        import streamlit as st
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    # Fallback a variable de entorno
    return os.getenv(key, default)

# ─── Mercado Libre ────────────────────────────────────────────────
ML_APP_ID       = _get("ML_APP_ID")
ML_SECRET_KEY   = _get("ML_SECRET_KEY")
ML_REDIRECT_URI = _get("ML_REDIRECT_URI", "https://www.google.com")
ML_SITE_ID      = _get("ML_SITE_ID", "MLU")

# ─── Dropbox ──────────────────────────────────────────────────────
DROPBOX_ACCESS_TOKEN  = _get("DROPBOX_ACCESS_TOKEN", "")
DROPBOX_REFRESH_TOKEN = _get("DROPBOX_REFRESH_TOKEN", "")
DROPBOX_APP_KEY       = _get("DROPBOX_APP_KEY", "")
DROPBOX_APP_SECRET    = _get("DROPBOX_APP_SECRET", "")

# Carpeta raíz dentro de Dropbox
DROPBOX_BASE_PATH = "/ml_analytics"

# ─── Tokens ML ───────────────────────────────────────────────────
TOKENS_LOCAL_PATH = ".tokens.json"

# ─── API URLs ─────────────────────────────────────────────────────
ML_API_BASE  = "https://api.mercadolibre.com"
ML_AUTH_URL  = "https://auth.mercadolibre.com.uy/authorization"
ML_TOKEN_URL = "https://api.mercadolibre.com/oauth/token"

# ─── Configuración de extracción ──────────────────────────────────
DEFAULT_DAYS_BACK     = 30
REQUEST_DELAY_SECONDS = 0.15
