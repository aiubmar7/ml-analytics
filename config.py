"""
Configuración central del proyecto.
Cargá tus credenciales en un archivo .env (nunca subas este archivo con datos reales a git).
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── Mercado Libre ────────────────────────────────────────────────
ML_APP_ID       = os.getenv("ML_APP_ID")         # Client ID de tu app
ML_SECRET_KEY   = os.getenv("ML_SECRET_KEY")      # Client Secret
ML_REDIRECT_URI = os.getenv("ML_REDIRECT_URI", "https://localhost")

# País (UY=Uruguay, AR=Argentina, MX=México, BR=Brasil, CL=Chile, CO=Colombia)
ML_SITE_ID = os.getenv("ML_SITE_ID", "MLU")

# ─── Dropbox ──────────────────────────────────────────────────────
DROPBOX_ACCESS_TOKEN  = os.getenv("DROPBOX_ACCESS_TOKEN")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
DROPBOX_APP_KEY       = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET    = os.getenv("DROPBOX_APP_SECRET")

# Carpeta raíz dentro de Dropbox donde se guardarán los datos
DROPBOX_BASE_PATH = "/ml_analytics"

# ─── Tokens ML (se guardan localmente y en Dropbox) ───────────────
TOKENS_LOCAL_PATH = ".tokens.json"   # cache local

# ─── API URLs ─────────────────────────────────────────────────────
ML_API_BASE    = "https://api.mercadolibre.com"
ML_AUTH_URL    = "https://auth.mercadolibre.com.uy/authorization"  # cambiá por tu país
ML_TOKEN_URL   = "https://api.mercadolibre.com/oauth/token"

# ─── Configuración de extracción ──────────────────────────────────
# Cuántos días hacia atrás traer en cada sync
DEFAULT_DAYS_BACK = 30

# Rate limit: ML permite ~10 req/seg con token propio
REQUEST_DELAY_SECONDS = 0.15
