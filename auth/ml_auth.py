"""
Autenticación con Mercado Libre usando OAuth2.
"""

import json
import time
import webbrowser
from pathlib import Path
from urllib.parse import urlencode

import requests

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config import (
    ML_APP_ID, ML_SECRET_KEY, ML_REDIRECT_URI,
    ML_AUTH_URL, ML_TOKEN_URL, TOKENS_LOCAL_PATH
)


def _load_tokens() -> dict:
    path = Path(TOKENS_LOCAL_PATH)
    if path.exists():
        return json.loads(path.read_text())
    return {}


def _save_tokens(tokens: dict) -> None:
    try:
        Path(TOKENS_LOCAL_PATH).write_text(json.dumps(tokens, indent=2))
    except Exception:
        pass  # En Streamlit Cloud no se puede escribir archivos


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
    resp = requests.post(ML_TOKEN_URL, data=payload)
    resp.raise_for_status()
    tokens = resp.json()
    tokens["obtained_at"] = time.time()
    _save_tokens(tokens)
    print("✅ Tokens obtenidos y guardados correctamente.")
    return tokens


def refresh_access_token(refresh_token: str) -> dict:
    payload = {
        "grant_type":    "refresh_token",
        "client_id":     ML_APP_ID,
        "client_secret": ML_SECRET_KEY,
        "refresh_token": refresh_token,
    }
    resp = requests.post(ML_TOKEN_URL, data=payload)
    resp.raise_for_status()
    tokens = resp.json()
    tokens["obtained_at"] = time.time()
    _save_tokens(tokens)
    return tokens


def get_valid_access_token() -> str:
    """
    Retorna un access_token vigente.
    En Streamlit Cloud usa el refresh token para obtener uno nuevo.
    En local usa el archivo .tokens.json con auto-refresh.
    """
    # 1. Intentar usar refresh token desde Streamlit Secrets
    try:
        import streamlit as st
        ml_refresh = st.secrets["ML_REFRESH_TOKEN"]
        ml_app_id  = st.secrets["ML_APP_ID"]
        ml_secret  = st.secrets["ML_SECRET_KEY"]
        if ml_refresh and ml_app_id and ml_secret:
            payload = {
                "grant_type":    "refresh_token",
                "client_id":     ml_app_id,
                "client_secret": ml_secret,
                "refresh_token": ml_refresh,
            }
            resp = requests.post(ML_TOKEN_URL, data=payload)
            if resp.status_code == 200:
                return resp.json()["access_token"]
    except Exception:
        pass

    # 2. Leer desde archivo local
    tokens = _load_tokens()

    if not tokens:
        raise RuntimeError(
            "No hay tokens guardados. Ejecutá `python auth/ml_auth.py` para autorizarte."
        )

    obtained_at    = tokens.get("obtained_at", 0)
    expires_in     = tokens.get("expires_in", 21600)
    time_elapsed   = time.time() - obtained_at
    time_remaining = expires_in - time_elapsed

    if time_remaining < 300:
        tokens = refresh_access_token(tokens["refresh_token"])

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
