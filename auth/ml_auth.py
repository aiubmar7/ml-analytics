"""
Autenticación con Mercado Libre usando OAuth2.

Flujo:
  1. Primera vez: genera URL de autorización → usuario autoriza → obtenés code
  2. Intercambiás code por access_token + refresh_token
  3. Guardás tokens (local + Dropbox)
  4. Cuando el access_token vence (6hs), usás refresh_token para renovarlo automáticamente
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


# ─── Helpers de persistencia ─────────────────────────────────────

def _load_tokens() -> dict:
    """Carga tokens desde archivo local."""
    path = Path(TOKENS_LOCAL_PATH)
    if path.exists():
        return json.loads(path.read_text())
    return {}


def _save_tokens(tokens: dict) -> None:
    """Guarda tokens en archivo local."""
    Path(TOKENS_LOCAL_PATH).write_text(json.dumps(tokens, indent=2))


# ─── Paso 1: Generar URL de autorización ─────────────────────────

def get_auth_url() -> str:
    """
    Retorna la URL a la que el usuario debe ir para autorizar la app.
    Después de autorizar, ML redirige a REDIRECT_URI?code=XXXX
    """
    params = {
        "response_type": "code",
        "client_id": ML_APP_ID,
        "redirect_uri": ML_REDIRECT_URI,
    }
    return f"{ML_AUTH_URL}?{urlencode(params)}"


# ─── Paso 2: Intercambiar code por tokens ────────────────────────

def exchange_code_for_tokens(code: str) -> dict:
    """
    Intercambia el código de autorización por access_token y refresh_token.
    Llama a esto una sola vez con el código que ML te devuelve en la URL.
    """
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


# ─── Paso 3: Renovar access_token con refresh_token ──────────────

def refresh_access_token(refresh_token: str) -> dict:
    """
    Renueva el access_token usando el refresh_token.
    ML access_tokens duran 6 horas.
    """
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
    print("🔄 Access token renovado.")
    return tokens


# ─── Paso 4: Obtener token válido (auto-refresh) ─────────────────

def get_valid_access_token() -> str:
    """
    Retorna un access_token vigente.
    En Streamlit Cloud lee el token desde los secrets.
    En local usa el archivo .tokens.json.
    """
    # Intentar leer desde Streamlit Secrets (Streamlit Cloud)
    try:
        import streamlit as st
        token = st.secrets.get("ML_ACCESS_TOKEN")
        if token:
            return token
    except Exception:
        pass

    tokens = _load_tokens()

    if not tokens:
        raise RuntimeError(
            "No hay tokens guardados. Ejecutá `python auth/ml_auth.py` para autorizarte."
        )

    # Verificar si el token está por vencer
    obtained_at   = tokens.get("obtained_at", 0)
    expires_in    = tokens.get("expires_in", 21600)  # 6hs por defecto
    time_elapsed  = time.time() - obtained_at
    time_remaining = expires_in - time_elapsed

    if time_remaining < 300:  # menos de 5 minutos → renovar
        print(f"⚠️  Token vence en {int(time_remaining)}s. Renovando...")
        tokens = refresh_access_token(tokens["refresh_token"])

    return tokens["access_token"]


# ─── CLI: Flujo de autorización inicial ──────────────────────────

def authorize_interactive():
    """
    Flujo interactivo para autorizar la app por primera vez.
    Ejecutá: python auth/ml_auth.py
    """
    print("\n🔐 Autorización de Mercado Libre")
    print("=" * 40)

    # Verificar si ya hay tokens
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
    print("   https://localhost?code=TU_CODIGO_AQUI\n")

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
