import os
import re
import time
import hmac
import base64
import hashlib
import requests
import pdfplumber
import pandas as pd
import unicodedata
from flask import Flask, request, jsonify
from io import BytesIO

app = Flask(__name__)

DROPBOX_APP_SECRET     = os.environ["DROPBOX_APP_SECRET"]
DROPBOX_REFRESH_TOKEN  = os.environ.get("DROPBOX_REFRESH_TOKEN", "")
DROPBOX_APP_KEY        = os.environ["DROPBOX_APP_KEY"]
ML_CLIENT_ID           = os.environ["ML_CLIENT_ID"]
ML_CLIENT_SECRET       = os.environ["ML_CLIENT_SECRET"]
ML_REFRESH_TOKEN       = os.environ["ML_REFRESH_TOKEN"]

CARPETA_ENTRADA    = "/facturas compartidas ML"
CARPETA_PROCESADOS = "/facturas compartidas ML/procesados"


def get_dropbox_token():
    token = os.environ.get("DROPBOX_ACCESS_TOKEN", "").strip()
    if token:
        return token
    r = requests.post("https://api.dropbox.com/oauth2/token", data={
        "grant_type":    "refresh_token",
        "refresh_token": DROPBOX_REFRESH_TOKEN,
        "client_id":     DROPBOX_APP_KEY,
        "client_secret": DROPBOX_APP_SECRET,
    })
    data = r.json()
    if "access_token" not in data:
        raise Exception(f"Dropbox error: {data}")
    return data["access_token"]


def listar_pdfs_nuevos(token):
    r = requests.post(
        "https://api.dropboxapi.com/2/files/list_folder",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"path": CARPETA_ENTRADA, "recursive": False}
    )
    archivos = r.json().get("entries", [])
    return [a for a in archivos if a[".tag"] == "file" and a["name"].endswith(".pdf")]


def descargar_pdf(token, path):
    r = requests.post(
        "https://content.dropboxapi.com/2/files/download",
        headers={
            "Authorization":   f"Bearer {token}",
            "Dropbox-API-Arg": f'{{"path": "{path}"}}'
        }
    )
    return r.content


def mover_a_procesados(token, path, nombre):
    requests.post(
        "https://api.dropboxapi.com/2/files/move_v2",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "from_path": path,
            "to_path":   f"{CARPETA_PROCESADOS}/{nombre}",
            "autorename": True
        }
    )


def get_ml_token():
    r = requests.post("https://api.mercadolibre.com/oauth/token", data={
        "grant_type":    "refresh_token",
        "client_id":     ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "refresh_token": ML_REFRESH_TOKEN,
    })
    return r.json()["access_token"]


def extraer_order_id(pdf_bytes):
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            texto = "\n".join(p.extract_text() or "" for p in pdf.pages)
        match = re.search(r'ML#(\d+)', texto)
        return match.group(1) if match else None
    except Exception as e:
        print(f"  Error leyendo PDF: {e}")
        return None


def obtener_pack_id(token, order_id):
    r = requests.get(
        f"https://api.mercadolibre.com/orders/{order_id}",
        headers={"Authorization": f"Bearer {token}"}
    )
    data = r.json()
    pack_id = data.get("pack_id")
    return str(pack_id) if pack_id else order_id


def subir_factura_ml(token, pack_id, pdf_bytes, nombre_archivo):
    url = f"https://api.mercadolibre.com/packs/{pack_id}/fiscal_documents"
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        files={"fiscal_document": (nombre_archivo, BytesIO(pdf_bytes), "application/pdf")}
    )
    return r.status_code, r.text


def extraer_cfe_de_nombre(nombre_archivo):
    match = re.search(r'[A-Z]-(\d+)\.pdf$', nombre_archivo)
    return match.group(1) if match else None


def normalizar_nombre(nombre):
    nombre = str(nombre).strip().upper()
    nombre = unicodedata.normalize('NFD', nombre)
    nombre = ''.join(c for c in nombre if unicodedata.category(c) != 'Mn')
    return nombre


def similitud_nombres(a, b):
    a = normalizar_nombre(a)
    b = normalizar_nombre(b)
    palabras_a = set(a.split())
    palabras_b = set(b.split())
    if not palabras_a or not palabras_b:
        return 0
    coincidencias = palabras_a & palabras_b
    return len(coincidencias) / max(len(palabras_a), len(palabras_b))


def leer_excel(file_obj):
    data = file_obj.read()
    try:
        return pd.read_excel(BytesIO(data), engine="xlrd")
    except Exception:
        return pd.read_excel(BytesIO(data), engine="openpyxl")


def procesar_pdfs():
    dbx_token = get_dropbox_token()
    ml_token  = get_ml_token()
    pdfs      = listar_pdfs_nuevos(dbx_token)

    resultados = []
    for archivo in pdfs:
        nombre = archivo["name"]
        path   = archivo["path_display"]

        pdf_bytes = descargar_pdf(dbx_token, path)
        order_id  = extraer_order_id(pdf_bytes)

        if not order_id:
            resultados.append({"archivo": nombre, "estado": "sin_order_id"})
            continue

        pack_id = obtener_pack_id(ml_token, order_id)
        status, respuesta = subir_factura_ml(ml_token, pack_id, pdf_bytes, nombre)

        if status in (200, 201):
            mover_a_procesados(dbx_token, path, nombre)
            resultados.append({"archivo": nombre, "order_id": order_id, "pack_id": pack_id, "estado": "ok"})
        else:
            resultados.append({"archivo": nombre, "order_id": order_id, "pack_id": pack_id, "estado": f"error_{status}", "detalle": respuesta})

    return resultados


@app.route("/webhook/dropbox", methods=["GET"])
def dropbox_challenge():
    challenge = request.args.get("challenge", "")
    return challenge, 200, {"Content-Type": "text/plain"}


@app.route("/webhook/dropbox", methods=["POST"])
def dropbox_webhook():
    signature = request.headers.get("X-Dropbox-Signature", "")
    expected  = hmac.new(DROPBOX_APP_SECRET.encode(), request.data, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return "Firma inválida", 403
    time.sleep(3)
    resultados = procesar_pdfs()
    return jsonify({"procesados": resultados}), 200


@app.route("/procesar", methods=["GET"])
def procesar_manual():
    resultados = procesar_pdfs()
    return jsonify({"procesados": resultados}), 200


@app.route("/", methods=["GET"])
def health():
    return jsonify({"estado": "ok", "servicio": "facturas-ml"}), 200


@app.route("/app", methods=["GET"])
def interfaz():
    html = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Trackings DAC - eldomelbazar</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: #0a0a0a; font-family: Arial, sans-serif; min-height: 100vh; }
.header { border-bottom: 1px solid #1e1e1e; padding: 20px 40px; display: flex; align-items: center; justify-content: space-between; }
.logo-text { font-size: 20px; font-weight: 700; color: #ffffff; letter-spacing: -0.5px; }
.logo-text span { color: #00bcd4; }
.tag { font-size: 11px; color: #555; background: #1a1a1a; padding: 4px 10px; border-radius: 20px; }
.content { max-width: 480px; margin: 0 auto; padding: 48px 24px; }
.title { font-size: 18px; font-weight: 700; color: #f0f0f0; margin-bottom: 4px; }
.subtitle { font-size: 13px; color: #666; margin-bottom: 36px; }
.upload-zone { border: 1.5px solid #2a2a3a; border-radius: 10px; padding: 18px 20px; margin-bottom: 12px; cursor: pointer; display: flex; align-items: center; gap: 14px; transition: border-color 0.15s; background: #12121f; }
.upload-zone:hover { border-color: #00bcd4; background: #13161f; }
.upload-zone.done { border-color: #1d9e75; background: #0d1f18; }
.zone-icon { width: 36px; height: 36px; border-radius: 8px; background: #1e1e30; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
.zone-icon.done { background: #0f2a1e; }
.zone-label { font-size: 11px; color: #555; margin-bottom: 3px; }
.zone-name { font-size: 13px; font-weight: 500; color: #aaa; }
.zone-check { margin-left: auto; color: #1d9e75; font-size: 18px; }
input[type=file] { display: none; }
.btn { width: 100%; padding: 14px; background: #00bcd4; color: #000000; border: none; border-radius: 10px; font-size: 15px; font-weight: 700; cursor: pointer; margin-top: 4px; transition: background 0.2s, color 0.2s; }
.btn:disabled { background: #1e1e2e; color: #444; cursor: not-allowed; border: 1.5px solid #2a2a3a; }
.btn:hover:not(:disabled) { background: #00d4ef; }
.divider { border: none; border-top: 1px solid #1a1a1a; margin: 32px 0; }
.summary { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 20px; }
.metric { text-align: center; padding: 16px 8px; background: #12121f; border-radius: 10px; border: 1.5px solid #2a2a3a; }
.metric-val { font-size: 26px; font-weight: 700; color: #00bcd4; }
.metric-lbl { font-size: 11px; color: #555; margin-top: 2px; }
.result-row { display: flex; justify-content: space-between; align-items: center; padding: 12px 0; border-bottom: 1px solid #1a1a1a; }
.r-name { font-size: 13px; font-weight: 600; color: #e0e0e0; }
.r-link { font-size: 11px; color: #444; margin-top: 2px; }
.badge { font-size: 11px; padding: 3px 10px; border-radius: 20px; font-weight: 600; }
.badge-ok { background: #0d2a1e; color: #4ade80; border: 1px solid #1d9e75; }
.badge-err { background: #2a0d0d; color: #f87171; border: 1px solid #a32d2d; }
.badge-warn { background: #2a1e0d; color: #fbbf24; border: 1px solid #854f0b; }
.procesando { color: #555; font-size: 13px; text-align: center; padding: 20px 0; }
</style>
</head>
<body>
<div class="header">
  <div style="display:flex;align-items:center;gap:8px;">
    <svg width="22" height="22" viewBox="0 0 22 22">
      <circle cx="11" cy="11" r="9" fill="none" stroke="#00bcd4" stroke-width="2.2"/>
      <line x1="11" y1="4" x2="11" y2="11" stroke="#00bcd4" stroke-width="2.2" stroke-linecap="round"/>
    </svg>
    <span class="logo-text"><span>eldomel</span>bazar.com.uy</span>
  </div>
  <span class="tag">Trackings DAC</span>
</div>

<div class="content">
  <div class="title">Envio de trackings</div>
  <div class="subtitle">Subí los archivos del dia y hace clic en procesar.</div>

  <div class="upload-zone" id="zone-dac" onclick="document.getElementById('input-dac').click()">
    <div class="zone-icon" id="icon-dac">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#00bcd4" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
    </div>
    <div>
      <div class="zone-label">Archivo DAC</div>
      <div class="zone-name" id="nombre-dac">Seleccionar archivo .xlsx</div>
    </div>
    <div class="zone-check" id="check-dac" style="display:none;">&#10003;</div>
    <input type="file" id="input-dac" accept=".xlsx,.xls">
  </div>

  <div class="upload-zone" id="zone-remito" onclick="document.getElementById('input-remito').click()">
    <div class="zone-icon" id="icon-remito">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#00bcd4" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
    </div>
    <div>
      <div class="zone-label">Remito de tu sistema</div>
      <div class="zone-name" id="nombre-remito">Seleccionar archivo .xls / .xlsx</div>
    </div>
    <div class="zone-check" id="check-remito" style="display:none;">&#10003;</div>
    <input type="file" id="input-remito" accept=".xlsx,.xls">
  </div>

  <button class="btn" id="btn" disabled onclick="procesar()">Procesar trackings</button>

  <div id="results"></div>
</div>

<script>
  const inputDac = document.getElementById('input-dac');
  const inputRemito = document.getElementById('input-remito');
  const btn = document.getElementById('btn');

  function check() {
    btn.disabled = !(inputDac.files.length && inputRemito.files.length);
  }

  inputDac.addEventListener('change', () => {
    if (inputDac.files.length) {
      document.getElementById('nombre-dac').textContent = inputDac.files[0].name;
      document.getElementById('zone-dac').classList.add('done');
      document.getElementById('icon-dac').classList.add('done');
      document.getElementById('icon-dac').innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#1d9e75" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>';
      document.getElementById('check-dac').style.display = 'block';
    }
    check();
  });

  inputRemito.addEventListener('change', () => {
    if (inputRemito.files.length) {
      document.getElementById('nombre-remito').textContent = inputRemito.files[0].name;
      document.getElementById('zone-remito').classList.add('done');
      document.getElementById('icon-remito').classList.add('done');
      document.getElementById('icon-remito').innerHTML = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#1d9e75" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>';
      document.getElementById('check-remito').style.display = 'block';
    }
    check();
  });

  async function procesar() {
    btn.disabled = true;
    btn.style.background = '#1a2e6e';
    btn.style.color = '#ffffff';
    btn.textContent = 'Procesando...';
    const res = document.getElementById('results');
    res.innerHTML = '<div class="procesando">Procesando... puede tardar hasta 60 segundos la primera vez.</div>';

    const form = new FormData();
    form.append('dac', inputDac.files[0]);
    form.append('remito', inputRemito.files[0]);

    try {
      const r = await fetch('/enviar-tracking', { method: 'POST', body: form });
      const data = await r.json();

      if (data.error) {
        res.innerHTML = '<div class="result-row"><div><div class="r-name">Error</div><div class="r-link">' + data.error + '</div></div><span class="badge badge-err">Error</span></div>';
        return;
      }

      const items = data.resultados || [];
      const ok = items.filter(i => i.mensaje === 'ok').length;
      const errores = items.filter(i => i.mensaje !== 'ok' || i.estado).length;

      let html = '<hr class="divider"><div class="summary">';
      html += '<div class="metric"><div class="metric-val">' + ok + '</div><div class="metric-lbl">Enviados</div></div>';
      html += '<div class="metric"><div class="metric-val">' + errores + '</div><div class="metric-lbl">Con problema</div></div>';
      html += '<div class="metric"><div class="metric-val">' + items.length + '</div><div class="metric-lbl">Total</div></div>';
      html += '</div>';

      items.forEach(item => {
        let badge, link = '';
        if (item.estado && item.estado !== 'ok') {
          const e = { cfe_no_encontrado: 'CFE no encontrado', pdf_no_encontrado_en_dropbox: 'PDF no encontrado en Dropbox', order_id_no_encontrado_en_pdf: 'Orden ML no encontrada' };
          badge = '<span class="badge badge-warn">' + (e[item.estado] || item.estado) + '</span>';
        } else {
          badge = item.mensaje === 'ok' ? '<span class="badge badge-ok">Enviado</span>' : '<span class="badge badge-err">Error</span>';
          if (item.link_dac) link = '<div class="r-link">' + item.link_dac + '</div>';
        }
        html += '<div class="result-row"><div><div class="r-name">' + item.cliente + '</div>' + link + '</div>' + badge + '</div>';
      });

      res.innerHTML = html;
    } catch(e) {
      res.innerHTML = '<div class="result-row"><div><div class="r-name">Error de conexion</div><div class="r-link">' + e.message + '</div></div><span class="badge badge-err">Error</span></div>';
    } finally {
      btn.disabled = false;
      btn.style.background = '#00bcd4';
      btn.style.color = '#000000';
      btn.textContent = 'Procesar trackings';
    }
  }
</script>
</body>
</html>"""
    return html


@app.route("/enviar-tracking", methods=["POST"])
def enviar_tracking():
    if "remito" not in request.files or "dac" not in request.files:
        return jsonify({"error": "Se requieren los archivos 'remito' y 'dac'"}), 400

    remito_file = request.files["remito"]
    dac_file    = request.files["dac"]

    remito_df = leer_excel(remito_file)
    remito_df.columns = remito_df.columns.str.strip()
    dac_df = leer_excel(dac_file)
    dac_df = dac_df[dac_df["Oficina Origen"] != "JUAN LACAZE"].drop_duplicates("Guia")
    dac_df["nombre_norm"] = dac_df["Destinatario"].apply(normalizar_nombre)
    remito_df["nombre_norm"] = remito_df["Cliente"].apply(normalizar_nombre)

    merged = remito_df.merge(dac_df[["nombre_norm", "Guia"]], on="nombre_norm", how="inner")

    remito_no_cruzado = remito_df[~remito_df["nombre_norm"].isin(merged["nombre_norm"])]
    filas_fuzzy = []
    for _, fila_r in remito_no_cruzado.iterrows():
        mejor_match = None
        mejor_score = 0
        for _, fila_d in dac_df.iterrows():
            score = similitud_nombres(fila_r["nombre_norm"], fila_d["nombre_norm"])
            if score > mejor_score:
                mejor_score = score
                mejor_match = fila_d
        if mejor_score >= 0.35 and mejor_match is not None:
            fila_combinada = fila_r.copy()
            fila_combinada["Guia"] = mejor_match["Guia"]
            fila_combinada["match_score"] = round(mejor_score, 2)
            filas_fuzzy.append(fila_combinada)

    if filas_fuzzy:
        merged_fuzzy = pd.DataFrame(filas_fuzzy)
        merged = pd.concat([merged, merged_fuzzy], ignore_index=True)

    if merged.empty:
        return jsonify({"error": "No se encontraron cruces entre remito y DAC"}), 200

    dbx_token = get_dropbox_token()
    ml_token  = get_ml_token()

    seller_resp = requests.get(
        "https://api.mercadolibre.com/users/me",
        headers={"Authorization": f"Bearer {ml_token}"}
    )
    seller_id = seller_resp.json()["id"]

    todos_pdfs = listar_pdfs_nuevos(dbx_token)
    r_proc = requests.post(
        "https://api.dropboxapi.com/2/files/list_folder",
        headers={"Authorization": f"Bearer {dbx_token}", "Content-Type": "application/json"},
        json={"path": CARPETA_PROCESADOS, "recursive": False}
    )
    procesados = [a for a in r_proc.json().get("entries", []) if a[".tag"] == "file" and a["name"].endswith(".pdf")]
    todos_pdfs = todos_pdfs + procesados

    mapa_cfe_pdf = {}
    for archivo in todos_pdfs:
        cfe = extraer_cfe_de_nombre(archivo["name"])
        if cfe:
            mapa_cfe_pdf[cfe] = archivo["path_display"]

    resultados = []
    for _, fila in merged.iterrows():
        cliente  = fila["Cliente"]
        guia     = str(int(fila["Guia"]))
        link_dac = f"https://www.dac.com.uy/envios/rastreo/Codigo_Rastreo/{guia}"

        cfe_raw   = str(fila.get("Fac N°Int/Cfe", ""))
        cfe_match = re.search(r'-(\d+)\s*$', cfe_raw.strip())
        if not cfe_match:
            resultados.append({"cliente": cliente, "estado": "cfe_no_encontrado", "cfe_raw": cfe_raw})
            continue
        cfe = cfe_match.group(1)

        pdf_path = mapa_cfe_pdf.get(cfe)
        if not pdf_path:
            resultados.append({"cliente": cliente, "cfe": cfe, "estado": "pdf_no_encontrado_en_dropbox"})
            continue

        pdf_bytes = descargar_pdf(dbx_token, pdf_path)
        order_id  = extraer_order_id(pdf_bytes)
        if not order_id:
            resultados.append({"cliente": cliente, "cfe": cfe, "estado": "order_id_no_encontrado_en_pdf"})
            continue

        pack_id = obtener_pack_id(ml_token, order_id)

        mensaje = (
            f"¡Hola {cliente}! Tu pedido ya está en camino 🚚\n"
            f"Podés seguir tu envío en tiempo real acá:\n{link_dac}"
        )

        order_data = requests.get(
            f"https://api.mercadolibre.com/orders/{order_id}",
            headers={"Authorization": f"Bearer {ml_token}"}
        ).json()
        buyer_id = str(order_data.get("buyer", {}).get("id", ""))

        r_msg = requests.post(
            f"https://api.mercadolibre.com/messages/packs/{pack_id}/sellers/{seller_id}?tag=post_sale",
            headers={"Authorization": f"Bearer {ml_token}", "Content-Type": "application/json"},
            json={
                "from": {"user_id": str(seller_id)},
                "to": {"user_id": buyer_id},
                "text": mensaje
            }
        )

        r_nota = requests.post(
            f"https://api.mercadolibre.com/orders/{order_id}/notes",
            headers={"Authorization": f"Bearer {ml_token}", "Content-Type": "application/json"},
            json={"note": link_dac}
        )

        shipment_id = order_data.get("shipping", {}).get("id")
        r_ship = None
        if shipment_id:
            service_id = order_data.get("shipping_option", {}).get("shipping_method_id")
            if not service_id:
                shipment_data = requests.get(
                    f"https://api.mercadolibre.com/shipments/{shipment_id}",
                    headers={"Authorization": f"Bearer {ml_token}"}
                ).json()
                service_id = shipment_data.get("shipping_option", {}).get("shipping_method_id")

            ship_body = {
                "status": "shipped",
                "tracking_number": link_dac,
                "tracking_method": "Otros"
            }
            if service_id:
                ship_body["service_id"] = service_id

            r_ship = requests.put(
                f"https://api.mercadolibre.com/shipments/{shipment_id}",
                headers={"Authorization": f"Bearer {ml_token}", "Content-Type": "application/json"},
                json=ship_body
            )
            print(f"  Shipment {shipment_id} → {r_ship.status_code} {r_ship.text[:200]}")

        resultados.append({
            "cliente":      cliente,
            "cfe":          cfe,
            "guia_dac":     guia,
            "order_id":     order_id,
            "pack_id":      pack_id,
            "link_dac":     link_dac,
            "mensaje":      "ok" if r_msg.status_code in (200, 201) else f"error_{r_msg.status_code}",
            "nota":         "ok" if r_nota.status_code in (200, 201) else f"error_{r_nota.status_code}",
            "envio_estado": "ok" if r_ship and r_ship.status_code in (200, 201) else f"error_{r_ship.status_code if r_ship else 'sin_shipment'}",
        })

    return jsonify({"resultados": resultados}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
