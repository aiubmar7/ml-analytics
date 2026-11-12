"""
Cálculo de RENTA mensual del canal ML (DELERAL / Eldom).

Fórmula validada con el usuario:

    renta = facturación × 0.81  (neto MP: quita 14,1% comisión + ~4,9%
                                 retenciones de impuesto por tarjeta)
            − COGS              (costo REAL del SKU vs precio REAL de venta)
            − ADS               (inversión en Product Ads del mes)
            − envíos ME2        (list_cost que paga el vendedor en envíos
                                 gratis ME2 drop-off; ME1 y los que paga
                                 el comprador se excluyen)

COGS automático, SIN markup: cada venta se cruza por el código interno
(seller_sku de ML ↔ código del listado FacturApp) y se toma el costo real
del SKU × el precio real al que se vendió. El ratio COGS/facturación real
medido se extrapola a la facturación proyectada; la cobertura del match se
reporta y, si es baja o el listado no está, el resultado se marca NO
confiable (evita mostrar una renta inflada).

Los costos del listado vienen en USD → se convierten a UYU con el TC del
mes (parámetro, default 40.45).

Datos sensibles: el listado de costos vive SOLO en Dropbox privado
(data/costos/costos.parquet), nunca en el repo público.
"""

import re
import logging
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

logger = logging.getLogger(__name__)

COST_LIST_PATH   = "data/costos/costos.parquet"
RENTA_CACHE_PATH = "config/renta_cache.json"
RENTA_PARAMS_PATH = "config/renta_params.json"

NETO_MP_RATE = 0.81          # igual a COBRANZA_NET_RATE de my_sales
DEFAULT_TC   = 40.45         # TC noviembre 2026 (parámetro)


# ─── Normalización de códigos (llave de costeo) ───────────────────────

def _norm_code(x) -> str:
    """Normaliza un código interno / SKU para matchear exacto: str, sin
    espacios, minúsculas; a los numéricos les saca ceros a la izquierda
    (08189 == 8189)."""
    if x is None:
        return ""
    s = str(x).strip().lower()
    if s in ("", "nan", "none"):
        return ""
    if s.isdigit():
        s = s.lstrip("0") or "0"
    return s

def _parse_money(x) -> float:
    """Convierte un valor de costo a float. Tolera 'U$S 274,00', '$1.234,56'
    (formato UY: . miles , decimal), '274.00' (US) y números planos."""
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return 0.0
    s = re.sub(r"[^\d.,-]", "", str(x))
    if not s:
        return 0.0
    if "." in s and "," in s:
        # el último separador es el decimal
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")                       # '.' decimal (US)
        else:
            s = s.replace(".", "").replace(",", ".")     # ',' decimal (UY)
    elif "," in s:
        s = s.replace(",", ".") if len(s.split(",")[-1]) in (1, 2) else s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0


class RentaCalculator:
    def __init__(self, sales_extractor, storage):
        self.sales   = sales_extractor
        self.storage = storage
        self._cost_df = None
        self._cost_by_code = None # código interno -> {costo_usd, descripcion} (llave exacta)

    # ─── Listado de costos ────────────────────────────────────────────

    def load_cost_list(self) -> pd.DataFrame:
        if self._cost_df is not None:
            return self._cost_df
        df = self.storage.load_dataframe(COST_LIST_PATH)
        if df is None or df.empty:
            self._cost_df = pd.DataFrame(columns=["codigo","descripcion","grupo","costo_usd"])
            return self._cost_df
        df = df[df["costo_usd"] > 0].copy().reset_index(drop=True)
        # LLAVE EXACTA: código interno -> costo. En ML cada venta trae ese
        # código en seller_sku, así que el costeo es costo real × venta real.
        by_code = {}
        for _, r in df.iterrows():
            k = _norm_code(r["codigo"])
            if k:
                by_code[k] = {"costo_usd": float(r["costo_usd"]),
                              "descripcion": str(r["descripcion"])}
        self._cost_df, self._cost_by_code = df, by_code
        return df

    def save_cost_list(self, raw: "pd.DataFrame") -> tuple:
        """Normaliza y guarda un listado de costos subido por el usuario
        (CSV/Excel/parquet de FacturApp) a Dropbox, con la conexión de la app
        (no depende del sync del Dropbox de escritorio). Detecta solo las
        columnas de código y costo. Devuelve (n_filas, mensaje)."""
        if raw is None or raw.empty:
            return 0, "El archivo vino vacío."
        cols = {str(c).lower().strip(): c for c in raw.columns}
        def _find(*subs):
            for low, orig in cols.items():
                if any(s in low for s in subs):
                    return orig
            return None
        cod_col  = _find("codigo", "código", "sku", "cod ", "cod.") or cols.get("cod") or cols.get("codigo")
        cost_col = _find("compra") or _find("costo_usd", "costo", "precio costo") or _find("precio")
        desc_col = _find("descrip", "nombre", "articulo", "artículo", "detalle", "producto")
        if not cod_col or not cost_col:
            return 0, (f"No pude detectar las columnas. Vi: {list(raw.columns)}. "
                       "Necesito una columna de código y una de costo/precio de compra.")
        out = pd.DataFrame()
        out["codigo"]      = raw[cod_col].map(_norm_code)
        out["descripcion"] = raw[desc_col].astype(str) if desc_col else ""
        out["costo_usd"]   = raw[cost_col].map(_parse_money)
        out = out[(out["codigo"] != "") & (out["costo_usd"] > 0)].drop_duplicates("codigo", keep="last")
        if out.empty:
            return 0, f"Detecté columnas ({cod_col} / {cost_col}) pero no quedaron filas con costo > 0."
        self.storage.save_dataframe(out[["codigo", "descripcion", "costo_usd"]], COST_LIST_PATH)
        self._cost_df = None; self._cost_by_code = None   # invalidar cache en memoria
        return len(out), f"Guardados {len(out)} costos en Dropbox (columnas: {cod_col} → {cost_col})."

    def save_uploaded_cost(self, uploaded_file) -> tuple:
        """Lee un archivo de costos subido y lo guarda en Dropbox. Maneja:
        - parquet / excel (vía save_cost_list genérico),
        - CSV UTF-8 limpio (columnas codigo/costo), y
        - el export 'Listado de Stock' de FacturApp: latin-1, 2 filas de título,
          columnas dispersas y costo como 'U$S' + número en celdas separadas."""
        import io, csv as _csv
        name = (getattr(uploaded_file, "name", "") or "").lower()
        data = uploaded_file.read()
        if name.endswith(".parquet"):
            return self.save_cost_list(pd.read_parquet(io.BytesIO(data)))
        if name.endswith((".xlsx", ".xls")):
            return self.save_cost_list(pd.read_excel(io.BytesIO(data)))
        # CSV: decodificar probando encodings (FacturApp exporta en latin-1).
        text = None
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                text = data.decode(enc); break
            except Exception:
                continue
        if text is None:
            return 0, "No pude decodificar el archivo (encoding desconocido)."
        rows = [r for r in _csv.reader(io.StringIO(text)) if any(str(c).strip() for c in r)]
        if not rows:
            return 0, "El archivo vino vacío."
        # ¿Formato FacturApp? fila header con 'Código' + celdas 'U$S' en los datos.
        _n = lambda s: str(s).strip().lower()
        hdr_i = next((i for i, r in enumerate(rows[:15])
                      if any(_n(c) in ("código", "codigo") for c in r)), None)
        is_fapp = hdr_i is not None and any(
            _n(c) == "u$s" for r in rows[hdr_i + 1:hdr_i + 8] for c in r)
        if is_fapp:
            hdr = [_n(c) for c in rows[hdr_i]]
            cod_col  = next((j for j, c in enumerate(hdr) if c in ("código", "codigo")), None)
            desc_col = next((j for j, c in enumerate(hdr) if "descrip" in c), None)
            out = []
            for r in rows[hdr_i + 1:]:
                if cod_col is None or cod_col >= len(r):
                    continue
                cod = _norm_code(r[cod_col])
                if not cod or not cod.isdigit():
                    continue
                cost = 0.0
                for j, c in enumerate(r):                    # primer 'U$S' = Precio Compra
                    if str(c).strip() == "U$S" and j + 1 < len(r):
                        cost = _parse_money(r[j + 1]); break
                if cost <= 0:
                    continue
                desc = r[desc_col] if (desc_col is not None and desc_col < len(r)) else ""
                if re.search(r"costo env|retiro de local", str(desc), re.I):
                    continue
                out.append({"codigo": cod, "descripcion": str(desc).strip(), "costo_usd": cost})
            if not out:
                return 0, "Detecté formato FacturApp pero no extraje costos > 0."
            df = pd.DataFrame(out).drop_duplicates("codigo", keep="last")
            self.storage.save_dataframe(df[["codigo", "descripcion", "costo_usd"]], COST_LIST_PATH)
            self._cost_df = None; self._cost_by_code = None
            return len(df), f"Guardados {len(df)} costos (formato FacturApp)."
        # Genérico: CSV con columnas claras.
        return self.save_cost_list(pd.read_csv(io.StringIO(text), sep=None, engine="python"))

    def cost_for_sku(self, seller_sku, seller_custom_field=None):
        """Costo USD por LLAVE EXACTA de SKU (código interno). Prueba
        seller_sku y, si no, seller_custom_field. Devuelve (costo_usd, codigo)
        o (None, None)."""
        self.load_cost_list()
        by = self._cost_by_code or {}
        for raw in (seller_sku, seller_custom_field):
            k = _norm_code(raw)
            if k and k in by:
                return by[k]["costo_usd"], k
        return None, None

    # ─── Envíos en paralelo ───────────────────────────────────────────

    def _fetch_shipping_econ(self, shipping_ids, workers: int = 8) -> dict:
        """Trae la economía de envío de muchos shipping_id EN PARALELO:
        sid -> (me2_costo_vendedor, me1_ingreso). Reutiliza shipment_econ de
        MySalesExtractor. Baja ~750 envíos de ~5 min a ~30 s."""
        if not shipping_ids:
            return {}
        def _one(sid):
            try:
                return sid, self.sales.shipment_econ(sid)
            except Exception:
                return sid, (0.0, 0.0)
        out = {}
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for sid, econ in ex.map(_one, shipping_ids):
                out[sid] = econ
        return out

    # ─── Parámetros (TC, ADS) persistidos en Dropbox ──────────────────

    def load_params(self) -> dict:
        try:
            p = self.storage.load_json(RENTA_PARAMS_PATH) or {}
        except Exception:
            p = {}
        return {"tc": float(p.get("tc", DEFAULT_TC)), "ads": float(p.get("ads", 0.0)),
                "me1_cost": float(p.get("me1_cost", 50.0))}

    def save_params(self, tc: float, ads: float, me1_cost: float = 50.0) -> None:
        try:
            self.storage.save_json({"tc": float(tc), "ads": float(ads),
                                    "me1_cost": float(me1_cost),
                                    "updated_at": datetime.now().isoformat()},
                                   RENTA_PARAMS_PATH)
        except Exception as e:
            logger.warning(f"No se pudo guardar params de renta: {e}")

    # ─── Cálculo principal ────────────────────────────────────────────

    def get_renta(self, as_of=None, tc: float = DEFAULT_TC, ads: float = 0.0,
                  me1_cost_pct: float = 50.0, forecast: dict = None) -> dict:
        """Renta del mes en curso (canal ML), AUTOMÁTICA.

        COGS = costo real del SKU (listado FacturApp, por seller_sku) × el
        precio real al que se vendió ese SKU en ML. Sin markup ni perillas: el
        margen sale solo, SKU por SKU. Lo no cubierto se proyecta con el mismo
        ratio real medido (no con un markup inventado)."""
        if forecast is None or "error" in (forecast or {}):
            forecast = self.sales.get_monthly_forecast(as_of=as_of)
        if "error" in forecast:
            return {"error": forecast["error"]}

        fact_proy = float(forecast["forecast_revenue"])
        rev_sofar = float(forecast["revenue_so_far"]) or 1.0
        d_elapsed = int(forecast["days_elapsed"])

        # Ventas del mes a la fecha, a nivel ítem. SIN envíos acá (los traigo
        # en paralelo más abajo: recorrer ~750 envíos secuencial tardaba ~5 min).
        df = self.sales.get_orders(days_back=d_elapsed + 1, with_shipping=False)
        if df is None or df.empty:
            return {"error": "sin ventas del mes para costear"}
        df = df[df["status"] == "paid"].copy()
        hoy = as_of or date.today()
        df = df[(df["date_created"].dt.year == hoy.year) &
                (df["date_created"].dt.month == hoy.month)]
        if df.empty:
            return {"error": "sin ventas pagas del mes"}

        # Facturación a nivel ítem (unit_price × quantity).
        df["rev_item"] = df["unit_price"] * df["quantity"]

        # COSTEO POR LLAVE EXACTA DE SKU: código interno (seller_sku) ↔ listado.
        # Margen real = costo real × precio real de venta, SKU por SKU.
        self.load_cost_list()
        _sku_col = df["seller_sku"] if "seller_sku" in df.columns else [None] * len(df)
        _cf_col  = df["seller_custom_field"] if "seller_custom_field" in df.columns else [None] * len(df)
        costos_usd, matched, codes = [], [], []
        for sku, cf, q in zip(_sku_col, _cf_col, df["quantity"]):
            c, cod = self.cost_for_sku(sku, cf)
            if c is None:
                costos_usd.append(0.0); matched.append(False); codes.append(None)
            else:
                costos_usd.append(c * float(q)); matched.append(True); codes.append(cod)
        df["cogs_usd"]  = costos_usd                      # costo en USD (pre-TC)
        df["cogs_item"] = df["cogs_usd"] * tc             # COGS en UYU
        df["matched"]   = matched
        df["cod"]       = codes

        rev_total        = float(df["rev_item"].sum())
        rev_matched      = float(df.loc[df["matched"], "rev_item"].sum())
        cogs_usd_matched = float(df.loc[df["matched"], "cogs_usd"].sum())
        cogs_matched     = cogs_usd_matched * tc
        coverage   = rev_matched / rev_total if rev_total > 0 else 0.0
        # Ratio COGS REAL sobre lo cubierto (costo real / venta real).
        cogs_ratio_real = cogs_matched / rev_matched if rev_matched > 0 else 0.0
        # Guardia anti-número-falso: si el listado no está o casi nada matchea,
        # el COGS da ~0 y la renta se infla. Se marca como NO confiable para
        # que el panel avise en vez de mostrar una ganancia irreal.
        n_cost_codes = len(self._cost_by_code or {})

        # Economía de envío a la fecha: 1 consulta /shipments por ORDEN, en
        # PARALELO. Devuelve (me2_costo_vendedor, me1_ingreso). shipping_id sale
        # como float del DataFrame -> int o /shipments/123.0 da 404.
        _sids = [int(s) for s in df["shipping_id"].dropna().unique().tolist()]
        _econ = self._fetch_shipping_econ(_sids)
        def _me2_of(sid):
            return _econ.get(int(sid), (0.0, 0.0))[0] if pd.notna(sid) else 0.0
        def _me1_of(sid):
            return _econ.get(int(sid), (0.0, 0.0))[1] if pd.notna(sid) else 0.0
        # Repartir por ítem (por valor dentro de la orden) para el margen por SKU.
        _ord_rev = df.groupby("order_id")["rev_item"].transform("sum").replace(0, float("nan"))
        df["ship_item"] = (df["shipping_id"].map(_me2_of) * (df["rev_item"] / _ord_rev)).fillna(0.0)
        df["me1_item"]  = (df["shipping_id"].map(_me1_of) * (df["rev_item"] / _ord_rev)).fillna(0.0)
        envios_sofar   = float(df["ship_item"].sum())   # ME2 a tu cargo
        me1_ingreso_sofar = float(df["me1_item"].sum())  # ingreso bruto ME1

        # Diagnóstico: top SKU por facturación, con costo, envío ME2 a tu cargo,
        # ganancia de envío ME1 y margen real.
        me1_keep = 1.0 - float(me1_cost_pct or 0.0) / 100.0   # % que te queda del ME1
        _diag = []
        if bool(df["matched"].any()):
            g = (df[df["matched"]]
                 .groupby(["cod", "item_title"], as_index=False)
                 .agg(rev=("rev_item", "sum"), cogs=("cogs_item", "sum"),
                      ship=("ship_item", "sum"), me1=("me1_item", "sum"),
                      un=("quantity", "sum")))
            g = g.sort_values("rev", ascending=False).head(20)
            for _, r in g.iterrows():
                _rev = float(r["rev"]); _cg = float(r["cogs"]); _sh = float(r["ship"])
                _m1g = float(r["me1"]) * me1_keep
                _mg = (_rev - _cg - _sh + _m1g) / _rev * 100 if _rev > 0 else None
                _diag.append({
                    "item": str(r["item_title"])[:60], "cod": r["cod"],
                    "rev": round(_rev), "cogs": round(_cg), "ship": round(_sh),
                    "me1": round(_m1g), "margen_pct": round(_mg, 1) if _mg is not None else None,
                    "un": int(r["un"]),
                })

        # Diagnóstico de FALTANTES: SKU que se venden pero NO tienen costo en el
        # listado (son los que bajan la cobertura). Muestra el SKU para cargarlo.
        _diag_falta = []
        _un = df[~df["matched"]]
        if not _un.empty:
            _skc = _un["seller_sku"].fillna(_un.get("seller_custom_field"))
            gu = (_un.assign(_sku=_skc.astype(str))
                     .groupby(["_sku", "item_title"], as_index=False)
                     .agg(rev=("rev_item", "sum"), un=("quantity", "sum")))
            gu = gu.sort_values("rev", ascending=False).head(15)
            for _, r in gu.iterrows():
                _diag_falta.append({
                    "sku": str(r["_sku"]), "item": str(r["item_title"])[:60],
                    "rev": round(float(r["rev"])), "un": int(r["un"]),
                })

        # Proyección a fin de mes (escala por facturación).
        scale = fact_proy / rev_sofar if rev_sofar > 0 else 1.0
        neto_mp   = fact_proy * NETO_MP_RATE
        cogs_proy = cogs_ratio_real * fact_proy
        envios_proy   = envios_sofar * scale
        me1_gain_proy = me1_ingreso_sofar * me1_keep * scale   # ganancia neta ME1
        ads = float(ads or 0.0)

        renta = neto_mp - cogs_proy - ads - envios_proy + me1_gain_proy
        margen = renta / fact_proy if fact_proy > 0 else 0.0

        out = {
            "mes":              forecast.get("month"),
            "tc":               tc,
            "me1_cost_pct":     round(float(me1_cost_pct or 0.0), 1),
            "facturacion":      round(fact_proy, 2),
            "neto_mp":          round(neto_mp, 2),
            "comision_imp":     round(fact_proy * (1 - NETO_MP_RATE), 2),
            "cogs":             round(cogs_proy, 2),
            "ads":              round(ads, 2),
            "envios":           round(envios_proy, 2),
            "me1_gain":         round(me1_gain_proy, 2),
            "renta":            round(renta, 2),
            "margen_pct":       round(margen * 100, 1),
            "cogs_ratio_pct":   round(cogs_ratio_real * 100, 1),
            "cogs_ratio_real_pct": round(cogs_ratio_real * 100, 1),
            "match_coverage_pct": round(coverage * 100, 1),
            "envios_sofar":     round(envios_sofar, 2),
            "me1_ingreso_sofar": round(me1_ingreso_sofar, 2),
            "rev_sofar":        round(rev_sofar, 2),
            "cogs_usd_matched": round(cogs_usd_matched, 2),   # base para recalcular con otro TC
            "rev_matched":      round(rev_matched, 2),
            "rev_total":        round(rev_total, 2),
            "n_items_match":    int(df["matched"].sum()),
            "n_items_total":    int(len(df)),
            "cost_list_n":      n_cost_codes,
            "reliable":         bool(n_cost_codes > 0 and coverage >= 0.5),
            "diagnostico":      _diag,
            "diag_falta":       _diag_falta,
            "computed_at":      datetime.now().isoformat(),
        }
        out.update(self._todate_view(rev_sofar, envios_sofar, me1_ingreso_sofar,
                                     cogs_ratio_real, ads, me1_keep, fact_proy))
        return out

    # ─── Cache en Dropbox (el cálculo pega N veces a /shipments) ───────

    @staticmethod
    def _todate_view(rev_sofar, envios_sofar, me1_ing_sofar, cogs_ratio,
                     ads, me1_keep, fact) -> dict:
        """Renta REALIZADA a hoy (no proyectada): usa lo facturado a la fecha y
        los envíos/ME1 YA medidos. ADS se prorratea por la fracción de mes
        facturada."""
        fact_hoy   = float(rev_sofar)
        neto_hoy   = fact_hoy * NETO_MP_RATE
        cogs_hoy   = cogs_ratio * fact_hoy
        ads_hoy    = float(ads) * (fact_hoy / fact) if fact > 0 else 0.0
        envios_hoy = float(envios_sofar)
        me1_hoy    = float(me1_ing_sofar) * me1_keep
        renta_hoy  = neto_hoy - cogs_hoy - ads_hoy - envios_hoy + me1_hoy
        return {
            "facturacion_hoy": round(fact_hoy, 2), "neto_hoy": round(neto_hoy, 2),
            "cogs_hoy": round(cogs_hoy, 2), "ads_hoy": round(ads_hoy, 2),
            "envios_hoy": round(envios_hoy, 2), "me1_gain_hoy": round(me1_hoy, 2),
            "renta_hoy": round(renta_hoy, 2),
            "margen_hoy": round((renta_hoy / fact_hoy * 100) if fact_hoy > 0 else 0, 1),
        }

    @staticmethod
    def _apply_params(res: dict, ads: float, tc: float, fact_live: float = None,
                      me1_cost_pct: float = None) -> dict:
        """Re-deriva la parte BARATA sobre la base cara ya calculada
        (costo USD matcheado, envíos ME2 e ingreso ME1 a la fecha, ventas
        cubiertas). Permite cambiar TC/ADS/costo-ME1 y usar la FACTURACIÓN VIVA
        del pronóstico (fact_live) para que todo se re-escale solo. No re-barre
        envíos."""
        r = dict(res)
        fact        = float(fact_live) if fact_live else float(r.get("facturacion", 0.0))
        rev_sofar   = float(r.get("rev_sofar", 0.0)) or 1.0
        envios_sofar = float(r.get("envios_sofar", 0.0))
        me1_ing_sofar = float(r.get("me1_ingreso_sofar", 0.0))
        rev_matched = float(r.get("rev_matched", 0.0))
        cogs_usd_m  = float(r.get("cogs_usd_matched", 0.0))
        ads = float(ads or 0.0); tc = float(tc or DEFAULT_TC)
        if me1_cost_pct is None:
            me1_cost_pct = float(r.get("me1_cost_pct", 50.0))
        me1_keep = 1.0 - float(me1_cost_pct) / 100.0
        scale = fact / rev_sofar if rev_sofar > 0 else 1.0
        neto = fact * NETO_MP_RATE
        cogs_ratio = (cogs_usd_m * tc / rev_matched) if rev_matched > 0 else 0.0
        cogs  = cogs_ratio * fact
        envios = envios_sofar * scale
        me1_gain = me1_ing_sofar * me1_keep * scale
        renta = neto - cogs - ads - envios + me1_gain
        r["tc"]                  = round(tc, 2)
        r["ads"]                 = round(ads, 2)
        r["me1_cost_pct"]        = round(float(me1_cost_pct), 1)
        r["facturacion"]         = round(fact, 2)
        r["neto_mp"]             = round(neto, 2)
        r["comision_imp"]        = round(fact * (1 - NETO_MP_RATE), 2)
        r["cogs"]                = round(cogs, 2)
        r["cogs_ratio_pct"]      = round(cogs_ratio * 100, 1)
        r["cogs_ratio_real_pct"] = round(cogs_ratio * 100, 1)
        r["envios"]              = round(envios, 2)
        r["me1_gain"]            = round(me1_gain, 2)
        r["renta"]               = round(renta, 2)
        r["margen_pct"]          = round((renta / fact * 100) if fact > 0 else 0, 1)
        r.update(RentaCalculator._todate_view(rev_sofar, envios_sofar, me1_ing_sofar,
                                              cogs_ratio, ads, me1_keep, fact))
        return r

    @staticmethod
    def _fact_of(forecast: dict):
        """Facturación proyectada viva del pronóstico, o None si no sirve."""
        if forecast and "error" not in forecast and forecast.get("forecast_revenue"):
            return float(forecast["forecast_revenue"])
        return None

    def peek_cache(self, tc: float = DEFAULT_TC, ads: float = 0.0,
                   me1_cost_pct: float = 50.0, forecast: dict = None):
        """Lee el cache SIN calcular (carga normal del dashboard) y re-deriva
        con la facturación viva del pronóstico. (None, None) si no hay cache."""
        try:
            c = self.storage.load_json(RENTA_CACHE_PATH)
            if c and c.get("ts") and c.get("result") and "cogs_usd_matched" in c["result"]:
                age = (datetime.now().timestamp() - c["ts"]) / 3600.0
                return self._apply_params(c["result"], ads, tc, self._fact_of(forecast), me1_cost_pct), age
        except Exception:
            pass
        return None, None

    def get_renta_cached(self, max_age_h: float = 12, force: bool = False,
                         tc: float = DEFAULT_TC, ads: float = 0.0,
                         me1_cost_pct: float = 50.0, forecast: dict = None):
        """Devuelve (renta, edad_horas). El barrido de envíos (caro) sólo corre
        si force=True o si el cache venció; TC/ADS/costo-ME1 y la facturación
        viva se recalculan al vuelo."""
        if not force:
            try:
                c = self.storage.load_json(RENTA_CACHE_PATH)
                if c and c.get("ts") and c.get("result") and "cogs_usd_matched" in c["result"]:
                    age = (datetime.now().timestamp() - c["ts"]) / 3600.0
                    if age < max_age_h:
                        return self._apply_params(c["result"], ads, tc, self._fact_of(forecast), me1_cost_pct), age
            except Exception:
                pass
        res = self.get_renta(tc=tc, ads=ads, me1_cost_pct=me1_cost_pct, forecast=forecast)
        if "error" not in res:
            try:
                self.storage.save_json(
                    {"ts": datetime.now().timestamp(), "result": res},
                    RENTA_CACHE_PATH)
            except Exception:
                pass
        return res, 0.0
