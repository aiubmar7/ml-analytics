"""
Cálculo de RENTA mensual del canal ML (DELERAL / Eldom).

Fórmula validada con el usuario:

    renta = facturación × 0.81  (neto MP: quita 14,1% comisión + ~4,9%
                                 retenciones de impuesto por tarjeta)
            − COGS              (costo de mercadería, markup real del
                                 listado de costos, a TC del mes)
            − ADS               (inversión en Product Ads del mes)
            − envíos ME2        (list_cost que paga el vendedor en envíos
                                 gratis ME2 drop-off; ME1 y los que paga
                                 el comprador se excluyen)

El COGS del mes PROYECTADO se estima aplicando el ratio COGS/facturación
medido sobre las ventas reales del mes a la fecha (sobre los ítems
matcheados con el listado de costos). Se reporta la cobertura del match
para transparencia.

Los costos del listado vienen en USD → se convierten a UYU con el TC del
mes (parámetro, default 40.45).

Datos sensibles: el listado de costos vive SOLO en Dropbox privado
(data/costos/costos.parquet), nunca en el repo público.
"""

import re
import time
import logging
import unicodedata
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

logger = logging.getLogger(__name__)

COST_LIST_PATH   = "data/costos/costos.parquet"
RENTA_CACHE_PATH = "config/renta_cache.json"
RENTA_PARAMS_PATH = "config/renta_params.json"

NETO_MP_RATE = 0.81          # igual a COBRANZA_NET_RATE de my_sales
DEFAULT_TC   = 40.45         # TC noviembre 2026 (parámetro)


# ─── Normalización y matching de títulos ──────────────────────────────

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")

def _norm(s: str) -> str:
    """minúsculas, sin acentos, BTU unificado (12000->12k), sin ruido."""
    s = _strip_accents(str(s).lower())
    s = s.replace("btu", " ").replace("''", " ").replace('"', " ")
    # 12000 -> 12k ; 9000 -> 9k  (unifica BTU de aires)
    s = re.sub(r"\b(\d{1,2})000\b", r"\1k", s)
    s = re.sub(r"[^\w\s\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

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

_STOP = {"de","la","el","con","para","y","en","a","2","1","cm","cms","un","una",
         "x","color","blanco","negro","gris","uni","mm","lt","lts","w"}

def _key_tokens(norm_title: str) -> set:
    """Tokens distintivos: códigos de modelo (letra+dígito), nros >=4
    dígitos, y BTU (12k/9k/18k). Son los que identifican un producto."""
    toks = set()
    for t in norm_title.split():
        if t in _STOP or len(t) < 2:
            continue
        has_d = any(c.isdigit() for c in t)
        has_a = any(c.isalpha() for c in t)
        if (has_d and has_a) or (t.isdigit() and len(t) >= 4) or re.fullmatch(r"\d{1,2}k", t):
            toks.add(t)
    return toks

def _strong_tokens(norm_title: str) -> set:
    """Identificadores FUERTES: códigos de modelo / nros de parte. Excluye el
    BTU (12k/9k), que es débil (muchos aires distintos comparten '12k' y
    generaban falsos positivos). Un match exige compartir al menos uno de
    estos (o dos key-tokens corroborados)."""
    toks = set()
    for t in norm_title.split():
        if t in _STOP or len(t) < 3:
            continue
        if re.fullmatch(r"\d{1,2}k", t):          # BTU => débil, no fuerte
            continue
        has_d = any(c.isdigit() for c in t)
        has_a = any(c.isalpha() for c in t)
        if (has_d and has_a and len(t) >= 4) or (t.isdigit() and len(t) >= 4):
            toks.add(t)
    return toks

def _all_tokens(norm_title: str) -> set:
    return {t for t in norm_title.split() if t not in _STOP and len(t) >= 3}


class RentaCalculator:
    def __init__(self, sales_extractor, storage):
        self.sales   = sales_extractor
        self.storage = storage
        self._cost_df = None
        self._cost_idx = None     # token -> set(row_pos)  (match por título, fallback)
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
        df["_norm"]   = df["descripcion"].map(_norm)
        df["_keys"]   = df["_norm"].map(_key_tokens)
        df["_strong"] = df["_norm"].map(_strong_tokens)
        df["_toks"]   = df["_norm"].map(_all_tokens)
        # índice invertido por token FUERTE (match por título, sólo fallback).
        idx = {}
        for pos, strong in enumerate(df["_strong"]):
            for k in strong:
                idx.setdefault(k, set()).add(pos)
        # LLAVE EXACTA: código interno -> costo. En ML cada venta trae ese
        # código en seller_sku, así que el costeo es costo real × venta real.
        by_code = {}
        for _, r in df.iterrows():
            k = _norm_code(r["codigo"])
            if k:
                by_code[k] = {"costo_usd": float(r["costo_usd"]),
                              "descripcion": str(r["descripcion"])}
        self._cost_df, self._cost_idx, self._cost_by_code = df, idx, by_code
        return df

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

    def match_cost(self, item_title: str):
        """Devuelve (costo_usd, codigo) del mejor match, o (None, None).
        Exige compartir al menos un token FUERTE (código/modelo); el BTU y los
        tokens genéricos sólo desempatan. Evita los falsos positivos que
        inflaban el COGS."""
        df = self.load_cost_list()
        if df.empty:
            return None, None
        n = _norm(item_title)
        strong = _strong_tokens(n)
        keys   = _key_tokens(n)
        toks   = _all_tokens(n)
        if not strong:
            return None, None
        # candidatos: filas que comparten al menos un token fuerte
        cands = set()
        for k in strong:
            cands |= self._cost_idx.get(k, set())
        if not cands:
            return None, None
        best, best_score, best_strong = None, 0.0, 0
        for pos in cands:
            row = df.iloc[pos]
            s_strong = len(strong & row["_strong"])
            s_keys   = len(keys   & row["_keys"])
            s_toks   = len(toks   & row["_toks"])
            score = s_strong * 10 + s_keys * 2 + s_toks
            if score > best_score:
                best, best_score, best_strong = row, score, s_strong
        # exigir al menos UN token fuerte compartido
        if best is not None and best_strong >= 1:
            return float(best["costo_usd"]), str(best["codigo"])
        return None, None

    # ─── Envíos en paralelo ───────────────────────────────────────────

    def _fetch_shipping_costs(self, shipping_ids, workers: int = 8) -> dict:
        """Trae el costo de envío del vendedor para muchos shipping_id EN
        PARALELO (reutiliza _seller_shipping_cost de MySalesExtractor, que ya
        aplica la regla ME2-gratis→list_cost). Baja ~750 envíos de ~5 min a
        ~30 s. requests.Session es thread-safe para requests independientes."""
        if not shipping_ids:
            return {}
        def _one(sid):
            try:
                return sid, self.sales._seller_shipping_cost(sid)
            except Exception:
                return sid, 0.0
        out = {}
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for sid, cost in ex.map(_one, shipping_ids):
                out[sid] = cost
        return out

    # ─── Parámetros (TC, ADS) persistidos en Dropbox ──────────────────

    def load_params(self) -> dict:
        try:
            p = self.storage.load_json(RENTA_PARAMS_PATH) or {}
        except Exception:
            p = {}
        return {"tc": float(p.get("tc", DEFAULT_TC)), "ads": float(p.get("ads", 0.0))}

    def save_params(self, tc: float, ads: float) -> None:
        try:
            self.storage.save_json({"tc": float(tc), "ads": float(ads),
                                    "updated_at": datetime.now().isoformat()},
                                   RENTA_PARAMS_PATH)
        except Exception as e:
            logger.warning(f"No se pudo guardar params de renta: {e}")

    # ─── Cálculo principal ────────────────────────────────────────────

    def get_renta(self, as_of=None, tc: float = DEFAULT_TC, ads: float = 0.0,
                  forecast: dict = None) -> dict:
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

        # Diagnóstico: top SKU por facturación con su margen real.
        _diag = []
        if bool(df["matched"].any()):
            g = (df[df["matched"]]
                 .groupby(["cod", "item_title"], as_index=False)
                 .agg(rev=("rev_item", "sum"), cogs=("cogs_item", "sum"),
                      un=("quantity", "sum")))
            g["markup"] = (g["rev"] / g["cogs"].replace(0, float("nan")) - 1) * 100
            g = g.sort_values("rev", ascending=False).head(20)
            for _, r in g.iterrows():
                _diag.append({
                    "item": str(r["item_title"])[:60], "cod": r["cod"],
                    "rev": round(float(r["rev"])), "cogs": round(float(r["cogs"])),
                    "markup_pct": round(float(r["markup"]), 1) if r["cogs"] else None,
                    "un": int(r["un"]),
                })

        # Envíos a la fecha: 1 consulta /shipments por ORDEN, en PARALELO. Los
        # shipping_id salen del DataFrame como float (pandas), hay que pasarlos
        # a int o la URL /shipments/123.0 da 404 (bug que ponía envíos en 0).
        _sids = [int(s) for s in df["shipping_id"].dropna().unique().tolist()]
        _ship_map = self._fetch_shipping_costs(_sids)
        envios_sofar = float(sum(_ship_map.values()))

        # Proyección a fin de mes (escala por facturación).
        scale = fact_proy / rev_sofar if rev_sofar > 0 else 1.0
        neto_mp   = fact_proy * NETO_MP_RATE
        # COGS AUTOMÁTICO: ratio real medido por SKU, extrapolado a la
        # facturación proyectada. Sin markup. Con cobertura <100% se asume que
        # lo no cubierto tiene el mismo ratio real (la cobertura se muestra).
        cogs_proy = cogs_ratio_real * fact_proy
        envios_proy = envios_sofar * scale
        ads = float(ads or 0.0)

        renta = neto_mp - cogs_proy - ads - envios_proy
        margen = renta / fact_proy if fact_proy > 0 else 0.0

        return {
            "mes":              forecast.get("month"),
            "tc":               tc,
            "facturacion":      round(fact_proy, 2),
            "neto_mp":          round(neto_mp, 2),
            "comision_imp":     round(fact_proy * (1 - NETO_MP_RATE), 2),
            "cogs":             round(cogs_proy, 2),
            "ads":              round(ads, 2),
            "envios":           round(envios_proy, 2),
            "renta":            round(renta, 2),
            "margen_pct":       round(margen * 100, 1),
            "cogs_ratio_pct":   round(cogs_ratio_real * 100, 1),
            "cogs_ratio_real_pct": round(cogs_ratio_real * 100, 1),
            "match_coverage_pct": round(coverage * 100, 1),
            "envios_sofar":     round(envios_sofar, 2),
            "rev_sofar":        round(rev_sofar, 2),
            "cogs_usd_matched": round(cogs_usd_matched, 2),   # base para recalcular con otro TC
            "rev_matched":      round(rev_matched, 2),
            "rev_total":        round(rev_total, 2),
            "n_items_match":    int(df["matched"].sum()),
            "n_items_total":    int(len(df)),
            "diagnostico":      _diag,
            "computed_at":      datetime.now().isoformat(),
        }

    # ─── Cache en Dropbox (el cálculo pega N veces a /shipments) ───────

    @staticmethod
    def _apply_params(res: dict, ads: float, tc: float) -> dict:
        """Re-deriva la parte BARATA (COGS al TC actual, ADS, renta) sobre una
        base ya calculada (facturación, neto, envíos, costo USD matcheado).
        Evita re-barrer envíos cuando sólo cambian TC o ADS."""
        r = dict(res)
        fact        = float(r.get("facturacion", 0.0))
        neto        = float(r.get("neto_mp", 0.0))
        envios      = float(r.get("envios", 0.0))               # ya proyectado
        rev_matched = float(r.get("rev_matched", 0.0))
        cogs_usd_m  = float(r.get("cogs_usd_matched", 0.0))
        ads = float(ads or 0.0); tc = float(tc or DEFAULT_TC)
        # Ratio COGS real al TC actual (costo USD × TC / venta real cubierta).
        cogs_ratio = (cogs_usd_m * tc / rev_matched) if rev_matched > 0 else 0.0
        cogs  = cogs_ratio * fact
        renta = neto - cogs - ads - envios
        r["tc"]                 = round(tc, 2)
        r["ads"]                = round(ads, 2)
        r["cogs"]               = round(cogs, 2)
        r["cogs_ratio_pct"]     = round(cogs_ratio * 100, 1)
        r["cogs_ratio_real_pct"] = round(cogs_ratio * 100, 1)
        r["renta"]              = round(renta, 2)
        r["margen_pct"]         = round((renta / fact * 100) if fact > 0 else 0, 1)
        return r

    def peek_cache(self, tc: float = DEFAULT_TC, ads: float = 0.0):
        """Lee el cache SIN calcular (carga normal del dashboard) y re-deriva
        COGS/ADS para los parámetros actuales. (None, None) si no hay cache."""
        try:
            c = self.storage.load_json(RENTA_CACHE_PATH)
            if c and c.get("ts") and c.get("result") and "cogs_usd_matched" in c["result"]:
                age = (datetime.now().timestamp() - c["ts"]) / 3600.0
                return self._apply_params(c["result"], ads, tc), age
        except Exception:
            pass
        return None, None

    def get_renta_cached(self, max_age_h: float = 12, force: bool = False,
                         tc: float = DEFAULT_TC, ads: float = 0.0,
                         forecast: dict = None):
        """Devuelve (renta, edad_horas). El barrido de envíos (caro) sólo corre
        si force=True o si el cache venció; TC/ADS se recalculan al vuelo."""
        if not force:
            try:
                c = self.storage.load_json(RENTA_CACHE_PATH)
                if c and c.get("ts") and c.get("result") and "cogs_usd_matched" in c["result"]:
                    age = (datetime.now().timestamp() - c["ts"]) / 3600.0
                    if age < max_age_h:
                        return self._apply_params(c["result"], ads, tc), age
            except Exception:
                pass
        res = self.get_renta(tc=tc, ads=ads, forecast=forecast)
        if "error" not in res:
            try:
                self.storage.save_json(
                    {"ts": datetime.now().timestamp(), "result": res},
                    RENTA_CACHE_PATH)
            except Exception:
                pass
        return res, 0.0
