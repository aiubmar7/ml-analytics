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

def _all_tokens(norm_title: str) -> set:
    return {t for t in norm_title.split() if t not in _STOP and len(t) >= 3}


class RentaCalculator:
    def __init__(self, sales_extractor, storage):
        self.sales   = sales_extractor
        self.storage = storage
        self._cost_df = None
        self._cost_idx = None     # token -> set(row_pos)

    # ─── Listado de costos ────────────────────────────────────────────

    def load_cost_list(self) -> pd.DataFrame:
        if self._cost_df is not None:
            return self._cost_df
        df = self.storage.load_dataframe(COST_LIST_PATH)
        if df is None or df.empty:
            self._cost_df = pd.DataFrame(columns=["codigo","descripcion","grupo","costo_usd"])
            return self._cost_df
        df = df[df["costo_usd"] > 0].copy().reset_index(drop=True)
        df["_norm"] = df["descripcion"].map(_norm)
        df["_keys"] = df["_norm"].map(_key_tokens)
        df["_toks"] = df["_norm"].map(_all_tokens)
        # índice invertido por key-token para no escanear 700 filas por venta
        idx = {}
        for pos, keys in enumerate(df["_keys"]):
            for k in keys:
                idx.setdefault(k, set()).add(pos)
        self._cost_df, self._cost_idx = df, idx
        return df

    def match_cost(self, item_title: str):
        """Devuelve (costo_usd, codigo) del mejor match, o (None, None)."""
        df = self.load_cost_list()
        if df.empty:
            return None, None
        n = _norm(item_title)
        keys = _key_tokens(n)
        toks = _all_tokens(n)
        # candidatos: filas que comparten al menos un key-token
        cands = set()
        for k in keys:
            cands |= self._cost_idx.get(k, set())
        if not cands:
            return None, None
        best, best_score = None, 0.0
        for pos in cands:
            row = df.iloc[pos]
            shared_keys = len(keys & row["_keys"])
            shared_toks = len(toks & row["_toks"])
            # el peso fuerte lo dan los key-tokens (modelo/BTU)
            score = shared_keys * 3 + shared_toks
            if score > best_score:
                best, best_score = row, score
        # umbral: al menos un key-token en común
        if best is not None and best_score >= 3:
            return float(best["costo_usd"]), str(best["codigo"])
        return None, None

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
        """Renta del mes en curso (canal ML). Hace UNA pasada a la API con
        envíos: de ahí salen COGS (match de costos) y envíos juntos."""
        if forecast is None or "error" in (forecast or {}):
            forecast = self.sales.get_monthly_forecast(as_of=as_of)
        if "error" in forecast:
            return {"error": forecast["error"]}

        fact_proy = float(forecast["forecast_revenue"])
        rev_sofar = float(forecast["revenue_so_far"]) or 1.0
        d_elapsed = int(forecast["days_elapsed"])

        # Ventas del mes a la fecha, a nivel ítem, CON envíos (1 sola pasada).
        df = self.sales.get_orders(days_back=d_elapsed + 1, with_shipping=True)
        if df is None or df.empty:
            return {"error": "sin ventas del mes para costear"}
        df = df[df["status"] == "paid"].copy()
        hoy = as_of or date.today()
        df = df[(df["date_created"].dt.year == hoy.year) &
                (df["date_created"].dt.month == hoy.month)]
        if df.empty:
            return {"error": "sin ventas pagas del mes"}

        # Facturación a nivel ítem (unit_price × quantity) para el ratio.
        df["rev_item"] = df["unit_price"] * df["quantity"]

        # Match de costos (cacheado por título para no repetir).
        cache = {}
        def _cost(t):
            if t not in cache:
                cache[t] = self.match_cost(t)
            return cache[t]
        costos, matched = [], []
        for t, q in zip(df["item_title"], df["quantity"]):
            c, _cod = _cost(t)
            if c is None:
                costos.append(0.0); matched.append(False)
            else:
                costos.append(c * q * tc); matched.append(True)   # COGS en UYU
        df["cogs_item"] = costos
        df["matched"]   = matched

        rev_total   = float(df["rev_item"].sum())
        rev_matched = float(df.loc[df["matched"], "rev_item"].sum())
        cogs_matched = float(df.loc[df["matched"], "cogs_item"].sum())
        coverage = rev_matched / rev_total if rev_total > 0 else 0.0
        cogs_ratio = cogs_matched / rev_matched if rev_matched > 0 else 0.0

        # Envíos a la fecha (shipping_cost ya viene 1 vez por orden).
        envios_sofar = float(df["shipping_cost"].sum())

        # Proyección a fin de mes (escala por facturación).
        scale = fact_proy / rev_sofar if rev_sofar > 0 else 1.0
        neto_mp   = fact_proy * NETO_MP_RATE
        cogs_proy = cogs_ratio * fact_proy
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
            "cogs_ratio_pct":   round(cogs_ratio * 100, 1),
            "match_coverage_pct": round(coverage * 100, 1),
            "envios_sofar":     round(envios_sofar, 2),
            "rev_sofar":        round(rev_sofar, 2),
            "n_items_match":    int(df["matched"].sum()),
            "n_items_total":    int(len(df)),
            "computed_at":      datetime.now().isoformat(),
        }

    # ─── Cache en Dropbox (el cálculo pega N veces a /shipments) ───────

    def peek_cache(self, tc: float = DEFAULT_TC, ads: float = 0.0):
        """Lee el cache SIN calcular (para la carga normal del dashboard).
        Devuelve (result, edad_horas) o (None, None) si no hay cache válido
        para ese TC/ADS."""
        try:
            c = self.storage.load_json(RENTA_CACHE_PATH)
            if c and c.get("ts") and c.get("tc") == tc and c.get("ads") == ads:
                age = (datetime.now().timestamp() - c["ts"]) / 3600.0
                return c.get("result", {}), age
        except Exception:
            pass
        return None, None

    def get_renta_cached(self, max_age_h: float = 12, force: bool = False,
                         tc: float = DEFAULT_TC, ads: float = 0.0,
                         forecast: dict = None):
        """Devuelve (renta, edad_horas). Recalcula solo si no hay cache, si
        venció, o si cambian TC/ADS (el cálculo es caro: ~N /shipments)."""
        if not force:
            try:
                c = self.storage.load_json(RENTA_CACHE_PATH)
                if c and c.get("ts") and c.get("tc") == tc and c.get("ads") == ads:
                    age = (datetime.now().timestamp() - c["ts"]) / 3600.0
                    if age < max_age_h:
                        return c.get("result", {}), age
            except Exception:
                pass
        res = self.get_renta(tc=tc, ads=ads, forecast=forecast)
        if "error" not in res:
            try:
                self.storage.save_json(
                    {"ts": datetime.now().timestamp(), "tc": tc, "ads": ads, "result": res},
                    RENTA_CACHE_PATH)
            except Exception:
                pass
        return res, 0.0
