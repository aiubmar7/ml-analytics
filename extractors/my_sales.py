"""
Extractor de ventas y métricas propias.
"""

import logging
from datetime import datetime, timedelta, date, timezone

# Uruguay es UTC-3 fijo (sin horario de verano desde 2015). El servidor
# de Streamlit corre en UTC, así que calculamos "hoy" en hora local de
# Uruguay para que el día del mes no se adelante de noche.
UY_TZ = timezone(timedelta(hours=-3))


def _today_uy() -> date:
    return datetime.now(UY_TZ).date()
import calendar
from pathlib import Path

import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))
from auth.ml_client import MLClient
from storage.dropbox_client import DropboxClient
from config import DEFAULT_DAYS_BACK

logger = logging.getLogger(__name__)


class MySalesExtractor:

    def __init__(self):
        self.client  = MLClient()
        self.storage = DropboxClient()
        self.user_id = None
        self._orders_cache = {}   # {days_back: (timestamp, df)} cache con TTL
        self._hist_cache   = {}   # {(year, month): df} cache de meses históricos

    def _get_user_id(self) -> str:
        if not self.user_id:
            me = self.client.get_my_user()
            self.user_id = str(me["id"])
        return self.user_id

    def get_orders(self, days_back: int = DEFAULT_DAYS_BACK) -> pd.DataFrame:
        # Cache con TTL de 10 min: evita re-bajar la misma data en cada
        # carga/refresco de la página (lo que martillaba la API y
        # disparaba el rate limit). El objeto persiste entre reruns por
        # el @st.cache_resource de get_clients() en app.py.
        import time as _time
        _cached = self._orders_cache.get(days_back)
        if _cached and (_time.time() - _cached[0]) < 600:
            return _cached[1]

        user_id   = self._get_user_id()
        date_from = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00.000-03:00")
        logger.info(f"Extrayendo ordenes de los ultimos {days_back} dias...")
        orders = []
        for order in self.client.get_all_pages(
            "/orders/search",
            params={"seller": user_id, "sort": "date_desc", "order.date_created.from": date_from, "offset": 0},
            results_key="results",
            max_items=10000,
        ):
            for item in order.get("order_items", []):
                orders.append({
                    "order_id":       order["id"],
                    "date_created":   order["date_created"],
                    "date_closed":    order.get("date_closed"),
                    "status":         order["status"],
                    "total_amount":   order["total_amount"],
                    "currency_id":    order["currency_id"],
                    "item_id":        item["item"]["id"],
                    "item_title":     item["item"]["title"],
                    "quantity":       item["quantity"],
                    "unit_price":     item["unit_price"],
                    "sale_fee":       item.get("sale_fee", 0),
                    "buyer_id":       order.get("buyer", {}).get("id"),
                    "buyer_nickname": order.get("buyer", {}).get("nickname"),
                    "shipping_id":    order.get("shipping", {}).get("id"),
                })
        if not orders:
            return pd.DataFrame()
        df = pd.DataFrame(orders)
        df["date_created"] = pd.to_datetime(df["date_created"])
        df["date_closed"]  = pd.to_datetime(df["date_closed"], errors="coerce")
        df["net_amount"]   = df["total_amount"] - df["sale_fee"]
        self._orders_cache[days_back] = (_time.time(), df)
        return df

    def get_orders_by_daterange(self, date_from: date, date_to: date) -> pd.DataFrame:
        user_id  = self._get_user_id()
        from_str = date_from.strftime("%Y-%m-%dT00:00:00.000-03:00")
        to_str   = date_to.strftime("%Y-%m-%dT23:59:59.000-03:00")
        logger.info(f"Extrayendo ordenes del {date_from} al {date_to}...")
        orders = []
        for order in self.client.get_all_pages(
            "/orders/search",
            params={
                "seller": user_id,
                "sort": "date_desc",
                "order.date_created.from": from_str,
                "order.date_created.to":   to_str,
                "offset": 0,
            },
            results_key="results",
            max_items=10000,
        ):
            for item in order.get("order_items", []):
                orders.append({
                    "order_id":     order["id"],
                    "date_created": order["date_created"],
                    "status":       order["status"],
                    "total_amount": order["total_amount"],
                    "currency_id":  order["currency_id"],
                    "item_id":      item["item"]["id"],
                    "item_title":   item["item"]["title"],
                    "quantity":     item["quantity"],
                    "unit_price":   item["unit_price"],
                    "sale_fee":     item.get("sale_fee", 0),
                })
        if not orders:
            return pd.DataFrame()
        df = pd.DataFrame(orders)
        df["date_created"] = pd.to_datetime(df["date_created"])
        df["net_amount"]   = df["total_amount"] - df["sale_fee"]
        df["date"]         = df["date_created"].dt.date
        return df

    def get_period_summary(self, date_from: date, date_to: date) -> dict:
        df = self.get_orders_by_daterange(date_from, date_to)
        if df.empty:
            return {"revenue": 0, "net": 0, "orders": 0, "units": 0, "avg_ticket": 0, "df": df}
        df_paid = df[df["status"] == "paid"]
        return {
            "revenue":    round(float(df_paid["total_amount"].sum()), 2),
            "net":        round(float(df_paid["net_amount"].sum()), 2),
            "orders":     df_paid["order_id"].nunique(),
            "units":      int(df_paid["quantity"].sum()),
            "avg_ticket": round(float(df_paid["total_amount"].mean()), 2) if not df_paid.empty else 0,
            "df":         df_paid,
        }

    def sync_orders(self, days_back: int = DEFAULT_DAYS_BACK) -> pd.DataFrame:
        df = self.get_orders(days_back)
        if not df.empty:
            month_str = datetime.now().strftime("%Y-%m")
            path = f"data/my_sales/orders_{month_str}.parquet"
            self.storage.append_dataframe(df, path)
            self.storage.log_sync("my_sales", "ok", {"rows": len(df)})
        return df

    def get_my_items(self) -> pd.DataFrame:
        user_id = self._get_user_id()
        item_ids = list(self.client.get_all_pages(f"/users/{user_id}/items/search", results_key="results"))
        if not item_ids:
            return pd.DataFrame()
        items_data = self.client.get_items_bulk(item_ids)
        rows = []
        for item in items_data:
            rows.append({
                "item_id":       item["id"],
                "title":         item["title"],
                "category_id":   item["category_id"],
                "price":         item["price"],
                "currency_id":   item["currency_id"],
                "available_qty": item.get("available_quantity", 0),
                "sold_qty":      item.get("sold_quantity", 0),
                "status":        item["status"],
                "listing_type":  item.get("listing_type_id"),
                "condition":     item.get("condition"),
                "permalink":     item.get("permalink"),
                "date_created":  item.get("date_created"),
                "last_updated":  item.get("last_updated"),
                "health":        item.get("health"),
            })
        return pd.DataFrame(rows)

    def sync_my_items(self) -> pd.DataFrame:
        df = self.get_my_items()
        if not df.empty:
            month_str = datetime.now().strftime("%Y-%m")
            self.storage.save_dataframe(df, f"data/my_sales/items_{month_str}.parquet")
        return df

    def get_my_reputation(self) -> dict:
        try:
            user_id = self._get_user_id()
            data    = self.client.get(f"/users/{user_id}/seller_reputation")
            return {
                "level_id":               data.get("level_id"),
                "power_seller_status":    data.get("power_seller_status"),
                "transactions_total":     data.get("transactions", {}).get("total", 0),
                "transactions_completed": data.get("transactions", {}).get("completed", 0),
                "claims_rate":            data.get("metrics", {}).get("claims", {}).get("rate", 0),
                "delayed_handling_rate":  data.get("metrics", {}).get("delayed_handling_time", {}).get("rate", 0),
                "cancellations_rate":     data.get("metrics", {}).get("cancellations", {}).get("rate", 0),
            }
        except Exception as e:
            logger.warning(f"No se pudo obtener reputacion: {e}")
            return {"level_id": "Sin datos", "power_seller_status": "Sin datos", "claims_rate": 0}

    def get_summary(self, days_back: int = 30) -> dict:
        df = self.get_orders(days_back)
        if df.empty:
            return {"error": "Sin datos de ventas"}
        df_paid = df[df["status"] == "paid"]
        return {
            "period_days":   days_back,
            "total_orders":  df_paid["order_id"].nunique(),
            "total_units":   int(df_paid["quantity"].sum()),
            "total_revenue": round(float(df_paid["total_amount"].sum()), 2),
            "net_revenue":   round(float(df_paid["net_amount"].sum()), 2),
            "avg_ticket":    round(float(df_paid["total_amount"].mean()), 2) if not df_paid.empty else 0,
            "top_item":      df_paid.groupby("item_title")["quantity"].sum().idxmax() if not df_paid.empty else None,
            "reputation":    self.get_my_reputation(),
        }

    def _load_historical_month(self, year: int, month: int) -> pd.DataFrame:
        """Carga un mes desde Dropbox historial (con cache en memoria)."""
        key = (year, month)
        if key in self._hist_cache:
            return self._hist_cache[key]
        try:
            path = f"data/historical/{year:04d}-{month:02d}.parquet"
            df = self.storage.load_dataframe(path)
            if df is not None and not df.empty:
                df["date_created"] = pd.to_datetime(df["date_created"])
                df["date"] = df["date_created"].dt.date
                if "net_amount" not in df.columns:
                    df["net_amount"] = df["total_amount"] - df.get("sale_fee", 0)
            self._hist_cache[key] = df
            return df
        except Exception:
            return None

    def get_monthly_forecast(self, as_of=None, data_override=None, weights_by_bucket=None) -> dict:
        # as_of / data_override permiten re-correr el MISMO cálculo para una
        # fecha de corte pasada con datos históricos (lo usa el backtest, así
        # nunca se desincroniza de producción). weights_by_bucket habilita
        # pesos que varían por fase del mes (C/D). Todo default = producción.
        today          = as_of if as_of is not None else _today_uy()
        days_in_month  = calendar.monthrange(today.year, today.month)[1]
        days_elapsed   = today.day
        days_remaining = days_in_month - days_elapsed

        # Un solo pull de 120 días para TODO el pronóstico: mes actual,
        # mes previo, ventana del Factor 6 y nivel base salen de acá.
        # Antes se bajaba en 3 pulls separados que se solapaban y
        # enlentecían (a veces colgaban) la carga de la página.
        df_all = data_override if data_override is not None else self.get_orders(120)
        if df_all is None or df_all.empty:
            return {"error": "Sin datos suficientes para proyectar"}
        df_all = df_all[df_all["status"] == "paid"].copy()
        df_all["date"] = pd.to_datetime(df_all["date_created"]).dt.date

        df_paid = df_all[df_all["date"] >= date(today.year, today.month, 1)].copy()
        if df_paid.empty:
            return {"error": "Sin ventas este mes todavia"}

        revenue_so_far = float(df_paid["total_amount"].sum())
        units_so_far   = int(df_paid["quantity"].sum())
        orders_so_far  = df_paid["order_id"].nunique()
        net_so_far     = float(df_paid["net_amount"].sum())

        daily_avg_revenue = revenue_so_far / days_elapsed
        daily_avg_units   = units_so_far   / days_elapsed
        daily_avg_orders  = orders_so_far  / days_elapsed

        # ── Factor 1: Promedio diario del mes (25%) ───────────────
        proj1_revenue = revenue_so_far + (daily_avg_revenue * days_remaining)
        proj1_units   = units_so_far   + (daily_avg_units   * days_remaining)
        proj1_orders  = orders_so_far  + (daily_avg_orders  * days_remaining)

        # ── Factor 2: Tendencia últimos 7 días (30%) ──────────────
        last7 = df_paid[df_paid["date"] >= (today - timedelta(days=6))]
        days7 = max(len(last7["date"].unique()), 1)
        daily_trend_revenue = float(last7["total_amount"].sum()) / days7
        daily_trend_units   = float(last7["quantity"].sum())     / days7
        daily_trend_orders  = last7["order_id"].nunique()        / days7
        proj2_revenue = revenue_so_far + (daily_trend_revenue * days_remaining)
        proj2_units   = units_so_far   + (daily_trend_units   * days_remaining)
        proj2_orders  = orders_so_far  + (daily_trend_orders  * days_remaining)

        # ── Factor 3: Mismo mes año anterior (20%) ────────────────
        try:
            prev_month  = today.month - 1 if today.month > 1 else 12
            prev_year   = today.year if today.month > 1 else today.year - 1
            prev_days   = calendar.monthrange(prev_year, prev_month)[1]
            df_prev_paid = df_all[
                (df_all["date"] >= date(prev_year, prev_month, 1)) &
                (df_all["date"] <= date(prev_year, prev_month, prev_days))
            ]
            prev_revenue = float(df_prev_paid["total_amount"].sum()) if not df_prev_paid.empty else None
        except Exception:
            prev_revenue = None

        proj3_revenue = proj1_revenue
        proj3_units   = proj1_units
        proj3_orders  = proj1_orders
        ly_revenue    = None
        df_ly_paid    = None

        # Base: total del mismo mes del año pasado (Dropbox).
        try:
            df_ly = self._load_historical_month(today.year - 1, today.month)
            if df_ly is not None and not df_ly.empty:
                df_ly_paid = df_ly[df_ly["status"] == "paid"].copy()
                if "date" not in df_ly_paid.columns:
                    df_ly_paid["date"] = pd.to_datetime(df_ly_paid["date_created"]).dt.date
                ly_revenue = float(df_ly_paid["total_amount"].sum())
        except Exception:
            pass

        # ANCLA ESTABLE (distinta de F9): crece ese total por el YoY de los
        # últimos 3 meses COMPLETOS — 2026 desde la API (ventana 120d) y 2025
        # desde Dropbox. Ese ritmo trimestral no se sacude con el arranque del
        # mes en curso, así que ancla firme al año pasado. F9, en cambio, usa
        # el tramo en curso y es la señal reactiva. Fallbacks: si falta
        # histórico completo, cae al YoY del tramo (método de F9) y por último
        # al promedio plano. Topeado 0.5x–3x.
        try:
            if ly_revenue and ly_revenue > 0 and df_ly_paid is not None:
                # Ingreso de un mes de ESTE año: primero de la API (si el mes
                # entró en la ventana), si no de Dropbox.
                def _rev_curr(y, m):
                    dim = calendar.monthrange(y, m)[1]
                    r = float(df_all[
                        (df_all["date"] >= date(y, m, 1)) &
                        (df_all["date"] <= date(y, m, dim))
                    ]["total_amount"].sum())
                    if r > 0:
                        return r
                    dfh = self._load_historical_month(y, m)
                    if dfh is not None and not dfh.empty:
                        return float(dfh[dfh["status"] == "paid"]["total_amount"].sum())
                    return 0.0

                # Ingreso de un mes histórico (año pasado): Dropbox.
                def _rev_hist(y, m):
                    dfh = self._load_historical_month(y, m)
                    if dfh is not None and not dfh.empty:
                        return float(dfh[dfh["status"] == "paid"]["total_amount"].sum())
                    return 0.0

                # Junta meses COMPLETOS con dato en ambos años caminando hacia
                # atrás, y BLINDA contra meses base anómalos: descarta un par si
                # el mes del año pasado es demasiado chico respecto a los otros
                # (p.ej. tu primer mes de ventas, abr-2025 con $259k), porque
                # dispararía el YoY. Con los sobrevivientes usa los 3 más
                # recientes. Robusto también al hueco de meses no snapshoteados.
                cand = []  # (cur, prev) del más reciente al más viejo
                mm, yy = today.month, today.year
                for _ in range(6):
                    mm -= 1
                    if mm == 0:
                        mm, yy = 12, yy - 1
                    cur    = _rev_curr(yy, mm)
                    prevyr = _rev_hist(yy - 1, mm)
                    if cur > 0 and prevyr > 0:
                        cand.append((cur, prevyr))

                growth = None
                if cand:
                    prevs = sorted(p for _, p in cand)
                    med   = prevs[len(prevs) // 2]                 # mediana de las bases
                    filtrados = [(c, p) for (c, p) in cand if p >= 0.30 * med]
                    usar = (filtrados or cand)[:3]                 # 3 más recientes válidos
                    num_cur  = sum(c for c, _ in usar)
                    den_prev = sum(p for _, p in usar)
                    if den_prev > 0:
                        growth = num_cur / den_prev                # YoY estable multi-mes

                if growth is None:                                 # fallback: YoY del tramo (= F9)
                    ly_dim    = calendar.monthrange(today.year - 1, today.month)[1]
                    ly_cutoff = date(today.year - 1, today.month, min(days_elapsed, ly_dim))
                    ly_period = float(df_ly_paid[df_ly_paid["date"] <= ly_cutoff]["total_amount"].sum())
                    growth    = (revenue_so_far / ly_period) if ly_period > 0 else 1.0

                growth = max(0.5, min(3.0, growth))
                proj3_revenue = ly_revenue * growth
                proj3_units   = float(df_ly_paid["quantity"].sum()) * growth
                proj3_orders  = df_ly_paid["order_id"].nunique()    * growth
        except Exception:
            pass

        # ── Factor 4: Estacionalidad histórica (5%) ───────────────
        # Para cada año disponible en Dropbox crece ESE mes completo por la
        # tasa YoY de su mismo tramo (días 1..N) contra este año, y promedia.
        # FIX: la versión anterior dividía por el promedio del mes entero y
        # la base estacional se cancelaba (colapsaba al Factor 1). Con 1 año
        # coincide con F3; con 2+ suaviza como estimador multi-anual.
        proj4_revenue = proj1_revenue
        proj4_units   = proj1_units
        proj4_orders  = proj1_orders
        seasonal_revenues = []
        seasonal_projs    = []
        for yr in range(today.year - 3, today.year):
            try:
                df_hist = self._load_historical_month(yr, today.month)
                if df_hist is None or df_hist.empty:
                    continue
                df_hist = df_hist[df_hist["status"] == "paid"].copy()
                if "date" not in df_hist.columns:
                    df_hist["date"] = pd.to_datetime(df_hist["date_created"]).dt.date
                hist_full = float(df_hist["total_amount"].sum())
                if hist_full <= 0:
                    continue
                seasonal_revenues.append(hist_full)

                h_dim    = calendar.monthrange(yr, today.month)[1]
                h_cutoff = date(yr, today.month, min(days_elapsed, h_dim))
                hist_period = float(df_hist[df_hist["date"] <= h_cutoff]["total_amount"].sum())
                if hist_period > 0:
                    rate = max(0.5, min(3.0, revenue_so_far / hist_period))
                    seasonal_projs.append(hist_full * rate)
            except Exception:
                pass

        if seasonal_projs:
            proj4_revenue = sum(seasonal_projs) / len(seasonal_projs)
            proj4_units   = proj1_units  * (proj4_revenue / proj1_revenue) if proj1_revenue > 0 else proj1_units
            proj4_orders  = proj1_orders * (proj4_revenue / proj1_revenue) if proj1_revenue > 0 else proj1_orders

        # ── Factor 5: Velocidad de crecimiento reciente (10%) ─────
        # Compara promedio últimos 3 días vs días 4-10
        last3  = df_paid[df_paid["date"] >= (today - timedelta(days=2))]
        prev7  = df_paid[(df_paid["date"] >= (today - timedelta(days=9))) &
                         (df_paid["date"] <  (today - timedelta(days=2)))]

        days3  = max(len(last3["date"].unique()), 1)
        days_p = max(len(prev7["date"].unique()), 1)

        avg3   = float(last3["total_amount"].sum()) / days3 if not last3.empty else daily_avg_revenue
        avg_p  = float(prev7["total_amount"].sum()) / days_p if not prev7.empty else daily_avg_revenue

        # Factor de aceleración (limitado entre 0.5x y 2x para evitar extremos)
        acceleration = max(0.5, min(2.0, avg3 / avg_p if avg_p > 0 else 1.0))
        proj5_revenue = revenue_so_far + (daily_avg_revenue * acceleration * days_remaining)
        proj5_units   = units_so_far   + (daily_avg_units   * acceleration * days_remaining)
        proj5_orders  = orders_so_far  + (daily_avg_orders  * acceleration * days_remaining)

        # ── Factor 6: Forma intra-mes (peso por día de la semana) ─
        # En lugar de proyectar los días restantes con un promedio plano
        # (lo que hacen los factores 1, 2 y 5), pondera cada día que falta
        # según cuánto rinde ese día de la semana en el histórico reciente
        # (~12 semanas). Corrige el sesgo de que a los días restantes les
        # toquen más (o menos) fines de semana / días fuertes que a los
        # días ya transcurridos del mes.
        wd_mult = {wd: 1.0 for wd in range(7)}  # 0=lunes .. 6=domingo
        df_recent = df_all   # mismo pull, ya filtrado a paid y con columna date
        try:
            daily = df_recent.groupby("date").agg(
                rev=("total_amount", "sum"),
                un=("quantity", "sum"),
                ords=("order_id", "nunique"),
            ).reset_index()
            daily["wd"] = daily["date"].apply(lambda d: d.weekday())

            overall_daily_rev = float(daily["rev"].mean()) if not daily.empty else daily_avg_revenue
            if overall_daily_rev > 0:
                for wd in range(7):
                    sub = daily[daily["wd"] == wd]
                    if not sub.empty:
                        wd_mult[wd] = float(sub["rev"].mean()) / overall_daily_rev
        except Exception:
            pass

        # Suma de pesos de los días que faltan del mes.
        # Si el patrón fuera plano (todos los multiplicadores ≈ 1), esta
        # suma ≈ days_remaining y el factor coincide con el factor 1.
        weight_remaining = 0.0
        for day_num in range(days_elapsed + 1, days_in_month + 1):
            wd = date(today.year, today.month, day_num).weekday()
            weight_remaining += wd_mult.get(wd, 1.0)

        proj6_revenue = revenue_so_far + (daily_avg_revenue * weight_remaining)
        proj6_units   = units_so_far   + (daily_avg_units   * weight_remaining)
        proj6_orders  = orders_so_far  + (daily_avg_orders  * weight_remaining)

        # ── Factor 7: Día de la semana del mes actual (10%) ─────
        # Igual al Factor 6 pero el patrón semanal se calcula SOLO con
        # datos del mes actual (no los 120 días). Elimina el sesgo de
        # meses anteriores estacionalmente diferentes.
        proj7_revenue = proj1_revenue
        proj7_units   = proj1_units
        proj7_orders  = proj1_orders
        try:
            wd_mult_current = {wd: 1.0 for wd in range(7)}
            if not df_paid.empty:
                daily_cur = df_paid.groupby("date").agg(
                    rev=("total_amount", "sum"),
                    un=("quantity", "sum"),
                    ords=("order_id", "nunique"),
                ).reset_index()
                daily_cur["wd"] = daily_cur["date"].apply(lambda d: d.weekday())
                od7 = float(daily_cur["rev"].mean()) if not daily_cur.empty else daily_avg_revenue
                if od7 > 0:
                    for wd in range(7):
                        sub7 = daily_cur[daily_cur["wd"] == wd]
                        if not sub7.empty:
                            wd_mult_current[wd] = float(sub7["rev"].mean()) / od7

            weight_rem_7 = 0.0
            for day_num in range(days_elapsed + 1, days_in_month + 1):
                wd = date(today.year, today.month, day_num).weekday()
                weight_rem_7 += wd_mult_current.get(wd, 1.0)

            proj7_revenue = revenue_so_far + (daily_avg_revenue * weight_rem_7)
            proj7_units   = units_so_far   + (daily_avg_units   * weight_rem_7)
            proj7_orders  = orders_so_far  + (daily_avg_orders  * weight_rem_7)
        except Exception:
            proj7_revenue = proj6_revenue
            proj7_units   = proj6_units
            proj7_orders  = proj6_orders

        # ── Factor 8: Mismos días del mes anterior (15%) ──────────
        # Compara del 1 al N del mes actual vs del 1 al N del mes anterior.
        # Elimina el efecto fin-de-mes: no usa los últimos 7 días corridos
        # (que cruzan meses) sino los mismos días calendarios del mes previo.
        proj8_revenue = proj1_revenue
        proj8_units   = proj1_units
        proj8_orders  = proj1_orders
        try:
            prev_month_8 = today.month - 1 if today.month > 1 else 12
            prev_year_8  = today.year if today.month > 1 else today.year - 1
            prev_days_8  = calendar.monthrange(prev_year_8, prev_month_8)[1]

            # Buscar mes anterior en df_all primero, si no en Dropbox
            df_same_days_prev = df_all[
                (df_all["date"] >= date(prev_year_8, prev_month_8, 1)) &
                (df_all["date"] <= date(prev_year_8, prev_month_8, min(days_elapsed, prev_days_8)))
            ]
            df_prev_full = df_all[
                (df_all["date"] >= date(prev_year_8, prev_month_8, 1)) &
                (df_all["date"] <= date(prev_year_8, prev_month_8, prev_days_8))
            ]

            # Si no está en df_all, cargar desde Dropbox historial
            if df_same_days_prev.empty or df_prev_full.empty:
                df_hist_prev = self._load_historical_month(prev_year_8, prev_month_8)
                if df_hist_prev is not None and not df_hist_prev.empty:
                    df_hist_prev = df_hist_prev[df_hist_prev["status"] == "paid"].copy()
                    if "date" not in df_hist_prev.columns:
                        df_hist_prev["date"] = pd.to_datetime(df_hist_prev["date_created"]).dt.date
                    df_same_days_prev = df_hist_prev[
                        df_hist_prev["date"] <= date(prev_year_8, prev_month_8, min(days_elapsed, prev_days_8))
                    ]
                    df_prev_full = df_hist_prev

            if not df_same_days_prev.empty and not df_prev_full.empty:
                rev_same_prev = float(df_same_days_prev["total_amount"].sum())
                rev_full_prev = float(df_prev_full["total_amount"].sum())
                if rev_same_prev > 0 and rev_full_prev > 0:
                    growth_8 = revenue_so_far / rev_same_prev
                    proj8_revenue = rev_full_prev * growth_8
                    proj8_units   = float(df_prev_full["quantity"].sum()) * growth_8
                    proj8_orders  = df_prev_full["order_id"].nunique() * growth_8
        except Exception:
            pass

        # ── Factor 9: Tendencia interanual YoY - mismos días (5%) ──
        # Calcula el crecimiento YoY usando los MISMOS días del período
        # actual vs el año anterior (no meses completos).
        # Ej: si hoy es día 6 de sep, compara los 6 primeros días de
        # sep 2026 vs los 6 primeros días de sep 2025.
        # Esto captura el crecimiento real del período actual, no el
        # arrastre de meses anteriores que pueden haber crecido más.
        proj9_revenue = proj1_revenue
        proj9_units   = proj1_units
        proj9_orders  = proj1_orders
        try:
            # Días del mes actual transcurridos: del 1 al days_elapsed
            # Mismo período año anterior desde Dropbox
            df_ly_same = self._load_historical_month(today.year - 1, today.month)
            if df_ly_same is not None and not df_ly_same.empty:
                df_ly_same = df_ly_same[df_ly_same["status"] == "paid"].copy()
                if "date" not in df_ly_same.columns:
                    df_ly_same["date"] = pd.to_datetime(df_ly_same["date_created"]).dt.date
                # Solo los mismos días transcurridos del año anterior
                df_ly_period = df_ly_same[
                    df_ly_same["date"] <= date(today.year - 1, today.month, min(days_elapsed, calendar.monthrange(today.year - 1, today.month)[1]))
                ]
                rev_ly_period = float(df_ly_period["total_amount"].sum()) if not df_ly_period.empty else 0
                rev_ly_full   = float(df_ly_same["total_amount"].sum()) if not df_ly_same.empty else 0

                if rev_ly_period > 0 and rev_ly_full > 0 and revenue_so_far > 0:
                    # Tasa de crecimiento YoY en los mismos días, TOPEADA 0.5x–3x.
                    # Sin tope, un archivo del año pasado incompleto (base chica)
                    # dispara la proyección a valores absurdos (p.ej. $47M).
                    yoy_rate_real = max(0.5, min(3.0, revenue_so_far / rev_ly_period))
                    # Proyectar el mes completo: total año anterior * tasa real
                    proj9_revenue = rev_ly_full * yoy_rate_real
                    proj9_units   = proj1_units * (proj9_revenue / proj1_revenue) if proj1_revenue > 0 else proj1_units
                    proj9_orders  = proj1_orders * (proj9_revenue / proj1_revenue) if proj1_revenue > 0 else proj1_orders
        except Exception:
            pass

        # ── Proyección ponderada (9 factores) ─────────────────────
        # Pesos actualizados con los 3 factores nuevos.
        # P2 baja de 25% a 15% (P8 cubre mejor el inicio de mes).
        # P3 baja de 20% a 15% (P9 más preciso cuando hay historial YoY).
        # P7 = P6 en implementación pero con otro ángulo conceptual.
        # Pesos: por defecto los base. Si viene weights_by_bucket, elige por
        # fase del mes (temprano ≤10, medio ≤20, tarde >20) — esto habilita
        # D (pesos que varían con el día) y C (optimizados por el backtest).
        _default_w = [0.05, 0.10, 0.20, 0.05, 0.05, 0.10, 0.10, 0.20, 0.15]
        wv = _default_w
        if weights_by_bucket:
            bucket = "early" if days_elapsed <= 10 else ("mid" if days_elapsed <= 20 else "late")
            cand = weights_by_bucket.get(bucket) or weights_by_bucket.get("all")
            if cand and len(cand) == 9 and abs(sum(cand) - 1.0) < 0.05:
                wv = list(cand)
        w1, w2, w3, w4, w5, w6, w7, w8, w9 = wv
        forecast_revenue = (proj1_revenue * w1) + (proj2_revenue * w2) + (proj3_revenue * w3) + (proj4_revenue * w4) + (proj5_revenue * w5) + (proj6_revenue * w6) + (proj7_revenue * w7) + (proj8_revenue * w8) + (proj9_revenue * w9)
        forecast_units   = (proj1_units   * w1) + (proj2_units   * w2) + (proj3_units   * w3) + (proj4_units   * w4) + (proj5_units   * w5) + (proj6_units   * w6) + (proj7_units   * w7) + (proj8_units   * w8) + (proj9_units   * w9)
        forecast_orders  = (proj1_orders  * w1) + (proj2_orders  * w2) + (proj3_orders  * w3) + (proj4_orders  * w4) + (proj5_orders  * w5) + (proj6_orders  * w6) + (proj7_orders  * w7) + (proj8_orders  * w8) + (proj9_orders  * w9)

        ensemble_revenue = forecast_revenue   # versión pre-F10
        ensemble_units   = forecast_units
        ensemble_orders  = forecast_orders

        # ── Factor 10: Regresión a la media ───────────────────────────
        # A principio de mes el ensemble extrapola pocos días y un arranque
        # atípico (caliente o frío) lo desvía. F10 lo corrige tirándolo hacia
        # la expectativa histórica CON crecimiento: el promedio de los factores
        # "backward" (F3 ancla estable, F8 mes anterior, F9 YoY), que ya traen
        # el crecimiento adentro. OJO: NO hacia un promedio plano de meses —
        # eso ya se probó (blend viejo) y empeoraba, porque el negocio crece y
        # el promedio histórico queda corto. El peso de corrección λ es mayor a
        # principio de mes (cuando el arranque es menos confiable) y se
        # desvanece hacia fin de mes, cuando el acumulado real ya manda:
        #   λ = (1 - transcurrido/mes) * FUERZA,  topeado en F10_MAX.
        F10_FUERZA = 0.50   # intensidad de la reversión (0 = apagado, subí para moderar más)
        F10_MAX    = 0.50   # tope de λ
        rev_target = (proj3_revenue + proj8_revenue + proj9_revenue) / 3.0
        un_target  = (proj3_units   + proj8_units   + proj9_units)   / 3.0
        or_target  = (proj3_orders  + proj8_orders  + proj9_orders)  / 3.0
        lam = max(0.0, min(F10_MAX, (1.0 - days_elapsed / days_in_month) * F10_FUERZA))
        forecast_revenue = ensemble_revenue * (1 - lam) + rev_target * lam
        forecast_units   = ensemble_units   * (1 - lam) + un_target  * lam
        forecast_orders  = ensemble_orders  * (1 - lam) + or_target  * lam

        # ── Comparaciones ─────────────────────────────────────────
        vs_prev_pct = None
        if prev_revenue and prev_revenue > 0:
            vs_prev_pct = round((forecast_revenue - prev_revenue) / prev_revenue * 100, 1)

        vs_ly_pct = None
        if ly_revenue and ly_revenue > 0:
            vs_ly_pct = round((forecast_revenue - ly_revenue) / ly_revenue * 100, 1)

        return {
            "month":               today.strftime("%B %Y"),
            "days_elapsed":        days_elapsed,
            "days_remaining":      days_remaining,
            "days_in_month":       days_in_month,
            "revenue_so_far":      round(revenue_so_far, 2),
            "units_so_far":        units_so_far,
            "orders_so_far":       orders_so_far,
            "net_so_far":          round(net_so_far, 2),
            "forecast_revenue":    round(forecast_revenue, 2),
            "forecast_units":      round(forecast_units),
            "forecast_orders":     round(forecast_orders),
            "forecast_net":        round(forecast_revenue * (net_so_far / revenue_so_far) if revenue_so_far > 0 else 0, 2),
            "vs_prev_month_pct":   vs_prev_pct,
            "vs_last_year_pct":    vs_ly_pct,
            "prev_month_revenue":  round(prev_revenue, 2) if prev_revenue else None,
            "last_year_revenue":   round(ly_revenue, 2) if ly_revenue else None,
            "proj_daily_avg":      round(proj1_revenue, 2),
            "proj_trend_7d":       round(proj2_revenue, 2),
            "proj_last_year":      round(proj3_revenue, 2),
            "proj_seasonal":       round(proj4_revenue, 2),
            "proj_acceleration":   round(proj5_revenue, 2),
            "proj_calendar":       round(proj6_revenue, 2),
            "proj_weekday":        round(proj7_revenue, 2),
            "proj_same_days_prev": round(proj8_revenue, 2),
            "proj_yoy_trend":      round(proj9_revenue, 2),
            "acceleration_factor": round(acceleration, 2),
            "weekday_weights":     {int(k): round(v, 2) for k, v in wd_mult.items()},
            "calendar_shape_pct":  round((weight_remaining / days_remaining - 1) * 100, 1) if days_remaining > 0 else 0.0,
            "seasonal_years":      len(seasonal_revenues),
            "ensemble_revenue":    round(ensemble_revenue, 2),
            "reversion_target":    round(rev_target, 2),
            "reversion_lambda":    round(lam, 3),
            "daily_avg_revenue":   round(daily_avg_revenue, 2),
            "daily_trend_revenue": round(daily_trend_revenue, 2),
        }

    def backtest_forecast(self, months_back: int = 12, cutoffs=(5, 10, 15, 20, 25)) -> dict:
        """Backtest REAL. Re-corre get_monthly_forecast() —la MISMA función de
        producción, con los 10 factores y sus pesos— como si fuera el día X de
        meses ya cerrados, alimentándola con el historial de Dropbox, y compara
        el pronóstico contra el total real del mes.

        A diferencia de la versión vieja (que reimplementaba 4 factores con
        pesos viejos y se desincronizaba), esto mide exactamente lo que corre
        en producción. Devuelve MAPE global y por día de corte, y las muestras
        (proyección de cada factor + real) para poder optimizar los pesos sin
        re-correr el pronóstico.
        """
        import numpy as np
        today = _today_uy()

        # Meses cerrados a testear (excluye el mes en curso)
        months = []
        y, m = today.year, today.month
        for _ in range(months_back):
            m -= 1
            if m == 0:
                m, y = 12, y - 1
            months.append((y, m))

        samples = []
        errs_by_cut = {c: [] for c in cutoffs}

        for (my, mm) in months:
            dfm = self._load_historical_month(my, mm)
            if dfm is None or dfm.empty:
                continue
            dfm_paid = dfm[dfm["status"] == "paid"].copy()
            if "date" not in dfm_paid.columns:
                dfm_paid["date"] = pd.to_datetime(dfm_paid["date_created"]).dt.date
            actual = float(dfm_paid["total_amount"].sum())
            if actual <= 0:
                continue
            dim = calendar.monthrange(my, mm)[1]

            # Meses previos completos (para F2/F6/F8 y el ancla de F3)
            prev_frames = []
            pm, py = mm, my
            for _ in range(4):
                pm -= 1
                if pm == 0:
                    pm, py = 12, py - 1
                dfp = self._load_historical_month(py, pm)
                if dfp is not None and not dfp.empty:
                    prev_frames.append(dfp)

            for c in cutoffs:
                if c >= dim:
                    continue
                cutoff_date = date(my, mm, c)
                dfm_cut = dfm_paid[dfm_paid["date"] <= cutoff_date]
                if dfm_cut.empty:
                    continue
                frames = [dfm_cut] + prev_frames
                data_override = pd.concat(frames, ignore_index=True)
                fc = self.get_monthly_forecast(as_of=cutoff_date, data_override=data_override)
                if not fc or "error" in fc:
                    continue
                pred = fc.get("forecast_revenue")
                if not pred or pred <= 0:
                    continue
                errs_by_cut[c].append(abs(pred - actual) / actual)
                samples.append({
                    "day": c, "dim": dim, "actual": actual,
                    "p": [fc["proj_daily_avg"], fc["proj_trend_7d"], fc["proj_last_year"],
                          fc["proj_seasonal"], fc["proj_acceleration"], fc["proj_calendar"],
                          fc["proj_weekday"], fc["proj_same_days_prev"], fc["proj_yoy_trend"]],
                })

        mape_by_cutoff = {c: round(float(np.mean(v)) * 100, 1) for c, v in errs_by_cut.items() if v}
        all_errs = [e for v in errs_by_cut.values() for e in v]
        mape_global = round(float(np.mean(all_errs)) * 100, 1) if all_errs else None

        return {
            "months_tested": len([1 for (my, mm) in months
                                  if self._load_historical_month(my, mm) is not None]),
            "samples":       len(samples),
            "cutoffs":       list(cutoffs),
            "mape_global":   mape_global,
            "mape_by_cutoff": mape_by_cutoff,
            "_samples":      samples,
        }

    @staticmethod
    def _mape_of_weights(w, subset):
        """MAPE de un vector de pesos sobre un subconjunto de muestras,
        replicando el ensemble + Factor 10 tal como producción."""
        import numpy as np
        if not subset:
            return None
        errs = []
        for s in subset:
            p   = s["p"]
            ens = float(np.dot(w, p))
            tgt = (p[2] + p[7] + p[8]) / 3.0                       # F10 target = prom(F3,F8,F9)
            lam = max(0.0, min(0.5, (1.0 - s["day"] / s["dim"]) * 0.5))
            pred = ens * (1 - lam) + tgt * lam
            errs.append(abs(pred - s["actual"]) / s["actual"])
        return float(np.mean(errs))

    def optimize_weights(self, samples) -> dict:
        """Busca los pesos (9 factores, símplex: ≥0 y suman 1) que minimizan el
        MAPE del backtest. Coordinate descent: mueve peso entre factores en
        pasos decrecientes. Optimiza por fase del mes (temprano/medio/tarde) —
        eso es D — y también un vector global. NO cambia producción; solo
        sugiere. Los pesos se aplican aparte, guardándolos en Dropbox."""
        import numpy as np
        if not samples:
            return {}
        base = np.array([0.05, 0.10, 0.20, 0.05, 0.05, 0.10, 0.10, 0.20, 0.15])

        def optimize(subset):
            if len(subset) < 3:                                    # muy pocas muestras: no optimizar
                return None, None
            w    = base.copy()
            best = self._mape_of_weights(w, subset)
            step = 0.05
            for _ in range(300):
                improved = False
                for i in range(9):
                    for j in range(9):
                        if i == j or w[i] - step < 0:
                            continue
                        cand = w.copy(); cand[i] -= step; cand[j] += step
                        mp = self._mape_of_weights(cand, subset)
                        if mp is not None and mp < best - 1e-9:
                            w, best, improved = cand, mp, True
                if not improved:
                    step /= 2.0
                    if step < 0.01:
                        break
            return w, best

        out = {"weights_by_bucket": {}, "mape_by_bucket": {}}
        groups = {
            "early": [s for s in samples if s["day"] <= 10],
            "mid":   [s for s in samples if 10 < s["day"] <= 20],
            "late":  [s for s in samples if s["day"] > 20],
        }
        for name, sub in groups.items():
            w, mp = optimize(sub)
            if w is not None:
                out["weights_by_bucket"][name] = [round(float(x), 3) for x in w]
                out["mape_by_bucket"][name] = {
                    "base": round(self._mape_of_weights(base, sub) * 100, 1),
                    "opt":  round(mp * 100, 1),
                    "n":    len(sub),
                }
        wg, mpg = optimize(samples)
        out["weights_global"]    = [round(float(x), 3) for x in wg] if wg is not None else None
        out["mape_global_base"]  = round(self._mape_of_weights(base, samples) * 100, 1)
        out["mape_global_opt"]   = round(mpg * 100, 1) if mpg else None
        return out
