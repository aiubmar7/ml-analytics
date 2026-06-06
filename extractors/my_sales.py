"""
Extractor de ventas y métricas propias.
"""

import logging
from datetime import datetime, timedelta, date
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

    def _get_user_id(self) -> str:
        if not self.user_id:
            me = self.client.get_my_user()
            self.user_id = str(me["id"])
        return self.user_id

    def get_orders(self, days_back: int = DEFAULT_DAYS_BACK) -> pd.DataFrame:
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
        """Carga un mes desde Dropbox historial."""
        try:
            path = f"data/historical/{year:04d}-{month:02d}.parquet"
            df = self.storage.load_dataframe(path)
            if df is not None and not df.empty:
                df["date_created"] = pd.to_datetime(df["date_created"])
                df["date"] = df["date_created"].dt.date
                if "net_amount" not in df.columns:
                    df["net_amount"] = df["total_amount"] - df.get("sale_fee", 0)
            return df
        except Exception:
            return None

    def get_monthly_forecast(self) -> dict:
        today          = date.today()
        days_in_month  = calendar.monthrange(today.year, today.month)[1]
        days_elapsed   = today.day
        days_remaining = days_in_month - days_elapsed

        df_month = self.get_orders(days_elapsed)
        if df_month.empty:
            return {"error": "Sin datos suficientes para proyectar"}

        df_paid = df_month[df_month["status"] == "paid"].copy()
        df_paid["date"] = df_paid["date_created"].dt.date
        df_paid = df_paid[df_paid["date"] >= date(today.year, today.month, 1)]

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
            df_prev     = self.get_orders(days_elapsed + prev_days + 5)
            df_prev_paid = df_prev[df_prev["status"] == "paid"].copy()
            df_prev_paid["date"] = df_prev_paid["date_created"].dt.date
            df_prev_paid = df_prev_paid[
                (df_prev_paid["date"] >= date(prev_year, prev_month, 1)) &
                (df_prev_paid["date"] <= date(prev_year, prev_month, prev_days))
            ]
            prev_revenue = float(df_prev_paid["total_amount"].sum()) if not df_prev_paid.empty else None
        except Exception:
            prev_revenue = None

        proj3_revenue = proj1_revenue
        proj3_units   = proj1_units
        proj3_orders  = proj1_orders
        ly_revenue    = None

        # Intentar con año anterior desde historial Dropbox
        try:
            df_ly = self._load_historical_month(today.year - 1, today.month)
            if df_ly is not None and not df_ly.empty:
                df_ly_paid = df_ly[df_ly["status"] == "paid"]
                ly_revenue = float(df_ly_paid["total_amount"].sum())
                if ly_revenue > 0:
                    ly_daily   = ly_revenue / days_in_month
                    growth_factor = daily_avg_revenue / ly_daily if ly_daily > 0 else 1
                    proj3_revenue = ly_revenue * growth_factor
                    proj3_units   = float(df_ly_paid["quantity"].sum()) * growth_factor
                    proj3_orders  = df_ly_paid["order_id"].nunique() * growth_factor
        except Exception:
            pass

        # ── Factor 4: Estacionalidad histórica (15%) ──────────────
        # Promedio de este mismo mes en los últimos años disponibles en Dropbox
        proj4_revenue = proj1_revenue
        seasonal_revenues = []
        for yr in range(today.year - 3, today.year):
            try:
                df_hist = self._load_historical_month(yr, today.month)
                if df_hist is not None and not df_hist.empty:
                    df_hist_paid = df_hist[df_hist["status"] == "paid"]
                    hist_rev = float(df_hist_paid["total_amount"].sum())
                    if hist_rev > 0:
                        seasonal_revenues.append(hist_rev)
            except Exception:
                pass

        if seasonal_revenues:
            avg_seasonal = sum(seasonal_revenues) / len(seasonal_revenues)
            # Ajustar por crecimiento actual vs histórico
            if avg_seasonal > 0:
                growth_rate   = daily_avg_revenue / (avg_seasonal / days_in_month)
                proj4_revenue = avg_seasonal * growth_rate
                proj4_units   = proj1_units * (proj4_revenue / proj1_revenue) if proj1_revenue > 0 else proj1_units
                proj4_orders  = proj1_orders * (proj4_revenue / proj1_revenue) if proj1_revenue > 0 else proj1_orders
        else:
            proj4_units  = proj1_units
            proj4_orders = proj1_orders

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
        try:
            df_hist90 = self.get_orders(90)
            df_h = df_hist90[df_hist90["status"] == "paid"].copy()
            df_h["date"] = df_h["date_created"].dt.date

            daily = df_h.groupby("date").agg(
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

        # ── Proyección ponderada (6 factores) ─────────────────────
        # Se le baja peso al promedio plano (factor 1) y se le da al
        # factor 6 (forma intra-mes), que es la versión "inteligente" de
        # ese mismo cálculo.
        w1, w2, w3, w4, w5, w6 = 0.15, 0.25, 0.20, 0.15, 0.10, 0.15
        forecast_revenue = (proj1_revenue * w1) + (proj2_revenue * w2) + (proj3_revenue * w3) + (proj4_revenue * w4) + (proj5_revenue * w5) + (proj6_revenue * w6)
        forecast_units   = (proj1_units   * w1) + (proj2_units   * w2) + (proj3_units   * w3) + (proj4_units   * w4) + (proj5_units   * w5) + (proj6_units   * w6)
        forecast_orders  = (proj1_orders  * w1) + (proj2_orders  * w2) + (proj3_orders  * w3) + (proj4_orders  * w4) + (proj5_orders  * w5) + (proj6_orders  * w6)

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
            "acceleration_factor": round(acceleration, 2),
            "weekday_weights":     {int(k): round(v, 2) for k, v in wd_mult.items()},
            "calendar_shape_pct":  round((weight_remaining / days_remaining - 1) * 100, 1) if days_remaining > 0 else 0.0,
            "seasonal_years":      len(seasonal_revenues),
            "daily_avg_revenue":   round(daily_avg_revenue, 2),
            "daily_trend_revenue": round(daily_trend_revenue, 2),
        }

    def backtest_forecast(self, months_back: int = 6, cutoffs=(5, 10, 15, 20)) -> dict:
        """
        Backtest del pronóstico: re-corre la proyección como si fuera el
        día X de meses ya cerrados y mide el error (MAPE) contra el total
        real de cada mes.

        Solo backtestea los factores reproducibles con la data de la API:
          - proj1 (promedio diario), proj2 (tendencia 7d),
            proj5 (velocidad), proj6 (forma intra-mes).
        Los factores 3 (año anterior) y 4 (estacionalidad) dependen de
        historial en Dropbox que no existe para meses pasados; en el
        ensemble actual colapsan al factor 1 (igual que en producción),
        por eso el peso efectivo de proj1 es w1+w3+w4 = 0.50.

        Optimizado: baja el peso por día de la semana una sola vez y
        cada mes a testear una sola vez (sin solapamientos), para
        minimizar las llamadas a la API.

        Devuelve: MAPE por factor, MAPE del ensemble con los pesos
        actuales, y los pesos óptimos que minimizan el MAPE sobre estos
        datos (búsqueda en grilla sobre el símplex).
        """
        import numpy as np

        today = date.today()

        # Meses completos a testear (excluye el mes actual)
        months = []
        y, m = today.year, today.month
        for _ in range(months_back):
            m -= 1
            if m == 0:
                m, y = 12, y - 1
            months.append((y, m))

        # ── Peso por día de la semana: se calcula UNA sola vez ────
        # (en vez de re-bajarlo por cada mes). El patrón semanal es
        # estable, así que usar una ventana reciente única es buena
        # aproximación y baja muchísimo las llamadas a la API.
        wd_mult_global = {wd: 1.0 for wd in range(7)}
        try:
            df_wd = self.get_orders(120)
            df_wd = df_wd[df_wd["status"] == "paid"].copy()
            df_wd["date"] = df_wd["date_created"].dt.date
            daily_wd = df_wd.groupby("date")["total_amount"].sum().reset_index()
            daily_wd["wd"] = daily_wd["date"].apply(lambda d: d.weekday())
            od = float(daily_wd["total_amount"].mean()) if not daily_wd.empty else 0.0
            if od > 0:
                for wd in range(7):
                    sub = daily_wd[daily_wd["wd"] == wd]
                    if not sub.empty:
                        wd_mult_global[wd] = float(sub["total_amount"].mean()) / od
        except Exception:
            pass

        samples = []        # cada uno: [p1, p2, p5, p6, actual]
        sample_cuts = []    # el día de corte K de cada muestra (paralelo a samples)
        tested_months = set()

        for (yy, mm) in months:
            dim     = calendar.monthrange(yy, mm)[1]
            m_start = date(yy, mm, 1)
            m_end   = date(yy, mm, dim)

            # Solo el mes a testear (sin solapamiento entre meses)
            try:
                df_m = self.get_orders_by_daterange(m_start, m_end)
            except Exception:
                continue
            if df_m.empty or "date" not in df_m.columns:
                continue

            df_m   = df_m[df_m["status"] == "paid"].copy()
            actual = float(df_m["total_amount"].sum())
            if actual <= 0:
                continue

            for K in cutoffs:
                if K >= dim:
                    continue
                cutoff = date(yy, mm, K)
                df_el  = df_m[df_m["date"] <= cutoff]
                if df_el.empty:
                    continue
                rev_sf = float(df_el["total_amount"].sum())
                if rev_sf <= 0:
                    continue

                days_rem  = dim - K
                daily_avg = rev_sf / K

                # Factor 1: promedio diario plano
                p1 = rev_sf + daily_avg * days_rem

                # Factor 2: tendencia 7 días previos al corte
                last7 = df_el[df_el["date"] >= (cutoff - timedelta(days=6))]
                d7    = max(len(last7["date"].unique()), 1)
                trend = float(last7["total_amount"].sum()) / d7
                p2    = rev_sf + trend * days_rem

                # Factor 5: aceleración (últ. 3d vs días 4-10 dentro del mes)
                l3 = df_el[df_el["date"] >= (cutoff - timedelta(days=2))]
                pp = df_el[(df_el["date"] >= (cutoff - timedelta(days=9))) &
                           (df_el["date"] <  (cutoff - timedelta(days=2)))]
                d3 = max(len(l3["date"].unique()), 1)
                dp = max(len(pp["date"].unique()), 1)
                a3 = float(l3["total_amount"].sum()) / d3 if not l3.empty else daily_avg
                ap = float(pp["total_amount"].sum()) / dp if not pp.empty else daily_avg
                accel = max(0.5, min(2.0, a3 / ap if ap > 0 else 1.0))
                p5 = rev_sf + daily_avg * accel * days_rem

                # Factor 6: forma intra-mes (usa el peso por día calculado una vez)
                wr = 0.0
                for dn in range(K + 1, dim + 1):
                    wr += wd_mult_global.get(date(yy, mm, dn).weekday(), 1.0)
                p6 = rev_sf + daily_avg * wr

                samples.append([p1, p2, p5, p6, actual])
                sample_cuts.append(K)
                tested_months.add((yy, mm))

        if not samples:
            return {"samples": 0, "months_tested": 0, "cutoffs": list(cutoffs),
                    "error": "No hay meses cerrados con datos suficientes en la ventana de la API"}

        arr    = np.array(samples, dtype=float)   # (n, 5)
        P      = arr[:, :4]                        # p1, p2, p5, p6
        y_true = arr[:, 4]

        def mape(pred):
            return float(np.mean(np.abs(pred - y_true) / y_true) * 100)

        factor_mape = {
            "proj1": mape(P[:, 0]),
            "proj2": mape(P[:, 1]),
            "proj5": mape(P[:, 2]),
            "proj6": mape(P[:, 3]),
        }

        # Ensemble con pesos ACTUALES (3 y 4 colapsan a p1):
        # efectivo -> p1=w1+w3+w4=0.50, p2=0.25, p5=0.10, p6=0.15
        w_cur   = np.array([0.50, 0.25, 0.10, 0.15])
        ens_cur = mape(P @ w_cur)

        # Pesos óptimos: grilla sobre el símplex (paso 0.05) que minimiza MAPE
        step = 0.05
        n    = int(round(1 / step))
        best_w, best_mape = w_cur, ens_cur
        for a in range(n + 1):
            for b in range(n + 1 - a):
                for c in range(n + 1 - a - b):
                    d = n - a - b - c
                    w = np.array([a, b, c, d], dtype=float) * step
                    mp = mape(P @ w)
                    if mp < best_mape:
                        best_mape, best_w = mp, w

        # MAPE del ensemble actual separado por día de corte.
        # Dice a partir de qué día del mes el pronóstico ya es confiable.
        cuts_arr = np.array(sample_cuts)
        ens_pred = P @ w_cur
        mape_by_cutoff = {}
        for K in sorted(set(sample_cuts)):
            mask = cuts_arr == K
            if mask.any():
                err = np.abs(ens_pred[mask] - y_true[mask]) / y_true[mask]
                mape_by_cutoff[int(K)] = round(float(np.mean(err) * 100), 1)

        return {
            "samples":                 len(samples),
            "months_tested":           len(tested_months),
            "cutoffs":                 list(cutoffs),
            "factor_mape":             {k: round(v, 1) for k, v in factor_mape.items()},
            "ensemble_mape_current":   round(ens_cur, 1),
            "ensemble_mape_optimized": round(best_mape, 1),
            "mape_by_cutoff":          mape_by_cutoff,
            "current_weights_eff":     {"proj1": 0.50, "proj2": 0.25, "proj5": 0.10, "proj6": 0.15},
            "optimized_weights":       {
                "proj1": round(float(best_w[0]), 2),
                "proj2": round(float(best_w[1]), 2),
                "proj5": round(float(best_w[2]), 2),
                "proj6": round(float(best_w[3]), 2),
            },
        }
