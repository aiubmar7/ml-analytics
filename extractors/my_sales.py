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

        # ── Proyección ponderada (5 factores) ─────────────────────
        w1, w2, w3, w4, w5 = 0.25, 0.30, 0.20, 0.15, 0.10
        forecast_revenue = (proj1_revenue * w1) + (proj2_revenue * w2) + (proj3_revenue * w3) + (proj4_revenue * w4) + (proj5_revenue * w5)
        forecast_units   = (proj1_units   * w1) + (proj2_units   * w2) + (proj3_units   * w3) + (proj4_units   * w4) + (proj5_units   * w5)
        forecast_orders  = (proj1_orders  * w1) + (proj2_orders  * w2) + (proj3_orders  * w3) + (proj4_orders  * w4) + (proj5_orders  * w5)

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
            "acceleration_factor": round(acceleration, 2),
            "seasonal_years":      len(seasonal_revenues),
            "daily_avg_revenue":   round(daily_avg_revenue, 2),
            "daily_trend_revenue": round(daily_trend_revenue, 2),
        }
