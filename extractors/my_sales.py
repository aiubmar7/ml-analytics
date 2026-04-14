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
        self.client = MLClient()
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

        proj1_revenue = revenue_so_far + (daily_avg_revenue * days_remaining)
        proj1_units   = units_so_far   + (daily_avg_units   * days_remaining)
        proj1_orders  = orders_so_far  + (daily_avg_orders  * days_remaining)

        last7 = df_paid[df_paid["date"] >= (today - timedelta(days=6))]
        days7 = max(len(last7["date"].unique()), 1)

        daily_trend_revenue = float(last7["total_amount"].sum()) / days7
        daily_trend_units   = float(last7["quantity"].sum())     / days7
        daily_trend_orders  = last7["order_id"].nunique()        / days7

        proj2_revenue = revenue_so_far + (daily_trend_revenue * days_remaining)
        proj2_units   = units_so_far   + (daily_trend_units   * days_remaining)
        proj2_orders  = orders_so_far  + (daily_trend_orders  * days_remaining)

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

        w1, w2, w3 = 0.35, 0.40, 0.25
        forecast_revenue = (proj1_revenue * w1) + (proj2_revenue * w2) + (proj3_revenue * w3)
        forecast_units   = (proj1_units   * w1) + (proj2_units   * w2) + (proj3_units   * w3)
        forecast_orders  = (proj1_orders  * w1) + (proj2_orders  * w2) + (proj3_orders  * w3)

        vs_prev_pct = None
        if prev_revenue and prev_revenue > 0:
            vs_prev_pct = round((forecast_revenue - prev_revenue) / prev_revenue * 100, 1)

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
            "vs_last_year_pct":    None,
            "prev_month_revenue":  round(prev_revenue, 2) if prev_revenue else None,
            "last_year_revenue":   None,
            "proj_daily_avg":      round(proj1_revenue, 2),
            "proj_trend_7d":       round(proj2_revenue, 2),
            "proj_last_year":      round(proj3_revenue, 2),
            "daily_avg_revenue":   round(daily_avg_revenue, 2),
            "daily_trend_revenue": round(daily_trend_revenue, 2),
        }
