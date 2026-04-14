"""
Extractor de datos de competencia (cuentas y publicaciones públicas).
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))
from auth.ml_client import MLClient
from storage.dropbox_client import DropboxClient
from config import ML_SITE_ID

logger = logging.getLogger(__name__)


class CompetitionExtractor:

    def __init__(self):
        self.client  = MLClient()
        self.storage = DropboxClient()

    def search_seller_by_nickname(self, nickname: str) -> Optional[dict]:
        try:
            data = self.client.get("/users/search", params={"nickname": nickname})
            if data:
                user = data[0] if isinstance(data, list) else data
                return user
        except Exception as e:
            logger.error(f"No se encontro el vendedor '{nickname}': {e}")
        return None

    def get_seller_profile(self, seller_id: str) -> dict:
        user = self.client.get_user(seller_id)
        try:
            rep = self.client.get(f"/users/{seller_id}/seller_reputation")
        except Exception:
            rep = {}
        return {
            "seller_id":              seller_id,
            "nickname":               user.get("nickname"),
            "registration_date":      user.get("registration_date"),
            "country_id":             user.get("country_id"),
            "level_id":               rep.get("level_id", "Sin datos"),
            "power_seller_status":    rep.get("power_seller_status", "Sin datos"),
            "transactions_total":     rep.get("transactions", {}).get("total", 0),
            "transactions_completed": rep.get("transactions", {}).get("completed", 0),
            "claims_rate":            rep.get("metrics", {}).get("claims", {}).get("rate", 0),
            "cancellations_rate":     rep.get("metrics", {}).get("cancellations", {}).get("rate", 0),
            "snapshot_date":          datetime.now().isoformat(),
        }

    def get_seller_items(self, seller_id: str, max_items: int = 200) -> pd.DataFrame:
        logger.info(f"Extrayendo publicaciones del vendedor {seller_id}...")
        rows   = []
        offset = 0
        limit  = 50

        while len(rows) < max_items:
            try:
                data = self.client.get(
                    f"/sites/{ML_SITE_ID}/search",
                    params={"seller_id": seller_id, "limit": limit, "offset": offset}
                )
            except Exception as e:
                logger.warning(f"Error buscando items del vendedor {seller_id}: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            for item in results:
                rows.append({
                    "seller_id":     seller_id,
                    "item_id":       item["id"],
                    "title":         item["title"],
                    "category_id":   item.get("category_id", ""),
                    "price":         item["price"],
                    "currency_id":   item["currency_id"],
                    "available_qty": item.get("available_quantity", 0),
                    "sold_qty":      item.get("sold_quantity", 0),
                    "status":        item.get("status", ""),
                    "listing_type":  item.get("listing_type_id"),
                    "condition":     item.get("condition"),
                    "permalink":     item.get("permalink"),
                    "date_created":  item.get("date_created"),
                    "snapshot_date": datetime.now().isoformat(),
                })

            paging = data.get("paging", {})
            offset += limit
            if offset >= min(paging.get("total", 0), 9950) or len(rows) >= max_items:
                break

        if not rows:
            logger.warning(f"No se encontraron publicaciones para {seller_id}")
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        logger.info(f"OK {len(df)} publicaciones del vendedor {seller_id}")
        return df

    def sync_seller(self, seller_id: str, max_items: int = 200) -> pd.DataFrame:
        df = self.get_seller_items(seller_id, max_items)
        if not df.empty:
            month_str = datetime.now().strftime("%Y-%m")
            path = f"data/competition/{seller_id}_{month_str}.parquet"
            self.storage.append_dataframe(df, path)
            profile = self.get_seller_profile(seller_id)
            self.storage.save_json(profile, f"data/competition/{seller_id}_profile.json")
            self.storage.log_sync("competition", "ok", {"seller_id": seller_id, "rows": len(df)})
        return df

    def compare_prices(self, my_items_df: pd.DataFrame, competitor_ids: list) -> pd.DataFrame:
        if my_items_df.empty:
            return pd.DataFrame()
        my_categories = set(my_items_df["category_id"].unique())
        comp_rows = []
        for seller_id in competitor_ids:
            df_comp = self.get_seller_items(seller_id)
            df_comp = df_comp[df_comp["category_id"].isin(my_categories)]
            comp_rows.append(df_comp)
        if not comp_rows:
            return pd.DataFrame()
        df_all_comp = pd.concat(comp_rows, ignore_index=True)
        comp_avg = (
            df_all_comp.groupby("category_id")["price"]
            .agg(["mean", "min", "max", "count"])
            .rename(columns={"mean": "comp_avg_price", "min": "comp_min_price",
                             "max": "comp_max_price", "count": "comp_item_count"})
            .reset_index()
        )
        my_avg = (
            my_items_df.groupby("category_id")["price"]
            .mean()
            .reset_index()
            .rename(columns={"price": "my_avg_price"})
        )
        comparison = my_avg.merge(comp_avg, on="category_id", how="left")
        comparison["price_diff_pct"] = (
            (comparison["my_avg_price"] - comparison["comp_avg_price"])
            / comparison["comp_avg_price"] * 100
        ).round(2)
        return comparison

    def daily_snapshot(self, seller_ids: list) -> None:
        date_str = datetime.now().strftime("%Y-%m-%d")
        for seller_id in seller_ids:
            df = self.get_seller_items(seller_id)
            if not df.empty:
                self.storage.save_dataframe(
                    df,
                    f"data/competition/snapshots/{seller_id}/{date_str}.parquet"
                )
        logger.info(f"Snapshots del {date_str} guardados.")
