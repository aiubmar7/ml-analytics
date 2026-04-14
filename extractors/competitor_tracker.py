"""
Tracker específico para monitorear un competidor via búsquedas por keyword.
Funciona con apps de nivel básico de ML.

Estrategia:
  - Busca por keywords de las categorías donde compite el vendedor
  - Filtra los resultados del vendedor objetivo
  - Guarda snapshots diarios para detectar cambios de precio, stock y nuevas publicaciones
"""

import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))
from auth.ml_client import MLClient
from storage.dropbox_client import DropboxClient
from config import ML_SITE_ID

logger = logging.getLogger(__name__)

# Keywords por categoría para rastrear a La Tentación
CATEGORY_KEYWORDS = {
    "electrodomesticos": [
        "lavarropas", "heladera", "freezer", "microondas", "lavarropa",
        "aspiradora", "aire acondicionado", "ventilador", "licuadora",
        "batidora", "plancha ropa", "cafetera", "tostadora"
    ],
    "calefaccion": [
        "calefon", "termotanque", "estufa", "calefactor", "radiador",
        "caloventor", "caldera", "estufa electrica", "estufa a gas",
        "panel electrico", "calefon electrico", "termotanque electrico"
    ],
    "cocinas": [
        "cocina gas", "cocina electrica", "horno electrico", "horno empotrable",
        "anafe", "campana extractora", "cocina 4 hornallas", "cocina 5 hornallas",
        "horno a gas", "microondas grill"
    ],
}


class CompetitorTracker:
    """
    Rastrea publicaciones de un competidor específico via búsquedas por keyword.

    Uso:
        tracker = CompetitorTracker(seller_id=175850089, seller_nickname="LATENTACIONSRL")
        df = tracker.scan_all_categories()
        new_items = tracker.detect_new_items()
        price_changes = tracker.detect_price_changes()
    """

    def __init__(self, seller_id: int, seller_nickname: str):
        self.seller_id       = str(seller_id)
        self.seller_nickname = seller_nickname
        self.client          = MLClient()
        self.storage         = DropboxClient()

    def _search_keyword(self, keyword: str, limit: int = 50) -> list[dict]:
        """Busca un keyword y filtra resultados del vendedor objetivo."""
        try:
            data = self.client.get(
                f"/sites/{ML_SITE_ID}/search",
                params={"q": keyword, "limit": limit}
            )
            results = data.get("results", [])
            # Filtrar solo los del vendedor objetivo
            return [
                item for item in results
                if str(item.get("seller", {}).get("id", "")) == self.seller_id
            ]
        except Exception as e:
            logger.warning(f"Error buscando '{keyword}': {e}")
            return []

    def scan_category(self, category: str) -> pd.DataFrame:
        """
        Escanea todos los keywords de una categoría y retorna
        las publicaciones del competidor encontradas.
        """
        keywords = CATEGORY_KEYWORDS.get(category, [])
        if not keywords:
            logger.warning(f"Categoría '{category}' no tiene keywords configurados")
            return pd.DataFrame()

        logger.info(f"Escaneando categoría '{category}' con {len(keywords)} keywords...")

        all_items = {}  # usar dict para deduplicar por item_id

        for keyword in keywords:
            items = self._search_keyword(keyword)
            for item in items:
                item_id = item["id"]
                if item_id not in all_items:
                    all_items[item_id] = {
                        "seller_id":       self.seller_id,
                        "seller_nickname": self.seller_nickname,
                        "item_id":         item_id,
                        "title":           item["title"],
                        "price":           item["price"],
                        "currency_id":     item["currency_id"],
                        "available_qty":   item.get("available_quantity", 0),
                        "sold_qty":        item.get("sold_quantity", 0),
                        "listing_type":    item.get("listing_type_id"),
                        "condition":       item.get("condition"),
                        "permalink":       item.get("permalink"),
                        "category_id":     item.get("category_id"),
                        "found_keyword":   keyword,
                        "category":        category,
                        "snapshot_date":   datetime.now().isoformat(),
                        "snapshot_day":    date.today().isoformat(),
                    }

        df = pd.DataFrame(list(all_items.values()))
        logger.info(f"  → {len(df)} publicaciones encontradas en '{category}'")
        return df

    def scan_all_categories(self) -> pd.DataFrame:
        """Escanea todas las categorías configuradas."""
        logger.info(f"Iniciando scan completo de {self.seller_nickname}...")
        dfs = []
        for category in CATEGORY_KEYWORDS:
            df = self.scan_category(category)
            if not df.empty:
                dfs.append(df)

        if not dfs:
            logger.warning(f"No se encontraron publicaciones de {self.seller_nickname}")
            return pd.DataFrame()

        df_all = pd.concat(dfs, ignore_index=True)
        df_all = df_all.drop_duplicates(subset=["item_id"], keep="first")
        logger.info(f"Total: {len(df_all)} publicaciones únicas de {self.seller_nickname}")
        return df_all

    def save_snapshot(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """Guarda snapshot del día actual en Dropbox."""
        if df is None:
            df = self.scan_all_categories()

        if df.empty:
            return df

        today = date.today().isoformat()
        path  = f"data/competition/tracker/{self.seller_id}/{today}.parquet"
        self.storage.save_dataframe(df, path)

        # También guardar el último snapshot para comparación rápida
        self.storage.save_dataframe(df, f"data/competition/tracker/{self.seller_id}/latest.parquet")
        self.storage.log_sync("competitor_tracker", "ok", {
            "seller": self.seller_nickname,
            "items":  len(df),
            "date":   today,
        })
        logger.info(f"Snapshot guardado: {len(df)} items de {self.seller_nickname}")
        return df

    def load_snapshot(self, day: str = None) -> Optional[pd.DataFrame]:
        """Carga un snapshot. Si day=None carga el último."""
        if day is None:
            path = f"data/competition/tracker/{self.seller_id}/latest.parquet"
        else:
            path = f"data/competition/tracker/{self.seller_id}/{day}.parquet"
        return self.storage.load_dataframe(path)

    def detect_new_items(self) -> pd.DataFrame:
        """
        Detecta publicaciones nuevas comparando el snapshot de hoy
        con el anterior. Retorna solo los items nuevos.
        """
        df_today = self.load_snapshot()
        if df_today is None or df_today.empty:
            df_today = self.save_snapshot()

        # Buscar snapshot anterior (del día anterior o el más reciente guardado)
        files = self.storage.list_files(f"data/competition/tracker/{self.seller_id}")
        snapshots = sorted([f for f in files if f != "latest.parquet" and f.endswith(".parquet")])

        if len(snapshots) < 2:
            logger.info("Solo hay un snapshot, no se puede comparar.")
            return pd.DataFrame()

        prev_day  = snapshots[-2].replace(".parquet", "")
        df_prev   = self.load_snapshot(prev_day)

        if df_prev is None or df_prev.empty:
            return pd.DataFrame()

        prev_ids  = set(df_prev["item_id"].tolist())
        today_ids = set(df_today["item_id"].tolist())
        new_ids   = today_ids - prev_ids

        df_new = df_today[df_today["item_id"].isin(new_ids)].copy()
        logger.info(f"Publicaciones nuevas de {self.seller_nickname}: {len(df_new)}")
        return df_new

    def detect_price_changes(self) -> pd.DataFrame:
        """
        Detecta cambios de precio comparando snapshot de hoy vs anterior.
        Retorna items con precio modificado, indicando subida o bajada.
        """
        df_today = self.load_snapshot()
        if df_today is None or df_today.empty:
            df_today = self.save_snapshot()

        files = self.storage.list_files(f"data/competition/tracker/{self.seller_id}")
        snapshots = sorted([f for f in files if f != "latest.parquet" and f.endswith(".parquet")])

        if len(snapshots) < 2:
            return pd.DataFrame()

        prev_day = snapshots[-2].replace(".parquet", "")
        df_prev  = self.load_snapshot(prev_day)

        if df_prev is None or df_prev.empty:
            return pd.DataFrame()

        # Merge por item_id
        merged = df_today[["item_id", "title", "price", "available_qty", "permalink"]].merge(
            df_prev[["item_id", "price", "available_qty"]].rename(
                columns={"price": "prev_price", "available_qty": "prev_qty"}
            ),
            on="item_id",
            how="inner"
        )

        # Filtrar solo los que cambiaron
        changed = merged[merged["price"] != merged["prev_price"]].copy()
        changed["price_diff"]     = changed["price"] - changed["prev_price"]
        changed["price_diff_pct"] = ((changed["price_diff"] / changed["prev_price"]) * 100).round(2)
        changed["direction"]      = changed["price_diff"].apply(lambda x: "📈 Subió" if x > 0 else "📉 Bajó")

        logger.info(f"Cambios de precio en {self.seller_nickname}: {len(changed)}")
        return changed.sort_values("price_diff_pct")

    def get_summary(self) -> dict:
        """Resumen rápido del competidor para el dashboard."""
        df = self.load_snapshot()
        if df is None:
            df = self.save_snapshot()

        if df is None or df.empty:
            return {"error": f"No se encontraron publicaciones de {self.seller_nickname}"}

        return {
            "seller_nickname":   self.seller_nickname,
            "total_items":       len(df),
            "avg_price":         round(float(df["price"].mean()), 2),
            "min_price":         round(float(df["price"].min()), 2),
            "max_price":         round(float(df["price"].max()), 2),
            "total_sold":        int(df["sold_qty"].sum()),
            "categories_found":  df["category"].value_counts().to_dict(),
            "last_snapshot":     df["snapshot_day"].max() if "snapshot_day" in df.columns else "—",
        }
