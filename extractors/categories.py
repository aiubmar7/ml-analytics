"""
Extractor de tendencias de mercado y análisis de categorías.

Datos que extrae:
  - Categorías más vendidas en el sitio
  - Tendencias de búsqueda en una categoría
  - Top items de una categoría (más vendidos)
  - Demanda insatisfecha (búsquedas sin buen resultado)
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))
from auth.ml_client import MLClient
from storage.dropbox_client import DropboxClient
from config import ML_SITE_ID

logger = logging.getLogger(__name__)


class CategoriesExtractor:
    """
    Analiza tendencias de mercado y categorías de ML.

    Uso:
        cats = CategoriesExtractor()
        df = cats.get_top_items_in_category("MLU5726")   # Electrónica
        trends = cats.get_category_trends("MLU5726")
    """

    def __init__(self):
        self.client  = MLClient()
        self.storage = DropboxClient()

    # ─── Árbol de categorías ──────────────────────────────────────

    def get_categories_tree(self) -> pd.DataFrame:
        """Retorna todas las categorías raíz del sitio."""
        data = self.client.get(f"/sites/{ML_SITE_ID}/categories")
        rows = [{"category_id": c["id"], "name": c["name"]} for c in data]
        return pd.DataFrame(rows)

    def get_subcategories(self, category_id: str) -> pd.DataFrame:
        """Retorna subcategorías de una categoría."""
        data = self.client.get(f"/categories/{category_id}")
        children = data.get("children_categories", [])
        rows = [{"category_id": c["id"], "name": c["name"]} for c in children]
        return pd.DataFrame(rows)

    # ─── Top items de una categoría ──────────────────────────────

    def get_top_items_in_category(
        self,
        category_id: str,
        limit: int = 50,
    ) -> pd.DataFrame:
        """
        Retorna los items más relevantes/vendidos de una categoría.
        Útil para entender qué productos dominan el mercado.
        """
        logger.info(f"📊 Top items en categoría {category_id}...")

        data = self.client.get(
            f"/sites/{ML_SITE_ID}/search",
            params={
                "category": category_id,
                "sort":     "sold_quantity_desc",
                "limit":    min(limit, 50),
            }
        )

        rows = []
        for item in data.get("results", []):
            rows.append({
                "item_id":       item["id"],
                "title":         item["title"],
                "seller_id":     item.get("seller", {}).get("id"),
                "seller_nickname": item.get("seller", {}).get("nickname"),
                "price":         item["price"],
                "currency_id":   item["currency_id"],
                "sold_qty":      item.get("sold_quantity", 0),
                "available_qty": item.get("available_quantity", 0),
                "listing_type":  item.get("listing_type_id"),
                "condition":     item.get("condition"),
                "category_id":   category_id,
                "permalink":     item.get("permalink"),
                "snapshot_date": datetime.now().isoformat(),
            })

        df = pd.DataFrame(rows)
        logger.info(f"✅ {len(df)} items extraídos de la categoría {category_id}")
        return df

    # ─── Tendencias de búsqueda ───────────────────────────────────

    def get_search_trends(self, category_id: str) -> pd.DataFrame:
        """
        Retorna las palabras más buscadas en una categoría.
        Muy útil para optimizar títulos de tus publicaciones.
        """
        try:
            data = self.client.get(
                f"/trends/{ML_SITE_ID}/category/{category_id}"
            )
            rows = []
            for i, keyword in enumerate(data, 1):
                rows.append({
                    "rank":        i,
                    "keyword":     keyword.get("keyword"),
                    "url":         keyword.get("url"),
                    "category_id": category_id,
                    "snapshot_date": datetime.now().isoformat(),
                })
            return pd.DataFrame(rows)
        except Exception as e:
            logger.warning(f"No se pudieron obtener trends para {category_id}: {e}")
            return pd.DataFrame()

    # ─── Análisis de demanda por keyword ─────────────────────────

    def analyze_keyword_demand(
        self,
        keyword: str,
        category_id: str = None,
        limit: int = 50,
    ) -> dict:
        """
        Analiza la demanda de un keyword:
        - Cuántos resultados hay
        - Precio promedio/min/max
        - Cuántos tienen gold premium
        - Oportunidades (alta demanda, poca oferta)
        """
        params = {"q": keyword, "limit": limit}
        if category_id:
            params["category"] = category_id

        data = self.client.get(f"/sites/{ML_SITE_ID}/search", params=params)

        results = data.get("results", [])
        paging  = data.get("paging", {})

        if not results:
            return {"keyword": keyword, "total_results": 0}

        prices  = [r["price"] for r in results if r.get("price")]
        listing_types = [r.get("listing_type_id") for r in results]

        return {
            "keyword":           keyword,
            "total_results":     paging.get("total", 0),
            "avg_price":         round(sum(prices) / len(prices), 2) if prices else 0,
            "min_price":         min(prices) if prices else 0,
            "max_price":         max(prices) if prices else 0,
            "gold_premium_pct":  round(listing_types.count("gold_premium") / len(listing_types) * 100, 1),
            "gold_special_pct":  round(listing_types.count("gold_special") / len(listing_types) * 100, 1),
            "snapshot_date":     datetime.now().isoformat(),
        }

    # ─── Oportunidades de mercado ─────────────────────────────────

    def find_opportunities(
        self,
        category_id: str,
        min_sold: int = 10,
        max_sellers: int = 5,
    ) -> pd.DataFrame:
        """
        Detecta oportunidades: keywords/productos con alta demanda
        pero poca competencia (pocos vendedores con buenas ventas).

        Args:
            category_id: Categoría a analizar
            min_sold:    Mínimo de unidades vendidas para considerar demanda
            max_sellers: Máximo de sellers para considerar "poca competencia"
        """
        df_top = self.get_top_items_in_category(category_id, limit=50)

        if df_top.empty:
            return pd.DataFrame()

        # Agrupar por cantidad de sellers que venden items similares
        seller_count = (
            df_top.groupby("title")["seller_id"]
            .nunique()
            .reset_index()
            .rename(columns={"seller_id": "seller_count"})
        )

        df_merged = df_top.merge(seller_count, on="title")

        # Filtrar: alta demanda + poca competencia
        opportunities = df_merged[
            (df_merged["sold_qty"] >= min_sold) &
            (df_merged["seller_count"] <= max_sellers)
        ].copy()

        opportunities["opportunity_score"] = (
            opportunities["sold_qty"] / opportunities["seller_count"]
        ).round(2)

        return opportunities.sort_values("opportunity_score", ascending=False)

    def sync_category(self, category_id: str) -> None:
        """Extrae y guarda datos de una categoría en Dropbox."""
        month_str = datetime.now().strftime("%Y-%m")

        df_top = self.get_top_items_in_category(category_id)
        if not df_top.empty:
            self.storage.append_dataframe(
                df_top,
                f"data/categories/{category_id}_{month_str}.parquet"
            )

        df_trends = self.get_search_trends(category_id)
        if not df_trends.empty:
            self.storage.save_dataframe(
                df_trends,
                f"data/categories/{category_id}_trends_{month_str}.parquet"
            )

        self.storage.log_sync("categories", "ok", {"category_id": category_id})
        logger.info(f"✅ Categoría {category_id} sincronizada.")
