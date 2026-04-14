"""
Extractor de palabras clave y análisis de búsquedas.

Datos que extrae:
  - Autocompletado de ML (sugerencias de búsqueda)
  - Volumen estimado de búsquedas por keyword
  - Keywords relacionados
  - Análisis de títulos de competidores (palabras más usadas)
"""

import re
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))
from auth.ml_client import MLClient
from storage.dropbox_client import DropboxClient
from config import ML_SITE_ID

logger = logging.getLogger(__name__)

# Palabras vacías en español (stopwords básicas)
STOPWORDS = {
    "de", "la", "el", "en", "y", "a", "los", "las", "un", "una",
    "con", "para", "por", "del", "al", "se", "que", "es", "su",
    "cm", "kg", "gr", "ml", "mm", "lt", "mts", "pack", "kit",
    "nuevo", "nueva", "original", "importado", "genérico"
}


class KeywordsExtractor:
    """
    Analiza palabras clave y oportunidades de búsqueda en ML.

    Uso:
        kw = KeywordsExtractor()
        suggestions = kw.get_autocomplete("notebook lenovo")
        df = kw.analyze_titles_in_category("MLU5726")
    """

    def __init__(self):
        self.client  = MLClient()
        self.storage = DropboxClient()

    # ─── Autocompletado de ML ─────────────────────────────────────

    def get_autocomplete(self, query: str, limit: int = 8) -> list[str]:
        """
        Obtiene sugerencias de autocompletado de ML para un término.
        Refleja búsquedas reales de usuarios.
        """
        try:
            data = self.client.get(
                f"/sites/{ML_SITE_ID}/autosuggest",
                params={"showFilters": True, "limit": limit, "q": query}
            )
            suggestions = data.get("suggested_queries", [])
            return [s.get("q", "") for s in suggestions]
        except Exception as e:
            logger.warning(f"Autocompletado falló para '{query}': {e}")
            return []

    # ─── Expansión de keywords ────────────────────────────────────

    def expand_keywords(self, seed_keyword: str, depth: int = 2) -> pd.DataFrame:
        """
        Expande un keyword semilla usando el autocompletado de ML.
        depth=1: solo sugerencias directas
        depth=2: también expande cada sugerencia (árbol de keywords)

        Retorna DataFrame con keyword, volumen estimado y fuente.
        """
        logger.info(f"🔍 Expandiendo keyword: '{seed_keyword}'")

        all_keywords = {seed_keyword}
        queue = [seed_keyword]

        for _ in range(depth):
            next_queue = []
            for kw in queue:
                suggestions = self.get_autocomplete(kw)
                new_kws = set(suggestions) - all_keywords
                all_keywords.update(new_kws)
                next_queue.extend(new_kws)
            queue = next_queue[:10]  # limitar explosión

        # Enriquecer con datos de demanda
        rows = []
        for kw in sorted(all_keywords):
            try:
                data = self.client.get(
                    f"/sites/{ML_SITE_ID}/search",
                    params={"q": kw, "limit": 1}
                )
                total = data.get("paging", {}).get("total", 0)
                rows.append({
                    "keyword":      kw,
                    "total_results": total,
                    "seed":         seed_keyword,
                    "snapshot_date": datetime.now().isoformat(),
                })
            except Exception:
                pass

        df = pd.DataFrame(rows).sort_values("total_results", ascending=False)
        logger.info(f"✅ {len(df)} keywords expandidos desde '{seed_keyword}'")
        return df

    # ─── Análisis de títulos de competidores ─────────────────────

    def analyze_titles_in_category(
        self,
        category_id: str,
        top_n: int = 100,
    ) -> pd.DataFrame:
        """
        Analiza los títulos de los top N items de una categoría
        para identificar palabras clave frecuentes.
        Útil para optimizar tus títulos.

        Retorna ranking de palabras por frecuencia.
        """
        logger.info(f"📝 Analizando títulos en categoría {category_id}...")

        data = self.client.get(
            f"/sites/{ML_SITE_ID}/search",
            params={
                "category": category_id,
                "sort":     "sold_quantity_desc",
                "limit":    50,
            }
        )

        titles = [item["title"] for item in data.get("results", [])]

        if not titles:
            return pd.DataFrame()

        # Tokenizar y limpiar
        all_words = []
        for title in titles:
            words = re.findall(r'\b[a-záéíóúñü]+\b', title.lower())
            filtered = [w for w in words if w not in STOPWORDS and len(w) > 2]
            all_words.extend(filtered)

        word_counts = Counter(all_words)
        rows = [
            {"word": word, "frequency": count, "category_id": category_id}
            for word, count in word_counts.most_common(50)
        ]

        return pd.DataFrame(rows)

    # ─── Score de un título ───────────────────────────────────────

    def score_title(
        self,
        title: str,
        category_id: str,
    ) -> dict:
        """
        Evalúa qué tan bien optimizado está un título para ML.
        Compara con las palabras clave más frecuentes de la categoría.

        Retorna score 0-100 y sugerencias de mejora.
        """
        df_keywords = self.analyze_titles_in_category(category_id)

        if df_keywords.empty:
            return {"score": 0, "error": "No se pudo analizar la categoría"}

        top_keywords = set(df_keywords.head(20)["word"].tolist())
        title_words  = set(re.findall(r'\b[a-záéíóúñü]+\b', title.lower()))
        title_words -= STOPWORDS

        matches       = title_words & top_keywords
        missing       = top_keywords - title_words
        score         = round(len(matches) / len(top_keywords) * 100, 1) if top_keywords else 0

        return {
            "title":          title,
            "score":          score,
            "matched_keywords":  list(matches),
            "suggested_keywords": list(missing)[:5],
            "title_length":   len(title),
            "optimal_length": 60,
            "too_long":       len(title) > 60,
        }

    # ─── Sync ─────────────────────────────────────────────────────

    def sync_keywords(self, seed_keywords: list[str]) -> None:
        """Expande y guarda keywords en Dropbox."""
        month_str = datetime.now().strftime("%Y-%m")
        all_dfs = []

        for seed in seed_keywords:
            df = self.expand_keywords(seed)
            if not df.empty:
                all_dfs.append(df)

        if all_dfs:
            df_all = pd.concat(all_dfs, ignore_index=True)
            self.storage.save_dataframe(
                df_all,
                f"data/keywords/keywords_{month_str}.parquet"
            )
            self.storage.log_sync("keywords", "ok", {"count": len(df_all)})
            logger.info(f"✅ {len(df_all)} keywords guardados en Dropbox.")
