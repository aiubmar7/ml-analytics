"""
Dashboard principal - Streamlit.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, date
import calendar

import sys as _sys
from pathlib import Path as _Path
_ROOT = _Path(__file__).resolve().parent.parent
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from auth.ml_auth import get_valid_access_token
from auth.ml_client import MLClient
from extractors.my_sales import MySalesExtractor
from extractors.competition import CompetitionExtractor
from extractors.competitor_tracker import CompetitorTracker
from extractors.categories import CategoriesExtractor
from extractors.keywords import KeywordsExtractor
from storage.dropbox_client import DropboxClient

logging.basicConfig(level=logging.INFO)

st.set_page_config(
    page_title="ML Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

@st.cache_resource
def get_clients():
    return {
        "sales":       MySalesExtractor(),
        "competition": CompetitionExtractor(),
        "categories":  CategoriesExtractor(),
        "keywords":    KeywordsExtractor(),
        "storage":     DropboxClient(),
    }

st.sidebar.title("📊 ML Analytics")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Módulo",
    ["🏠 Resumen", "💰 Mis Ventas", "📊 Reportes", "🔍 Competencia", "📈 Tendencias", "🔑 Keywords"],
)

st.sidebar.markdown("---")
days_back = st.sidebar.slider("Días a analizar", 7, 90, 30)
st.sidebar.markdown(f"*Período: últimos {days_back} días*")

def fmt_currency(value: float, currency: str = "UYU") -> str:
    return f"${value:,.0f} {currency}"

def period_metrics(summary: dict, label: str):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"{label} — Ingresos", fmt_currency(summary["revenue"]))
    c2.metric("Neto", fmt_currency(summary["net"]))
    c3.metric("Órdenes", f"{summary['orders']:,}")
    c4.metric("Unidades", f"{summary['units']:,}")

# ══════════════════════════════════════════════════════════════════
# PÁGINA: RESUMEN
# ══════════════════════════════════════════════════════════════════

if page == "🏠 Resumen":
    st.title("🏠 Resumen Ejecutivo")
    clients = get_clients()

    with st.spinner("Cargando datos..."):
        try:
            summary = clients["sales"].get_summary(days_back)
        except Exception as e:
            st.error(f"Error al cargar datos: {e}")
            st.info("Asegurate de haber completado la autorización (`python auth/ml_auth.py`)")
            st.stop()

    if "error" in summary:
        st.warning(summary["error"])
        st.stop()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Órdenes", summary["total_orders"])
    with col2:
        st.metric("Unidades vendidas", summary["total_units"])
    with col3:
        st.metric("Ingresos brutos", fmt_currency(summary["total_revenue"]))
    with col4:
        st.metric("Ingresos netos", fmt_currency(summary["net_revenue"]))

    st.markdown("---")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Reputación")
        rep = summary.get("reputation", {})
        st.metric("Nivel", rep.get("level_id", "—").replace("_", " ").title())
        st.metric("Power Seller", rep.get("power_seller_status", "—"))
        claims_rate = rep.get("claims_rate", 0) * 100
        color = "🟢" if claims_rate < 2 else "🟡" if claims_rate < 5 else "🔴"
        st.metric(f"Tasa de reclamos {color}", f"{claims_rate:.2f}%")

    with col_right:
        st.subheader("Producto estrella")
        if summary.get("top_item"):
            st.info(f"**{summary['top_item']}**")
        st.metric("Ticket promedio", fmt_currency(summary.get("avg_ticket", 0)))

    st.markdown("---")
    st.subheader("📅 Pronóstico de facturación mensual")

    with st.spinner("Calculando pronóstico..."):
        try:
            forecast = clients["sales"].get_monthly_forecast()
        except Exception as e:
            forecast = {"error": str(e)}

    if "error" in forecast:
        st.warning(f"No se pudo calcular el pronóstico: {forecast['error']}")
    else:
        elapsed    = forecast["days_elapsed"]
        remaining  = forecast["days_remaining"]
        total_days = forecast["days_in_month"]

        st.caption(f"📆 {forecast['month']} — día {elapsed} de {total_days} ({remaining} días restantes)")
        st.progress(elapsed / total_days, text=f"Progreso del mes: {elapsed}/{total_days} días")

        st.markdown("#### Proyección final del mes")
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            delta_rev = f"{forecast['vs_prev_month_pct']:+.1f}% vs mes ant." if forecast.get("vs_prev_month_pct") is not None else None
            st.metric("💰 Facturación proyectada", fmt_currency(forecast["forecast_revenue"]), delta=delta_rev)
        with fc2:
            st.metric("📦 Unidades proyectadas", f"{int(forecast['forecast_units']):,}")
        with fc3:
            st.metric("🛒 Órdenes proyectadas", f"{int(forecast['forecast_orders']):,}")
        with fc4:
            st.metric("💵 Neto proyectado", fmt_currency(forecast["forecast_net"]))

        st.markdown("#### Acumulado real vs proyección")
        ac1, ac2, ac3 = st.columns(3)
        with ac1:
            pct_done = round(forecast["revenue_so_far"] / forecast["forecast_revenue"] * 100, 1) if forecast["forecast_revenue"] > 0 else 0
            st.metric("Facturado hasta hoy", fmt_currency(forecast["revenue_so_far"]), delta=f"{pct_done}% del objetivo")
        with ac2:
            st.metric("Promedio diario (mes)", fmt_currency(forecast["daily_avg_revenue"]))
        with ac3:
            st.metric("Promedio diario (últ. 7d)", fmt_currency(forecast["daily_trend_revenue"]))

        with st.expander("🔍 Detalle del cálculo (3 factores)"):
            d1, d2, d3 = st.columns(3)
            with d1:
                st.metric("Promedio diario del mes", fmt_currency(forecast["proj_daily_avg"]), help="Peso: 35%")
            with d2:
                st.metric("Tendencia últimos 7 días", fmt_currency(forecast["proj_trend_7d"]), help="Peso: 40%")
            with d3:
                ly = forecast.get("last_year_revenue")
                st.metric("Mismo mes año anterior", fmt_currency(ly) if ly else "Sin datos", help="Peso: 25%")


# ══════════════════════════════════════════════════════════════════
# PÁGINA: MIS VENTAS
# ══════════════════════════════════════════════════════════════════

elif page == "💰 Mis Ventas":
    st.title("💰 Mis Ventas")
    clients = get_clients()

    col_btn1, _ = st.columns([1, 4])
    with col_btn1:
        refresh = st.button("🔄 Actualizar datos")

    with st.spinner("Extrayendo órdenes de ML..."):
        if refresh:
            df = clients["sales"].sync_orders(days_back)
        else:
            month_str = datetime.now().strftime("%Y-%m")
            df = clients["storage"].load_dataframe(f"data/my_sales/orders_{month_str}.parquet")
            if df is None:
                df = clients["sales"].sync_orders(days_back)

    if df is None or df.empty:
        st.warning("No hay datos de ventas disponibles.")
        st.stop()

    df_paid = df[df["status"] == "paid"].copy()

    st.subheader("Ventas diarias")
    df_paid["date"] = df_paid["date_created"].dt.date
    daily = df_paid.groupby("date").agg(
        total_amount=("total_amount", "sum"),
        orders=("order_id", "nunique"),
    ).reset_index()

    fig = px.bar(daily, x="date", y="total_amount",
                 labels={"date": "Fecha", "total_amount": "Ingresos"},
                 color_discrete_sequence=["#FFE600"])
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top productos por ingresos")
    top_items = (
        df_paid.groupby("item_title")
        .agg(total=("total_amount", "sum"), units=("quantity", "sum"))
        .sort_values("total", ascending=False)
        .head(10)
        .reset_index()
    )
    fig2 = px.bar(top_items, x="total", y="item_title", orientation="h",
                  labels={"total": "Ingresos", "item_title": "Producto"},
                  color_discrete_sequence=["#3483FA"])
    fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("📋 Ver datos completos"):
        st.dataframe(df_paid, use_container_width=True)


# ══════════════════════════════════════════════════════════════════
# PÁGINA: REPORTES Y COMPARACIONES
# ══════════════════════════════════════════════════════════════════

elif page == "📊 Reportes":
    st.title("📊 Reportes y Comparaciones")
    clients = get_clients()

    tipo = st.radio(
        "Tipo de comparación",
        ["📅 Mes actual vs mes anterior", "📆 Mes actual vs mismo mes año pasado", "🗓️ Rango personalizado"],
        horizontal=True,
    )

    today = date.today()

    # ── Mes actual vs mes anterior ────────────────────────────────
    if tipo == "📅 Mes actual vs mes anterior":
        st.subheader("Mes actual vs mes anterior")

        # Mes actual
        mes_actual_inicio = date(today.year, today.month, 1)
        mes_actual_fin    = today

        # Mes anterior
        prev_month     = today.month - 1 if today.month > 1 else 12
        prev_year      = today.year if today.month > 1 else today.year - 1
        prev_days      = calendar.monthrange(prev_year, prev_month)[1]
        mes_prev_inicio = date(prev_year, prev_month, 1)
        mes_prev_fin    = date(prev_year, prev_month, prev_days)

        with st.spinner("Cargando datos de ambos meses..."):
            try:
                actual = clients["sales"].get_period_summary(mes_actual_inicio, mes_actual_fin)
                prev   = clients["sales"].get_period_summary(mes_prev_inicio, mes_prev_fin)
            except Exception as e:
                st.error(f"Error: {e}")
                st.stop()

        # KPIs comparativos
        st.markdown(f"#### {mes_actual_inicio.strftime('%B %Y')} vs {mes_prev_inicio.strftime('%B %Y')}")

        c1, c2, c3, c4 = st.columns(4)
        def delta_pct(actual, prev):
            if prev and prev > 0:
                return f"{((actual - prev) / prev * 100):+.1f}%"
            return None

        c1.metric("Ingresos brutos", fmt_currency(actual["revenue"]),
                  delta=delta_pct(actual["revenue"], prev["revenue"]))
        c2.metric("Ingresos netos", fmt_currency(actual["net"]),
                  delta=delta_pct(actual["net"], prev["net"]))
        c3.metric("Órdenes", f"{actual['orders']:,}",
                  delta=delta_pct(actual["orders"], prev["orders"]))
        c4.metric("Unidades", f"{actual['units']:,}",
                  delta=delta_pct(actual["units"], prev["units"]))

        st.markdown("---")

        # Gráfico comparativo diario
        if not actual["df"].empty and not prev["df"].empty:
            df_act = actual["df"].copy()
            df_prv = prev["df"].copy()

            df_act["dia"] = df_act["date_created"].dt.day
            df_prv["dia"] = df_prv["date_created"].dt.day

            daily_act = df_act.groupby("dia")["total_amount"].sum().reset_index()
            daily_prv = df_prv.groupby("dia")["total_amount"].sum().reset_index()

            daily_act["mes"] = mes_actual_inicio.strftime("%B %Y")
            daily_prv["mes"] = mes_prev_inicio.strftime("%B %Y")

            df_chart = pd.concat([daily_act, daily_prv])
            fig = px.line(df_chart, x="dia", y="total_amount", color="mes",
                          title="Facturación diaria comparada",
                          labels={"dia": "Día del mes", "total_amount": "Ingresos", "mes": "Mes"},
                          color_discrete_sequence=["#3483FA", "#FFE600"])
            st.plotly_chart(fig, use_container_width=True)

        # Tabla resumen
        st.markdown("#### Resumen comparativo")
        resumen = pd.DataFrame({
            "Métrica": ["Ingresos brutos", "Ingresos netos", "Órdenes", "Unidades", "Ticket promedio"],
            mes_actual_inicio.strftime("%B %Y"): [
                fmt_currency(actual["revenue"]), fmt_currency(actual["net"]),
                actual["orders"], actual["units"], fmt_currency(actual["avg_ticket"])
            ],
            mes_prev_inicio.strftime("%B %Y"): [
                fmt_currency(prev["revenue"]), fmt_currency(prev["net"]),
                prev["orders"], prev["units"], fmt_currency(prev["avg_ticket"])
            ],
        })
        st.dataframe(resumen, use_container_width=True, hide_index=True)

    # ── Mes actual vs mismo mes año pasado ────────────────────────
    elif tipo == "📆 Mes actual vs mismo mes año pasado":
        st.subheader("Mes actual vs mismo mes del año pasado")

        mes_actual_inicio = date(today.year, today.month, 1)
        mes_actual_fin    = today

        ly_inicio = date(today.year - 1, today.month, 1)
        ly_dias   = calendar.monthrange(today.year - 1, today.month)[1]
        ly_fin    = date(today.year - 1, today.month, ly_dias)

        with st.spinner("Cargando datos..."):
            try:
                actual = clients["sales"].get_period_summary(mes_actual_inicio, mes_actual_fin)
                ly     = clients["sales"].get_period_summary(ly_inicio, ly_fin)
            except Exception as e:
                st.error(f"Error: {e}")
                st.stop()

        st.markdown(f"#### {mes_actual_inicio.strftime('%B %Y')} vs {ly_inicio.strftime('%B %Y')}")

        c1, c2, c3, c4 = st.columns(4)
        def delta_pct(actual, prev):
            if prev and prev > 0:
                return f"{((actual - prev) / prev * 100):+.1f}%"
            return None

        c1.metric("Ingresos brutos", fmt_currency(actual["revenue"]),
                  delta=delta_pct(actual["revenue"], ly["revenue"]))
        c2.metric("Ingresos netos", fmt_currency(actual["net"]),
                  delta=delta_pct(actual["net"], ly["net"]))
        c3.metric("Órdenes", f"{actual['orders']:,}",
                  delta=delta_pct(actual["orders"], ly["orders"]))
        c4.metric("Unidades", f"{actual['units']:,}",
                  delta=delta_pct(actual["units"], ly["units"]))

        if ly["revenue"] == 0:
            st.info("No hay datos del año pasado disponibles en la API de ML.")

        # Tabla resumen
        st.markdown("#### Resumen comparativo")
        resumen = pd.DataFrame({
            "Métrica": ["Ingresos brutos", "Ingresos netos", "Órdenes", "Unidades", "Ticket promedio"],
            mes_actual_inicio.strftime("%B %Y"): [
                fmt_currency(actual["revenue"]), fmt_currency(actual["net"]),
                actual["orders"], actual["units"], fmt_currency(actual["avg_ticket"])
            ],
            ly_inicio.strftime("%B %Y"): [
                fmt_currency(ly["revenue"]), fmt_currency(ly["net"]),
                ly["orders"], ly["units"], fmt_currency(ly["avg_ticket"])
            ],
        })
        st.dataframe(resumen, use_container_width=True, hide_index=True)

    # ── Rango personalizado ───────────────────────────────────────
    elif tipo == "🗓️ Rango personalizado":
        st.subheader("Comparar dos períodos personalizados")

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Período A**")
            a_desde = st.date_input("Desde", value=date(today.year, today.month, 1), key="a_desde")
            a_hasta = st.date_input("Hasta", value=today, key="a_hasta")
        with col_b:
            st.markdown("**Período B**")
            prev_month  = today.month - 1 if today.month > 1 else 12
            prev_year   = today.year if today.month > 1 else today.year - 1
            prev_days_n = calendar.monthrange(prev_year, prev_month)[1]
            b_desde = st.date_input("Desde", value=date(prev_year, prev_month, 1), key="b_desde")
            b_hasta = st.date_input("Hasta", value=date(prev_year, prev_month, prev_days_n), key="b_hasta")

        comparar_btn = st.button("📊 Comparar períodos")

        if comparar_btn:
            with st.spinner("Cargando datos de ambos períodos..."):
                try:
                    periodo_a = clients["sales"].get_period_summary(a_desde, a_hasta)
                    periodo_b = clients["sales"].get_period_summary(b_desde, b_hasta)
                except Exception as e:
                    st.error(f"Error: {e}")
                    st.stop()

            label_a = f"{a_desde} → {a_hasta}"
            label_b = f"{b_desde} → {b_hasta}"

            st.markdown(f"#### {label_a} vs {label_b}")

            c1, c2, c3, c4 = st.columns(4)
            def delta_pct(actual, prev):
                if prev and prev > 0:
                    return f"{((actual - prev) / prev * 100):+.1f}%"
                return None

            c1.metric("Ingresos brutos", fmt_currency(periodo_a["revenue"]),
                      delta=delta_pct(periodo_a["revenue"], periodo_b["revenue"]))
            c2.metric("Ingresos netos", fmt_currency(periodo_a["net"]),
                      delta=delta_pct(periodo_a["net"], periodo_b["net"]))
            c3.metric("Órdenes", f"{periodo_a['orders']:,}",
                      delta=delta_pct(periodo_a["orders"], periodo_b["orders"]))
            c4.metric("Unidades", f"{periodo_a['units']:,}",
                      delta=delta_pct(periodo_a["units"], periodo_b["units"]))

            # Gráfico comparativo
            if not periodo_a["df"].empty and not periodo_b["df"].empty:
                df_a = periodo_a["df"].copy()
                df_b = periodo_b["df"].copy()

                df_a["periodo"] = label_a
                df_b["periodo"] = label_b

                df_all = pd.concat([df_a, df_b])
                df_all["fecha"] = df_all["date_created"].dt.date

                daily_all = df_all.groupby(["fecha", "periodo"])["total_amount"].sum().reset_index()
                fig = px.line(daily_all, x="fecha", y="total_amount", color="periodo",
                              title="Facturación diaria comparada",
                              labels={"fecha": "Fecha", "total_amount": "Ingresos", "periodo": "Período"},
                              color_discrete_sequence=["#3483FA", "#FFE600"])
                st.plotly_chart(fig, use_container_width=True)

            # Tabla resumen
            resumen = pd.DataFrame({
                "Métrica": ["Ingresos brutos", "Ingresos netos", "Órdenes", "Unidades", "Ticket promedio"],
                label_a: [
                    fmt_currency(periodo_a["revenue"]), fmt_currency(periodo_a["net"]),
                    periodo_a["orders"], periodo_a["units"], fmt_currency(periodo_a["avg_ticket"])
                ],
                label_b: [
                    fmt_currency(periodo_b["revenue"]), fmt_currency(periodo_b["net"]),
                    periodo_b["orders"], periodo_b["units"], fmt_currency(periodo_b["avg_ticket"])
                ],
            })
            st.dataframe(resumen, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════
# PÁGINA: COMPETENCIA
# ══════════════════════════════════════════════════════════════════

elif page == "🔍 Competencia":
    st.title("🔍 Análisis de Competencia")
    clients = get_clients()

    tab_tracker, tab_search = st.tabs(["🎯 La Tentación", "🔍 Buscar otro competidor"])

    with tab_tracker:
        col_scan, _ = st.columns([1, 3])
        with col_scan:
            scan_btn = st.button("🔄 Escanear ahora")

        tracker = CompetitorTracker(seller_id=175850089, seller_nickname="LATENTACIONSRL")

        if scan_btn:
            with st.spinner("Escaneando publicaciones de La Tentación..."):
                df_tracker = tracker.save_snapshot()
        else:
            with st.spinner("Cargando último snapshot..."):
                df_tracker = tracker.load_snapshot()
                if df_tracker is None:
                    st.info("No hay datos aún. Hacé clic en 'Escanear ahora'.")
                    df_tracker = pd.DataFrame()

        if not df_tracker.empty:
            summary = tracker.get_summary()
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Publicaciones encontradas", summary["total_items"])
            m2.metric("Precio promedio", f"${summary['avg_price']:,.0f}")
            m3.metric("Precio mínimo", f"${summary['min_price']:,.0f}")
            m4.metric("Precio máximo", f"${summary['max_price']:,.0f}")
            st.caption(f"Último scan: {summary.get('last_snapshot', '—')}")

            cats = summary.get("categories_found", {})
            if cats:
                st.markdown("**Items por categoría:**")
                cols_cat = st.columns(len(cats))
                for i, (cat, count) in enumerate(cats.items()):
                    cols_cat[i].metric(cat.title(), count)

            t1, t2, t3 = st.tabs(["📋 Todas las publicaciones", "🆕 Nuevas publicaciones", "💰 Cambios de precio"])
            with t1:
                cols_show = ["title", "price", "available_qty", "sold_qty", "category", "listing_type", "permalink"]
                st.dataframe(df_tracker[[c for c in cols_show if c in df_tracker.columns]], use_container_width=True)
            with t2:
                df_new = tracker.detect_new_items()
                if df_new.empty:
                    st.info("No hay publicaciones nuevas desde el último scan.")
                else:
                    st.success(f"🆕 {len(df_new)} publicaciones nuevas detectadas")
                    st.dataframe(df_new[["title", "price", "category", "permalink"]], use_container_width=True)
            with t3:
                df_price = tracker.detect_price_changes()
                if df_price.empty:
                    st.info("No hay cambios de precio desde el último scan.")
                else:
                    st.warning(f"💰 {len(df_price)} cambios de precio detectados")
                    st.dataframe(df_price[["title", "prev_price", "price", "price_diff", "price_diff_pct", "direction"]], use_container_width=True)

    with tab_search:
        st.subheader("Buscar competidor")
        col_input, col_btn = st.columns([3, 1])
        with col_input:
            seller_input = st.text_input("Nickname o User ID del competidor", placeholder="Ej: nombre_vendedor o 123456789")
        with col_btn:
            st.write("")
            search_btn = st.button("🔍 Analizar")

        if search_btn and seller_input:
            with st.spinner(f"Analizando {seller_input}..."):
                if not seller_input.isdigit():
                    user = clients["competition"].search_seller_by_nickname(seller_input)
                    if not user:
                        st.error(f"No se encontró el vendedor '{seller_input}'")
                        st.stop()
                    seller_id = str(user["id"])
                    st.success(f"Vendedor encontrado: **{user.get('nickname')}** (ID: {seller_id})")
                else:
                    seller_id = seller_input

                profile = clients["competition"].get_seller_profile(seller_id)
                col1, col2, col3 = st.columns(3)
                col1.metric("Nivel", profile.get("level_id", "—"))
                col2.metric("Transacciones", f"{profile.get('transactions_total', 0):,}")
                col3.metric("Power Seller", profile.get("power_seller_status", "—"))

                st.subheader("Publicaciones activas")
                df_items = clients["competition"].sync_seller(seller_id)
                if not df_items.empty:
                    col_a, col_b, col_c = st.columns(3)
                    col_a.metric("Total publicaciones", len(df_items))
                    col_b.metric("Precio promedio", fmt_currency(df_items["price"].mean()))
                    col_c.metric("Unidades vendidas totales", f"{df_items['sold_qty'].sum():,}")
                    fig = px.histogram(df_items, x="price", nbins=20, title="Distribución de precios", color_discrete_sequence=["#3483FA"])
                    st.plotly_chart(fig, use_container_width=True)
                    cols_show = ["title", "price", "sold_qty", "available_qty", "listing_type", "permalink"]
                    st.dataframe(df_items[[c for c in cols_show if c in df_items.columns]], use_container_width=True)


# ══════════════════════════════════════════════════════════════════
# PÁGINA: TENDENCIAS
# ══════════════════════════════════════════════════════════════════

elif page == "📈 Tendencias":
    st.title("📈 Tendencias de Mercado")
    clients = get_clients()

    @st.cache_data(ttl=3600)
    def load_categories():
        return clients["categories"].get_categories_tree()

    df_cats = load_categories()
    if df_cats.empty:
        st.error("No se pudieron cargar las categorías.")
        st.stop()

    cat_options = {row["name"]: row["category_id"] for _, row in df_cats.iterrows()}
    selected_cat_name = st.selectbox("Seleccioná una categoría", list(cat_options.keys()))
    selected_cat_id   = cat_options[selected_cat_name]

    col_btn, _ = st.columns([1, 4])
    with col_btn:
        analyze_btn = st.button("📊 Analizar categoría")

    if analyze_btn:
        with st.spinner(f"Analizando {selected_cat_name}..."):
            tab1, tab2, tab3 = st.tabs(["🏆 Top Items", "🔍 Tendencias", "💡 Oportunidades"])
            with tab1:
                df_top = clients["categories"].get_top_items_in_category(selected_cat_id)
                if not df_top.empty:
                    st.dataframe(df_top[["title", "price", "sold_qty", "seller_nickname", "permalink"]], use_container_width=True)
                else:
                    st.info("Sin datos.")
            with tab2:
                df_trends = clients["categories"].get_search_trends(selected_cat_id)
                if not df_trends.empty:
                    for _, row in df_trends.iterrows():
                        st.write(f"**#{row['rank']}** {row['keyword']}")
                else:
                    st.info("Sin datos de tendencias para esta categoría.")
            with tab3:
                df_opp = clients["categories"].find_opportunities(selected_cat_id)
                if not df_opp.empty:
                    st.success(f"Se encontraron {len(df_opp)} oportunidades")
                    st.dataframe(df_opp[["title", "sold_qty", "seller_count", "opportunity_score", "price"]], use_container_width=True)
                else:
                    st.info("Sin oportunidades claras en esta categoría.")


# ══════════════════════════════════════════════════════════════════
# PÁGINA: KEYWORDS
# ══════════════════════════════════════════════════════════════════

elif page == "🔑 Keywords":
    st.title("🔑 Investigación de Keywords")
    clients = get_clients()

    st.subheader("Expandir keywords")
    seed = st.text_input("Keyword semilla", placeholder="Ej: zapatillas running")

    col1, col2 = st.columns(2)
    with col1:
        depth = st.selectbox("Profundidad de expansión", [1, 2], index=1)
    with col2:
        expand_btn = st.button("🔍 Expandir")

    if expand_btn and seed:
        with st.spinner(f"Expandiendo '{seed}'..."):
            df_kw = clients["keywords"].expand_keywords(seed, depth=depth)
        if not df_kw.empty:
            fig = px.bar(df_kw.head(20), x="keyword", y="total_results",
                         title="Resultados por keyword",
                         color_discrete_sequence=["#3483FA"])
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df_kw, use_container_width=True)

    st.markdown("---")
    st.subheader("Evaluar título")
    title_input  = st.text_input("Título a evaluar", placeholder="Zapatillas Running Hombre Nike Air Max Talle 42")
    cat_id_input = st.text_input("Category ID (opcional)", placeholder="MLU5726")

    if st.button("⭐ Evaluar") and title_input and cat_id_input:
        with st.spinner("Evaluando..."):
            result = clients["keywords"].score_title(title_input, cat_id_input)
        score = result.get("score", 0)
        color = "🟢" if score >= 70 else "🟡" if score >= 40 else "🔴"
        st.metric(f"Score del título {color}", f"{score}/100")
        if result.get("matched_keywords"):
            st.success("✅ Keywords encontradas: " + ", ".join(result["matched_keywords"]))
        if result.get("suggested_keywords"):
            st.warning("💡 Sugerencias para agregar: " + ", ".join(result["suggested_keywords"]))
        if result.get("too_long"):
            st.warning(f"⚠️ Título muy largo ({result['title_length']} chars). Recomendado: máx 60.")
