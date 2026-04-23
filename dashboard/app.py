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

st.markdown("""
<style>
@import url('https://fonts.cdnfonts.com/css/samsung-sans');
html, body, [class*="css"], p, div, span, label, input, button {
    font-family: 'Samsung Sans', sans-serif !important;
    font-size: 14px !important;
}
h1 { font-size: 24px !important; }
h2 { font-size: 20px !important; }
h3 { font-size: 17px !important; }
[data-testid="metric-container"] label { font-size: 13px !important; }
[data-testid="metric-container"] [data-testid="stMetricValue"] { font-size: 20px !important; }
</style>
""", unsafe_allow_html=True)

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
    ["🏠 Resumen", "💰 Mis Ventas", "📊 Reportes", "🗄️ Historial", "🔍 Competencia", "📈 Tendencias", "🔑 Keywords"],
)

st.sidebar.markdown("---")
days_back = st.sidebar.slider("Días a analizar", 7, 90, 30)
st.sidebar.markdown(f"*Período: últimos {days_back} días*")

def fmt_currency(value: float, currency: str = "UYU") -> str:
    return f"${value:,.0f} {currency}"

def show_period_detail(summary: dict, label: str):
    if summary["revenue"] == 0:
        st.warning(f"Sin datos para {label}. Cargá el historial desde 🗄️ Historial.")
        return
    df = summary["df"]
    if df.empty:
        return

    st.markdown(f"##### {label}")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Facturación", fmt_currency(summary["revenue"]))
    k2.metric("Neto", fmt_currency(summary["net"]))
    k3.metric("Órdenes", f"{summary['orders']:,}")
    k4.metric("Unidades", f"{summary['units']:,}")
    k5.metric("Ticket prom.", fmt_currency(summary["avg_ticket"]))

    tab_un, tab_din, tab_pareto = st.tabs(["📦 Top 10 por unidades", "💰 Top 10 por facturación", "📊 Pareto"])

    with tab_un:
        top_units = (
            df.groupby("item_title")["quantity"].sum()
            .sort_values(ascending=False).head(10)
            .reset_index().rename(columns={"item_title": "Producto", "quantity": "Unidades"})
        )
        fig = px.bar(top_units, x="Unidades", y="Producto", orientation="h",
                     color_discrete_sequence=["#3483FA"])
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    with tab_din:
        top_rev = (
            df.groupby("item_title")["total_amount"].sum()
            .sort_values(ascending=False).head(10)
            .reset_index().rename(columns={"item_title": "Producto", "total_amount": "Facturación"})
        )
        fig2 = px.bar(top_rev, x="Facturación", y="Producto", orientation="h",
                      color_discrete_sequence=["#FFE600"])
        fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig2, use_container_width=True)

    with tab_pareto:
        pareto = (
            df.groupby("item_title")["total_amount"].sum()
            .sort_values(ascending=False).reset_index()
        )
        pareto["acumulado"]     = pareto["total_amount"].cumsum()
        pareto["pct_acumulado"] = pareto["acumulado"] / pareto["total_amount"].sum() * 100
        pareto["rank"]          = range(1, len(pareto) + 1)

        corte_80       = pareto[pareto["pct_acumulado"] <= 80]
        n_productos_80 = len(corte_80)
        pct_prod_80    = round(n_productos_80 / len(pareto) * 100, 1)

        st.info(f"**{n_productos_80} productos** ({pct_prod_80}% del catálogo) generan el **80% de los ingresos**")

        fig3 = px.bar(pareto.head(20), x="item_title", y="total_amount",
                      title="Pareto — Top 20 productos",
                      labels={"item_title": "Producto", "total_amount": "Facturación"},
                      color_discrete_sequence=["#3483FA"])
        fig3.add_scatter(x=pareto.head(20)["item_title"], y=pareto.head(20)["pct_acumulado"],
                         mode="lines+markers", name="% Acumulado", yaxis="y2",
                         line=dict(color="#FFE600", width=2))
        fig3.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis_tickangle=-45,
            yaxis2=dict(title="% Acumulado", overlaying="y", side="right", range=[0, 105]),
        )
        st.plotly_chart(fig3, use_container_width=True)

        st.dataframe(
            pareto[["rank", "item_title", "total_amount", "pct_acumulado"]].head(20)
            .rename(columns={"rank": "#", "item_title": "Producto", "total_amount": "Facturación", "pct_acumulado": "% Acumulado"})
            .assign(Facturación=lambda x: x["Facturación"].apply(fmt_currency)),
            use_container_width=True, hide_index=True
        )

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

        with st.expander("🔍 Detalle del cálculo (5 factores)"):
            d1, d2, d3 = st.columns(3)
            with d1:
                st.metric("1️⃣ Promedio diario del mes", fmt_currency(forecast["proj_daily_avg"]), help="Peso: 25%")
            with d2:
                st.metric("2️⃣ Tendencia últimos 7 días", fmt_currency(forecast["proj_trend_7d"]), help="Peso: 30%")
            with d3:
                ly = forecast.get("last_year_revenue")
                st.metric("3️⃣ Mismo mes año anterior", fmt_currency(ly) if ly else "Sin datos", help="Peso: 20%")
            d4, d5 = st.columns(2)
            with d4:
                seasonal = forecast.get("proj_seasonal")
                yrs = forecast.get("seasonal_years", 0)
                label = f"4️⃣ Estacionalidad ({yrs} años)" if yrs > 0 else "4️⃣ Estacionalidad histórica"
                st.metric(label, fmt_currency(seasonal) if seasonal else "Sin datos", help="Peso: 15%")
            with d5:
                acc = forecast.get("acceleration_factor", 1.0)
                arrow = "📈" if acc > 1 else "📉" if acc < 1 else "➡️"
                st.metric("5️⃣ Velocidad de crecimiento", fmt_currency(forecast.get("proj_acceleration", 0)),
                          delta=f"{arrow} Factor: {acc:.2f}x", help="Peso: 10%")
            if forecast.get("vs_last_year_pct") is not None:
                st.info(f"📊 Crecimiento vs mismo mes del año anterior: **{forecast['vs_last_year_pct']:+.1f}%**")


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

    def delta_pct(actual, prev):
        if prev and prev > 0:
            return f"{((actual - prev) / prev * 100):+.1f}%"
        return None

    if tipo == "📅 Mes actual vs mes anterior":
        st.subheader("Mes actual vs mes anterior")
        mes_actual_inicio = date(today.year, today.month, 1)
        mes_actual_fin    = today
        prev_month        = today.month - 1 if today.month > 1 else 12
        prev_year         = today.year if today.month > 1 else today.year - 1
        prev_days         = calendar.monthrange(prev_year, prev_month)[1]
        mes_prev_inicio   = date(prev_year, prev_month, 1)
        mes_prev_fin      = date(prev_year, prev_month, prev_days)

        with st.spinner("Cargando datos..."):
            try:
                actual = clients["sales"].get_period_summary(mes_actual_inicio, mes_actual_fin)
                prev   = clients["sales"].get_period_summary(mes_prev_inicio, mes_prev_fin)
            except Exception as e:
                st.error(f"Error: {e}")
                st.stop()

        st.markdown(f"#### {mes_actual_inicio.strftime('%B %Y')} vs {mes_prev_inicio.strftime('%B %Y')}")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Ingresos brutos", fmt_currency(actual["revenue"]), delta=delta_pct(actual["revenue"], prev["revenue"]))
        c2.metric("Ingresos netos", fmt_currency(actual["net"]), delta=delta_pct(actual["net"], prev["net"]))
        c3.metric("Órdenes", f"{actual['orders']:,}", delta=delta_pct(actual["orders"], prev["orders"]))
        c4.metric("Unidades", f"{actual['units']:,}", delta=delta_pct(actual["units"], prev["units"]))

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

        st.markdown("---")
        col_a, col_b = st.columns(2)
        with col_a:
            show_period_detail(actual, mes_actual_inicio.strftime("%B %Y"))
        with col_b:
            show_period_detail(prev, mes_prev_inicio.strftime("%B %Y"))

    elif tipo == "📆 Mes actual vs mismo mes año pasado":
        st.subheader("Mes actual vs mismo mes del año pasado")
        mes_actual_inicio = date(today.year, today.month, 1)
        mes_actual_fin    = today
        ly_inicio         = date(today.year - 1, today.month, 1)
        ly_dias           = calendar.monthrange(today.year - 1, today.month)[1]
        ly_fin            = date(today.year - 1, today.month, ly_dias)

        with st.spinner("Cargando datos..."):
            try:
                actual = clients["sales"].get_period_summary(mes_actual_inicio, mes_actual_fin)
                ly     = clients["sales"].get_period_summary(ly_inicio, ly_fin)
            except Exception as e:
                st.error(f"Error: {e}")
                st.stop()

        st.markdown(f"#### {mes_actual_inicio.strftime('%B %Y')} vs {ly_inicio.strftime('%B %Y')}")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Ingresos brutos", fmt_currency(actual["revenue"]), delta=delta_pct(actual["revenue"], ly["revenue"]))
        c2.metric("Ingresos netos", fmt_currency(actual["net"]), delta=delta_pct(actual["net"], ly["net"]))
        c3.metric("Órdenes", f"{actual['orders']:,}", delta=delta_pct(actual["orders"], ly["orders"]))
        c4.metric("Unidades", f"{actual['units']:,}", delta=delta_pct(actual["units"], ly["units"]))

        if ly["revenue"] == 0:
            st.warning("No hay datos del año pasado. Cargá el historial desde 🗄️ Historial.")

        st.markdown("---")
        col_a, col_b = st.columns(2)
        with col_a:
            show_period_detail(actual, mes_actual_inicio.strftime("%B %Y"))
        with col_b:
            show_period_detail(ly, ly_inicio.strftime("%B %Y"))

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
            def load_from_dropbox_or_api(date_from, date_to, storage, sales):
                dfs = []
                current = date(date_from.year, date_from.month, 1)
                while current <= date_to:
                    path = f"data/historical/{current.strftime('%Y-%m')}.parquet"
                    df_cached = storage.load_dataframe(path)
                    if df_cached is not None:
                        dfs.append(df_cached)
                    if current.month == 12:
                        current = date(current.year + 1, 1, 1)
                    else:
                        current = date(current.year, current.month + 1, 1)

                if dfs:
                    df_all = pd.concat(dfs, ignore_index=True)
                    df_all["date_created"] = pd.to_datetime(df_all["date_created"])
                    df_all["date"] = df_all["date_created"].dt.date
                    df_filtered = df_all[(df_all["date"] >= date_from) & (df_all["date"] <= date_to)]
                    if not df_filtered.empty:
                        df_paid = df_filtered[df_filtered["status"] == "paid"].copy()
                        if "net_amount" not in df_paid.columns:
                            df_paid["net_amount"] = df_paid["total_amount"] - df_paid.get("sale_fee", 0)
                        return {
                            "revenue":    round(float(df_paid["total_amount"].sum()), 2),
                            "net":        round(float(df_paid["net_amount"].sum()), 2),
                            "orders":     df_paid["order_id"].nunique(),
                            "units":      int(df_paid["quantity"].sum()),
                            "avg_ticket": round(float(df_paid["total_amount"].mean()), 2) if not df_paid.empty else 0,
                            "df":         df_paid,
                        }
                return sales.get_period_summary(date_from, date_to)

            with st.spinner("Cargando datos de ambos períodos..."):
                try:
                    periodo_a = load_from_dropbox_or_api(a_desde, a_hasta, clients["storage"], clients["sales"])
                    periodo_b = load_from_dropbox_or_api(b_desde, b_hasta, clients["storage"], clients["sales"])
                except Exception as e:
                    st.error(f"Error: {e}")
                    st.stop()

            label_a = f"{a_desde} → {a_hasta}"
            label_b = f"{b_desde} → {b_hasta}"

            st.markdown(f"#### {label_a} vs {label_b}")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Ingresos brutos", fmt_currency(periodo_a["revenue"]), delta=delta_pct(periodo_a["revenue"], periodo_b["revenue"]))
            c2.metric("Ingresos netos", fmt_currency(periodo_a["net"]), delta=delta_pct(periodo_a["net"], periodo_b["net"]))
            c3.metric("Órdenes", f"{periodo_a['orders']:,}", delta=delta_pct(periodo_a["orders"], periodo_b["orders"]))
            c4.metric("Unidades", f"{periodo_a['units']:,}", delta=delta_pct(periodo_a["units"], periodo_b["units"]))

            if not periodo_a["df"].empty and not periodo_b["df"].empty:
                df_a = periodo_a["df"].copy()
                df_b = periodo_b["df"].copy()
                df_a["dia"] = (pd.to_datetime(df_a["date_created"]) - pd.Timestamp(a_desde)).dt.days + 1
                df_b["dia"] = (pd.to_datetime(df_b["date_created"]) - pd.Timestamp(b_desde)).dt.days + 1
                daily_a = df_a.groupby("dia")["total_amount"].sum().reset_index()
                daily_b = df_b.groupby("dia")["total_amount"].sum().reset_index()
                daily_a["periodo"] = label_a
                daily_b["periodo"] = label_b
                df_chart = pd.concat([daily_a, daily_b])
                fig = px.line(df_chart, x="dia", y="total_amount", color="periodo",
                              title="Facturación por día del período",
                              labels={"dia": "Día del período", "total_amount": "Ingresos", "periodo": "Período"},
                              color_discrete_sequence=["#FFE600", "#3483FA"])
                st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")
            col_det_a, col_det_b = st.columns(2)
            with col_det_a:
                show_period_detail(periodo_a, label_a)
            with col_det_b:
                show_period_detail(periodo_b, label_b)


# ══════════════════════════════════════════════════════════════════
# PÁGINA: HISTORIAL
# ══════════════════════════════════════════════════════════════════

elif page == "🗄️ Historial":
    st.title("🗄️ Carga de Historial")
    clients = get_clients()

    st.info("Cargá datos históricos mes a mes en Dropbox. Solo necesitás hacerlo una vez por período.")

    col1, col2 = st.columns(2)
    with col1:
        fecha_desde = st.date_input("Desde", value=date(2024, 1, 1))
    with col2:
        fecha_hasta = st.date_input("Hasta", value=date.today())

    cargar_btn = st.button("📥 Cargar historial en Dropbox")

    if cargar_btn:
        if fecha_desde >= fecha_hasta:
            st.error("La fecha de inicio debe ser anterior a la fecha final.")
            st.stop()

        meses = []
        current = date(fecha_desde.year, fecha_desde.month, 1)
        while current <= fecha_hasta:
            days_in_month = calendar.monthrange(current.year, current.month)[1]
            mes_fin = date(current.year, current.month, min(days_in_month,
                          fecha_hasta.day if current.year == fecha_hasta.year and current.month == fecha_hasta.month
                          else days_in_month))
            meses.append((current, mes_fin))
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)

        st.info(f"Se cargarán **{len(meses)} meses** desde {fecha_desde} hasta {fecha_hasta}")

        progress_bar = st.progress(0)
        status_text  = st.empty()
        results      = []

        for i, (mes_inicio, mes_fin) in enumerate(meses):
            label = mes_inicio.strftime("%B %Y")
            status_text.text(f"Cargando {label}...")
            try:
                df = clients["sales"].get_orders_by_daterange(mes_inicio, mes_fin)
                if not df.empty:
                    path = f"data/historical/{mes_inicio.strftime('%Y-%m')}.parquet"
                    clients["storage"].save_dataframe(df, path)
                    results.append({"Mes": label, "Órdenes": len(df), "Estado": "OK"})
                else:
                    results.append({"Mes": label, "Órdenes": 0, "Estado": "Sin datos"})
            except Exception as e:
                results.append({"Mes": label, "Órdenes": 0, "Estado": f"Error: {str(e)[:50]}"})
            progress_bar.progress((i + 1) / len(meses))

        status_text.text("Carga completada")
        st.success(f"Historial cargado. {len([r for r in results if r['Estado'] == 'OK'])} meses exitosos.")
        st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)


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
