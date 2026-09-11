diff --git a/dashboard/app.py b/dashboard/app.py
index 828664e..697b9ee 100644
--- a/dashboard/app.py
+++ b/dashboard/app.py
@@ -258,14 +258,19 @@ if page == "🏠 Resumen":
             with d2:
                 st.metric("2️⃣ Tendencia últimos 7 días", fmt_currency(forecast["proj_trend_7d"]), help="Peso: 15%")
             with d3:
-                ly = forecast.get("last_year_revenue")
-                st.metric("3️⃣ Mismo mes año anterior", fmt_currency(ly) if ly else "Sin datos", help="Peso: 15%")
+                proj_ly = forecast.get("proj_last_year")
+                raw_ly  = forecast.get("last_year_revenue")
+                st.metric("3️⃣ Mismo mes año anterior",
+                          fmt_currency(proj_ly) if proj_ly else "Sin datos",
+                          delta=(f"base {fmt_currency(raw_ly)}" if raw_ly else None),
+                          delta_color="off",
+                          help="Peso: 20% · total año pasado × crecimiento YoY del tramo")
             d4, d5, d6 = st.columns(3)
             with d4:
                 seasonal = forecast.get("proj_seasonal")
                 yrs = forecast.get("seasonal_years", 0)
                 label = f"4️⃣ Estacionalidad ({yrs} años)" if yrs > 0 else "4️⃣ Estacionalidad histórica"
-                st.metric(label, fmt_currency(seasonal) if seasonal else "Sin datos", help="Peso: 10%")
+                st.metric(label, fmt_currency(seasonal) if seasonal else "Sin datos", help="Peso: 5%")
             with d5:
                 acc = forecast.get("acceleration_factor", 1.0)
                 arrow = "📈" if acc > 1 else "📉" if acc < 1 else "➡️"
diff --git a/extractors/my_sales.py b/extractors/my_sales.py
index 1d08eed..6a252c8 100644
--- a/extractors/my_sales.py
+++ b/extractors/my_sales.py
@@ -291,47 +291,70 @@ class MySalesExtractor:
         proj3_orders  = proj1_orders
         ly_revenue    = None
 
-        # Intentar con año anterior desde historial Dropbox
+        # Año anterior desde historial Dropbox (mismo mes).
+        # FIX: antes crecía por daily_avg / (ly_revenue/dias_mes); ese
+        # divisor cancelaba ly_revenue y el factor colapsaba al promedio
+        # plano (= Factor 1), ignorando por completo el año pasado. Ahora
+        # crece el TOTAL del año pasado por la tasa YoY del MISMO tramo
+        # transcurrido (días 1..N), que sí refleja el crecimiento real.
+        # Topeada 0.5x–3x para que un arranque atípico no la dispare.
         try:
             df_ly = self._load_historical_month(today.year - 1, today.month)
             if df_ly is not None and not df_ly.empty:
-                df_ly_paid = df_ly[df_ly["status"] == "paid"]
+                df_ly_paid = df_ly[df_ly["status"] == "paid"].copy()
+                if "date" not in df_ly_paid.columns:
+                    df_ly_paid["date"] = pd.to_datetime(df_ly_paid["date_created"]).dt.date
                 ly_revenue = float(df_ly_paid["total_amount"].sum())
-                if ly_revenue > 0:
-                    ly_daily   = ly_revenue / days_in_month
-                    growth_factor = daily_avg_revenue / ly_daily if ly_daily > 0 else 1
-                    proj3_revenue = ly_revenue * growth_factor
-                    proj3_units   = float(df_ly_paid["quantity"].sum()) * growth_factor
-                    proj3_orders  = df_ly_paid["order_id"].nunique() * growth_factor
+
+                ly_dim    = calendar.monthrange(today.year - 1, today.month)[1]
+                ly_cutoff = date(today.year - 1, today.month, min(days_elapsed, ly_dim))
+                ly_period = float(df_ly_paid[df_ly_paid["date"] <= ly_cutoff]["total_amount"].sum())
+
+                if ly_revenue > 0 and ly_period > 0:
+                    yoy_rate = max(0.5, min(3.0, revenue_so_far / ly_period))
+                    proj3_revenue = ly_revenue * yoy_rate
+                    proj3_units   = float(df_ly_paid["quantity"].sum()) * yoy_rate
+                    proj3_orders  = df_ly_paid["order_id"].nunique()    * yoy_rate
         except Exception:
             pass
 
-        # ── Factor 4: Estacionalidad histórica (15%) ──────────────
-        # Promedio de este mismo mes en los últimos años disponibles en Dropbox
+        # ── Factor 4: Estacionalidad histórica (5%) ───────────────
+        # Para cada año disponible en Dropbox crece ESE mes completo por la
+        # tasa YoY de su mismo tramo (días 1..N) contra este año, y promedia.
+        # FIX: la versión anterior dividía por el promedio del mes entero y
+        # la base estacional se cancelaba (colapsaba al Factor 1). Con 1 año
+        # coincide con F3; con 2+ suaviza como estimador multi-anual.
         proj4_revenue = proj1_revenue
+        proj4_units   = proj1_units
+        proj4_orders  = proj1_orders
         seasonal_revenues = []
+        seasonal_projs    = []
         for yr in range(today.year - 3, today.year):
             try:
                 df_hist = self._load_historical_month(yr, today.month)
-                if df_hist is not None and not df_hist.empty:
-                    df_hist_paid = df_hist[df_hist["status"] == "paid"]
-                    hist_rev = float(df_hist_paid["total_amount"].sum())
-                    if hist_rev > 0:
-                        seasonal_revenues.append(hist_rev)
+                if df_hist is None or df_hist.empty:
+                    continue
+                df_hist = df_hist[df_hist["status"] == "paid"].copy()
+                if "date" not in df_hist.columns:
+                    df_hist["date"] = pd.to_datetime(df_hist["date_created"]).dt.date
+                hist_full = float(df_hist["total_amount"].sum())
+                if hist_full <= 0:
+                    continue
+                seasonal_revenues.append(hist_full)
+
+                h_dim    = calendar.monthrange(yr, today.month)[1]
+                h_cutoff = date(yr, today.month, min(days_elapsed, h_dim))
+                hist_period = float(df_hist[df_hist["date"] <= h_cutoff]["total_amount"].sum())
+                if hist_period > 0:
+                    rate = max(0.5, min(3.0, revenue_so_far / hist_period))
+                    seasonal_projs.append(hist_full * rate)
             except Exception:
                 pass
 
-        if seasonal_revenues:
-            avg_seasonal = sum(seasonal_revenues) / len(seasonal_revenues)
-            # Ajustar por crecimiento actual vs histórico
-            if avg_seasonal > 0:
-                growth_rate   = daily_avg_revenue / (avg_seasonal / days_in_month)
-                proj4_revenue = avg_seasonal * growth_rate
-                proj4_units   = proj1_units * (proj4_revenue / proj1_revenue) if proj1_revenue > 0 else proj1_units
-                proj4_orders  = proj1_orders * (proj4_revenue / proj1_revenue) if proj1_revenue > 0 else proj1_orders
-        else:
-            proj4_units  = proj1_units
-            proj4_orders = proj1_orders
+        if seasonal_projs:
+            proj4_revenue = sum(seasonal_projs) / len(seasonal_projs)
+            proj4_units   = proj1_units  * (proj4_revenue / proj1_revenue) if proj1_revenue > 0 else proj1_units
+            proj4_orders  = proj1_orders * (proj4_revenue / proj1_revenue) if proj1_revenue > 0 else proj1_orders
 
         # ── Factor 5: Velocidad de crecimiento reciente (10%) ─────
         # Compara promedio últimos 3 días vs días 4-10
