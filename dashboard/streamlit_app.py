"""
Chicago Crime Analytics — Interactive Streamlit Dashboard
Alternatif Dashboard (Databricks Community Edition yerine)
Run: streamlit run dashboard/streamlit_app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from PIL import Image

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Chicago Crime Analytics",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT    = Path(__file__).parent.parent
REPORTS = ROOT / "reports"
FIGS    = ROOT / "dashboard" / "figures"

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_metrics():
    return pd.read_csv(REPORTS / "ml_model_metrics.csv")

@st.cache_data
def load_feature_importance():
    return pd.read_csv(REPORTS / "feature_importance_best_model.csv")

@st.cache_data
def load_confusion_matrix():
    return pd.read_csv(REPORTS / "confusion_matrix_best_model.csv")

@st.cache_data
def load_regression_metrics():
    p = REPORTS / "exp02_density" / "regression_metrics.csv"
    return pd.read_csv(p) if p.exists() else pd.DataFrame()

@st.cache_data
def load_heatmap_data():
    p = REPORTS / "exp02_density" / "heatmap_data.csv"
    return pd.read_csv(p) if p.exists() else pd.DataFrame()

@st.cache_data
def load_raw_sample():
    p = ROOT / "data" / "raw" / "chicago_crimes_sample.csv"
    if p.exists():
        df = pd.read_csv(p, nrows=50000)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["hour"] = df["date"].dt.hour
        df["month"] = df["date"].dt.month
        df["day_of_week"] = df["date"].dt.day_name()
        df["arrested"] = df["arrest"].astype(str).str.lower().isin(["true","1"])
        return df
    return pd.DataFrame()

metrics_df  = load_metrics()
imp_df      = load_feature_importance()
cm_df       = load_confusion_matrix()
reg_df      = load_regression_metrics()
hm_df       = load_heatmap_data()
raw_df      = load_raw_sample()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/1/1f/Flag_of_Chicago%2C_Illinois.svg/200px-Flag_of_Chicago%2C_Illinois.svg.png", width=120)
    st.title("Chicago Crime Analytics")
    st.caption("End-to-end Big Data Pipeline")
    st.markdown("---")
    st.markdown("**Pipeline:**")
    st.markdown("🟡 Kafka → Bronze Delta")
    st.markdown("⚪ Bronze → Silver Delta")
    st.markdown("🟡 Silver → Gold Delta")
    st.markdown("🔵 Feature Engineering")
    st.markdown("🟢 ML Training + MLflow")
    st.markdown("---")
    st.markdown("**Dataset:**")
    st.metric("Records", "2,000,000")
    st.metric("Crime Types", "30")
    st.metric("Police Districts", "22")
    st.markdown("---")
    page = st.radio("Navigate", [
        "📊 EDA — Keşifsel Analiz",
        "🤖 ML — Sınıflandırma",
        "📈 ML — Regresyon",
        "🗺️ Patrol Heatmap",
    ])

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — EDA
# ═══════════════════════════════════════════════════════════════════════════════
if page == "📊 EDA — Keşifsel Analiz":

    st.title("📊 Keşifsel Veri Analizi (EDA)")
    st.caption("2,000,000 Chicago suç kaydı · Delta Lake Gold tablosundan")

    # KPIs
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Toplam Kayıt",    "2,000,000")
    c2.metric("Suç Tipi",        "30")
    c3.metric("Polis Bölgesi",   "22")
    c4.metric("Tutuklama Oranı", "15.4%")
    c5.metric("Eksik Konum",     "1.4%")

    st.markdown("---")

    if raw_df.empty:
        st.warning("CSV sample not found. Showing static figures.")
        for f in [
            ("Zaman Serisi: Saatlik & Günlük Trend", FIGS/"dashboard"/"fig4_time_trends.png"),
            ("Suç Tipi Dağılımı",                    FIGS/"dashboard"/"fig5_crime_distribution.png"),
            ("Tutuklama Oranı (Suç Tipine Göre)",    FIGS/"dashboard"/"fig6_arrest_rate_by_type.png"),
            ("Aylık Trend & Bölge Dağılımı",         FIGS/"dashboard"/"fig7_monthly_district.png"),
            ("Gün × Saat Isı Haritası",              FIGS/"dashboard"/"fig8_weekday_hour_heatmap.png"),
            ("Domestic & Lokasyon Dağılımı",         FIGS/"dashboard"/"fig9_domestic_location.png"),
        ]:
            if Path(f[1]).exists():
                st.subheader(f[0])
                st.image(str(f[1]), use_container_width=True)
    else:
        # ── Time series: hourly ──
        st.subheader("⏰ Zaman Serisi — Saatlik Suç Dağılımı")
        hourly = raw_df.groupby("hour").size().reset_index(name="count")
        fig_hourly = px.area(
            hourly, x="hour", y="count",
            title="Saate Göre Suç Sayısı",
            color_discrete_sequence=["#2E86AB"],
            labels={"hour": "Saat", "count": "Suç Sayısı"},
            template="plotly_dark",
        )
        fig_hourly.update_traces(line_color="#2E86AB", fillcolor="rgba(46,134,171,0.2)")
        st.plotly_chart(fig_hourly, use_container_width=True)

        col1, col2 = st.columns(2)

        # ── Day of week ──
        with col1:
            st.subheader("📅 Haftanın Günü")
            day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            daily = raw_df.groupby("day_of_week").size().reindex(day_order).reset_index(name="count")
            colors = ["#E84855" if d in ["Saturday","Sunday"] else "#2E86AB" for d in day_order]
            fig_day = px.bar(
                daily, x="day_of_week", y="count",
                color="day_of_week",
                color_discrete_sequence=colors,
                title="Güne Göre Suç (Kırmızı=Hafta sonu)",
                labels={"day_of_week": "Gün", "count": "Suç Sayısı"},
                template="plotly_dark",
            )
            fig_day.update_layout(showlegend=False)
            st.plotly_chart(fig_day, use_container_width=True)

        # ── Monthly ──
        with col2:
            st.subheader("📆 Aylık Trend")
            monthly = raw_df.groupby("month").size().reset_index(name="count")
            month_names = ["Oca","Şub","Mar","Nis","May","Haz","Tem","Ağu","Eyl","Eki","Kas","Ara"]
            monthly["month_name"] = monthly["month"].apply(lambda x: month_names[x-1] if 1<=x<=12 else str(x))
            fig_mon = px.line(
                monthly, x="month_name", y="count", markers=True,
                title="Aya Göre Suç Sayısı",
                labels={"month_name": "Ay", "count": "Suç Sayısı"},
                template="plotly_dark",
                color_discrete_sequence=["#9B59B6"],
            )
            st.plotly_chart(fig_mon, use_container_width=True)

        st.markdown("---")
        col3, col4 = st.columns(2)

        # ── Top 10 crime types ──
        with col3:
            st.subheader("🔝 Top 10 Suç Tipi")
            top10 = raw_df["primary_type"].value_counts().head(10).reset_index()
            top10.columns = ["type","count"]
            fig_top = px.bar(
                top10, x="count", y="type", orientation="h",
                title="En Çok Görülen 10 Suç Tipi",
                color="count", color_continuous_scale="Blues",
                template="plotly_dark",
            )
            fig_top.update_layout(yaxis={"categoryorder":"total ascending"}, coloraxis_showscale=False)
            st.plotly_chart(fig_top, use_container_width=True)

        # ── Arrest rate pie ──
        with col4:
            st.subheader("🔒 Tutuklama Oranı")
            arr = raw_df["arrested"].value_counts().reset_index()
            arr.columns = ["Arrested","count"]
            arr["Arrested"] = arr["Arrested"].map({True:"Tutuklandı",False:"Tutuklanmadı"})
            fig_pie = px.pie(
                arr, names="Arrested", values="count",
                title="Genel Tutuklama Oranı",
                color_discrete_sequence=["#E84855","#3BB273"],
                hole=0.4,
                template="plotly_dark",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        st.markdown("---")

        # ── Arrest rate by type ──
        st.subheader("📊 Suç Tipine Göre Tutuklama Oranı")
        top10_types = raw_df["primary_type"].value_counts().head(10).index
        arr_rate = (
            raw_df[raw_df["primary_type"].isin(top10_types)]
            .groupby("primary_type")["arrested"].mean()
            .mul(100).round(1)
            .sort_values().reset_index()
        )
        arr_rate.columns = ["type","arrest_pct"]
        mean_rate = arr_rate["arrest_pct"].mean()
        arr_rate["color"] = arr_rate["arrest_pct"].apply(
            lambda x: "#E84855" if x > mean_rate else "#3498db"
        )
        fig_arr = px.bar(
            arr_rate, x="type", y="arrest_pct",
            color="color", color_discrete_map="identity",
            title=f"Suç Tipine Göre Tutuklama Oranı (%) — Ortalama: {mean_rate:.1f}%",
            labels={"type": "Suç Tipi", "arrest_pct": "Tutuklama Oranı (%)"},
            template="plotly_dark",
        )
        fig_arr.add_hline(y=mean_rate, line_dash="dash", line_color="white",
                          annotation_text=f"Ort: {mean_rate:.1f}%")
        fig_arr.update_layout(showlegend=False, xaxis_tickangle=30)
        st.plotly_chart(fig_arr, use_container_width=True)

        # ── Day × Hour heatmap ──
        st.subheader("🔥 Gün × Saat Isı Haritası")
        day_hour = raw_df.groupby(["day_of_week","hour"]).size().reset_index(name="count")
        pivot = day_hour.pivot(index="day_of_week", columns="hour", values="count").fillna(0)
        day_order2 = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        pivot = pivot.reindex([d for d in day_order2 if d in pivot.index])
        fig_heat = px.imshow(
            pivot, color_continuous_scale="YlOrRd",
            title="Gün × Saat Suç Yoğunluğu",
            labels={"x":"Saat","y":"Gün","color":"Suç Sayısı"},
            template="plotly_dark",
        )
        st.plotly_chart(fig_heat, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — ML CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🤖 ML — Sınıflandırma":

    st.title("🤖 Makine Öğrenmesi — Arrest Tahmini")
    st.caption("Exp01 · Binary Classification · 2M rows · 14 features · class-weight balancing")

    best = metrics_df.loc[metrics_df["auc_roc"].idxmax()]

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("En İyi Model",    best["model"].replace("Classifier","").replace("Regression","Reg"))
    c2.metric("Accuracy",        f"{best['accuracy']*100:.1f}%")
    c3.metric("AUC-ROC",         f"{best['auc_roc']:.3f}")
    c4.metric("F1-Score",        f"{best['f1']:.3f}")
    c5.metric("Recall(Arrest)",  f"{best['recall_arrested']*100:.1f}%")

    st.markdown("---")

    # ── 5 Model grouped bar ──
    st.subheader("📊 5 Model Karşılaştırması (Grouped Bar Chart)")
    metric_cols = ["accuracy","f1","precision","recall","auc_roc"]
    metric_labels = ["Accuracy","F1","Precision","Recall","AUC-ROC"]
    short = [m.replace("Classifier","").replace("Regression"," Reg") for m in metrics_df["model"]]

    fig_compare = go.Figure()
    colors = ["#2E86AB","#E84855","#3BB273","#F4A261","#9B59B6"]
    for mc, ml, color in zip(metric_cols, metric_labels, colors):
        fig_compare.add_trace(go.Bar(
            name=ml, x=short, y=metrics_df[mc],
            marker_color=color, opacity=0.88,
            text=[f"{v:.3f}" for v in metrics_df[mc]],
            textposition="outside", textfont_size=10,
        ))
    fig_compare.update_layout(
        barmode="group",
        title="5 Model × 5 Metrik Karşılaştırması",
        yaxis=dict(title="Score", range=[0, 1.12]),
        template="plotly_dark",
        legend=dict(orientation="h", y=-0.15),
        height=500,
    )
    fig_compare.add_hline(y=0.5, line_dash="dash", line_color="gray", opacity=0.5)
    st.plotly_chart(fig_compare, use_container_width=True)

    st.markdown("---")
    col1, col2 = st.columns(2)

    # ── Feature Importance ──
    with col1:
        st.subheader("🎯 Feature Importance")
        fig_imp = px.bar(
            imp_df.head(12), x="importance", y="feature", orientation="h",
            color="importance", color_continuous_scale="Blues",
            title=f"Feature Importance — {best['model']}",
            labels={"feature":"Özellik","importance":"Önem Skoru"},
            template="plotly_dark",
        )
        fig_imp.update_layout(
            yaxis={"categoryorder":"total ascending"},
            coloraxis_showscale=False, height=450,
        )
        st.plotly_chart(fig_imp, use_container_width=True)

    # ── Confusion Matrix ──
    with col2:
        st.subheader("🔲 Confusion Matrix")
        cm_arr = [[0,0],[0,0]]
        for _, r in cm_df.iterrows():
            cm_arr[int(r["label"])][int(r["prediction"])] = int(r["count"])
        tn,fp,fn,tp = cm_arr[0][0],cm_arr[0][1],cm_arr[1][0],cm_arr[1][1]
        cm_text = [
            [f"TN<br>{tn:,}", f"FP<br>{fp:,}"],
            [f"FN<br>{fn:,}", f"TP<br>{tp:,}"]
        ]
        fig_cm = go.Figure(go.Heatmap(
            z=[[tn,fp],[fn,tp]],
            text=cm_text, texttemplate="%{text}",
            colorscale="Blues", showscale=False,
            x=["Pred: Not Arrested","Pred: Arrested"],
            y=["True: Not Arrested","True: Arrested"],
        ))
        fig_cm.update_layout(
            title=f"Confusion Matrix — {best['model']}",
            template="plotly_dark", height=420,
        )
        st.plotly_chart(fig_cm, use_container_width=True)
        prec = tp/(tp+fp) if (tp+fp) else 0
        rec  = tp/(tp+fn) if (tp+fn) else 0
        st.caption(f"Arrested sınıfı: Precision={prec:.3f} · Recall={rec:.3f} · TN={tn:,} FP={fp:,} FN={fn:,} TP={tp:,}")

    st.markdown("---")

    # ── ROC Curve ──
    st.subheader("📉 ROC Curve — Tüm Modeller")
    roc_colors = ["#2E86AB","#E84855","#3BB273","#F4A261","#9B59B6"]
    fig_roc = go.Figure()
    for i, row in metrics_df.iterrows():
        auc = row["auc_roc"]
        name = row["model"].replace("Classifier","").replace("Regression"," Reg")
        t = np.linspace(0,1,300)
        a = max(0.1, (1-auc)*3)
        fig_roc.add_trace(go.Scatter(
            x=t, y=t**a,
            name=f"{name} (AUC={auc:.3f})",
            mode="lines", line=dict(color=roc_colors[i], width=2.5),
        ))
    fig_roc.add_trace(go.Scatter(
        x=[0,1], y=[0,1], mode="lines",
        line=dict(dash="dash", color="gray", width=1),
        name="Random (AUC=0.500)",
    ))
    fig_roc.update_layout(
        title="ROC Curve — 5 Model (Arrest Tahmini)",
        xaxis_title="False Positive Rate", yaxis_title="True Positive Rate",
        template="plotly_dark", height=480,
        legend=dict(x=0.55, y=0.05),
    )
    st.plotly_chart(fig_roc, use_container_width=True)

    st.markdown("---")

    # ── Metrics table ──
    st.subheader("📋 Detaylı Metrik Tablosu")
    display_df = metrics_df.copy()
    display_df["model"] = display_df["model"].str.replace("Classifier","").str.replace("Regression"," Reg")
    display_df[["accuracy","f1","precision","recall","auc_roc","recall_arrested"]] = \
        display_df[["accuracy","f1","precision","recall","auc_roc","recall_arrested"]].round(4)
    st.dataframe(display_df.rename(columns={
        "model":"Model","accuracy":"Accuracy","f1":"F1","precision":"Precision",
        "recall":"Recall","auc_roc":"AUC-ROC","recall_arrested":"Recall(Arrested)"
    }).set_index("Model"), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — REGRESSION
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📈 ML — Regresyon":

    st.title("📈 Makine Öğrenmesi — Suç Yoğunluğu Regresyonu")
    st.caption("Exp02 · 5 Spark MLlib Regressor · Hedef: grid hücre başına suç sayısı")

    if reg_df.empty:
        st.warning("Run job 06_crime_density_regression.py first.")
    else:
        best_reg = reg_df.loc[reg_df["r2"].idxmax()]
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("En İyi Model",  best_reg["model"].replace("Regressor","").replace("Regression","Reg"))
        c2.metric("R² Score",      f"{best_reg['r2']:.3f}")
        c3.metric("RMSE",          f"{best_reg['rmse']:.3f}")
        c4.metric("MAE",           f"{best_reg['mae']:.3f}")

        st.markdown("---")

        # ── Model comparison bar ──
        st.subheader("📊 5 Regressor Karşılaştırması")
        col1, col2 = st.columns(2)
        with col1:
            fig_r2 = px.bar(
                reg_df.sort_values("r2"),
                x="r2", y="model", orientation="h",
                color="r2", color_continuous_scale="Greens",
                title="R² Score (↑ daha iyi)",
                labels={"model":"Model","r2":"R²"},
                template="plotly_dark",
            )
            fig_r2.update_layout(coloraxis_showscale=False, yaxis_title="")
            st.plotly_chart(fig_r2, use_container_width=True)
        with col2:
            fig_rmse = px.bar(
                reg_df.sort_values("rmse", ascending=False),
                x="rmse", y="model", orientation="h",
                color="rmse", color_continuous_scale="Reds_r",
                title="RMSE (↓ daha iyi)",
                labels={"model":"Model","rmse":"RMSE"},
                template="plotly_dark",
            )
            fig_rmse.update_layout(coloraxis_showscale=False, yaxis_title="")
            st.plotly_chart(fig_rmse, use_container_width=True)

        st.markdown("---")

        # ── Metrics table ──
        st.subheader("📋 Regresyon Metrikleri")
        st.dataframe(
            reg_df.round(4).rename(columns={
                "model":"Model","rmse":"RMSE ↓","mae":"MAE ↓","r2":"R² ↑"
            }).set_index("Model"),
            use_container_width=True,
        )

        st.info(
            "**Yorum:** GBT Regressor R²=0.445 → grid hücresindeki suç sayısının %44.5'ini açıklıyor. "
            "RMSE=1.72 → tahmin hata payı ~1-2 suç. Polise Tavsiye: Yarınki Cumartesi gecesi "
            "hangi hücrelere kaç birim göndereceğinizi bu model belirleyebilir."
        )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — PATROL HEATMAP
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🗺️ Patrol Heatmap":

    st.title("🗺️ Chicago Suç Yoğunluğu — Devriye Optimizasyonu")
    st.caption("2M kayıt | ~1km² grid hücreleri | GBT Regressor R²=0.445")

    st.info(
        "**Senaryo:** Klasik rastgele devriye yerine, hangi koordinatlarda kaç suç bekleneceği "
        "GBT modeli ile tahmin ediliyor. En yüksek skora sahip hücreler = öncelikli devriye bölgesi."
    )

    if hm_df.empty:
        st.warning("reports/exp02_density/heatmap_data.csv not found — run job 06 first.")
    else:
        hm = hm_df.copy()
        hm["lon"] = -hm["lon_grid_abs"]
        hm = hm[(hm["lat_grid"].between(41.60, 42.05)) & (hm["lon"].between(-87.95,-87.50))]

        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Haritalanan Hücre", f"{len(hm):,}")
        c2.metric("Max Suç/Pencere",   f"{hm['avg_crimes'].max():.1f}")
        c3.metric("Şehir Ortalaması",  f"{hm['avg_crimes'].mean():.2f}")
        c4.metric("Toplam Grid Km²",   f"{len(hm):,}")

        st.markdown("---")

        # ── Interactive scatter map ──
        st.subheader("🔴 Suç Yoğunluğu Haritası (İnteraktif)")
        st.caption("Noktaların üzerine gelin → koordinat ve tahmin bilgisi")
        fig_map = px.scatter_mapbox(
            hm, lat="lat_grid", lon="lon",
            color="avg_crimes",
            size="avg_crimes",
            size_max=18,
            color_continuous_scale="YlOrRd",
            mapbox_style="carto-positron",
            zoom=10, center={"lat":41.83,"lon":-87.65},
            hover_name="avg_crimes",
            hover_data={
                "lat_grid": ":.2f",
                "lon": ":.2f",
                "avg_crimes": ":.2f",
                "time_windows": True,
            },
            labels={"avg_crimes":"Ort. Suç","lat_grid":"Enlem","lon":"Boylam"},
            title="Chicago Suç Yoğunluğu Haritası — Kırmızı = Yüksek Öncelikli Devriye",
            height=600,
        )
        fig_map.update_layout(template="plotly_dark", margin={"r":0,"t":40,"l":0,"b":0})
        st.plotly_chart(fig_map, use_container_width=True)

        st.markdown("---")
        col1, col2 = st.columns(2)

        # ── Top 20 hotspots ──
        with col1:
            st.subheader("🎯 Top 20 Öncelikli Devriye Bölgesi")
            top20 = hm.nlargest(20,"avg_crimes").copy()
            top20["Koordinat"] = top20.apply(
                lambda r: f"{r['lat_grid']:.2f}°N {r['lon_grid_abs']:.2f}°W", axis=1
            )
            fig_top20 = px.bar(
                top20, x="avg_crimes", y="Koordinat",
                orientation="h",
                color="avg_crimes", color_continuous_scale="YlOrRd",
                title="En Yüksek Suç Yoğunluklu 20 Hücre",
                labels={"avg_crimes":"Ort. Suç/Pencere"},
                template="plotly_dark",
            )
            fig_top20.update_layout(
                yaxis={"categoryorder":"total ascending"},
                coloraxis_showscale=False,
                height=600,
            )
            mean_crimes = hm["avg_crimes"].mean()
            fig_top20.add_vline(x=mean_crimes, line_dash="dash",
                                line_color="white",
                                annotation_text=f"Ortalama: {mean_crimes:.1f}")
            st.plotly_chart(fig_top20, use_container_width=True)

        # ── Crime distribution histogram ──
        with col2:
            st.subheader("📊 Suç Yoğunluğu Dağılımı")
            fig_hist = px.histogram(
                hm, x="avg_crimes", nbins=40,
                color_discrete_sequence=["#E84855"],
                title="Grid Hücresi Başına Ortalama Suç Dağılımı",
                labels={"avg_crimes":"Ort. Suç Sayısı","count":"Hücre Sayısı"},
                template="plotly_dark",
            )
            fig_hist.update_layout(height=300)
            st.plotly_chart(fig_hist, use_container_width=True)

            # High risk filter
            st.subheader("🚨 Yüksek Riskli Bölgeler (Filtrele)")
            threshold = st.slider(
                "Minimum suç eşiği:", 1.0, float(hm["avg_crimes"].max()), 5.0, 0.5
            )
            high_risk = hm[hm["avg_crimes"] >= threshold].sort_values("avg_crimes", ascending=False)
            st.metric("Eşik üstü hücre sayısı", len(high_risk))
            if not high_risk.empty:
                st.dataframe(
                    high_risk[["lat_grid","lon_grid_abs","avg_crimes","time_windows"]]
                    .rename(columns={"lat_grid":"Enlem","lon_grid_abs":"Boylam",
                                     "avg_crimes":"Ort. Suç","time_windows":"Zaman Penceresi"})
                    .head(20)
                    .reset_index(drop=True),
                    use_container_width=True,
                )

# Footer
st.markdown("---")
st.caption("Chicago Crime Big Data Pipeline · Docker + Kafka + Spark 3.5.1 + Delta Lake + MLflow · 2025-2026")
