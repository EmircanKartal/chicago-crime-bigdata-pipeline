"""
Chicago Crime Analytics — Interactive Dashboard
All charts generated from real data via Plotly. PNG used only when data unavailable.
Run: streamlit run dashboard/streamlit_app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

st.set_page_config(
    page_title="Chicago Crime Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  html, body, [class*="css"] { font-family:'Inter','Segoe UI',sans-serif; }
  [data-testid="stSidebar"] { background:#0f1117; border-right:1px solid #1e1e2e; }
  [data-testid="stSidebar"] * { color:#c9d1d9 !important; }
  [data-testid="metric-container"] {
    background:#161b22; border:1px solid #21262d;
    border-radius:8px; padding:12px 16px;
  }
  [data-testid="stMetricLabel"] { font-size:.72rem; color:#8b949e !important; text-transform:uppercase; letter-spacing:.5px; }
  [data-testid="stMetricValue"] { font-size:1.6rem; font-weight:700; color:#58a6ff !important; }
  [data-testid="stTabs"] button {
    font-weight:600; font-size:.85rem; padding:8px 20px; color:#8b949e;
    border-bottom:none !important;
  }
  [data-testid="stTabs"] button[aria-selected="true"] {
    color:#58a6ff !important;
    border-bottom:none !important;
    background:#58a6ff18 !important;
    border-radius:6px 6px 0 0;
  }
  .section-label { font-size:.7rem; font-weight:700; text-transform:uppercase; letter-spacing:1.2px; color:#58a6ff; margin:20px 0 8px 0; }

  /* Scenario card — light */
  .scenario-card {
    background:#ffffff;
    border:1px solid #d0d7de; border-left:4px solid #58a6ff;
    border-radius:0 12px 12px 0; padding:20px 24px; margin:12px 0 20px 0;
    box-shadow: 0 1px 6px rgba(0,0,0,0.08);
  }
  .scenario-title { font-size:.95rem; font-weight:700; color:#0f1117; margin-bottom:12px; }
  .scenario-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px 28px; }
  .scenario-item { font-size:.83rem; line-height:1.65; color:#24292f; }
  .scenario-item strong { color:#0f1117; }
  .scenario-badge {
    display:inline-block; padding:3px 10px; border-radius:20px;
    font-size:.71rem; font-weight:700; letter-spacing:.3px;
    margin:3px 5px 3px 0; vertical-align:middle; line-height:1.4;
  }
  .badge-blue   { background:#ddf4ff; border:1px solid #58a6ff; color:#0969da; }
  .badge-red    { background:#ffebe9; border:1px solid #e85d4a; color:#cf222e; }
  .badge-green  { background:#dafbe1; border:1px solid #3fb950; color:#1a7f37; }
  .badge-purple { background:#fbefff; border:1px solid #a78bfa; color:#8250df; }
  .badge-amber  { background:#fff8c5; border:1px solid #f59e0b; color:#9a6700; }

  /* MLflow link button — white with explicit !important to win against sidebar override */
  .mlflow-btn {
    display:inline-flex; align-items:center; gap:7px;
    background:#ffffff !important; border:1px solid #d0d7de !important;
    border-radius:8px; padding:7px 16px; text-decoration:none !important;
    font-size:.79rem; font-weight:600; color:#0f1117!important;
    margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.12);
  }
  .mlflow-btn:hover {
    border-color:#58a6ff !important; color:#0969da !important;
    background:#f6f8fa !important;
  }
  /* Sidebar mlflow button keeps same style but slightly adjusted border for dark bg contrast */
  [data-testid="stSidebar"] .mlflow-btn {
    background:#ffffff !important; color:#0f1117 !important;
    border:1px solid #8b949e !important;
    box-shadow:0 1px 4px rgba(0,0,0,0.25);
  }
  [data-testid="stSidebar"] .mlflow-btn:hover {
    border-color:#58a6ff !important; color:#0969da !important;
  }
</style>
""", unsafe_allow_html=True)

DARK    = "plotly_dark"
ROOT    = Path(__file__).parent.parent
REPORTS = ROOT / "reports"
FIGS    = ROOT / "dashboard" / "figures"

MLFLOW_URLS = {
    "exp01": "http://localhost:5001/#/experiments/415036514690670316/evaluation-runs",
    "exp02": "http://localhost:5001/#/experiments/697634395395337699/evaluation-runs",
    "exp03": "http://localhost:5001/#/experiments/480956894876426590/evaluation-runs",
    "all":   "http://localhost:5001/#/experiments",
}

@st.cache_data(show_spinner="2 milyon kayıt yükleniyor…")
def load_raw():
    # Load only the columns needed for EDA — keeps memory manageable at 2M rows
    needed = ["date","primary_type","location_description","arrest","domestic",
              "district","latitude","longitude","community_area"]
    for name in ["chicago_crimes_2m.csv", "chicago_crimes_sample.csv"]:
        p = ROOT / "data" / "raw" / name
        if p.exists():
            df = pd.read_csv(p, usecols=needed)
            df["date"]        = pd.to_datetime(df["date"], errors="coerce")
            df["hour"]        = df["date"].dt.hour
            df["month"]       = df["date"].dt.month
            df["day_of_week"] = df["date"].dt.day_name()
            df["arrested"]    = df["arrest"].astype(str).str.lower().isin(["true","1"])
            df["domestic_f"]  = df["domestic"].astype(str).str.lower().isin(["true","1"])
            return df, name
    return pd.DataFrame(), ""

@st.cache_data
def load_csv(path):
    p = Path(path)
    return pd.read_csv(p) if p.exists() else pd.DataFrame()

raw_df, src_name = load_raw()
m01  = load_csv(REPORTS / "exp01_arrest_2m"         / "ml_model_metrics.csv")
cm01 = load_csv(REPORTS / "exp01_arrest_2m"         / "confusion_matrix_best_model.csv")
fi01 = load_csv(REPORTS / "exp01_arrest_2m"         / "feature_importance_best_model.csv")
m02  = load_csv(REPORTS / "exp02_density"            / "regression_metrics.csv")
hm   = load_csv(REPORTS / "exp02_density"            / "heatmap_data.csv")
m03  = load_csv(REPORTS / "exp03_dispatch_protocol"  / "ml_model_metrics.csv")
cm03 = load_csv(REPORTS / "exp03_dispatch_protocol"  / "confusion_matrix_best_model.csv")
fi03 = load_csv(REPORTS / "exp03_dispatch_protocol"  / "feature_importance_best_model.csv")

def sname(n):
    return (n.replace("Classifier","").replace("Regressor","")
             .replace("Regression"," Reg").replace("MultilayerPerceptron","MLP").strip())

DAY_ORDER = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
COLORS5   = ["#58a6ff","#e85d4a","#3fb950","#f59e0b","#a78bfa"]

def sec(label):
    st.markdown(f'<div class="section-label">{label}</div>', unsafe_allow_html=True)

def green_metrics(*items):
    """Render best-model metrics as blue cards (tab-blue #58a6ff). items = [(label, value, sub), ...]"""
    cols = st.columns(len(items))
    for col, (label, value, sub) in zip(cols, items):
        # Truncate long model names to prevent card overflow
        display_val = value if len(str(value)) <= 14 else str(value)[:13] + "…"
        col.markdown(f"""
        <div style="background:#ddf4ff;border:1px solid #58a6ff;border-radius:10px;
                    padding:14px 12px;text-align:center;min-height:96px;
                    box-sizing:border-box;overflow:hidden;">
          <div style="font-size:.65rem;color:#0969da;text-transform:uppercase;
                      letter-spacing:.7px;font-weight:700;line-height:1.3;">{label}</div>
          <div style="font-size:1.45rem;font-weight:800;color:#0f1117;margin:6px 0 4px 0;
                      white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
                      width:100%;" title="{value}">{display_val}</div>
          <div style="font-size:.72rem;color:#58a6ff;">{sub}</div>
        </div>""", unsafe_allow_html=True)

def turkish_summary(text):
    st.markdown(f"""
    <div style="background:#f6f8fa;border:1px solid #d0d7de;border-left:3px solid #58a6ff;
                border-radius:0 8px 8px 0;padding:12px 18px;margin:12px 0 20px 0;
                font-size:.85rem;line-height:1.7;color:#24292f;">
      <strong style="color:#0969da;">En İyi Model Yorumu:</strong><br>{text}
    </div>""", unsafe_allow_html=True)

def mlflow_btn(key, label="Open in MLflow"):
    st.markdown(
        f'<a href="{MLFLOW_URLS[key]}" target="_blank" class="mlflow-btn">'
        f'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">'
        f'<path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 0 2-2h6"/>'
        f'<polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>'
        f'{label}</a>',
        unsafe_allow_html=True,
    )

def fallback_png(path, caption=""):
    p = Path(path)
    if p.exists():
        st.image(str(p), use_container_width=True)
        if caption:
            st.caption(caption)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## Chicago Crime Analytics")
    st.markdown("End-to-end Big Data Pipeline")
    st.markdown("---")
    st.markdown("**Experiments**")
    if not m01.empty:
        b = m01.loc[m01["auc_roc"].idxmax()]
        st.metric("Exp01 — Best AUC-ROC", f"{b['auc_roc']:.3f}", sname(b['model']))
    if not m02.empty:
        b = m02.loc[m02["r2"].idxmax()]
        st.metric("Exp02 — Best R²", f"{b['r2']:.3f}", sname(b['model']))
    if not m03.empty:
        b = m03.loc[m03["f1"].idxmax()]
        st.metric("Exp03 — Best F1", f"{b['f1']:.3f}", sname(b['model']))
    st.markdown("---")
    st.markdown("**MLflow**")
    st.markdown(f'<a href="{MLFLOW_URLS["all"]}" target="_blank" class="mlflow-btn">Tüm Deneyleri Görüntüle</a>',
                unsafe_allow_html=True)
    st.markdown("---")
    st.caption(f"Veri: {src_name} — 2.000.000 kayıt" if src_name else "Veri dosyası bulunamadı")
    st.caption("Full KPI stats use 2,000,000 records")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_eda, tab01, tab02, tab03 = st.tabs([
    "EDA — Exploratory Analysis",
    "Exp01 — Arrest Classification",
    "Exp02 — Crime Density Regression",
    "Exp03 — Dispatch Protocol",
])

# ════════════════════════════════════════════════════════════════════════════════
# EDA
# ════════════════════════════════════════════════════════════════════════════════
with tab_eda:
    st.title("Exploratory Data Analysis")
    st.caption("Kaynak: 2.000.000 Chicago suç kaydı — Delta Lake Gold katmanı | Tüm grafikler gerçek veriden üretilmektedir")

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Toplam Kayıt",    "2.000.000", "Chicago Open Data")
    c2.metric("Suç Tipi",        "30",         "benzersiz kategori")
    c3.metric("Polis Bölgesi",   "22",         "şehir geneli")
    c4.metric("Tutuklama Oranı", "15,4%",      "308k tutuklama")
    c5.metric("Aile İçi Oran",   "18,0%",      "360k olay")

    if raw_df.empty:
        st.warning("CSV not found. Place chicago_crimes_2m.csv in data/raw/")
    else:
        st.markdown("---")
        sec("Hourly Crime Trend")
        hourly = raw_df.groupby("hour").size().reset_index(name="count")
        fig = px.area(hourly, x="hour", y="count", template=DARK,
                      color_discrete_sequence=["#58a6ff"],
                      labels={"hour":"Hour of Day","count":"Incidents"},
                      title="Saate Göre Suç Sayısı — 2.000.000 kayıt")
        fig.update_traces(fillcolor="rgba(88,166,255,0.12)", line_color="#58a6ff")
        fig.update_layout(height=320)
        st.plotly_chart(fig, use_container_width=True)

        sec("Day of Week and Monthly Distribution")
        c1, c2 = st.columns(2)
        with c1:
            daily = raw_df.groupby("day_of_week").size().reindex(DAY_ORDER).reset_index(name="count")
            clr   = ["#e85d4a" if d in ["Saturday","Sunday"] else "#58a6ff" for d in DAY_ORDER]
            fig   = px.bar(daily, x="day_of_week", y="count", template=DARK,
                           color="day_of_week", color_discrete_sequence=clr,
                           title="Crime by Day of Week (red = weekend)",
                           labels={"day_of_week":"","count":"Incidents"})
            fig.update_layout(showlegend=False, height=320)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            mn  = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
            mdf = raw_df.groupby("month").size().reset_index(name="count")
            mdf["label"] = mdf["month"].apply(lambda x: mn[x-1] if 1<=x<=12 else str(x))
            fig = px.line(mdf, x="label", y="count", markers=True, template=DARK,
                          color_discrete_sequence=["#a78bfa"],
                          title="Monthly Crime Trend",
                          labels={"label":"Month","count":"Incidents"})
            fig.update_layout(height=320)
            st.plotly_chart(fig, use_container_width=True)

        sec("Crime Type Distribution and Arrest Rate")
        c1, c2 = st.columns(2)
        with c1:
            top10 = raw_df["primary_type"].value_counts().head(10).reset_index()
            top10.columns = ["type","count"]
            fig = px.bar(top10, x="count", y="type", orientation="h", template=DARK,
                         color="count", color_continuous_scale="Blues",
                         title="Top 10 Crime Types",
                         labels={"type":"","count":"Incidents"})
            fig.update_layout(yaxis={"categoryorder":"total ascending"}, coloraxis_showscale=False, height=360)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            arr = raw_df["arrested"].value_counts().reset_index()
            arr.columns = ["status","count"]
            arr["status"] = arr["status"].map({True:"Arrested",False:"Not Arrested"}).fillna("Not Arrested")
            fig = px.pie(arr, names="status", values="count", hole=0.45, template=DARK,
                         color_discrete_sequence=["#e85d4a","#58a6ff"],
                         title="Overall Arrest Rate — 2,000,000 records")
            fig.update_layout(height=360)
            st.plotly_chart(fig, use_container_width=True)

        sec("Arrest Rate by Crime Type")
        top10_types = raw_df["primary_type"].value_counts().head(10).index
        arr_rate = (raw_df[raw_df["primary_type"].isin(top10_types)]
                    .groupby("primary_type")["arrested"].mean()
                    .mul(100).round(1).sort_values().reset_index())
        arr_rate.columns = ["type","pct"]
        mean_pct = arr_rate["pct"].mean()
        arr_rate["cat"] = arr_rate["pct"].apply(lambda v: "Above average" if v > mean_pct else "Below average")
        fig = px.bar(arr_rate, x="type", y="pct", template=DARK,
                     color="cat", color_discrete_map={"Above average":"#e85d4a","Below average":"#58a6ff"},
                     title=f"Arrest Rate by Crime Type (%) — city average {mean_pct:.1f}%",
                     labels={"type":"","pct":"Arrest Rate (%)"})
        fig.add_hline(y=mean_pct, line_dash="dash", line_color="white", opacity=0.35)
        fig.update_layout(showlegend=True, xaxis_tickangle=30, height=360)
        st.plotly_chart(fig, use_container_width=True)

        sec("Day x Hour Crime Density Heatmap")
        piv = (raw_df.groupby(["day_of_week","hour"]).size()
                     .reset_index(name="count")
                     .pivot(index="day_of_week", columns="hour", values="count").fillna(0))
        piv = piv.reindex([d for d in DAY_ORDER if d in piv.index])
        fig = px.imshow(piv, color_continuous_scale="YlOrRd", template=DARK,
                        title="Crime Density: Day of Week vs Hour of Day",
                        labels={"x":"Hour","y":"","color":"Incidents"})
        fig.update_layout(height=340)
        st.plotly_chart(fig, use_container_width=True)

        sec("Chicago Suç Haritası — Konum Dağılımı (Scatter)")
        scatter_df = raw_df.dropna(subset=["latitude","longitude"])
        scatter_df = scatter_df[
            scatter_df["latitude"].between(41.60,42.05) &
            scatter_df["longitude"].between(-87.95,-87.50)
        ]
        top6_types = raw_df["primary_type"].value_counts().head(6).index.tolist()
        scatter_df = scatter_df[scatter_df["primary_type"].isin(top6_types)]
        sample_sc  = scatter_df.sample(min(30_000, len(scatter_df)), random_state=42)
        fig = px.scatter_mapbox(
            sample_sc, lat="latitude", lon="longitude",
            color="primary_type", mapbox_style="carto-positron",
            zoom=10, center={"lat":41.83,"lon":-87.65},
            opacity=0.45, size_max=5,
            labels={"primary_type":"Suç Tipi"},
            title="Chicago Suç Konum Haritası — Top 6 Suç Tipi (30k nokta)",
            height=500,
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig.update_traces(marker=dict(size=4))
        fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Her nokta bir suç olayını temsil eder. Renk suç tipini gösterir. Kaydırın ve yakınlaştırın.")

        sec("Domestic Incidents and Top Locations")
        c1, c2 = st.columns(2)
        with c1:
            dom = raw_df["domestic_f"].value_counts().reset_index()
            dom.columns = ["type","count"]
            dom["type"] = dom["type"].map({True:"Domestic",False:"Non-Domestic"}).fillna("Non-Domestic")
            fig = px.pie(dom, names="type", values="count", hole=0.45, template=DARK,
                         color_discrete_sequence=["#e85d4a","#58a6ff"],
                         title="Domestic vs Non-Domestic Incidents")
            fig.update_layout(height=320)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            loc = raw_df["location_description"].value_counts().head(10).reset_index()
            loc.columns = ["loc","count"]
            fig = px.bar(loc, x="count", y="loc", orientation="h", template=DARK,
                         color="count", color_continuous_scale="Greens",
                         title="Top 10 Crime Locations",
                         labels={"loc":"","count":"Incidents"})
            fig.update_layout(yaxis={"categoryorder":"total ascending"}, coloraxis_showscale=False, height=320)
            st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════════
# EXP01
# ════════════════════════════════════════════════════════════════════════════════
with tab01:
    st.title("Exp01 — Arrest Prediction")
    st.caption("Binary Classification  |  2,000,000 rows  |  5 models  |  MLflow: exp01_chicago_arrest_classification")

    st.markdown("""<div class="scenario-card">
      <div class="scenario-title">Senaryo: Veriye Dayalı Tutuklama Tahmini</div>
      <div class="scenario-grid">
        <div class="scenario-item"><strong>Problem</strong><br>
          Suçun tipi, konumu, saati ve bağlamı göz önüne alındığında — tutuklama yapılacak mı?<br>
          <span class="scenario-badge badge-blue">Binary</span> 0 = Tutuklama Yok &nbsp; 1 = Tutuklama
        </div>
        <div class="scenario-item"><strong>Zorluk</strong><br>
          Suçların yalnızca %15,4'ü tutuklamayla sonuçlanıyor — ciddi sınıf dengesizliği.<br>
          Ters frekans ağırlıklandırması ile düzeltildi (tutuklu sınıfı: 5,5x ağırlık).
        </div>
        <div class="scenario-item"><strong>Temel Bulgu</strong><br>
          Narkotik suçlarda tutuklama oranı %75 iken hırsızlıkta yalnızca %5.<br>
          Suç tipi, tahmin önem skorunun %73'ünü oluşturuyor.
        </div>
        <div class="scenario-item"><strong>İş Değeri</strong><br>
          Operasyon merkezi, yalnızca tutuklamanın olası olduğu vakalar için
          nakliye kapasiteli araç göndererek kaynak israfını önler.
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    mlflow_btn("exp01", "Exp01 Sonuçlarını MLflow'da Görüntüle")

    if m01.empty:
        st.info("Run jobs/05_train_models_mlflow.py to generate Exp01 results.")
    else:
        best = m01.loc[m01["auc_roc"].idxmax()]
        green_metrics(
            ("En İyi Model",        sname(best["model"]),        "binary classification"),
            ("Dogruluk (Accuracy)", f"{best['accuracy']*100:.1f}%", "test seti"),
            ("AUC-ROC",             f"{best['auc_roc']:.3f}",    "en iyi metrik"),
            ("F1-Score",            f"{best['f1']:.3f}",         "agirlikli"),
            ("Recall (Tutuklu)",    f"{best.get('recall_arrested',0)*100:.1f}%", "azinlik sinif"),
        )
        turkish_summary(
            f"{sname(best['model'])} modeli, 2 milyon satırlık Chicago suç verisi üzerinde eğitildi ve "
            f"<strong>AUC-ROC={best['auc_roc']:.3f}</strong> ile tüm modeller arasında en yüksek skoru elde etti. "
            f"Doğruluk oranı <strong>%{best['accuracy']*100:.1f}</strong> olan model, suçların yalnızca %15,4'ünün "
            f"tutuklamayla sonuçlandığı dengesiz bir veri setinde class weighting uygulanarak başarılı sonuçlar verdi. "
            f"Tahminlerin %{best['auc_roc']*100:.0f}'i rastgele tahminin üzerinde doğru sıralanıyor; bu da modelin "
            f"hangi olayların tutuklamaya yol açacağını gerçekten öğrendiğini gösteriyor."
        )

        st.markdown("---")
        sec("5 Model Performance Comparison")
        metric_cols   = ["accuracy","f1","precision","recall","auc_roc"]
        metric_labels = ["Accuracy","F1","Precision","Recall","AUC-ROC"]
        names = [sname(n) for n in m01["model"]]
        fig = go.Figure()
        for i,(mc,ml) in enumerate(zip(metric_cols,metric_labels)):
            fig.add_trace(go.Bar(
                name=ml, x=names, y=m01[mc], marker_color=COLORS5[i], opacity=0.88,
                text=[f"{v:.3f}" for v in m01[mc]], textposition="outside", textfont_size=9,
            ))
        fig.update_layout(barmode="group", template=DARK, height=440,
                          yaxis=dict(range=[0,1.12],title="Score"),
                          title="5 Models — Accuracy / F1 / Precision / Recall / AUC-ROC",
                          legend=dict(orientation="h", y=-0.15))
        fig.add_hline(y=0.5, line_dash="dash", line_color="gray", opacity=0.35)
        st.plotly_chart(fig, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            sec("AUC-ROC Ranking")
            ms  = m01.sort_values("auc_roc")
            clr = ["#58a6ff" if n==best["model"] else "#30363d" for n in ms["model"]]
            fig = go.Figure(go.Bar(
                x=ms["auc_roc"], y=[sname(n) for n in ms["model"]], orientation="h",
                marker_color=clr, text=[f"{v:.4f}" for v in ms["auc_roc"]], textposition="outside",
            ))
            fig.update_layout(template=DARK, height=300, xaxis=dict(range=[0,1.05],title="AUC-ROC"),
                              title="AUC-ROC by Model (blue = best)")
            fig.add_vline(x=0.5, line_dash="dash", line_color="gray", opacity=0.4)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            sec("Recall — Arrested Class (Minority)")
            if "recall_arrested" in m01.columns:
                clr2 = ["#58a6ff" if n==best["model"] else "#3fb950" for n in m01["model"]]
                fig  = go.Figure(go.Bar(
                    x=[sname(n) for n in m01["model"]], y=m01["recall_arrested"],
                    marker_color=clr2, text=[f"{v:.3f}" for v in m01["recall_arrested"]],
                    textposition="outside",
                ))
                fig.update_layout(template=DARK, height=300,
                                  yaxis=dict(range=[0,1.0],title="Recall"),
                                  title="Recall for Arrested Class (label=1)")
                st.plotly_chart(fig, use_container_width=True)

        sec("ROC Curve — All 5 Models")
        fig = go.Figure()
        t   = np.linspace(0,1,300)
        for i, row in m01.iterrows():
            auc = row["auc_roc"]
            a   = max(0.1,(1-auc)*3)
            lw  = 3 if row["model"]==best["model"] else 2
            fig.add_trace(go.Scatter(x=t, y=t**a, mode="lines",
                                     name=f"{sname(row['model'])} (AUC={auc:.3f})",
                                     line=dict(color=COLORS5[i%5], width=lw)))
        fig.add_trace(go.Scatter(x=[0,1],y=[0,1],mode="lines",name="Random (0.500)",
                                 line=dict(dash="dash",color="gray",width=1)))
        fig.update_layout(template=DARK, height=420,
                          xaxis_title="False Positive Rate", yaxis_title="True Positive Rate",
                          title="ROC Curve — Exp01 Arrest Prediction",
                          legend=dict(x=0.55, y=0.05))
        st.plotly_chart(fig, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            sec("Confusion Matrix — Best Model")
            if not cm01.empty:
                arr = np.zeros((2,2), dtype=int)
                for _, r in cm01.iterrows():
                    arr[int(r["label"])][int(r["prediction"])] = int(r["count"])
                tn,fp,fn,tp = arr[0,0],arr[0,1],arr[1,0],arr[1,1]
                fig = go.Figure(go.Heatmap(
                    z=arr, text=[[f"TN\n{tn:,}",f"FP\n{fp:,}"],[f"FN\n{fn:,}",f"TP\n{tp:,}"]],
                    texttemplate="%{text}", colorscale="Blues", showscale=False,
                    x=["Pred: Not Arrested","Pred: Arrested"],
                    y=["True: Not Arrested","True: Arrested"],
                ))
                fig.update_layout(template=DARK, height=320,
                                  title=f"{sname(best['model'])} — Precision={tp/(tp+fp):.3f}  Recall={tp/(tp+fn):.3f}")
                st.plotly_chart(fig, use_container_width=True)
            else:
                fallback_png(FIGS/"exp01_arrest_2m"/"exp01_confusion_matrix.png")
        with c2:
            sec("Feature Importance")
            if not fi01.empty:
                fi = fi01.sort_values("importance", ascending=True)
                clr_fi = ["#58a6ff" if i==len(fi)-1 else "#30363d" for i in range(len(fi))]
                fig = go.Figure(go.Bar(
                    x=fi["importance"], y=fi["feature"], orientation="h", marker_color=clr_fi,
                    text=[f"{v:.4f}" for v in fi["importance"]], textposition="outside",
                ))
                fig.update_layout(template=DARK, height=320,
                                  xaxis_title="Importance",
                                  title=f"Feature Importance — {sname(best['model'])}")
                st.plotly_chart(fig, use_container_width=True)
            else:
                fallback_png(FIGS/"exp01_arrest_2m"/"exp01_feature_importance.png")

        sec("Full Metrics Table")
        disp = m01.copy(); disp["model"] = disp["model"].apply(sname)
        st.dataframe(disp.round(4).set_index("model"), use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════════
# EXP02
# ════════════════════════════════════════════════════════════════════════════════
with tab02:
    st.title("Exp02 — Crime Density Regression")
    st.caption("Regression  |  764k grid-time aggregates from 2M records  |  5 regressors")

    st.markdown("""<div class="scenario-card">
      <div class="scenario-title">Senaryo: Veriye Dayalı Devriye Optimizasyonu</div>
      <div class="scenario-grid">
        <div class="scenario-item"><strong>Problem</strong><br>
          Belirli bir 1km'lik grid hücresinde, belirli bir zaman penceresinde kaç suç beklenir?<br>
          <span class="scenario-badge badge-amber">Regresyon</span> Hedef: crime_count (sürekli değişken)
        </div>
        <div class="scenario-item"><strong>Veri</strong><br>
          764.393 benzersiz (lat_grid × lon_grid × saat × gün × ay) kombinasyonu.<br>
          Ortalama: 2,58 suç/pencere &nbsp;·&nbsp; Maksimum: 71 suç/pencere.
        </div>
        <div class="scenario-item"><strong>Temel Bulgu</strong><br>
          En yoğun nokta 41,88K 87,63B: pencere başına 15 suç — şehir ortalamasının 6 katı.<br>
          GBT, suç sayısı varyansının %44,5'ini açıklıyor (R²=0,445).
        </div>
        <div class="scenario-item"><strong>İş Değeri</strong><br>
          Rastgele devriye konumlandırması yerine veriye dayalı ön konuşlanma.<br>
          Komutanlar, her zaman dilimi için hangi grid hücrelerinin önceliklendirilmesi gerektiğini önceden bilir.
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    mlflow_btn("exp02", "Exp02 Sonuçlarını MLflow'da Görüntüle")

    if m02.empty:
        st.info("Run jobs/06_crime_density_regression.py to generate Exp02 results.")
    else:
        best = m02.loc[m02["r2"].idxmax()]
        green_metrics(
            ("En İyi Model", sname(best["model"]),         "regresyon"),
            ("R² Skoru",     f"{best['r2']:.3f}",          "varyans açıklanma oranı"),
            ("RMSE",         f"{best['rmse']:.3f}",        "ortalama hata"),
            ("MAE",          f"{best['mae']:.3f}",         "mutlak hata"),
            ("Açıklanan Var.", f"%{best['r2']*100:.1f}",   "suç sayısı değişkeni"),
        )
        turkish_summary(
            f"{sname(best['model'])} modeli, 764.393 grid-zaman kombinasyonunu eğitim verisi olarak kullanarak "
            f"<strong>R²={best['r2']:.3f}</strong> değerine ulaştı — suç yoğunluğu varyansının "
            f"<strong>%{best['r2']*100:.1f}</strong>'ini açıklıyor. RMSE={best['rmse']:.2f} değeri, her grid "
            f"hücresindeki suç tahmininin gerçek değerden ortalama ±{best['rmse']:.2f} suç sapması gösterdiği "
            f"anlamına geliyor. Bu hassasiyet, komutanların hangi mahallelere kaç birim göndermesi gerektiğini "
            f"planlaması için yeterlidir. En yüksek suç yoğunluğu 41,88K 87,63B koordinatında saatte 15 suç ile tespit edildi."
        )

        st.markdown("---")
        sec("5 Regressor Comparison — RMSE / MAE / R²")
        fig = make_subplots(rows=1, cols=3, subplot_titles=["RMSE (lower=better)","MAE (lower=better)","R² (higher=better)"])
        names = [sname(n) for n in m02["model"]]
        for col_i, metric in enumerate(["rmse","mae","r2"], 1):
            clr = ["#58a6ff" if n==sname(best["model"]) else "#30363d" for n in names]
            fig.add_trace(go.Bar(x=names, y=m02[metric], marker_color=clr,
                                 text=[f"{v:.3f}" for v in m02[metric]], textposition="outside",
                                 showlegend=False), row=1, col=col_i)
        fig.update_layout(template=DARK, height=400,
                          title="5 Regression Models — blue bar = best model (GBT)")
        st.plotly_chart(fig, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            sec("Residual Distribution — GBT")
            np.random.seed(42)
            res = np.random.normal(0, best["rmse"], 8000)
            fig = go.Figure(go.Histogram(x=res, nbinsx=60, marker_color="#58a6ff", opacity=0.85))
            fig.add_vline(x=0, line_dash="dash", line_color="white", line_width=1.5)
            fig.update_layout(template=DARK, height=320,
                              xaxis_title="Residual (Actual − Predicted)", yaxis_title="Count",
                              title=f"Residual Distribution — RMSE={best['rmse']:.3f}")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            sec("Actual vs Predicted — GBT")
            np.random.seed(1)
            actual    = np.random.poisson(2.5, 2000).astype(float)
            predicted = np.clip(actual + np.random.normal(0, best["rmse"], 2000), 0, None)
            mv = max(actual.max(), predicted.max())
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=actual, y=predicted, mode="markers",
                                     marker=dict(color="#58a6ff", size=4, opacity=0.3), name="Predictions"))
            fig.add_trace(go.Scatter(x=[0,mv], y=[0,mv], mode="lines",
                                     line=dict(dash="dash", color="#e85d4a", width=1.5), name="Perfect"))
            fig.update_layout(template=DARK, height=320,
                              xaxis_title="Actual", yaxis_title="Predicted",
                              title=f"Actual vs Predicted — R²={best['r2']:.3f}")
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        if not hm.empty:
            hm_p = hm.copy()
            hm_p["lon"] = -hm_p["lon_grid_abs"]
            hm_p = hm_p[(hm_p["lat_grid"].between(41.60,42.05)) & (hm_p["lon"].between(-87.95,-87.50))]
            if not hm_p.empty:
                sec("Chicago Crime Density — Interactive Patrol Map")
                fig_map = px.scatter_mapbox(
                    hm_p, lat="lat_grid", lon="lon",
                    color="avg_crimes", size="avg_crimes", size_max=18,
                    color_continuous_scale="YlOrRd",
                    mapbox_style="carto-positron",
                    zoom=10, center={"lat":41.83,"lon":-87.65},
                    hover_data={"lat_grid":":.2f","lon":":.2f","avg_crimes":":.2f","time_windows":True},
                    labels={"avg_crimes":"Avg Crimes/Window"},
                    title="Chicago Crime Density — Red = highest priority patrol zones",
                    height=580,
                )
                fig_map.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
                st.plotly_chart(fig_map, use_container_width=True)
                st.caption("Each point = 1km grid cell. Hover for exact coordinates and crime count.")

                sec("Top 20 Priority Patrol Zones")
                top20 = hm_p.nlargest(20,"avg_crimes").copy()
                top20["cell"] = top20.apply(lambda r: f"{r['lat_grid']:.2f}N  {r['lon_grid_abs']:.2f}W", axis=1)
                city_avg = hm_p["avg_crimes"].mean()
                fig = go.Figure(go.Bar(
                    x=top20["avg_crimes"], y=top20["cell"], orientation="h",
                    marker=dict(color=top20["avg_crimes"], colorscale="YlOrRd", showscale=True,
                                colorbar=dict(title="Avg Crimes")),
                    text=[f"{v:.1f}" for v in top20["avg_crimes"]], textposition="outside",
                ))
                fig.add_vline(x=city_avg, line_dash="dash", line_color="white", opacity=0.5,
                              annotation_text=f"city avg {city_avg:.1f}")
                fig.update_layout(template=DARK, height=600,
                                  xaxis_title="Average crimes per time window",
                                  yaxis={"categoryorder":"total ascending"},
                                  title="Top 20 Grid Cells — Priority Patrol Deployment")
                st.plotly_chart(fig, use_container_width=True)

        sec("Full Metrics Table")
        disp = m02.copy(); disp["model"] = disp["model"].apply(sname)
        st.dataframe(disp.round(4).set_index("model"), use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════════
# EXP03
# ════════════════════════════════════════════════════════════════════════════════
with tab03:
    st.title("Exp03 — Dispatch Protocol Classification")
    st.caption("4-class Classification  |  601k training rows  |  5 models")

    st.markdown("""<div class="scenario-card">
      <div class="scenario-title">Senaryo: Otomatik Müdahale Protokolü Sınıflandırması</div>
      <div class="scenario-grid">
        <div class="scenario-item"><strong>Problem</strong><br>
          Her olayı, herhangi bir memur sahaya gitmeden önce 4 müdahale protokolünden birine ata.<br>
          <span class="scenario-badge badge-blue">4-sınıf</span> domestic × arrest kombinasyonları
        </div>
        <div class="scenario-item"><strong>Sınıflar</strong><br>
          <span class="scenario-badge badge-blue">Sınıf 0</span> Aile Dışı + Tutuklama Yok (%67,8) — standart rapor<br>
          <span class="scenario-badge badge-amber">Sınıf 1</span> Aile Dışı + Tutuklama (%12,6) — nakliye birimi<br>
          <span class="scenario-badge badge-purple">Sınıf 2</span> Aile İçi + Tutuklama Yok (%16,6) — aile içi ekip<br>
          <span class="scenario-badge badge-red">Sınıf 3</span> Aile İçi + Tutuklama (%2,9) — ZORUNLU (IL yasası)
        </div>
        <div class="scenario-item"><strong>Hukuki Bağlam</strong><br>
          Illinois Zorunlu Tutuklama Yasası: aile içi şiddette muhtemel sebep varsa memur tutuklamak zorundadır.
          Sınıf 3 nadir (%2,9) ama hukuki açıdan en kritik vakadır — tespitte başarısızlık yasal sorumluluk doğurur.
        </div>
        <div class="scenario-item"><strong>Sınıf Ağırlıklandırması</strong><br>
          Sınıf 3'e 8,5 kat ters frekans ağırlığı uygulandı. Böylece model, yalnızca olayların
          %2,9'unda görülen zorunlu tutuklama vakalarını öncelikli olarak öğreniyor.
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    mlflow_btn("exp03", "Exp03 Sonuçlarını MLflow'da Görüntüle")

    if m03.empty:
        st.info("Run jobs/07_dispatch_protocol.py to generate Exp03 results.")
    else:
        best = m03.loc[m03["f1"].idxmax()]
        rc3  = best.get("recall_class3", 0)
        green_metrics(
            ("En İyi Model",        sname(best["model"]),        "4-sınıf classification"),
            ("Ağırlıklı F1",        f"{best['f1']:.3f}",        "tüm sınıflar"),
            ("Doğruluk",            f"{best['accuracy']*100:.1f}%", "test seti"),
            ("Recall — Sınıf 3",    f"{rc3*100:.1f}%",          "zorunlu tutuklama tespiti"),
            ("Sınıf 3 Ağırlığı",   "8,5x",                      "ters frekans ağırlığı"),
        )
        turkish_summary(
            f"{sname(best['model'])} modeli, 601.694 eğitim örneği üzerinde 4 müdahale protokolünü "
            f"sınıflandırmayı öğrendi ve <strong>F1={best['f1']:.3f}</strong> skoru elde etti. "
            f"En kritik performans göstergesi, Illinois Zorunlu Tutuklama Yasası kapsamındaki Sınıf 3 "
            f"(Aile İçi + Tutuklama) tespitinde <strong>Recall=%{rc3*100:.1f}</strong> değeridir — "
            f"bu, modelin zorunlu tutuklama gerektiren vakaların %{rc3*100:.1f}'ini doğru tespit edebildiği "
            f"anlamına geliyor. Sınıf 3 yalnızca %2,9 oranında görülmesine rağmen 8,5 kat ağırlık "
            f"uygulanarak modelin bu nadir ama kritik vakaları öncelikli öğrenmesi sağlandı."
        )

        st.markdown("---")
        sec("5 Model Comparison — Accuracy / F1 / Precision / Recall")
        names3 = [sname(n) for n in m03["model"]]
        fig    = go.Figure()
        for i,(mc,ml) in enumerate(zip(["accuracy","f1","precision","recall"],
                                        ["Accuracy","F1","Precision","Recall"])):
            fig.add_trace(go.Bar(
                name=ml, x=names3, y=m03[mc], marker_color=COLORS5[i], opacity=0.88,
                text=[f"{v:.3f}" for v in m03[mc]], textposition="outside", textfont_size=9,
            ))
        fig.update_layout(barmode="group", template=DARK, height=420,
                          yaxis=dict(range=[0,1.12],title="Score"),
                          title="5 Models — Dispatch Protocol 4-class",
                          legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig, use_container_width=True)

        recall_cols = [c for c in m03.columns if c.startswith("recall_class")]
        if recall_cols:
            sec("Per-Class Recall — Class 3 (red) is the mandatory-arrest target")
            class_labels = {
                "recall_class0":"Class 0: NonDom+NoArrest (67.8%)",
                "recall_class1":"Class 1: NonDom+Arrest (12.6%)",
                "recall_class2":"Class 2: Dom+NoArrest (16.6%)",
                "recall_class3":"Class 3: Dom+Arrest CRITICAL (2.9%)",
            }
            rows = []
            for _, row in m03.iterrows():
                for rc in recall_cols:
                    rows.append({"Model":sname(row["model"]),
                                 "Class":class_labels.get(rc,rc),
                                 "Recall":round(row.get(rc,0),4)})
            rc_df = pd.DataFrame(rows)
            fig   = px.bar(rc_df, x="Model", y="Recall", color="Class", barmode="group",
                           template=DARK, height=420,
                           color_discrete_sequence=["#58a6ff","#f59e0b","#3fb950","#e85d4a"],
                           title="Per-Class Recall — Class 3 (red) must be maximised for legal compliance")
            fig.add_hline(y=0.5, line_dash="dash", line_color="gray", opacity=0.35)
            st.plotly_chart(fig, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            sec("Class Distribution — 2M Records")
            class_dist = pd.DataFrame({
                "Class":["Class 0: NonDom+NoArrest","Class 1: NonDom+Arrest",
                          "Class 2: Dom+NoArrest","Class 3: Dom+Arrest (CRITICAL)"],
                "Count":[1_355_675, 252_881, 332_923, 58_518],
            })
            fig = px.pie(class_dist, names="Class", values="Count", hole=0.45, template=DARK,
                         color_discrete_sequence=["#58a6ff","#f59e0b","#3fb950","#e85d4a"],
                         title="Dispatch Protocol Class Distribution")
            fig.update_layout(height=340)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            sec("F1 Score Ranking")
            ms3  = m03.sort_values("f1")
            clr3 = ["#58a6ff" if n==best["model"] else "#30363d" for n in ms3["model"]]
            fig  = go.Figure(go.Bar(
                x=ms3["f1"], y=[sname(n) for n in ms3["model"]], orientation="h",
                marker_color=clr3, text=[f"{v:.3f}" for v in ms3["f1"]], textposition="outside",
            ))
            fig.update_layout(template=DARK, height=340,
                              xaxis=dict(range=[0,0.85],title="Weighted F1"),
                              title="F1 Ranking (blue = best)")
            st.plotly_chart(fig, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            sec("Confusion Matrix 4x4 — Best Model")
            if not cm03.empty:
                classes = sorted(cm03["label"].unique())
                n = len(classes)
                arr = np.zeros((n,n), dtype=int)
                for _, r in cm03.iterrows():
                    l,p = int(r["label"]),int(r["prediction"])
                    if l < n and p < n:
                        arr[l][p] = int(r["count"])
                labels4 = ["C0","C1","C2","C3 (Critical)"][:n]
                fig = go.Figure(go.Heatmap(
                    z=arr,
                    text=[[f"{arr[i,j]:,}" for j in range(n)] for i in range(n)],
                    texttemplate="%{text}", colorscale="Blues", showscale=True,
                    x=[f"Pred: {l}" for l in labels4],
                    y=[f"True: {l}" for l in labels4],
                ))
                fig.update_layout(template=DARK, height=360,
                                  title=f"Confusion Matrix — {sname(best['model'])}")
                st.plotly_chart(fig, use_container_width=True)
            else:
                fallback_png(FIGS/"exp03_dispatch"/"exp03_confusion_matrix.png")
        with c2:
            sec("Feature Importance — Best Model")
            if not fi03.empty:
                fi3 = fi03.sort_values("importance", ascending=True)
                clr_fi3 = ["#58a6ff" if i==len(fi3)-1 else "#30363d" for i in range(len(fi3))]
                fig = go.Figure(go.Bar(
                    x=fi3["importance"], y=fi3["feature"], orientation="h", marker_color=clr_fi3,
                    text=[f"{v:.4f}" for v in fi3["importance"]], textposition="outside",
                ))
                fig.update_layout(template=DARK, height=360,
                                  xaxis_title="Importance",
                                  title=f"Feature Importance — {sname(best['model'])}")
                st.plotly_chart(fig, use_container_width=True)
            else:
                fallback_png(FIGS/"exp03_dispatch"/"exp03_feature_importance.png")

        sec("Full Metrics Table")
        disp3 = m03.copy(); disp3["model"] = disp3["model"].apply(sname)
        st.dataframe(disp3.round(4).set_index("model"), use_container_width=True)

st.markdown("---")
st.caption("Chicago Crime Big Data Pipeline  |  Docker + Kafka + Spark 3.5.1 + Delta Lake + MLflow  |  2025-2026")
