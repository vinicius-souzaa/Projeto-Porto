"""Página 4 — Taxa de Ocupação dos berços (2020–2026)."""

import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from util.data import load_taxa_ocupacao_anual

st.title("📊 Taxa de Ocupação dos Berços")
st.info("Dados disponíveis a partir de 2020 (introdução pelo ANTAQ).")

@st.cache_data(show_spinner="Carregando taxa de ocupação...")
def _load():
    return load_taxa_ocupacao_anual()

try:
    df = _load()
except FileNotFoundError as e:
    st.error(str(e))
    st.stop()

if df.empty:
    st.warning("Dados de taxa de ocupação ainda não disponíveis no HF Hub.")
    st.stop()

df["Ano"] = pd.to_numeric(df.get("Ano", None), errors="coerce")

# Detecta coluna de porto e coluna numérica principal
porto_col = next((c for c in df.columns if "Porto" in c or "Complexo" in c), None)
num_cols = df.select_dtypes(include="float64").columns.tolist()
taxa_col = num_cols[0] if num_cols else None

if not porto_col or not taxa_col:
    st.error("Estrutura inesperada na tabela taxa_ocupacao_anual.")
    st.dataframe(df.head())
    st.stop()

# ── Filtros ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtros")
    anos = sorted(df["Ano"].dropna().unique().astype(int))
    ano_range = st.slider("Período", min_value=anos[0], max_value=anos[-1],
                          value=(anos[0], anos[-1]))
    portos = ["Todos"] + sorted(df[porto_col].dropna().unique().tolist())
    porto_sel = st.multiselect("Porto / Complexo", portos[1:], default=portos[1:6])

df_f = df[df["Ano"].between(*ano_range)]
if porto_sel:
    df_f = df_f[df_f[porto_col].isin(porto_sel)]

# ── KPIs ──────────────────────────────────────────────────────────────────────
c1, c2, c3 = st.columns(3)
c1.metric("Taxa média geral", f"{df_f[taxa_col].mean():.1%}")
c2.metric("Taxa máxima", f"{df_f[taxa_col].max():.1%}")
c3.metric("Porto mais ocupado",
          df_f.loc[df_f[taxa_col].idxmax(), porto_col] if not df_f.empty else "N/A")

st.divider()

# ── Evolução por porto ────────────────────────────────────────────────────────
fig = px.line(
    df_f.sort_values("Ano"),
    x="Ano", y=taxa_col, color=porto_col,
    markers=True,
    title=f"Evolução da taxa de ocupação por porto",
    labels={taxa_col: "Taxa de Ocupação", porto_col: "Porto"},
)
fig.update_yaxes(tickformat=".0%")
st.plotly_chart(fig, width="stretch")

# ── Heatmap porto × ano ───────────────────────────────────────────────────────
pivot = df_f.pivot_table(index=porto_col, columns="Ano", values=taxa_col, aggfunc="mean")
fig2 = px.imshow(
    pivot,
    color_continuous_scale="RdYlGn_r",
    title="Heatmap: taxa de ocupação por porto e ano",
    labels={"color": "Taxa", "x": "Ano", "y": "Porto"},
    aspect="auto",
)
fig2.update_coloraxes(colorbar_tickformat=".0%")
st.plotly_chart(fig2, width="stretch")

# ── Ranking último ano ────────────────────────────────────────────────────────
ultimo_ano = int(df_f["Ano"].max())
ranking = (
    df_f[df_f["Ano"] == ultimo_ano]
    .sort_values(taxa_col, ascending=False)
    [[porto_col, taxa_col]]
    .head(20)
)
fig3 = px.bar(
    ranking, x=taxa_col, y=porto_col, orientation="h",
    title=f"Top 20 portos por taxa de ocupação — {ultimo_ano}",
    labels={taxa_col: "Taxa", porto_col: ""},
    color=taxa_col, color_continuous_scale="RdYlGn_r",
)
fig3.update_xaxes(tickformat=".0%")
fig3.update_coloraxes(showscale=False)
st.plotly_chart(fig3, width="stretch")
