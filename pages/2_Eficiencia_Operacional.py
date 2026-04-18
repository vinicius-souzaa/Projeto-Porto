"""Página 2 — Eficiência Operacional: distribuição de tempos e benchmarks."""

import streamlit as st
import plotly.express as px
import plotly.figure_factory as ff
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from util.data import load_atracacao_master

st.title("⏱️ Eficiência Operacional")

@st.cache_data(show_spinner="Carregando dados...")
def _load():
    cols = ["IDAtracacao", "Ano", "Porto Atracação", "Região Geográfica", "UF",
            "Tipo de Navegação da Atracação", "Tipo de Operação",
            "TEstadia", "TOperacao", "TEsperaAtracacao", "TEsperaInicioOp",
            "TAtracado", "TEsperaDesatracacao"]
    return load_atracacao_master(cols)

try:
    df = _load()
except FileNotFoundError as e:
    st.error(str(e))
    st.stop()

df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")

# ── Filtros ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtros")
    anos = sorted(df["Ano"].dropna().unique().astype(int))
    ano_range = st.slider("Período", min_value=anos[0], max_value=anos[-1],
                          value=(max(anos[0], anos[-1] - 4), anos[-1]))
    portos = ["Todos"] + sorted(df["Porto Atracação"].dropna().unique().tolist())
    porto_sel = st.selectbox("Porto", portos)
    nav_opts = ["Todos"] + sorted(df["Tipo de Navegação da Atracação"].dropna().unique().tolist())
    nav_sel = st.selectbox("Tipo de Navegação", nav_opts)

mask = df["Ano"].between(*ano_range)
if porto_sel != "Todos":
    mask &= df["Porto Atracação"] == porto_sel
if nav_sel != "Todos":
    mask &= df["Tipo de Navegação da Atracação"] == nav_sel
df_f = df[mask].copy()

# ── KPIs ──────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Estadia média (h)", f"{df_f['TEstadia'].mean():.1f}")
c2.metric("Operação média (h)", f"{df_f['TOperacao'].mean():.1f}")
c3.metric("Espera média (h)", f"{df_f['TEsperaAtracacao'].mean():.1f}")
efficiency = df_f["TOperacao"] / df_f["TEstadia"].replace(0, np.nan)
c4.metric("Eficiência média", f"{efficiency.mean():.1%}")

st.divider()

# ── Evolução temporal ─────────────────────────────────────────────────────────
anual = df_f.groupby("Ano").agg(
    TEstadia=("TEstadia", "mean"),
    TOperacao=("TOperacao", "mean"),
    TEsperaAtracacao=("TEsperaAtracacao", "mean"),
).reset_index()

fig = px.line(
    anual.melt(id_vars="Ano", var_name="Tempo", value_name="Horas"),
    x="Ano", y="Horas", color="Tempo", markers=True,
    title="Evolução dos tempos operacionais (médias anuais)",
    labels={"Tempo": "Indicador"},
)
fig.add_vline(x=2020, line_dash="dash", line_color="red",
              annotation_text="COVID-19")
st.plotly_chart(fig, use_container_width=True)

# ── Distribuição por histograma ───────────────────────────────────────────────
col1, col2 = st.columns(2)

estadia_ok = df_f["TEstadia"].dropna()
estadia_ok = estadia_ok[estadia_ok.between(0, estadia_ok.quantile(0.99))]
with col1:
    fig2 = px.histogram(estadia_ok, nbins=80, title="Distribuição de TEstadia (h)",
                        labels={"value": "Horas", "count": "Frequência"})
    fig2.add_vline(x=float(estadia_ok.median()), line_dash="dash",
                   annotation_text=f"Mediana: {estadia_ok.median():.1f}h")
    st.plotly_chart(fig2, use_container_width=True)

op_ok = df_f["TOperacao"].dropna()
op_ok = op_ok[op_ok.between(0, op_ok.quantile(0.99))]
with col2:
    fig3 = px.histogram(op_ok, nbins=80, title="Distribuição de TOperacao (h)",
                        labels={"value": "Horas", "count": "Frequência"})
    fig3.add_vline(x=float(op_ok.median()), line_dash="dash",
                   annotation_text=f"Mediana: {op_ok.median():.1f}h")
    st.plotly_chart(fig3, use_container_width=True)

# ── Box plot por tipo de navegação ────────────────────────────────────────────
nav_grp = df_f[df_f["TEstadia"].notna() & df_f["TEstadia"].between(0, 500)]
fig4 = px.box(nav_grp, x="Tipo de Navegação da Atracação", y="TEstadia",
              title="Estadia por tipo de navegação",
              labels={"TEstadia": "Estadia (h)",
                      "Tipo de Navegação da Atracação": "Navegação"})
st.plotly_chart(fig4, use_container_width=True)

# ── Top 20 portos: estadia média ──────────────────────────────────────────────
top_porto = (
    df_f.groupby("Porto Atracação")["TEstadia"]
    .agg(["mean", "count"])
    .query("count >= 100")
    .sort_values("mean", ascending=True)
    .tail(20)
    .reset_index()
)
fig5 = px.bar(top_porto, x="mean", y="Porto Atracação", orientation="h",
              title="Top 20 portos por estadia média (mín. 100 atracações)",
              labels={"mean": "Estadia média (h)", "Porto Atracação": ""})
st.plotly_chart(fig5, use_container_width=True)
