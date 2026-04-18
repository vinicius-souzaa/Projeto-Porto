"""Página 6 — Sazonalidade & COVID: padrões mensais e impacto da pandemia."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from util.data import load_atracacao_master

st.title("📅 Sazonalidade & Impacto COVID-19")

@st.cache_data(show_spinner="Carregando dados...")
def _load():
    cols = ["IDAtracacao", "Ano", "Porto Atracação", "Região Geográfica",
            "Tipo de Navegação da Atracação", "Data Atracação",
            "TEstadia", "TOperacao", "peso_total"]
    return load_atracacao_master(cols)

try:
    df = _load()
except FileNotFoundError as e:
    st.error(str(e))
    st.stop()

df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")

# Parse data e extrai mês
if "Data Atracação" in df.columns:
    df["_data"] = pd.to_datetime(df["Data Atracação"],
                                  format="%d/%m/%Y %H:%M:%S", errors="coerce")
    df["Mes"] = df["_data"].dt.month
    df["AnoMes"] = df["_data"].dt.to_period("M").astype(str)
else:
    st.warning("Coluna 'Data Atracação' não encontrada.")
    st.stop()

MESES = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

# ── Filtros ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtros")
    regioes = ["Todas"] + sorted(df["Região Geográfica"].dropna().unique().tolist())
    regiao = st.selectbox("Região", regioes)
    nav_opts = ["Todos"] + sorted(df["Tipo de Navegação da Atracação"].dropna().unique().tolist())
    nav_sel = st.selectbox("Tipo de Navegação", nav_opts)

mask = df["Ano"].notna()
if regiao != "Todas":
    mask &= df["Região Geográfica"] == regiao
if nav_sel != "Todos":
    mask &= df["Tipo de Navegação da Atracação"] == nav_sel
df_f = df[mask].copy()

# ── Padrão sazonal médio ──────────────────────────────────────────────────────
sazonal = df_f.groupby("Mes").agg(
    atracacoes=("IDAtracacao", "count"),
    estadia_media=("TEstadia", "mean"),
    peso_medio=("peso_total", "mean"),
).reset_index()
sazonal["Mes_Nome"] = sazonal["Mes"].apply(lambda m: MESES[m-1])

col1, col2 = st.columns(2)
with col1:
    fig = px.bar(sazonal, x="Mes_Nome", y="atracacoes",
                 title="Atracações médias por mês (todos os anos)",
                 labels={"Mes_Nome": "Mês", "atracacoes": "Atracações"},
                 category_orders={"Mes_Nome": MESES})
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig2 = px.line(sazonal, x="Mes_Nome", y="estadia_media", markers=True,
                   title="Estadia média por mês",
                   labels={"Mes_Nome": "Mês", "estadia_media": "Estadia (h)"},
                   category_orders={"Mes_Nome": MESES})
    st.plotly_chart(fig2, use_container_width=True)

# ── Série temporal mensal ─────────────────────────────────────────────────────
mensal = (
    df_f.groupby("AnoMes")
    .agg(atracacoes=("IDAtracacao", "count"),
         estadia_media=("TEstadia", "mean"))
    .reset_index()
    .sort_values("AnoMes")
)

fig3 = px.line(mensal, x="AnoMes", y="atracacoes",
               title="Série temporal mensal de atracações")
# Marca período COVID
fig3.add_vrect(x0="2020-03", x1="2021-06", fillcolor="red", opacity=0.1,
               annotation_text="COVID-19", annotation_position="top left")
st.plotly_chart(fig3, use_container_width=True)

# ── Análise pré vs pós COVID ──────────────────────────────────────────────────
st.subheader("Comparação Pré × Durante × Pós COVID")
df_f["Periodo"] = pd.cut(
    df_f["Ano"],
    bins=[2009, 2019, 2020, 2021, 2026],
    labels=["Pré-COVID (2010-19)", "COVID 2020", "COVID 2021", "Pós-COVID (2022+)"]
)
covid_cmp = df_f.groupby("Periodo").agg(
    atracacoes=("IDAtracacao", "count"),
    estadia_media=("TEstadia", "mean"),
    operacao_media=("TOperacao", "mean"),
    peso_medio=("peso_total", "mean"),
).reset_index()

col3, col4 = st.columns(2)
with col3:
    fig4 = px.bar(covid_cmp, x="Periodo", y="atracacoes", color="Periodo",
                  title="Atracações por período",
                  labels={"Periodo": "", "atracacoes": "Atracações"})
    st.plotly_chart(fig4, use_container_width=True)

with col4:
    fig5 = px.bar(covid_cmp, x="Periodo", y="estadia_media", color="Periodo",
                  title="Estadia média por período (h)",
                  labels={"Periodo": "", "estadia_media": "Horas"})
    st.plotly_chart(fig5, use_container_width=True)

# ── Heatmap mes × ano ─────────────────────────────────────────────────────────
heat = df_f.pivot_table(index="Mes", columns="Ano", values="IDAtracacao",
                         aggfunc="count")
heat.index = [MESES[i-1] for i in heat.index]
fig6 = px.imshow(heat, color_continuous_scale="Blues",
                 title="Heatmap de atracações: mês × ano",
                 labels={"color": "Atracações", "x": "Ano", "y": "Mês"},
                 aspect="auto")
st.plotly_chart(fig6, use_container_width=True)
