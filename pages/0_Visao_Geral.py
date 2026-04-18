"""Página 0 — Visão Geral: KPIs e séries temporais macro 2010–2026."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from util.data import load_atracacao_master

st.title("🏠 Visão Geral · Portos do Brasil (2010–2026)")
st.caption("Fonte: ANTAQ — Agência Nacional de Transportes Aquaviários")

@st.cache_data(show_spinner="Carregando dados...")
def _load():
    cols = ["IDAtracacao", "Ano", "Porto Atracação", "Região Geográfica", "UF",
            "Tipo de Navegação da Atracação", "TEstadia", "TOperacao", "peso_total", "teu_total"]
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
                          value=(anos[0], anos[-1]))
    regioes = ["Todas"] + sorted(df["Região Geográfica"].dropna().unique().tolist())
    regiao = st.selectbox("Região", regioes)

mask = df["Ano"].between(*ano_range)
if regiao != "Todas":
    mask &= df["Região Geográfica"] == regiao
df_f = df[mask]

# ── KPIs ──────────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Atracações", f"{len(df_f):,.0f}")
c2.metric("Portos ativos", f"{df_f['Porto Atracação'].nunique():,}")
c3.metric("Estadia média (h)", f"{df_f['TEstadia'].mean():.1f}")
c4.metric("Peso total (Mt)", f"{df_f['peso_total'].sum()/1e6:.1f}")
c5.metric("TEUs", f"{df_f['teu_total'].sum():,.0f}")

st.divider()

# ── Atracações por ano ────────────────────────────────────────────────────────
anual = df_f.groupby("Ano").agg(
    atracacoes=("IDAtracacao", "count"),
    estadia_media=("TEstadia", "mean"),
    peso_total=("peso_total", "sum"),
).reset_index()

col1, col2 = st.columns(2)

with col1:
    fig = px.bar(anual, x="Ano", y="atracacoes",
                 title="Atracações por ano",
                 color="atracacoes", color_continuous_scale="Blues",
                 labels={"atracacoes": "Atracações"})
    fig.update_coloraxes(showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig2 = px.line(anual, x="Ano", y="estadia_media",
                   title="Tempo médio de estadia (horas)",
                   markers=True,
                   labels={"estadia_media": "Estadia média (h)"})
    fig2.add_vline(x=2020, line_dash="dash", line_color="red",
                   annotation_text="COVID-19")
    st.plotly_chart(fig2, use_container_width=True)

# ── Peso movimentado ──────────────────────────────────────────────────────────
fig3 = px.area(anual, x="Ano", y="peso_total",
               title="Carga movimentada total (toneladas)",
               labels={"peso_total": "Toneladas"})
st.plotly_chart(fig3, use_container_width=True)

# ── Mix por tipo de navegação ─────────────────────────────────────────────────
nav = df_f.groupby(["Ano", "Tipo de Navegação da Atracação"])["IDAtracacao"].count().reset_index()
nav.columns = ["Ano", "Navegação", "Atracações"]
fig4 = px.bar(nav, x="Ano", y="Atracações", color="Navegação", barmode="stack",
              title="Atracações por tipo de navegação")
st.plotly_chart(fig4, use_container_width=True)

# ── Por região ────────────────────────────────────────────────────────────────
reg = df_f.groupby("Região Geográfica")["IDAtracacao"].count().reset_index()
reg.columns = ["Região", "Atracações"]
fig5 = px.pie(reg, names="Região", values="Atracações",
              title="Distribuição por região geográfica", hole=0.4)
st.plotly_chart(fig5, use_container_width=True)
