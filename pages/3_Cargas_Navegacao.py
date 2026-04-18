"""Página 3 — Cargas & Navegação: tipos, sentidos, conteinerização."""

import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from util.data import load_atracacao_master, load_carga_hidrovia_anual

st.title("🚢 Cargas & Navegação")

@st.cache_data(show_spinner="Carregando dados...")
def _load():
    cols = ["IDAtracacao", "Ano", "Porto Atracação", "Região Geográfica", "UF",
            "Tipo de Navegação da Atracação", "Tipo de Operação",
            "peso_total", "teu_total", "peso_cont", "natureza_top", "sentido_top"]
    return load_atracacao_master(cols)

@st.cache_data(show_spinner="Carregando hidrovias...")
def _load_hidrovia():
    return load_carga_hidrovia_anual()

try:
    df = _load()
    hidrovia = _load_hidrovia()
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
df_f = df[mask].copy()

# ── KPIs ──────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Peso total (Mt)", f"{df_f['peso_total'].sum()/1e6:.1f}")
c2.metric("TEUs totais", f"{df_f['teu_total'].sum():,.0f}")
c3.metric("Peso conteinerizado (Mt)", f"{df_f['peso_cont'].sum()/1e6:.1f}")
pct_cont = (df_f["teu_total"] > 0).mean()
c4.metric("% atracações c/ contêiner", f"{pct_cont:.1%}")

st.divider()

# ── Carga por ano ─────────────────────────────────────────────────────────────
anual = df_f.groupby("Ano").agg(
    peso_total=("peso_total", "sum"),
    peso_cont=("peso_cont", "sum"),
    teu_total=("teu_total", "sum"),
).reset_index()

col1, col2 = st.columns(2)
with col1:
    fig = px.area(anual, x="Ano", y=["peso_total", "peso_cont"],
                  title="Carga total vs conteinerizada (t)",
                  labels={"value": "Toneladas", "variable": "Tipo"})
    st.plotly_chart(fig, width="stretch")

with col2:
    fig2 = px.bar(anual, x="Ano", y="teu_total",
                  title="TEUs movimentados por ano",
                  color="teu_total", color_continuous_scale="Teal")
    fig2.update_coloraxes(showscale=False)
    st.plotly_chart(fig2, width="stretch")

# ── Por tipo de navegação ─────────────────────────────────────────────────────
nav = df_f.groupby(["Ano", "Tipo de Navegação da Atracação"])["peso_total"].sum().reset_index()
fig3 = px.bar(nav, x="Ano", y="peso_total", color="Tipo de Navegação da Atracação",
              barmode="stack",
              title="Carga por tipo de navegação",
              labels={"peso_total": "Toneladas",
                      "Tipo de Navegação da Atracação": "Navegação"})
st.plotly_chart(fig3, width="stretch")

# ── Sentido ───────────────────────────────────────────────────────────────────
if "sentido_top" in df_f.columns:
    sentido = df_f.groupby("sentido_top")["peso_total"].sum().reset_index()
    sentido.columns = ["Sentido", "Peso (t)"]
    col3, col4 = st.columns(2)
    with col3:
        fig4 = px.pie(sentido, names="Sentido", values="Peso (t)",
                      title="Carga por sentido", hole=0.4)
        st.plotly_chart(fig4, width="stretch")

# ── Hidrovias ─────────────────────────────────────────────────────────────────
if not hidrovia.empty:
    st.subheader("Movimentação em Hidrovias")
    hidrovia["Ano"] = pd.to_numeric(hidrovia.get("Ano"), errors="coerce")
    hid_f = hidrovia[hidrovia["Ano"].between(*ano_range)]

    if "Hidrovia" in hid_f.columns and "tonelagem_total" in hid_f.columns:
        hid_anual = hid_f.groupby(["Ano", "Hidrovia"])["tonelagem_total"].sum().reset_index()
        fig5 = px.line(hid_anual, x="Ano", y="tonelagem_total", color="Hidrovia",
                       markers=True, title="Tonelagem por hidrovia (anual)",
                       labels={"tonelagem_total": "Toneladas"})
        st.plotly_chart(fig5, width="stretch")
