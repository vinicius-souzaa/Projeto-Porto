"""Página 5 — Paralisações: causas, tempo perdido e impacto na operação."""

import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from util.data import load_atracacao_master, load_paralisacoes_por_atracacao

st.title("🔴 Paralisações Operacionais")

@st.cache_data(show_spinner="Carregando dados...")
def _load():
    cols = ["IDAtracacao", "Ano", "Porto Atracação", "Região Geográfica",
            "Tipo de Navegação da Atracação", "TEstadia", "TOperacao"]
    master = load_atracacao_master(cols)
    par = load_paralisacoes_por_atracacao()
    return master, par

try:
    master, par = _load()
except FileNotFoundError as e:
    st.error(str(e))
    st.stop()

if par.empty:
    st.warning("Dados de paralisações ainda não disponíveis.")
    st.stop()

master["Ano"] = pd.to_numeric(master["Ano"], errors="coerce")
par["IDAtracacao"] = pd.to_numeric(par["IDAtracacao"], errors="coerce")

df = master.merge(par, on="IDAtracacao", how="inner")

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
c1.metric("Atracações com paralisação", f"{len(df_f):,}")
c2.metric("Total de paralisações", f"{df_f['n_paralisacoes'].sum():,.0f}")
c3.metric("Tempo perdido total (h)", f"{df_f['tempo_paralisacao'].sum():,.0f}")
c4.metric("Média por atracação (h)", f"{df_f['tempo_paralisacao'].mean():.1f}")

st.divider()

# ── Evolução anual ────────────────────────────────────────────────────────────
anual = df_f.groupby("Ano").agg(
    n_paralisacoes=("n_paralisacoes", "sum"),
    tempo_paralisacao=("tempo_paralisacao", "sum"),
    atracacoes=("IDAtracacao", "count"),
).reset_index()
anual["paralisacoes_por_100"] = anual["n_paralisacoes"] / anual["atracacoes"] * 100

col1, col2 = st.columns(2)
with col1:
    fig = px.bar(anual, x="Ano", y="tempo_paralisacao",
                 title="Tempo total de paralisação por ano (h)",
                 color="tempo_paralisacao", color_continuous_scale="Reds")
    fig.update_coloraxes(showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig2 = px.line(anual, x="Ano", y="paralisacoes_por_100", markers=True,
                   title="Paralisações por 100 atracações",
                   labels={"paralisacoes_por_100": "Paralisações/100 atr."})
    st.plotly_chart(fig2, use_container_width=True)

# ── Por porto ─────────────────────────────────────────────────────────────────
por_porto = (
    df_f.groupby("Porto Atracação")
    .agg(tempo_total=("tempo_paralisacao", "sum"),
         n_par=("n_paralisacoes", "sum"),
         atracacoes=("IDAtracacao", "count"))
    .query("atracacoes >= 50")
    .sort_values("tempo_total", ascending=False)
    .head(20)
    .reset_index()
)
fig3 = px.bar(por_porto, x="tempo_total", y="Porto Atracação", orientation="h",
              title="Top 20 portos por tempo de paralisação total (h)",
              labels={"tempo_total": "Horas", "Porto Atracação": ""},
              color="tempo_total", color_continuous_scale="Reds")
fig3.update_coloraxes(showscale=False)
st.plotly_chart(fig3, use_container_width=True)

# ── Relação paralisação × estadia ─────────────────────────────────────────────
scatter_df = df_f[
    df_f["tempo_paralisacao"].between(0, df_f["tempo_paralisacao"].quantile(0.95)) &
    df_f["TEstadia"].between(0, df_f["TEstadia"].quantile(0.95))
].sample(min(5000, len(df_f)), random_state=42)

fig4 = px.scatter(
    scatter_df, x="tempo_paralisacao", y="TEstadia",
    color="Região Geográfica", opacity=0.5, trendline="ols",
    title="Relação: tempo de paralisação × estadia total",
    labels={"tempo_paralisacao": "Paralisação (h)", "TEstadia": "Estadia (h)"},
)
st.plotly_chart(fig4, use_container_width=True)
