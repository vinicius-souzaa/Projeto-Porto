"""Página 7 — Ranking de Portos: comparativo multi-dimensonal."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from util.data import load_porto_perfil, load_atracacao_master

st.title("🏆 Ranking de Portos")

@st.cache_data(show_spinner="Carregando perfis...")
def _load_perfil():
    return load_porto_perfil()

@st.cache_data(show_spinner="Carregando dados históricos...")
def _load_master():
    cols = ["IDAtracacao", "Ano", "Porto Atracação", "Região Geográfica",
            "TEstadia", "TOperacao", "peso_total", "teu_total"]
    return load_atracacao_master(cols)

try:
    perfil = _load_perfil()
    master = _load_master()
except FileNotFoundError as e:
    st.error(str(e))
    st.stop()

master["Ano"] = pd.to_numeric(master["Ano"], errors="coerce")

# ── Filtros ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtros")
    regioes = ["Todas"] + sorted(perfil["regiao"].dropna().unique().tolist())
    regiao = st.selectbox("Região", regioes)
    top_n = st.slider("Mostrar Top N portos", 5, 30, 15)
    metrica = st.selectbox("Ordenar por",
                           ["n_atracacoes", "estadia_media", "operacao_media",
                            "peso_medio", "teu_medio", "pct_conteiner"])
    asc = st.checkbox("Ordem crescente", value=False)

df = perfil.copy()
if regiao != "Todas":
    df = df[df["regiao"] == regiao]
df = df.sort_values(metrica, ascending=asc).head(top_n)

label_map = {
    "n_atracacoes": "Atracações",
    "estadia_media": "Estadia média (h)",
    "operacao_media": "Operação média (h)",
    "peso_medio": "Peso médio (t)",
    "teu_medio": "TEU médio",
    "pct_conteiner": "% Conteiner",
}

# ── Ranking principal ─────────────────────────────────────────────────────────
fig = px.bar(
    df, x=metrica, y="Porto Atracação", orientation="h",
    color="regiao", title=f"Top {top_n} portos — {label_map[metrica]}",
    labels={metrica: label_map[metrica], "Porto Atracação": "",
            "regiao": "Região"},
)
if metrica == "pct_conteiner":
    fig.update_xaxes(tickformat=".0%")
st.plotly_chart(fig, use_container_width=True)

# ── Spider / Radar chart ──────────────────────────────────────────────────────
st.subheader("Comparação radar — selecione até 5 portos")
portos_opcao = sorted(perfil["Porto Atracação"].tolist())
portos_sel = st.multiselect("Portos para comparar", portos_opcao,
                             default=portos_opcao[:3])

if portos_sel:
    radar_df = perfil[perfil["Porto Atracação"].isin(portos_sel)].copy()
    dims = ["n_atracacoes", "estadia_media", "operacao_media", "peso_medio", "pct_conteiner"]
    dims = [d for d in dims if d in radar_df.columns]

    # Normaliza 0–1 para cada dimensão
    radar_norm = radar_df[["Porto Atracação"] + dims].copy()
    for d in dims:
        mn, mx = perfil[d].min(), perfil[d].max()
        radar_norm[d] = (radar_norm[d] - mn) / (mx - mn + 1e-9)

    fig_r = go.Figure()
    for _, row in radar_norm.iterrows():
        vals = [row[d] for d in dims] + [row[dims[0]]]
        fig_r.add_trace(go.Scatterpolar(
            r=vals, theta=dims + [dims[0]],
            fill="toself", name=row["Porto Atracação"]
        ))
    fig_r.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        title="Radar: perfil comparativo (normalizado)"
    )
    st.plotly_chart(fig_r, use_container_width=True)

# ── Evolução anual dos top portos ─────────────────────────────────────────────
st.subheader("Evolução histórica — top 10 por volume")
top10 = perfil.nlargest(10, "n_atracacoes")["Porto Atracação"].tolist()
evol = master[master["Porto Atracação"].isin(top10)].groupby(
    ["Ano", "Porto Atracação"])["IDAtracacao"].count().reset_index()
evol.columns = ["Ano", "Porto", "Atracações"]
fig2 = px.line(evol, x="Ano", y="Atracações", color="Porto", markers=True,
               title="Atracações por ano — top 10 portos")
st.plotly_chart(fig2, use_container_width=True)

# ── Scatter volume × eficiência ───────────────────────────────────────────────
fig3 = px.scatter(
    perfil, x="n_atracacoes", y="estadia_media",
    size="peso_medio", color="regiao", hover_name="Porto Atracação",
    title="Volume × Estadia média (tamanho = peso médio)",
    labels={"n_atracacoes": "Atracações totais",
            "estadia_media": "Estadia média (h)",
            "regiao": "Região"},
    log_x=True,
)
st.plotly_chart(fig3, use_container_width=True)
