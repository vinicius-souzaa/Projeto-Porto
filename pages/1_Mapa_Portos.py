"""Página 1 — Mapa interativo dos portos do Brasil."""

import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from util.data import load_porto_perfil, load_atracacao_master

st.title("🗺️ Mapa dos Portos do Brasil")

# Coordenadas aproximadas dos principais portos brasileiros
COORDS = {
    "SANTOS": (-23.955, -46.333),
    "PARANAGUÁ": (-25.520, -48.508),
    "ITAJAÍ": (-26.907, -48.672),
    "RIO DE JANEIRO": (-22.894, -43.177),
    "SUAPE": (-8.395, -34.973),
    "ITAQUI": (-2.577, -44.363),
    "VILA DO CONDE": (-1.533, -48.867),
    "PECÉM": (-3.517, -38.817),
    "SALVADOR": (-12.977, -38.493),
    "MANAUS": (-3.133, -60.017),
    "PORTO ALEGRE": (-30.028, -51.234),
    "FORTALEZA": (-3.722, -38.516),
    "RECIFE": (-8.063, -34.871),
    "BELÉM": (-1.452, -48.503),
    "VITÓRIA": (-20.319, -40.338),
    "ARATU": (-12.786, -38.484),
    "ANGRA DOS REIS": (-23.007, -44.313),
    "SÃO LUÍS": (-2.530, -44.302),
    "MACEIÓ": (-9.666, -35.739),
    "NATAL": (-5.801, -35.252),
}

@st.cache_data(show_spinner="Carregando perfil dos portos...")
def _load():
    return load_porto_perfil()

try:
    perfil = _load()
except FileNotFoundError as e:
    st.error(str(e))
    st.stop()

# Adiciona coordenadas (match parcial)
def _get_coord(nome):
    nome_up = str(nome).upper()
    for key, coord in COORDS.items():
        if key in nome_up or nome_up in key:
            return coord
    return None

perfil["_coord"] = perfil["Porto Atracação"].apply(_get_coord)
perfil_geo = perfil[perfil["_coord"].notna()].copy()
perfil_geo["lat"] = perfil_geo["_coord"].apply(lambda c: c[0])
perfil_geo["lon"] = perfil_geo["_coord"].apply(lambda c: c[1])

# ── Filtros ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtros")
    metric = st.selectbox("Tamanho do marcador",
                          ["n_atracacoes", "estadia_media", "operacao_media", "peso_medio"])
    uf_opts = ["Todos"] + sorted(perfil_geo["uf"].dropna().unique().tolist())
    uf_sel = st.selectbox("UF", uf_opts)

df_m = perfil_geo.copy()
if uf_sel != "Todos":
    df_m = df_m[df_m["uf"] == uf_sel]

labels = {
    "n_atracacoes": "Atracações",
    "estadia_media": "Estadia média (h)",
    "operacao_media": "Operação média (h)",
    "peso_medio": "Peso médio (t)",
}

fig = px.scatter_mapbox(
    df_m,
    lat="lat", lon="lon",
    size=metric,
    color="regiao",
    hover_name="Porto Atracação",
    hover_data={
        "n_atracacoes": True,
        "estadia_media": ":.1f",
        "pct_conteiner": ":.1%",
        "lat": False, "lon": False,
    },
    size_max=45,
    zoom=3,
    center={"lat": -14, "lon": -51},
    mapbox_style="carto-darkmatter",
    title=f"Portos brasileiros — {labels[metric]}",
    labels={"regiao": "Região"},
)
fig.update_layout(height=650, margin={"r": 0, "t": 40, "l": 0, "b": 0})
st.plotly_chart(fig, use_container_width=True)

# ── Tabela de perfis ──────────────────────────────────────────────────────────
st.subheader("Perfil operacional por porto")
cols_show = ["Porto Atracação", "uf", "regiao", "n_atracacoes",
             "estadia_media", "operacao_media", "peso_medio", "pct_conteiner"]
cols_show = [c for c in cols_show if c in perfil.columns]
st.dataframe(
    perfil[cols_show]
        .sort_values("n_atracacoes", ascending=False)
        .rename(columns={
            "Porto Atracação": "Porto",
            "uf": "UF", "regiao": "Região",
            "n_atracacoes": "Atracações",
            "estadia_media": "Estadia média (h)",
            "operacao_media": "Operação média (h)",
            "peso_medio": "Peso médio (t)",
            "pct_conteiner": "% conteiner",
        })
        .style.format({
            "Estadia média (h)": "{:.1f}",
            "Operação média (h)": "{:.1f}",
            "Peso médio (t)": "{:,.0f}",
            "% conteiner": "{:.1%}",
        }),
    use_container_width=True,
    height=400,
)
