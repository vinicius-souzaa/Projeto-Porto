"""
Portos do Brasil — ANTAQ Dashboard
====================================
Entry-point para o Streamlit. Registra todas as páginas via st.navigation().
"""

import streamlit as st

st.set_page_config(
    page_title="Portos do Brasil · ANTAQ",
    page_icon="⚓",
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = [
    st.Page("pages/0_Visao_Geral.py",          title="Visão Geral",          icon="🏠"),
    st.Page("pages/1_Mapa_Portos.py",           title="Mapa dos Portos",      icon="🗺️"),
    st.Page("pages/2_Eficiencia_Operacional.py",title="Eficiência Operacional",icon="⏱️"),
    st.Page("pages/3_Cargas_Navegacao.py",      title="Cargas & Navegação",   icon="🚢"),
    st.Page("pages/4_Taxa_Ocupacao.py",         title="Taxa de Ocupação",     icon="📊"),
    st.Page("pages/5_Paralisacoes.py",          title="Paralisações",         icon="🔴"),
    st.Page("pages/6_Sazonalidade_COVID.py",    title="Sazonalidade & COVID", icon="📅"),
    st.Page("pages/7_Ranking_Portos.py",        title="Ranking de Portos",    icon="🏆"),
    st.Page("pages/8_Modelo_Preditivo.py",      title="Modelo Preditivo",     icon="🤖"),
    st.Page("pages/9_Catalogo_Pipeline.py",     title="Catálogo & Pipeline",  icon="📚"),
]

pg = st.navigation(pages)
pg.run()
