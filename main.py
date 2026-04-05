import streamlit as st

st.set_page_config(
    page_title="Portos do Brasil · ANTAQ 2010–2026",
    layout="wide",
    page_icon="⚓",
    initial_sidebar_state="expanded",
)

pages = st.navigation([
    st.Page("pages/0_Visao_Geral.py",           title="⚓ Visão Geral"),
    st.Page("pages/1_Eficiencia_Operacional.py", title="⚡ Eficiência Operacional"),
    st.Page("pages/2_Cargas_e_Navegacao.py",     title="🌊 Cargas & Navegação"),
    st.Page("pages/3_Sazonalidade_COVID.py",     title="📅 Sazonalidade & COVID"),
    st.Page("pages/4_Ranking_Portos.py",         title="🏆 Ranking de Portos"),
    st.Page("pages/5_Modelo_Preditivo.py",       title="🤖 Modelo Preditivo"),
    st.Page("pages/6_Conclusoes.py",             title="💡 Conclusões"),
], position="sidebar")
pages.run()
