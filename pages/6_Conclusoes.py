import streamlit as st
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from util.style    import inject_css, kpi, insight, sec_tag, data_row, C
from util.data     import load_master, get_summary
from util.layout   import sidebar

inject_css()
df      = load_master()
summary = get_summary(df)
sidebar(summary)

st.markdown(
    f'<h1 style="font-size:1.9rem;font-weight:700;color:{C["text"]};margin:0 0 0.2rem;">'
    f'💡 Conclusões & Insights</h1>'
    f'<div style="font-size:0.84rem;color:{C["muted"]};margin-bottom:1.2rem;">'
    f'O que 17 anos de dados portuários revelam sobre o Brasil</div>',
    unsafe_allow_html=True
)

# Insight strips
st.markdown(
    f'<div style="background:{C["bg"]};border-left:4px solid {C["orange"]};'
    f'padding:1rem 1.4rem;margin-bottom:1.2rem;border-radius:0 8px 8px 0;">'
    f'<div style="font-size:2rem;font-weight:700;color:{C["orange"]};">'
    f'{summary["n_atracacoes"]:,}</div>'
    f'<div style="font-size:0.85rem;color:{C["muted"]};line-height:1.6;">'
    f'atracações registradas entre 2010 e 2026 · '
    f'{summary["n_portos"]} portos · 17 anos de dados ANTAQ · '
    f'maior movimentação da história em 2023 (1,3 bilhão de toneladas)</div>'
    f'</div>',
    unsafe_allow_html=True
)

st.markdown(sec_tag("GRANDES ACHADOS", "o que 1,3 milhão de atracações revelam"), unsafe_allow_html=True)

achados = [
    (C["orange"], "⚓", "O Arco Norte transformou o mapa portuário brasileiro",
     "A partir de 2018, portos do Norte e Nordeste passaram a dominar a exportação de soja e milho. "
     "Em 2022, o Arco Norte superou o Sul/Sudeste em tonelagem de grãos pela primeira vez. "
     "Hidrovias amazônicas como Baixo Amazonas e Baixo Tocantins movimentaram mais de 700 milhões de toneladas em 17 anos."),

    (C["red"], "⌛", "O tempo de espera é o maior gargalo — e não melhorou",
     "A espera média para atracar se manteve alta ao longo de toda a série. "
     "Paranaguá e Santos — os dois maiores portos do país — têm algumas das maiores esperas. "
     "O crescimento em volume não veio acompanhado de melhora proporcional em eficiência operacional."),

    (C["blue"], "🚢", "Cabotagem é o modal com maior potencial de crescimento",
     "A política BR do Mar (2021) acelerou o crescimento da cabotagem. "
     "Navios de cabotagem substituem caminhões nas rotas costeiras, reduzindo emissões e congestionamentos. "
     "A cabotagem ainda representa menos de 20% das atracações — muito abaixo do potencial do país."),

    (C["green"], "📈", "2023: recorde histórico após COVID",
     "A pandemia de 2020 reduziu atracações e aumentou estadias em muitos portos. "
     "A recuperação em 2021 foi imediata e em 2023 o setor bateu o recorde de 1,3 bilhão de toneladas. "
     "Portos de granel sólido se recuperaram mais rápido que terminais de passageiros."),

    (C["yellow"], "🤖", "Tonelagem e porto são os maiores preditores de estadia",
     "O modelo XGBoost (R²=0.78) confirmou que quanto mais toneladas o navio carrega, "
     "mais tempo ele fica no porto — relação mais forte que qualquer outra variável. "
     "O porto de atracação em si explica 28% da variância — a infraestrutura local importa mais que o tipo de carga."),

    (C["teal"], "🌿", "Navegação interior domina em número, longo curso em valor",
     "Mais de 40% das atracações são navegação interior — rios e hidrovias. "
     "Mas o Longo Curso (internacional) movimenta as maiores cargas e gera mais receita cambial. "
     "A combinação de hidrovias interiores + portos oceânicos de exportação é a vantagem competitiva do Brasil."),
]

c1, c2 = st.columns(2)
for i, (color, icon, title, text) in enumerate(achados):
    col = c1 if i % 2 == 0 else c2
    col.markdown(f"""
    <div style="background:{C['surface']};border:1px solid {C['border']};
                border-radius:8px;padding:1.1rem 1.3rem;margin-bottom:0.7rem;
                border-left:3px solid {color};">
        <div style="font-size:1rem;margin-bottom:0.25rem;">{icon}</div>
        <div style="font-size:0.9rem;font-weight:700;color:{color};margin-bottom:0.4rem;">{title}</div>
        <div style="font-size:0.8rem;color:{C['muted']};line-height:1.65;">{text}</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# Painel comparativo
st.markdown(sec_tag("PAINEL COMPARATIVO", "2010 vs 2025"), unsafe_allow_html=True)

anual = df.groupby("Ano").agg(
    n=("IDAtracacao","count"),
    estadia=("TEstadia","mean"),
    espera=("TEsperaAtracacao","mean"),
).reset_index()

first = anual[anual["Ano"] == 2010].iloc[0] if 2010 in anual["Ano"].values else None
last  = anual[anual["Ano"] == anual["Ano"].max()].iloc[0]

comparativo = []
if first is not None:
    comparativo = [
        ("Atracações anuais",    f"{int(first['n']):,}",   f"{int(last['n']):,}",
         f"+{(last['n']/first['n']-1)*100:.0f}%", C["orange"]),
        ("Estadia média (h)",    f"{first['estadia']:.0f}h", f"{last['estadia']:.0f}h",
         f"{(last['estadia']-first['estadia']):+.0f}h",
         C["green"] if last['estadia'] < first['estadia'] else C["red"]),
        ("Espera média (h)",     f"{first['espera']:.0f}h",  f"{last['espera']:.0f}h",
         f"{(last['espera']-first['espera']):+.0f}h",
         C["green"] if last['espera'] < first['espera'] else C["red"]),
    ]

comp_c = st.columns([3,2,2,1])
comp_c[0].markdown(f'<div style="font-size:0.62rem;color:{C["dim"]};text-transform:uppercase;letter-spacing:0.1em;padding:0.4rem 0;">Indicador</div>', unsafe_allow_html=True)
comp_c[1].markdown(f'<div style="font-size:0.62rem;color:{C["dim"]};text-transform:uppercase;letter-spacing:0.1em;padding:0.4rem 0;">2010</div>', unsafe_allow_html=True)
comp_c[2].markdown(f'<div style="font-size:0.62rem;color:{C["dim"]};text-transform:uppercase;letter-spacing:0.1em;padding:0.4rem 0;">{int(last["Ano"])}</div>', unsafe_allow_html=True)
comp_c[3].markdown(f'<div style="font-size:0.62rem;color:{C["dim"]};text-transform:uppercase;letter-spacing:0.1em;padding:0.4rem 0;">Var.</div>', unsafe_allow_html=True)

for ind, v_ini, v_fim, var, color in comparativo:
    r0, r1, r2, r3 = st.columns([3,2,2,1])
    r0.markdown(f'<div style="font-size:0.82rem;color:{C["cream"] if hasattr(C,"cream") else C["text"]};padding:0.35rem 0;">{ind}</div>', unsafe_allow_html=True)
    r1.markdown(f'<div style="font-size:0.82rem;color:{C["muted"]};padding:0.35rem 0;">{v_ini}</div>', unsafe_allow_html=True)
    r2.markdown(f'<div style="font-size:0.82rem;color:{C["text"]};font-weight:600;padding:0.35rem 0;">{v_fim}</div>', unsafe_allow_html=True)
    r3.markdown(f'<div style="font-size:0.82rem;color:{color};font-weight:700;padding:0.35rem 0;">{var}</div>', unsafe_allow_html=True)
    st.markdown(f'<div style="border-bottom:1px solid {C["border"]};"></div>', unsafe_allow_html=True)

st.divider()

# Ficha técnica
st.markdown(sec_tag("FICHA TÉCNICA", "sobre o projeto"), unsafe_allow_html=True)
st.markdown(f"""
<div style="background:{C['bg']};border:1px solid {C['border']};border-radius:10px;
            padding:1.3rem 1.6rem;">
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:1.2rem;">
    <div>
      <p style="color:{C['orange']};font-size:0.62rem;font-weight:600;letter-spacing:0.15em;
                text-transform:uppercase;margin-bottom:0.4rem;">FONTE</p>
      <p style="color:{C['muted']};font-size:0.76rem;line-height:1.7;margin:0;">
        ANTAQ — Agência Nacional de Transportes Aquaviários<br/>
        Estatístico Aquaviário<br/>
        Dados públicos · 2010–2026</p>
    </div>
    <div>
      <p style="color:{C['blue']};font-size:0.62rem;font-weight:600;letter-spacing:0.15em;
                text-transform:uppercase;margin-bottom:0.4rem;">DATASETS</p>
      <p style="color:{C['muted']};font-size:0.76rem;line-height:1.7;margin:0;">
        7 arquivos TXT originais<br/>
        196 milhões de linhas brutas<br/>
        Convertidos para 9 Parquets<br/>
        ~77 MB comprimido (zstd)</p>
    </div>
    <div>
      <p style="color:{C['green']};font-size:0.62rem;font-weight:600;letter-spacing:0.15em;
                text-transform:uppercase;margin-bottom:0.4rem;">MODELO</p>
      <p style="color:{C['muted']};font-size:0.76rem;line-height:1.7;margin:0;">
        XGBoost Regressor<br/>
        1,28M atracações de treino<br/>
        R²=0.78 · MAE=29h<br/>
        SHAP para explicabilidade</p>
    </div>
    <div>
      <p style="color:{C['teal']};font-size:0.62rem;font-weight:600;letter-spacing:0.15em;
                text-transform:uppercase;margin-bottom:0.4rem;">STACK</p>
      <p style="color:{C['muted']};font-size:0.76rem;line-height:1.7;margin:0;">
        Python · Pandas · PyArrow<br/>
        XGBoost · SHAP · scikit-learn<br/>
        Plotly · Streamlit<br/>
        Space Grotesk · Slate UI</p>
    </div>
  </div>
  <div style="margin-top:1rem;padding-top:0.9rem;border-top:1px solid {C['border']};">
    <p style="color:{C['muted']};font-size:0.71rem;margin:0;">
      <strong style="color:{C['text']};">Vinicius Abreu Ernestino Souza</strong> ·
      Data Analytics · São Paulo, SP ·
      G. Pierotti Serviços Marítimos
    </p>
  </div>
</div>""", unsafe_allow_html=True)
