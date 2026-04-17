import streamlit as st
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from util.style    import inject_css, kpi, insight, sec_tag, get_layout, hex_to_rgba, C
from util.data     import load_master, load_hidrovia, get_summary
from util.layout   import sidebar
from util.constants import NAV_COLORS, CARGA_COLORS

inject_css()
df       = load_master()
hidrovia = load_hidrovia()
summary  = get_summary(df)
sidebar(summary)

st.markdown(
    f'<h1 style="font-size:1.9rem;font-weight:700;color:{C["text"]};margin:0 0 0.2rem;">'
    f'🌊 Tipos de Carga & Navegação</h1>'
    f'<div style="font-size:0.84rem;color:{C["muted"]};margin-bottom:1.2rem;">'
    f'Granel, contêiner e líquido · Longo Curso, Cabotagem, Interior</div>',
    unsafe_allow_html=True
)

tab1, tab2, tab3 = st.tabs(["📦 Natureza da Carga", "🚢 Tipos de Navegação", "🌿 Arco Norte / Hidrovias"])

# ── TAB 1: NATUREZA DA CARGA ──────────────────────────────────
with tab1:
    st.markdown(sec_tag("NATUREZA DA CARGA", "evolução 2010–2026"), unsafe_allow_html=True)

    carga_anual = (df.groupby(["Ano", "natureza_top"])
                   .agg(n=("IDAtracacao","count"), peso=("peso_total","sum"))
                   .reset_index())

    fig = go.Figure()
    for nat, color in CARGA_COLORS.items():
        sub = carga_anual[carga_anual["natureza_top"] == nat]
        if len(sub) == 0: continue
        fig.add_trace(go.Bar(
            x=sub["Ano"], y=sub["n"], name=nat,
            marker_color=hex_to_rgba(color, 0.8),
            hovertemplate=f"<b>{nat}</b><br>%{{x}}: %{{y:,}}<extra></extra>",
        ))
    fig.update_layout(**get_layout(height=340, barmode="stack",
                                    yaxis_title="Nº de atracações",
                                    legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center")))
    st.plotly_chart(fig, use_container_width=True)

    # Tonelagem por natureza
    st.markdown(sec_tag("TONELAGEM POR NATUREZA"), unsafe_allow_html=True)
    fig2 = go.Figure()
    for nat, color in CARGA_COLORS.items():
        sub = carga_anual[carga_anual["natureza_top"] == nat]
        if len(sub) == 0 or sub["peso"].sum() == 0: continue
        fig2.add_trace(go.Scatter(
            x=sub["Ano"], y=sub["peso"]/1e6, name=nat,
            mode="lines", line=dict(color=color, width=2),
            fill="tonexty", fillcolor=hex_to_rgba(color, 0.06),
            hovertemplate=f"<b>{nat}</b><br>%{{x}}: %{{y:.1f}}M ton<extra></extra>",
        ))
    fig2.update_layout(**get_layout(height=280, yaxis_title="Tonelagem (milhões)",
                                     legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center")))
    st.plotly_chart(fig2, use_container_width=True)

    st.markdown(insight(
        "<strong>Granel Sólido domina em tonelagem</strong> — soja, milho e minério de ferro "
        "representam a maior parte do peso movimentado. Os contêineres dominam em número de "
        "atracações. A carga geral inclui produtos industrializados e veículos.",
        C["yellow"]
    ), unsafe_allow_html=True)

# ── TAB 2: TIPOS DE NAVEGAÇÃO ─────────────────────────────────
with tab2:
    st.markdown(sec_tag("LONGO CURSO vs CABOTAGEM vs INTERIOR"), unsafe_allow_html=True)

    nav_anual = (df.groupby(["Ano", "Tipo de Navegação da Atracação"])
                 .agg(n=("IDAtracacao","count"), peso=("peso_total","sum"))
                 .reset_index())

    # Participação relativa
    nav_pct = nav_anual.copy()
    total_ano = nav_pct.groupby("Ano")["n"].transform("sum")
    nav_pct["pct"] = nav_pct["n"] / total_ano * 100

    fig3 = go.Figure()
    for nav, color in NAV_COLORS.items():
        sub = nav_pct[nav_pct["Tipo de Navegação da Atracação"] == nav]
        if len(sub) == 0: continue
        fig3.add_trace(go.Scatter(
            x=sub["Ano"], y=sub["pct"], name=nav,
            mode="lines", fill="tonexty",
            line=dict(color=color, width=0),
            fillcolor=hex_to_rgba(color, 0.6),
            hovertemplate=f"<b>{nav}</b><br>%{{x}}: %{{y:.1f}}%<extra></extra>",
        ))
    fig3.add_vline(x=2021, line_color=C["blue"], line_dash="dot",
                   annotation_text="BR do Mar", annotation_font_color=C["blue"])
    fig3.update_layout(**get_layout(height=320, yaxis_title="% do total",
                                     yaxis_range=[0,100],
                                     legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center")))
    st.plotly_chart(fig3, use_container_width=True)

    # Porto Organizado vs Terminal Privado
    st.markdown(sec_tag("PORTO ORGANIZADO vs TERMINAL PRIVADO"), unsafe_allow_html=True)
    auto_anual = (df.groupby(["Ano","Tipo da Autoridade Portuária"])
                  .size().reset_index(name="n"))
    fig4 = go.Figure()
    for tipo, color in [("Porto Organizado", C["orange"]),
                        ("Terminal Autorizado", C["blue"])]:
        sub = auto_anual[auto_anual["Tipo da Autoridade Portuária"] == tipo]
        fig4.add_trace(go.Scatter(
            x=sub["Ano"], y=sub["n"],
            mode="lines+markers", name=tipo,
            line=dict(color=color, width=2),
            hovertemplate=f"<b>{tipo}</b><br>%{{x}}: %{{y:,}}<extra></extra>",
        ))
    fig4.update_layout(**get_layout(height=260, yaxis_title="Atracações",
                                     legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center")))
    st.plotly_chart(fig4, use_container_width=True)

    st.markdown(insight(
        "<strong>Terminais Privados (TUPs) cresceram mais rapidamente</strong> que os Portos Organizados. "
        "A privatização e os investimentos em terminais especializados (graneleiros, contêineres) "
        "aumentaram a participação do setor privado na movimentação portuária brasileira.",
        C["blue"]
    ), unsafe_allow_html=True)

# ── TAB 3: ARCO NORTE / HIDROVIAS ────────────────────────────
with tab3:
    st.markdown(sec_tag("O ARCO NORTE", "boom das hidrovias amazônicas — soja e milho"), unsafe_allow_html=True)

    # Hidrovias top
    top_hid = (hidrovia.groupby("Hidrovia")["tonelagem_total"]
               .sum().sort_values(ascending=False).head(10).reset_index())

    fig5 = go.Figure(go.Bar(
        y=top_hid["Hidrovia"],
        x=top_hid["tonelagem_total"]/1e6,
        orientation="h",
        marker_color=[C["orange"] if i == 0 else hex_to_rgba(C["orange"], 0.6)
                      for i in range(len(top_hid))],
        text=[f"{v/1e6:.0f}M ton" for v in top_hid["tonelagem_total"]],
        textposition="outside",
        textfont=dict(color=C["muted"], size=10),
    ))
    ly = get_layout(height=380, xaxis_title="Tonelagem total (milhões)",
                    margin=dict(l=16,r=80,t=16,b=16))
    ly["yaxis"] = dict(**get_layout()["yaxis"], categoryorder="total ascending")
    fig5.update_layout(**ly)
    st.plotly_chart(fig5, use_container_width=True)

    # Evolução temporal Arco Norte
    norte = hidrovia[hidrovia["Região Geográfica"].str.contains("Norte", na=False)]
    hid_ev = (norte.groupby(["Ano","Hidrovia"])["tonelagem_total"]
              .sum().reset_index())

    top5_hid = (norte.groupby("Hidrovia")["tonelagem_total"]
                .sum().nlargest(5).index.tolist())

    fig6 = go.Figure()
    colors6 = [C["orange"], C["blue"], C["green"], C["yellow"], C["teal"]]
    for i, hid in enumerate(top5_hid):
        sub = hid_ev[hid_ev["Hidrovia"] == hid].sort_values("Ano")
        if len(sub) == 0: continue
        fig6.add_trace(go.Scatter(
            x=sub["Ano"], y=sub["tonelagem_total"]/1e6,
            mode="lines+markers", name=hid[:35],
            line=dict(color=colors6[i % len(colors6)], width=2),
            hovertemplate=f"<b>{hid[:30]}</b><br>%{{x}}: %{{y:.1f}}M ton<extra></extra>",
        ))
    fig6.update_layout(**get_layout(height=300, yaxis_title="Tonelagem (milhões)",
                                     legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center")))
    st.plotly_chart(fig6, use_container_width=True)

    st.markdown(insight(
        "<strong>O Baixo Amazonas é a maior hidrovia em tonelagem.</strong> "
        "A expansão da produção de soja e milho no Centro-Oeste e Norte do Brasil "
        "transformou as hidrovias amazônicas em corredores estratégicos de exportação. "
        "Em 2022, o Arco Norte (portos ao norte do paralelo 16°S) superou o resto do Brasil "
        "em movimentação de grãos pela primeira vez na história.",
        C["green"]
    ), unsafe_allow_html=True)
