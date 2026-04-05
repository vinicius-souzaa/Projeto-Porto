import streamlit as st
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from util.style    import inject_css, kpi, insight, sec_tag, badge, get_layout, hex_to_rgba, C
from util.data     import load_master, load_hidrovia, get_summary, agg_anual, agg_porto
from util.layout   import sidebar
from util.constants import EVENTS, REGIAO_COLORS, NAV_COLORS

inject_css()
df      = load_master()
summary = get_summary(df)
sidebar(summary)

st.markdown(
    f'<h1 style="font-size:2rem;font-weight:700;color:{C["text"]};margin:0 0 0.2rem;">'
    f'⚓ Portos do Brasil</h1>'
    f'<div style="font-size:0.84rem;color:{C["muted"]};margin-bottom:1.2rem;">'
    f'Análise de 1,3 milhão de atracações · ANTAQ · 2010–2026</div>',
    unsafe_allow_html=True
)

# ── KPIs ──────────────────────────────────────────────────────
anual = agg_anual(df)
last  = anual[anual["Ano"] == summary["last_year"]].iloc[0]
prev  = anual[anual["Ano"] == summary["last_year"] - 1].iloc[0]

pct_atrac = (last["n_atracacoes"] / prev["n_atracacoes"] - 1) * 100
pct_peso  = (last["peso_total"]   / prev["peso_total"]   - 1) * 100 if prev["peso_total"] > 0 else 0

c1, c2, c3, c4 = st.columns(4)
with c1: st.markdown(kpi(
    f"{summary['n_atracacoes']:,}", "Total de atracações (2010–2026)",
    f"17 anos de dados · {summary['n_portos']} portos", C["orange"], "⚓"), unsafe_allow_html=True)
with c2: st.markdown(kpi(
    f"{last['n_atracacoes']:,}", f"Atracações em {summary['last_year']}",
    f"{pct_atrac:+.1f}% vs ano anterior", C["blue"], "📊"), unsafe_allow_html=True)
with c3: st.markdown(kpi(
    f"{summary['estadia_media']:.0f}h", "Estadia média histórica",
    "Chegada → desatracação", C["yellow"], "⏱️"), unsafe_allow_html=True)
with c4: st.markdown(kpi(
    f"{summary['espera_media']:.0f}h", "Espera média para atracar",
    "Tempo de fila antes do berço", C["red"], "⌛"), unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ── SÉRIE HISTÓRICA ───────────────────────────────────────────
st.markdown(sec_tag("01 · SÉRIE HISTÓRICA", "atracações por ano 2010–2026"), unsafe_allow_html=True)

fig = go.Figure()

# Barras por região
regioes = df.groupby(["Ano", "Região Geográfica"]).size().reset_index(name="n")
for reg, color in REGIAO_COLORS.items():
    sub = regioes[regioes["Região Geográfica"] == reg]
    fig.add_trace(go.Bar(
        x=sub["Ano"], y=sub["n"], name=reg,
        marker_color=hex_to_rgba(color, 0.8),
        hovertemplate=f"<b>{reg}</b><br>%{{x}}: %{{y:,}}<extra></extra>",
    ))

# Eventos
for ano, label, color, desc in EVENTS:
    sub = anual[anual["Ano"] == ano]
    if len(sub) == 0: continue
    y_val = sub.iloc[0]["n_atracacoes"]
    fig.add_annotation(
        x=ano, y=y_val * 1.02, text=label,
        showarrow=True, arrowhead=2, arrowcolor=color,
        font=dict(color=color, size=9), bgcolor=C["surface"],
        bordercolor=color, borderwidth=1, arrowwidth=1.5,
    )

fig.update_layout(**get_layout(
    height=400, barmode="stack",
    yaxis_title="Nº de atracações",
    legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center"),
))
st.plotly_chart(fig, use_container_width=True)

st.markdown(insight(
    f"<strong>1,3 milhão de atracações em 17 anos.</strong> "
    f"O setor portuário brasileiro cresceu de forma consistente apesar das crises. "
    f"A pandemia de COVID-19 (2020) causou queda visível, mas a recuperação em 2021 foi imediata. "
    f"A Região Norte cresce proporcionalmente mais rápido que todas as outras — reflexo do boom do Arco Norte.",
    C["orange"]
), unsafe_allow_html=True)

st.divider()

# ── EFICIÊNCIA HISTÓRICA ──────────────────────────────────────
st.markdown(sec_tag("02 · EFICIÊNCIA HISTÓRICA", "tempo médio de espera e estadia por ano"), unsafe_allow_html=True)

fig2 = go.Figure()
anual_filt = anual.dropna(subset=["estadia_media", "espera_media"])

fig2.add_trace(go.Scatter(
    x=anual_filt["Ano"], y=anual_filt["estadia_media"],
    mode="lines+markers", name="Estadia média (h)",
    line=dict(color=C["orange"], width=2),
    marker=dict(size=6),
    hovertemplate="<b>%{x}</b><br>Estadia: %{y:.1f}h<extra></extra>",
))
fig2.add_trace(go.Scatter(
    x=anual_filt["Ano"], y=anual_filt["espera_media"],
    mode="lines+markers", name="Espera média (h)",
    line=dict(color=C["red"], width=2, dash="dot"),
    marker=dict(size=6),
    hovertemplate="<b>%{x}</b><br>Espera: %{y:.1f}h<extra></extra>",
))
fig2.add_trace(go.Scatter(
    x=anual_filt["Ano"], y=anual_filt["op_media"],
    mode="lines+markers", name="Operação média (h)",
    line=dict(color=C["green"], width=2, dash="dash"),
    marker=dict(size=6),
    hovertemplate="<b>%{x}</b><br>Operação: %{y:.1f}h<extra></extra>",
))

# COVID annotation
fig2.add_vline(x=2020, line_color=C["red"], line_dash="dot", opacity=0.5,
               annotation_text="COVID 2020", annotation_font_color=C["red"])

fig2.update_layout(**get_layout(
    height=340, yaxis_title="Horas",
    legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center"),
))
st.plotly_chart(fig2, use_container_width=True)

st.markdown(insight(
    "<strong>Os portos brasileiros não melhoraram significativamente em eficiência em 17 anos.</strong> "
    "O tempo de estadia médio se manteve relativamente estável, indicando que o crescimento em volume "
    "foi acompanhado de investimentos em capacidade — mas não necessariamente em velocidade operacional. "
    "A espera para atracar continua sendo o principal gargalo.",
    C["yellow"]
), unsafe_allow_html=True)

st.divider()

# ── TOP PORTOS ────────────────────────────────────────────────
st.markdown(sec_tag("03 · TOP PORTOS", "volume total 2010–2026"), unsafe_allow_html=True)

porto_agg = agg_porto(df, min_atrac=500)
top20 = porto_agg.sort_values("n_atracacoes", ascending=False).head(20)

fig3 = go.Figure(go.Bar(
    y=top20["Porto Atracação"],
    x=top20["n_atracacoes"],
    orientation="h",
    marker_color=[C["orange"] if i == 0 else hex_to_rgba(C["orange"], 0.6)
                  for i in range(len(top20))],
    text=[f"{v:,}" for v in top20["n_atracacoes"]],
    textposition="outside",
    textfont=dict(color=C["muted"], size=10),
    hovertemplate="<b>%{y}</b><br>%{x:,} atracações<extra></extra>",
))
ly = get_layout(height=560, xaxis_title="Nº de atracações",
                margin=dict(l=16, r=80, t=20, b=16))
ly["yaxis"] = dict(**get_layout()["yaxis"], categoryorder="total ascending")
fig3.update_layout(**ly)
st.plotly_chart(fig3, use_container_width=True)

st.markdown(insight(
    f"<strong>Santos domina com {top20.iloc[0]['n_atracacoes']:,} atracações</strong> — "
    "mas a presença massiva de portos do Norte (Belém, Santarém, Manaus) revela "
    "a importância da navegação fluvial interior para o Brasil. "
    "O Arco Norte de exportação de grãos (Pará, Maranhão) ganhou força crescente a partir de 2018.",
    C["blue"]
), unsafe_allow_html=True)

st.divider()

# ── TIPO DE NAVEGAÇÃO ─────────────────────────────────────────
st.markdown(sec_tag("04 · TIPOS DE NAVEGAÇÃO", "evolução de longo curso, cabotagem e interior"), unsafe_allow_html=True)

nav_anual = df.groupby(["Ano", "Tipo de Navegação da Atracação"]).size().reset_index(name="n")

fig4 = go.Figure()
for nav, color in NAV_COLORS.items():
    sub = nav_anual[nav_anual["Tipo de Navegação da Atracação"] == nav]
    if len(sub) == 0: continue
    fig4.add_trace(go.Scatter(
        x=sub["Ano"], y=sub["n"],
        mode="lines", name=nav,
        line=dict(color=color, width=2),
        fill="tonexty" if nav == "Interior" else "none",
        fillcolor=hex_to_rgba(color, 0.05),
        hovertemplate=f"<b>{nav}</b><br>%{{x}}: %{{y:,}}<extra></extra>",
    ))

fig4.add_vline(x=2021, line_color=C["blue"], line_dash="dot", opacity=0.5,
               annotation_text="BR do Mar", annotation_font_color=C["blue"])
fig4.update_layout(**get_layout(
    height=320, yaxis_title="Nº de atracações",
    legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center"),
))
st.plotly_chart(fig4, use_container_width=True)

# Cabotagem growth
cab_2010 = nav_anual[(nav_anual["Tipo de Navegação da Atracação"]=="Cabotagem") & (nav_anual["Ano"]==2010)]
cab_last = nav_anual[(nav_anual["Tipo de Navegação da Atracação"]=="Cabotagem") & (nav_anual["Ano"]==summary["last_year"])]
cab_growth = ""
if len(cab_2010) and len(cab_last):
    g = (cab_last.iloc[0]["n"] / cab_2010.iloc[0]["n"] - 1) * 100
    cab_growth = f"A cabotagem cresceu +{g:.0f}% entre 2010 e {summary['last_year']}. "

st.markdown(insight(
    f"<strong>A navegação interior domina em volume de atracações</strong> — "
    f"rios amazônicos geram milhares de escalas por ano. "
    f"{cab_growth}"
    f"A política BR do Mar (2021) acelerou a cabotagem como alternativa ao transporte rodoviário.",
    C["green"]
), unsafe_allow_html=True)
