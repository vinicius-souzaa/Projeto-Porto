import streamlit as st
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from util.style    import inject_css, kpi, insight, sec_tag, data_row, get_layout, hex_to_rgba, C
from util.data     import load_master, get_summary, agg_porto
from util.layout   import sidebar
from util.constants import REGIAO_COLORS

inject_css()
df      = load_master()
summary = get_summary(df)
sidebar(summary)

st.markdown(
    f'<h1 style="font-size:1.9rem;font-weight:700;color:{C["text"]};margin:0 0 0.2rem;">'
    f'🏆 Ranking de Portos</h1>'
    f'<div style="font-size:0.84rem;color:{C["muted"]};margin-bottom:1.2rem;">'
    f'Comparativo lado a lado · volume, eficiência, crescimento · 2010–2026</div>',
    unsafe_allow_html=True
)

porto_agg = agg_porto(df, min_atrac=200)

# ── FILTROS ───────────────────────────────────────────────────
col_f1, col_f2 = st.columns([2,1])
with col_f1:
    regioes_sel = st.multiselect(
        "Filtrar por região:",
        options=sorted(porto_agg["regiao"].dropna().unique()),
        default=[],
    )
with col_f2:
    n_portos = st.slider("Top N portos:", 10, 40, 20)

df_rank = porto_agg.copy()
if regioes_sel:
    df_rank = df_rank[df_rank["regiao"].isin(regioes_sel)]

df_rank = df_rank.sort_values("n_atracacoes", ascending=False).head(n_portos)

# ── SCATTER: VOLUME vs EFICIÊNCIA ─────────────────────────────
st.markdown(sec_tag("VOLUME × EFICIÊNCIA", "cada bolha = um porto"), unsafe_allow_html=True)

fig = go.Figure()
for regiao, color in REGIAO_COLORS.items():
    sub = df_rank[df_rank["regiao"] == regiao]
    if len(sub) == 0: continue
    fig.add_trace(go.Scatter(
        x=sub["n_atracacoes"],
        y=sub["estadia_media"],
        mode="markers+text",
        name=regiao,
        marker=dict(
            size=np.sqrt(sub["peso_total"]/1e4).clip(8, 40),
            color=hex_to_rgba(color, 0.75),
            line=dict(color=color, width=1),
        ),
        text=sub["Porto Atracação"].str.split().str[0],
        textfont=dict(color=C["muted"], size=9),
        textposition="top center",
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "Atracações: %{x:,}<br>"
            "Estadia média: %{y:.1f}h<br>"
            "Espera média: %{customdata[1]:.1f}h<extra></extra>"
        ),
        customdata=sub[["Porto Atracação","espera_media"]].values,
    ))
fig.update_layout(**get_layout(
    height=440,
    xaxis_title="Nº de atracações (2010–2026)",
    yaxis_title="Estadia média (horas)",
    legend=dict(orientation="h", y=-0.12, x=0.5, xanchor="center"),
))
st.plotly_chart(fig, use_container_width=True)

st.markdown(insight(
    "<strong>Portos no canto inferior direito são os ideais:</strong> alto volume E baixa estadia. "
    "Portos no canto superior esquerdo têm baixo volume E alta estadia — candidatos a gargalo estrutural. "
    "O tamanho da bolha representa a tonelagem movimentada.",
    C["orange"]
), unsafe_allow_html=True)

st.divider()

# ── TABELA COMPARATIVA ────────────────────────────────────────
st.markdown(sec_tag("TABELA COMPARATIVA", "principais métricas por porto"), unsafe_allow_html=True)

# Crescimento: compara primeiro e último biênio disponível
df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")
ano_min = int(df["Ano"].min())
ano_max = int(df["Ano"].max())

early = df[df["Ano"].between(ano_min, ano_min+2)].groupby("Porto Atracação").size()
late  = df[df["Ano"].between(ano_max-2, ano_max)].groupby("Porto Atracação").size()
growth = ((late / early) - 1).mul(100).round(1)

for i in range(0, len(df_rank), 3):
    cols = st.columns(3)
    for j, col in enumerate(cols):
        if i+j >= len(df_rank): break
        row = df_rank.iloc[i+j]
        porto = row["Porto Atracação"]
        color = REGIAO_COLORS.get(row["regiao"], C["orange"])
        g     = growth.get(porto, None)
        g_str = f"{g:+.0f}%" if g is not None else "—"
        g_col = C["green"] if (g is not None and g > 0) else C["red"]

        col.markdown(f"""
        <div style="background:{C['surface']};border:1px solid {C['border']};
                    border-radius:8px;padding:0.9rem;margin-bottom:0.5rem;
                    border-left:3px solid {color};">
            <div style="font-size:0.65rem;color:{color};text-transform:uppercase;
                        letter-spacing:0.1em;margin-bottom:0.2rem;">{row['regiao']} · {row['uf']}</div>
            <div style="font-size:0.9rem;font-weight:700;color:{C['text']};
                        margin-bottom:0.5rem;line-height:1.2;">{porto[:30]}</div>
            <div style="font-size:0.72rem;">
                {data_row("Atracações",   f"{int(row['n_atracacoes']):,}")}
                {data_row("Estadia média",f"{row['estadia_media']:.0f}h", C["yellow"])}
                {data_row("Espera média", f"{row['espera_media']:.0f}h",  C["red"])}
                {data_row("Crescimento",  g_str, g_col)}
            </div>
        </div>
        """, unsafe_allow_html=True)

st.divider()

# ── RADAR: COMPARAR 4 PORTOS ──────────────────────────────────
st.markdown(sec_tag("COMPARADOR DE PORTOS", "selecione até 4 portos para comparar"), unsafe_allow_html=True)

all_portos = sorted(df_rank["Porto Atracação"].tolist())
default_4  = all_portos[:4]

portos_comp = st.multiselect(
    "Escolha os portos:", all_portos, default=default_4, max_selections=4)

if len(portos_comp) >= 2:
    comp_data = df_rank[df_rank["Porto Atracação"].isin(portos_comp)]

    # Normaliza 0-1 para radar (inverte eficiência: menos horas = melhor)
    cols_radar = ["n_atracacoes","peso_total","estadia_media","espera_media","teu_total"]
    labels_radar = ["Volume","Tonelagem","Estadia\n(inv)","Espera\n(inv)","Contêineres"]

    fig2 = go.Figure()
    for k, (_, row) in enumerate(comp_data.iterrows()):
        porto = row["Porto Atracação"]
        color = [C["orange"],C["blue"],C["green"],C["yellow"]][k % 4]
        # Normaliza: volume/tonelagem/teu → maior=melhor; estadia/espera → menor=melhor
        vals_norm = []
        for col in cols_radar:
            col_max = df_rank[col].max()
            col_min = df_rank[col].min()
            rng = col_max - col_min if col_max != col_min else 1
            v = (row[col] - col_min) / rng
            if col in ["estadia_media","espera_media"]:
                v = 1 - v  # inverte: menos tempo = melhor score
            vals_norm.append(round(v, 3))

        fig2.add_trace(go.Scatterpolar(
            r=vals_norm + [vals_norm[0]],
            theta=labels_radar + [labels_radar[0]],
            fill="toself",
            name=porto[:20],
            line=dict(color=color, width=2),
            fillcolor=hex_to_rgba(color, 0.12),
        ))

    fig2.update_layout(
        polar=dict(
            bgcolor=C["surface"],
            radialaxis=dict(visible=True, range=[0,1],
                           tickfont=dict(color=C["muted"], size=9),
                           gridcolor=C["border"]),
            angularaxis=dict(tickfont=dict(color=C["text"], size=11),
                            gridcolor=C["border"]),
        ),
        showlegend=True,
        paper_bgcolor=C["surface"],
        legend=dict(font=dict(color=C["muted"]), bgcolor=C["bg"]),
        height=400,
        margin=dict(l=40,r=40,t=30,b=30),
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.markdown(
        f'<div style="font-size:0.72rem;color:{C["muted"]};">'
        f'Score 0–1 normalizado. Estadia e Espera são invertidos: '
        f'score mais alto = menor tempo = mais eficiente.</div>',
        unsafe_allow_html=True
    )
