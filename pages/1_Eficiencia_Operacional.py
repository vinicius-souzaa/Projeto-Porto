import streamlit as st
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from util.style    import inject_css, kpi, insight, sec_tag, badge, data_row, get_layout, hex_to_rgba, C
from util.data     import load_master, get_summary, agg_porto
from util.layout   import sidebar
from util.constants import NAV_COLORS

inject_css()
df      = load_master()
summary = get_summary(df)
sidebar(summary)

st.markdown(
    f'<h1 style="font-size:1.9rem;font-weight:700;color:{C["text"]};margin:0 0 0.2rem;">'
    f'⚡ Eficiência Operacional</h1>'
    f'<div style="font-size:0.84rem;color:{C["muted"]};margin-bottom:1.2rem;">'
    f'Tempos de espera, operação e estadia por porto · 2010–2026</div>',
    unsafe_allow_html=True
)

# Filtra só atracações com tempos válidos
df_op = df[
    df["TEstadia"].notna() &
    df["TEstadia"].between(0, 720)
].copy()

# ── KPIs ──────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1: st.markdown(kpi(
    f"{df_op['TEstadia'].median():.0f}h", "Estadia mediana (P50)",
    "50% dos navios saem em menos de X horas", C["orange"], "⏱️"), unsafe_allow_html=True)
with c2: st.markdown(kpi(
    f"{df_op['TEstadia'].quantile(0.75):.0f}h", "Estadia P75",
    "25% dos navios ficam mais que isso", C["yellow"], "⌛"), unsafe_allow_html=True)
with c3: st.markdown(kpi(
    f"{df_op['TEsperaAtracacao'].median():.0f}h", "Espera mediana para atracar",
    "Tempo na fila antes do berço", C["red"], "🚢"), unsafe_allow_html=True)
with c4: st.markdown(kpi(
    f"{df_op['TOperacao'].median():.0f}h", "Operação mediana",
    "Tempo efetivo de carga/descarga", C["green"], "🏗️"), unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["🏆 Ranking de Portos", "📈 Evolução Histórica", "🔍 Comparativo por Tipo"])

# ── TAB 1: RANKING ────────────────────────────────────────────
with tab1:
    st.markdown(sec_tag("RANKING DE EFICIÊNCIA", "portos com ≥ 200 atracações"), unsafe_allow_html=True)

    col_metric = st.radio(
        "Ordenar por:", ["Espera para atracar", "Tempo de estadia", "Tempo de operação"],
        horizontal=True
    )

    metric_map = {
        "Espera para atracar":  "espera_media",
        "Tempo de estadia":     "estadia_media",
        "Tempo de operação":    "op_media",
    }
    metric_col = metric_map[col_metric]
    metric_label = col_metric

    porto_ef = (df_op.groupby("Porto Atracação")
                .agg(
                    n             = ("IDAtracacao",      "count"),
                    estadia_media = ("TEstadia",          "mean"),
                    espera_media  = ("TEsperaAtracacao",  "mean"),
                    op_media      = ("TOperacao",         "mean"),
                    regiao        = ("Região Geográfica", lambda x: x.mode()[0] if len(x)>0 else ""),
                )
                .reset_index())
    porto_ef = porto_ef[porto_ef["n"] >= 200].dropna(subset=[metric_col])

    n_show = st.slider("Número de portos", 10, 30, 20)

    # Piores e melhores
    piores = porto_ef.sort_values(metric_col, ascending=False).head(n_show)
    melhores = porto_ef.sort_values(metric_col, ascending=True).head(n_show)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f'<div style="font-size:0.75rem;color:{C["red"]};font-weight:600;'
                    f'text-transform:uppercase;letter-spacing:0.1em;margin-bottom:0.5rem;">'
                    f'⚠️ Maiores tempos ({metric_label})</div>', unsafe_allow_html=True)
        fig1 = go.Figure(go.Bar(
            y=piores["Porto Atracação"],
            x=piores[metric_col],
            orientation="h",
            marker_color=hex_to_rgba(C["red"], 0.7),
            text=[f"{v:.0f}h" for v in piores[metric_col]],
            textposition="outside",
            textfont=dict(color=C["muted"], size=9),
        ))
        ly = get_layout(height=max(300, n_show*22),
                        xaxis_title="Horas", margin=dict(l=16,r=60,t=16,b=16))
        ly["yaxis"] = dict(**get_layout()["yaxis"], categoryorder="total ascending")
        fig1.update_layout(**ly)
        st.plotly_chart(fig1, use_container_width=True)

    with c2:
        st.markdown(f'<div style="font-size:0.75rem;color:{C["green"]};font-weight:600;'
                    f'text-transform:uppercase;letter-spacing:0.1em;margin-bottom:0.5rem;">'
                    f'✅ Menores tempos ({metric_label})</div>', unsafe_allow_html=True)
        fig2 = go.Figure(go.Bar(
            y=melhores["Porto Atracação"],
            x=melhores[metric_col],
            orientation="h",
            marker_color=hex_to_rgba(C["green"], 0.7),
            text=[f"{v:.0f}h" for v in melhores[metric_col]],
            textposition="outside",
            textfont=dict(color=C["muted"], size=9),
        ))
        ly2 = get_layout(height=max(300, n_show*22),
                         xaxis_title="Horas", margin=dict(l=16,r=60,t=16,b=16))
        ly2["yaxis"] = dict(**get_layout()["yaxis"], categoryorder="total ascending")
        fig2.update_layout(**ly2)
        st.plotly_chart(fig2, use_container_width=True)

    worst = piores.iloc[0]
    best  = melhores.iloc[0]
    st.markdown(insight(
        f"<strong>{worst['Porto Atracação']} tem o maior {metric_label.lower()}: "
        f"{worst[metric_col]:.0f}h em média.</strong> "
        f"{best['Porto Atracação']} é o mais eficiente com {best[metric_col]:.0f}h. "
        f"A diferença de {worst[metric_col]-best[metric_col]:.0f}h entre o melhor e o pior porto "
        f"representa dias de custo para armadores e embarcadores.",
        C["orange"]
    ), unsafe_allow_html=True)

# ── TAB 2: EVOLUÇÃO HISTÓRICA ─────────────────────────────────
with tab2:
    st.markdown(sec_tag("EVOLUÇÃO HISTÓRICA", "tempo de estadia e espera por ano"), unsafe_allow_html=True)

    # Seletor de porto
    portos_list = sorted(df_op["Porto Atracação"].dropna().unique())
    portos_sel = st.multiselect(
        "Selecionar portos (deixe vazio para média nacional):",
        options=portos_list,
        default=["Santos", "Paranaguá", "Rio Grande", "Belém"]
            if all(p in portos_list for p in ["Santos","Paranaguá","Rio Grande","Belém"])
            else portos_list[:4],
        max_selections=6,
    )

    df_sel = df_op[df_op["Porto Atracação"].isin(portos_sel)] if portos_sel else df_op

    ev_porto = (df_sel.groupby(["Ano", "Porto Atracação"] if portos_sel else ["Ano"])
                .agg(estadia=("TEstadia","mean"), espera=("TEsperaAtracacao","mean"))
                .reset_index())

    fig3 = go.Figure()
    colors_cycle = [C["orange"], C["blue"], C["green"], C["yellow"], C["red"], C["teal"]]

    if portos_sel:
        for i, porto in enumerate(portos_sel):
            sub = ev_porto[ev_porto["Porto Atracação"] == porto]
            color = colors_cycle[i % len(colors_cycle)]
            fig3.add_trace(go.Scatter(
                x=sub["Ano"], y=sub["estadia"],
                mode="lines+markers", name=porto,
                line=dict(color=color, width=2), marker=dict(size=5),
                hovertemplate=f"<b>{porto}</b><br>%{{x}}: %{{y:.1f}}h<extra></extra>",
            ))
    else:
        fig3.add_trace(go.Scatter(
            x=ev_porto["Ano"], y=ev_porto["estadia"],
            mode="lines+markers", name="Média nacional",
            line=dict(color=C["orange"], width=2.5), marker=dict(size=6),
        ))

    fig3.add_vline(x=2020, line_color=C["red"], line_dash="dot", opacity=0.4,
                   annotation_text="COVID", annotation_font_color=C["red"])
    fig3.update_layout(**get_layout(
        height=340, yaxis_title="Estadia média (horas)",
        legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center"),
    ))
    st.plotly_chart(fig3, use_container_width=True)

    st.markdown(insight(
        "<strong>COVID 2020 acelerou a burocracia portuária em alguns portos</strong> — "
        "protocolos sanitários e restrições de tripulação aumentaram o tempo de estadia. "
        "A recuperação em 2021–2022 foi heterogênea: portos de granel se recuperaram mais rápido "
        "que terminais de passageiros e carga geral.",
        C["blue"]
    ), unsafe_allow_html=True)

# ── TAB 3: POR TIPO ───────────────────────────────────────────
with tab3:
    st.markdown(sec_tag("EFICIÊNCIA POR TIPO DE NAVEGAÇÃO"), unsafe_allow_html=True)

    nav_ef = (df_op.groupby("Tipo de Navegação da Atracação")
              .agg(
                  n             = ("IDAtracacao",     "count"),
                  estadia_media = ("TEstadia",         "mean"),
                  estadia_p50   = ("TEstadia",         "median"),
                  estadia_p75   = ("TEstadia",         lambda x: x.quantile(0.75)),
                  espera_media  = ("TEsperaAtracacao", "mean"),
                  op_media      = ("TOperacao",        "mean"),
              )
              .reset_index()
              .sort_values("estadia_media", ascending=False))

    fig4 = go.Figure()
    for _, row in nav_ef.iterrows():
        nav   = row["Tipo de Navegação da Atracação"]
        color = NAV_COLORS.get(nav, C["muted"])
        fig4.add_trace(go.Bar(
            name=nav,
            x=["Estadia média","Estadia P50","Estadia P75","Espera média","Operação média"],
            y=[row["estadia_media"], row["estadia_p50"], row["estadia_p75"],
               row["espera_media"],  row["op_media"]],
            marker_color=hex_to_rgba(color, 0.75),
            hovertemplate=f"<b>{nav}</b><br>%{{x}}: %{{y:.1f}}h<extra></extra>",
        ))
    fig4.update_layout(**get_layout(
        height=340, barmode="group", yaxis_title="Horas",
        legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center"),
    ))
    st.plotly_chart(fig4, use_container_width=True)

    # Cards por tipo
    cols = st.columns(len(nav_ef))
    for i, (_, row) in enumerate(nav_ef.iterrows()):
        nav   = row["Tipo de Navegação da Atracação"]
        color = NAV_COLORS.get(nav, C["muted"])
        cols[i].markdown(f"""
        <div style="background:{C['surface']};border:1px solid {C['border']};
                    border-radius:8px;padding:0.85rem;border-top:2px solid {color};">
            <div style="font-size:0.65rem;color:{C['muted']};text-transform:uppercase;
                        letter-spacing:0.1em;margin-bottom:0.3rem;">{nav}</div>
            <div style="font-size:1.5rem;font-weight:700;color:{color};">
                {row['estadia_media']:.0f}h</div>
            <div style="font-size:0.7rem;color:{C['muted']};margin-top:0.3rem;">
                {data_row("Mediana", f"{row['estadia_p50']:.0f}h")}
                {data_row("Espera",  f"{row['espera_media']:.0f}h", C["red"])}
                {data_row("N atrac", f"{int(row['n']):,}")}
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown(insight(
        "<strong>Longo Curso tem a maior estadia média</strong> — navios internacionais "
        "movimentam volumes maiores e passam por mais burocracia (despacho aduaneiro, "
        "inspeção de carga, documentação internacional). "
        "A navegação interior tem estadia curta pois opera com embarcações menores "
        "e processos simplificados em portos fluviais.",
        C["teal"]
    ), unsafe_allow_html=True)
