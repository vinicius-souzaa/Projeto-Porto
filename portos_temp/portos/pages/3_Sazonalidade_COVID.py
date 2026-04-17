import streamlit as st
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from util.style    import inject_css, kpi, insight, sec_tag, get_layout, hex_to_rgba, C
from util.data     import load_master, get_summary, agg_mensal
from util.layout   import sidebar
from util.constants import MESES_ORDER, MESES_LABEL, NAV_COLORS

inject_css()
df      = load_master()
summary = get_summary(df)
sidebar(summary)

st.markdown(
    f'<h1 style="font-size:1.9rem;font-weight:700;color:{C["text"]};margin:0 0 0.2rem;">'
    f'📅 Sazonalidade & COVID</h1>'
    f'<div style="font-size:0.84rem;color:{C["muted"]};margin-bottom:1.2rem;">'
    f'Padrões mensais · impacto da pandemia · recuperação 2021–2023</div>',
    unsafe_allow_html=True
)

tab1, tab2 = st.tabs(["📅 Sazonalidade", "🦠 Impacto COVID"])

# ── TAB 1: SAZONALIDADE ───────────────────────────────────────
with tab1:
    st.markdown(sec_tag("SAZONALIDADE MENSAL", "média histórica por mês 2010–2026"), unsafe_allow_html=True)

    mensal = agg_mensal(df)

    fig = go.Figure(go.Bar(
        x=[MESES_LABEL.get(m, m) for m in mensal["Mes"]],
        y=mensal["n_atracacoes"],
        marker_color=[C["orange"] if v == mensal["n_atracacoes"].max() else
                      hex_to_rgba(C["orange"], 0.6) for v in mensal["n_atracacoes"]],
        text=[f"{v:,}" for v in mensal["n_atracacoes"]],
        textposition="outside",
        textfont=dict(color=C["muted"], size=10),
        hovertemplate="<b>%{x}</b><br>%{y:,} atracações<extra></extra>",
    ))
    fig.update_layout(**get_layout(height=300, yaxis_title="Média de atracações"))
    st.plotly_chart(fig, use_container_width=True)

    # Sazonalidade por tipo de navegação
    st.markdown(sec_tag("SAZONALIDADE POR TIPO DE NAVEGAÇÃO"), unsafe_allow_html=True)

    nav_mes = (df.groupby(["Mes","Tipo de Navegação da Atracação"])
               .size().reset_index(name="n"))
    nav_mes["Mes_ord"] = nav_mes["Mes"].map({m:i for i,m in enumerate(MESES_ORDER)})
    nav_mes = nav_mes.sort_values("Mes_ord")

    fig2 = go.Figure()
    for nav, color in NAV_COLORS.items():
        sub = nav_mes[nav_mes["Tipo de Navegação da Atracação"] == nav]
        if len(sub) == 0: continue
        fig2.add_trace(go.Scatter(
            x=[MESES_LABEL.get(m, m) for m in sub["Mes"]],
            y=sub["n"],
            mode="lines+markers", name=nav,
            line=dict(color=color, width=2), marker=dict(size=5),
            hovertemplate=f"<b>{nav}</b><br>%{{x}}: %{{y:,}}<extra></extra>",
        ))
    fig2.update_layout(**get_layout(height=300, yaxis_title="Atracações",
                                     legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center")))
    st.plotly_chart(fig2, use_container_width=True)

    # Heatmap ano x mês
    st.markdown(sec_tag("HEATMAP ANO × MÊS"), unsafe_allow_html=True)
    pivot = (df.groupby(["Ano","Mes"]).size().reset_index(name="n"))
    pivot_wide = pivot.pivot(index="Ano", columns="Mes", values="n")
    pivot_wide = pivot_wide.reindex(columns=MESES_ORDER)

    fig3 = go.Figure(go.Heatmap(
        z=pivot_wide.values,
        x=[MESES_LABEL.get(m,m) for m in pivot_wide.columns],
        y=pivot_wide.index.tolist(),
        colorscale=[[0, C["surface2"]], [0.5, hex_to_rgba(C["orange"], 0.8)], [1, C["orange"]]],
        hovertemplate="<b>%{y}</b> · %{x}<br>%{z:,} atracações<extra></extra>",
        colorbar=dict(tickfont=dict(color=C["muted"])),
    ))
    fig3.update_layout(**get_layout(height=400, margin=dict(l=50,r=16,t=20,b=50)))
    st.plotly_chart(fig3, use_container_width=True)

    st.markdown(insight(
        "<strong>Julho e agosto são os meses mais movimentados</strong> — safra de grãos "
        "do Centro-Oeste chega aos portos no segundo semestre. "
        "Janeiro e fevereiro são os mais lentos — efeito do recesso e menor demanda industrial. "
        "O padrão é consistente em todos os anos da série.",
        C["orange"]
    ), unsafe_allow_html=True)

# ── TAB 2: COVID ──────────────────────────────────────────────
with tab2:
    st.markdown(sec_tag("IMPACTO COVID-19", "2020 vs anos anteriores e recuperação"), unsafe_allow_html=True)

    # Atracações mensais 2018–2023
    df["Ano_Mes"] = df["Ano"].astype(str) + "-" + df["Mes"].map(
        {m: str(i+1).zfill(2) for i, m in enumerate(MESES_ORDER)})
    df_covid = df[df["Ano"].between(2018, 2023)]
    covid_mes = (df_covid.groupby(["Ano","Mes"])
                 .agg(n=("IDAtracacao","count"), estadia=("TEstadia","mean"))
                 .reset_index())
    covid_mes["Mes_ord"] = covid_mes["Mes"].map({m:i for i,m in enumerate(MESES_ORDER)})
    covid_mes = covid_mes.sort_values(["Ano","Mes_ord"])

    fig4 = go.Figure()
    colors_covid = {2018: C["dim"], 2019: C["muted"], 2020: C["red"],
                    2021: C["yellow"], 2022: C["green"], 2023: C["orange"]}
    for ano, color in colors_covid.items():
        sub = covid_mes[covid_mes["Ano"] == ano]
        if len(sub) == 0: continue
        fig4.add_trace(go.Scatter(
            x=[MESES_LABEL.get(m,m) for m in sub["Mes"]],
            y=sub["n"],
            mode="lines+markers", name=str(ano),
            line=dict(color=color, width=2.5 if ano in [2020,2023] else 1.5),
            marker=dict(size=5),
            hovertemplate=f"<b>{ano}</b><br>%{{x}}: %{{y:,}}<extra></extra>",
        ))
    fig4.update_layout(**get_layout(height=340, yaxis_title="Atracações",
                                     legend=dict(orientation="h", y=-0.14, x=0.5, xanchor="center")))
    st.plotly_chart(fig4, use_container_width=True)

    # Queda e recuperação
    anual_covid = df_covid.groupby("Ano").size().reset_index(name="n")
    n_2019 = anual_covid[anual_covid["Ano"]==2019]["n"].values[0] if 2019 in anual_covid["Ano"].values else 1
    n_2020 = anual_covid[anual_covid["Ano"]==2020]["n"].values[0] if 2020 in anual_covid["Ano"].values else 0
    n_2023 = anual_covid[anual_covid["Ano"]==2023]["n"].values[0] if 2023 in anual_covid["Ano"].values else 0
    queda = (n_2020/n_2019 - 1)*100
    recuper = (n_2023/n_2019 - 1)*100

    c1, c2, c3 = st.columns(3)
    with c1: st.markdown(kpi(f"{n_2019:,}", "Atracações 2019", "Pré-pandemia", C["green"], "📊"), unsafe_allow_html=True)
    with c2: st.markdown(kpi(f"{n_2020:,}", "Atracações 2020", f"{queda:+.1f}% vs 2019", C["red"], "🦠"), unsafe_allow_html=True)
    with c3: st.markdown(kpi(f"{n_2023:,}", "Atracações 2023", f"{recuper:+.1f}% vs 2019", C["orange"], "🚀"), unsafe_allow_html=True)

    # Estadia durante COVID por porto
    st.markdown(sec_tag("TEMPO DE ESTADIA — ANTES, DURANTE E DEPOIS"), unsafe_allow_html=True)
    df_pcovid = df[df["Ano"].between(2018,2023) & df["TEstadia"].between(0,720)]
    covid_porto = (df_pcovid.groupby(["Porto Atracação","Ano"])["TEstadia"]
                   .mean().reset_index())
    covid_porto = covid_porto[covid_porto["Ano"].isin([2019,2020,2021])]

    top_portos_covid = (covid_porto.groupby("Porto Atracação")
                        .filter(lambda x: len(x) == 3)["Porto Atracação"]
                        .value_counts().head(15).index.tolist())

    covid_porto_top = covid_porto[covid_porto["Porto Atracação"].isin(top_portos_covid)]
    covid_wide = covid_porto_top.pivot(index="Porto Atracação", columns="Ano", values="TEstadia")
    covid_wide["variacao_2020"] = covid_wide.get(2020, 0) - covid_wide.get(2019, 0)
    covid_wide = covid_wide.sort_values("variacao_2020", ascending=False).head(15)

    fig5 = go.Figure()
    for ano, color in [(2019, C["muted"]), (2020, C["red"]), (2021, C["green"])]:
        if ano not in covid_wide.columns: continue
        fig5.add_trace(go.Bar(
            name=str(ano),
            y=covid_wide.index,
            x=covid_wide[ano],
            orientation="h",
            marker_color=hex_to_rgba(color, 0.75),
            hovertemplate=f"<b>%{{y}}</b><br>{ano}: %{{x:.1f}}h<extra></extra>",
        ))
    ly = get_layout(height=460, barmode="group",
                    xaxis_title="Estadia média (h)", margin=dict(l=16,r=16,t=20,b=16))
    ly["yaxis"] = dict(**get_layout()["yaxis"], categoryorder="total ascending")
    fig5.update_layout(**ly)
    st.plotly_chart(fig5, use_container_width=True)

    st.markdown(insight(
        f"<strong>A pandemia aumentou o tempo de estadia em portos de maior movimentação.</strong> "
        f"Protocolos sanitários, restrições de tripulação e menor disponibilidade de mão de obra "
        f"operacional atrasaram operações. Em 2021, a maioria dos portos já havia se recuperado "
        f"— e em 2023 o setor bateu o recorde histórico de 1,3 bilhão de toneladas.",
        C["blue"]
    ), unsafe_allow_html=True)
