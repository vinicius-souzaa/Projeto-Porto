import streamlit as st
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from util.style    import inject_css, kpi, insight, sec_tag, data_row, get_layout, hex_to_rgba, C
from util.data     import load_master, load_model, load_shap, get_summary
from util.layout   import sidebar

inject_css()
df      = load_master()
summary = get_summary(df)
sidebar(summary)

st.markdown(
    f'<h1 style="font-size:1.9rem;font-weight:700;color:{C["text"]};margin:0 0 0.2rem;">'
    f'🤖 Modelo Preditivo — Estadia do Navio</h1>'
    f'<div style="font-size:0.84rem;color:{C["muted"]};margin-bottom:1.2rem;">'
    f'XGBoost · Previsão de TEstadia em horas · 1,28M atracações de treino</div>',
    unsafe_allow_html=True
)

# Carrega modelo
try:
    model, meta, encoders = load_model()
    shap_imp = load_shap()
    model_ok = True
except Exception as e:
    st.error(f"Erro ao carregar modelo: {e}")
    model_ok = False
    st.stop()

# ── KPIs DO MODELO ────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1: st.markdown(kpi(
    f"{meta['r2_log']:.2%}", "R² (espaço log)",
    "Variância explicada pelo modelo", C["green"], "📊"), unsafe_allow_html=True)
with c2: st.markdown(kpi(
    f"{meta['mae_horas']:.0f}h", "MAE médio",
    "Erro médio absoluto de previsão", C["orange"], "📏"), unsafe_allow_html=True)
with c3: st.markdown(kpi(
    f"{meta['n_train']:,}", "Atracações de treino",
    "80% do dataset histórico", C["blue"], "🏋️"), unsafe_allow_html=True)
with c4: st.markdown(kpi(
    "XGBoost", "Algoritmo",
    "Gradient Boosting + SHAP", C["teal"], "🌳"), unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["🎯 Simulador", "📊 SHAP — Fatores", "📈 Performance"])

# ── TAB 1: SIMULADOR ──────────────────────────────────────────
with tab1:
    st.markdown(sec_tag("SIMULADOR DE ESTADIA", "preveja o tempo que um navio ficará no porto"), unsafe_allow_html=True)

    # Contexto
    st.markdown(f"""
    <div style="background:{C['surface']};border:1px solid {C['border']};
                border-radius:8px;padding:0.9rem 1.1rem;margin-bottom:1rem;">
        <div style="font-size:0.8rem;color:{C['muted']};line-height:1.65;">
            <strong style="color:{C['text']};">Como funciona:</strong>
            O modelo foi treinado com 1,28 milhão de atracações reais (2010–2026).
            Preencha as características da operação e o modelo prevê o tempo total de estadia —
            desde a chegada na área de fundeio até a desatracação final.
        </div>
    </div>
    """, unsafe_allow_html=True)

    col_inp, col_out = st.columns([1, 1])

    with col_inp:
        porto_sel = st.selectbox("Porto de atracação", options=sorted(meta["portos"]))
        tipo_nav  = st.selectbox("Tipo de navegação", options=meta["tipos_nav"])
        tipo_op   = st.selectbox("Tipo de operação",  options=meta["tipos_op"])
        natureza  = st.selectbox("Natureza da carga", options=meta["naturezas"])
        mes_sel   = st.selectbox("Mês previsto", options=meta["meses"])
        regiao    = st.selectbox("Região geográfica", options=meta["regioes"])

        st.markdown("**Carga estimada:**")
        peso_ton  = st.number_input("Tonelagem (ton)", min_value=0.0,
                                     max_value=500_000.0, value=10_000.0, step=1000.0)
        teu_qt    = st.number_input("Contêineres (TEU)", min_value=0.0,
                                     max_value=20_000.0, value=0.0, step=100.0)
        qt_carga  = st.number_input("Qtd. de itens de carga", min_value=0.0,
                                     max_value=100_000.0, value=100.0, step=100.0)
        n_itens   = st.number_input("Nº de registros de carga", min_value=0,
                                     max_value=5000, value=5, step=1)

        st.markdown("**Tempos de processo:**")
        t_esp_ini = st.number_input("Espera para iniciar operação (h)",
                                     min_value=0.0, max_value=200.0, value=2.0, step=0.5)
        t_esp_des = st.number_input("Espera para desatracar (h)",
                                     min_value=0.0, max_value=100.0, value=1.0, step=0.5)
        ano_sel   = st.number_input("Ano", min_value=2010, max_value=2030, value=2024)

    with col_out:
        # Monta vetor de input
        def encode_cat(col_name, value):
            le = encoders.get(col_name)
            if le is None: return 0
            try:
                return int(le.transform([value])[0])
            except ValueError:
                # Valor não visto — usa classe mais próxima
                classes = list(le.classes_)
                closest = min(classes, key=lambda x: abs(len(x)-len(value)))
                return int(le.transform([closest])[0])

        input_dict = {
            "peso_total":   peso_ton,
            "teu_total":    teu_qt,
            "qt_total":     qt_carga,
            "n_itens":      float(n_itens),
            "TEsperaInicioOp":      t_esp_ini,
            "TEsperaDesatracacao":  t_esp_des,
            "Ano":          float(ano_sel),
            "Porto_Atracacao_enc": encode_cat("Porto Atracação", porto_sel),
            "Tipo_de_Operacao_enc": encode_cat("Tipo de Operação", tipo_op),
            "Tipo_de_Navegacao_da_Atracacao_enc": encode_cat("Tipo de Navegação da Atracação", tipo_nav),
            "Regiao_Geografica_enc": encode_cat("Região Geográfica", regiao),
            "natureza_top_enc": encode_cat("natureza_top", natureza),
            "sentido_top_enc": encode_cat("sentido_top", "Desembarcados"),
            "Mes_enc": encode_cat("Mes", mes_sel),
        }

        X_input = pd.DataFrame([input_dict])
        log_pred = float(model.predict(X_input)[0])
        pred_h   = np.expm1(log_pred)

        # Intervalo baseado no MAE histórico por faixa
        if pred_h < 12:   mae_faixa = 3.7
        elif pred_h < 48: mae_faixa = 12.0
        elif pred_h < 168:mae_faixa = 38.2
        else:              mae_faixa = 80.0

        low_h  = max(0, pred_h - mae_faixa)
        high_h = pred_h + mae_faixa

        # Display resultado
        st.markdown(f"""
        <div style="background:{C['bg']};border:2px solid {C['orange']};
                    border-radius:12px;padding:1.5rem;text-align:center;margin-bottom:1rem;">
            <div style="font-size:0.7rem;color:{C['muted']};text-transform:uppercase;
                        letter-spacing:0.15em;margin-bottom:0.5rem;">Estadia prevista</div>
            <div style="font-size:3.5rem;font-weight:700;color:{C['orange']};line-height:1;">
                {pred_h:.0f}h</div>
            <div style="font-size:1rem;color:{C['muted']};margin-top:0.3rem;">
                {pred_h/24:.1f} dias</div>
            <div style="font-size:0.8rem;color:{C['dim']};margin-top:0.5rem;">
                Intervalo provável: {low_h:.0f}h – {high_h:.0f}h</div>
        </div>
        """, unsafe_allow_html=True)

        # Barra de intervalo
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=pred_h,
            number=dict(suffix="h", font=dict(color=C["orange"], size=28)),
            gauge=dict(
                axis=dict(range=[0, min(720, high_h*2)],
                          tickfont=dict(color=C["muted"], size=10)),
                bar=dict(color=C["orange"]),
                bgcolor=C["surface"],
                bordercolor=C["border"],
                steps=[
                    dict(range=[0,12],    color=hex_to_rgba(C["green"],0.2)),
                    dict(range=[12,48],   color=hex_to_rgba(C["yellow"],0.2)),
                    dict(range=[48,168],  color=hex_to_rgba(C["orange"],0.2)),
                    dict(range=[168,720], color=hex_to_rgba(C["red"],0.2)),
                ],
                threshold=dict(line=dict(color=C["red"],width=2), value=168),
            ),
        ))
        fig_gauge.update_layout(
            paper_bgcolor=C["surface"], height=220,
            margin=dict(l=20,r=20,t=20,b=10),
            font=dict(color=C["muted"]),
        )
        st.plotly_chart(fig_gauge, use_container_width=True)

        st.markdown(f"""
        <div style="font-size:0.73rem;color:{C['muted']};margin-top:0.5rem;">
            <strong style="color:{C['text']};">Faixas de referência:</strong>
            <span style="color:{C['green']};">■ 0–12h</span> rápido ·
            <span style="color:{C['yellow']};">■ 12–48h</span> normal ·
            <span style="color:{C['orange']};">■ 48–168h</span> longo ·
            <span style="color:{C['red']};">■ >168h</span> muito longo
        </div>
        """, unsafe_allow_html=True)

# ── TAB 2: SHAP ───────────────────────────────────────────────
with tab2:
    st.markdown(sec_tag("SHAP — IMPORTÂNCIA DAS FEATURES", "o que mais influencia o tempo de estadia?"), unsafe_allow_html=True)

    # Nomes legíveis
    feature_labels = {
        "peso_total":                           "Tonelagem da carga",
        "Porto_Atracacao_enc":                  "Porto de atracação",
        "TEsperaDesatracacao":                  "Espera para desatracar",
        "Tipo_de_Navegacao_da_Atracacao_enc":   "Tipo de navegação",
        "TEsperaInicioOp":                      "Espera para iniciar ops",
        "teu_total":                            "Nº de contêineres (TEU)",
        "natureza_top_enc":                     "Natureza da carga",
        "Tipo_de_Operacao_enc":                 "Tipo de operação",
        "Ano":                                  "Ano",
        "Regiao_Geografica_enc":                "Região geográfica",
        "qt_total":                             "Qtd. unidades de carga",
        "n_itens":                              "Nº registros de carga",
        "sentido_top_enc":                      "Sentido (emb./desemb.)",
        "Mes_enc":                              "Mês",
    }

    shap_imp["label"] = shap_imp["feature"].map(feature_labels).fillna(shap_imp["feature"])
    shap_sorted = shap_imp.sort_values("shap_mean", ascending=True)

    fig7 = go.Figure(go.Bar(
        y=shap_sorted["label"],
        x=shap_sorted["shap_mean"],
        orientation="h",
        marker_color=[C["orange"] if v == shap_sorted["shap_mean"].max()
                      else hex_to_rgba(C["orange"], 0.6)
                      for v in shap_sorted["shap_mean"]],
        text=[f"{v:.4f}" for v in shap_sorted["shap_mean"]],
        textposition="outside",
        textfont=dict(color=C["muted"], size=10),
        hovertemplate="<b>%{y}</b><br>SHAP médio: %{x:.4f}<extra></extra>",
    ))
    ly = get_layout(height=460, xaxis_title="SHAP value médio (impacto no log TEstadia)",
                    margin=dict(l=16, r=80, t=16, b=16))
    ly["yaxis"] = dict(**get_layout()["yaxis"], categoryorder="total ascending")
    fig7.update_layout(**ly)
    st.plotly_chart(fig7, use_container_width=True)

    st.markdown(insight(
        "<strong>A tonelagem da carga é o maior preditor individual</strong> — "
        "quanto mais peso, mais tempo de operação. "
        "<strong>O porto de atracação é o segundo fator mais importante</strong> — "
        "a infraestrutura e os processos de cada porto explicam diferenças de dezenas de horas. "
        "Curiosamente, o tipo de navegação e o mês têm impacto pequeno — "
        "o que importa mais é quanto o navio carrega e onde ele está.",
        C["blue"]
    ), unsafe_allow_html=True)

    # Explicação metodológica
    st.markdown(f"""
    <div style="background:{C['surface']};border:1px solid {C['border']};border-radius:8px;
                padding:0.9rem 1.1rem;margin-top:0.8rem;">
        <div style="font-size:0.75rem;color:{C['muted']};line-height:1.7;">
            <strong style="color:{C['text']};">Por que XGBoost e não Prophet ou ARIMA?</strong><br/>
            O tempo de estadia é determinado por fatores operacionais (tonelagem, porto, tipo de carga),
            não por tendência temporal. Prophet e ARIMA modelam séries temporais com tendência e sazonalidade —
            mas a sazonalidade mensal aqui tem correlação de apenas η²=0.0004 com TEstadia.
            O XGBoost captura interações complexas entre variáveis (ex: Paranaguá + Granel Sólido + 50k ton)
            que modelos lineares e temporais não conseguem. <strong style="color:{C['text']};">R²=0.78 no espaço log</strong>
            confirma que o modelo explica 78% da variância de TEstadia.
        </div>
    </div>
    """, unsafe_allow_html=True)

# ── TAB 3: PERFORMANCE ────────────────────────────────────────
with tab3:
    st.markdown(sec_tag("PERFORMANCE DO MODELO", "métricas de avaliação"), unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(kpi(f"{meta['r2_log']:.4f}",  "R² (log)",   "Variância explicada", C["green"],  "📊"), unsafe_allow_html=True)
    with c2: st.markdown(kpi(f"{meta['r2_horas']:.4f}", "R² (horas)", "Variância em horas",  C["yellow"], "📏"), unsafe_allow_html=True)
    with c3: st.markdown(kpi(f"{meta['mae_horas']:.1f}h", "MAE",       "Erro médio absoluto", C["orange"], "🎯"), unsafe_allow_html=True)
    with c4: st.markdown(kpi(f"{meta['rmse_horas']:.1f}h","RMSE",      "Erro quadrático médio",C["red"],  "📐"), unsafe_allow_html=True)

    # MAE por faixa
    faixas = ["0–12h\n(rápido)", "12–48h\n(normal)", "48–168h\n(longo)", "168–720h\n(muito longo)"]
    maes   = [3.7, 12.0, 38.2, 162.1]
    ns     = [85_912, 88_182, 58_963, 23_626]

    fig8 = go.Figure()
    fig8.add_trace(go.Bar(
        x=faixas, y=maes,
        marker_color=[C["green"], C["yellow"], C["orange"], C["red"]],
        text=[f"MAE={m:.1f}h\n(n={n:,})" for m, n in zip(maes, ns)],
        textposition="outside",
        textfont=dict(color=C["muted"], size=10),
        hovertemplate="<b>%{x}</b><br>MAE: %{y:.1f}h<extra></extra>",
    ))
    fig8.update_layout(**get_layout(height=300, yaxis_title="MAE (horas)",
                                     xaxis=dict(**get_layout()["xaxis"], tickangle=0)))
    st.plotly_chart(fig8, use_container_width=True)

    st.markdown(insight(
        "<strong>O modelo é muito mais preciso para estadias curtas.</strong> "
        "Para navios que ficam menos de 12h, o erro médio é de apenas 3,7h. "
        "Para estadias longas (>7 dias), o erro sobe para 162h — "
        "pois essas situações geralmente envolvem paralisações, reparos ou greves "
        "que nenhum modelo consegue prever sem dados do evento em si.",
        C["teal"]
    ), unsafe_allow_html=True)

    st.markdown(f"""
    <div style="background:{C['surface']};border:1px solid {C['border']};border-radius:8px;
                padding:0.9rem 1.1rem;margin-top:0.5rem;">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;font-size:0.78rem;color:{C['muted']};">
            <div>
                <strong style="color:{C['text']};">Dataset</strong><br/>
                {meta['n_train']:,} atracações de treino (80%)<br/>
                {meta['n_test']:,} atracações de teste (20%)<br/>
                Filtro: TEstadia entre 0 e 720h
            </div>
            <div>
                <strong style="color:{C['text']};">Configuração XGBoost</strong><br/>
                500 árvores · max_depth=8<br/>
                learning_rate=0.05 · early stopping=30<br/>
                Target: log1p(TEstadia) → skewness 3.20→0.09
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
