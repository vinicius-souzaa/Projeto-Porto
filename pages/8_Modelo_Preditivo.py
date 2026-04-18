"""Página 8 — Modelo Preditivo: predict com IC P10/P90, SHAP waterfall."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from util.data import load_model_meta, load_shap_importance, load_encoders_map

st.title("🤖 Modelo Preditivo de Tempos Portuários")

# ── Métricas e SHAP (apenas parquet — sempre funcionam) ───────────────────────

@st.cache_data(show_spinner="Carregando metadados...")
def _load_meta():
    return load_model_meta()

@st.cache_data(show_spinner="Carregando importâncias SHAP...")
def _load_shap():
    return load_shap_importance()

@st.cache_data
def _load_encoders():
    return load_encoders_map()

try:
    meta     = _load_meta()
    shap_imp = _load_shap()
    encoders = _load_encoders()
except FileNotFoundError as e:
    st.error(str(e))
    st.info("Execute o pipeline completo (01→04) e faça upload com 05_upload_hub.py.")
    st.stop()
except Exception as e:
    st.error(f"Erro ao carregar metadados: {e}")
    st.stop()

# ── Métricas do modelo ────────────────────────────────────────────────────────
st.subheader("Performance do Modelo (TEstadia)")
cv = meta.get("cv_results", {})
if cv:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("MAE médio (h)", f"{cv.get('mae_mean', 0):.2f}")
    c2.metric("RMSE médio (h)", f"{cv.get('rmse_mean', 0):.2f}")
    c3.metric("R² médio", f"{cv.get('r2_mean', 0):.3f}")
    c4.metric("MAPE médio", f"{cv.get('mape_mean', 0):.1f}%")

# ── Importância SHAP ──────────────────────────────────────────────────────────
st.subheader("Importância das Features (SHAP global)")
if not shap_imp.empty:
    col_feat = shap_imp.columns[0]
    col_imp  = shap_imp.columns[1] if len(shap_imp.columns) > 1 else shap_imp.columns[0]
    top_shap = shap_imp.nlargest(15, col_imp)

    fig = px.bar(top_shap, x=col_imp, y=col_feat, orientation="h",
                 title="Top 15 features por impacto SHAP médio",
                 labels={col_feat: "", col_imp: "|SHAP| médio"},
                 color=col_imp, color_continuous_scale="Blues")
    fig.update_coloraxes(showscale=False)
    st.plotly_chart(fig, width="stretch")

# ── Performance por subgrupo ──────────────────────────────────────────────────
subgrupos = meta.get("metricas_subgrupo", {})
if subgrupos:
    st.subheader("Performance por subgrupo")
    subg_df = pd.DataFrame(subgrupos).T.reset_index()
    subg_df.columns = ["Grupo"] + list(subg_df.columns[1:])
    st.dataframe(subg_df, width="stretch")

st.divider()

# ── Simulação de predição ─────────────────────────────────────────────────────
st.subheader("Simular uma predição")

# Carrega modelos sob demanda (lazy) — separado para não crashar a página inteira
_MODEL_ERROR = None
_MODELS_LOADED = False

@st.cache_resource(show_spinner="Carregando modelos ML...")
def _load_models():
    from util.data import load_model, load_model_operacao, load_model_quantile
    m     = load_model()
    m_op  = load_model_operacao()
    m_q10 = load_model_quantile("q10")
    m_q90 = load_model_quantile("q90")
    return m, m_op, m_q10, m_q90

# Formulário de entrada ────────────────────────────────────────────────────────
def _enc_options(key):
    return list(encoders.get(key, {}).values())

nav_opts     = _enc_options("Tipo de Navegação da Atracação") or ["Longo Curso", "Cabotagem", "Interior"]
op_opts      = _enc_options("Tipo de Operação")               or ["Carga", "Descarga", "Misto"]
nat_opts     = _enc_options("natureza_top")                   or ["Granel Sólido", "Contêiner", "Carga Geral"]
sentido_opts = _enc_options("sentido_top")                    or ["Embarque", "Desembarque"]
reg_opts     = _enc_options("Região Geográfica")              or ["Sudeste", "Sul", "Nordeste"]

col1, col2, col3 = st.columns(3)
with col1:
    nav     = st.selectbox("Tipo de navegação", nav_opts)
    op_tipo = st.selectbox("Tipo de operação", op_opts)
    nat     = st.selectbox("Natureza da carga", nat_opts)
with col2:
    sentido = st.selectbox("Sentido", sentido_opts)
    regiao  = st.selectbox("Região geográfica", reg_opts)
    mes     = st.slider("Mês de atracação", 1, 12, 6)
with col3:
    peso  = st.number_input("Peso total (t)", min_value=0.0, value=5000.0, step=500.0)
    teu   = st.number_input("TEUs", min_value=0, value=0, step=10)
    n_par = st.slider("Histórico de paralisações", 0, 20, 0)

def _encode(val, key):
    mp = {v: int(k) for k, v in encoders.get(key, {}).items()}
    return mp.get(val, 0)

feats = {
    "mes": mes,
    "trimestre": (mes - 1) // 3 + 1,
    "dia_semana": 2,
    "peso_total": peso,
    "teu_total": float(teu),
    "peso_cont": float(teu) * 10,
    "flag_conteiner": int(teu > 0),
    "flag_carga_pesada": int(peso > 10000),
    "n_paralisacoes": n_par,
    "tempo_paralisacao": 0.0,
    "n_atracacoes_porto_ano": 500,
    "taxa_ocupacao_media": 0.5,
    "TEstadia_media_3a": 60.0,
    "TOperacao_media_3a": 40.0,
    "TEsperaAtracacao_media_3a": 10.0,
    "Tipo de Navegação da Atracação_cod": _encode(nav, "Tipo de Navegação da Atracação"),
    "Tipo de Operação_cod": _encode(op_tipo, "Tipo de Operação"),
    "natureza_top_cod": _encode(nat, "natureza_top"),
    "sentido_top_cod": _encode(sentido, "sentido_top"),
    "Região Geográfica_cod": _encode(regiao, "Região Geográfica"),
    "UF_cod": 0,
}
feat_df = pd.DataFrame([feats])

if st.button("Prever", type="primary"):
    try:
        model, model_op, model_q10, model_q90 = _load_models()

        # Alinha colunas com o modelo
        try:
            model_feats = model.get_booster().feature_names
            for f in model_feats:
                if f not in feat_df.columns:
                    feat_df[f] = 0
            feat_df = feat_df[model_feats]
        except Exception:
            pass

        pred_median = float(model.predict(feat_df)[0])
        pred_q10    = float(model_q10.predict(feat_df)[0])
        pred_q90    = float(model_q90.predict(feat_df)[0])

        st.success("Predição concluída!")
        rc1, rc2, rc3 = st.columns(3)
        rc1.metric("P10 — otimista (h)", f"{pred_q10:.1f}")
        rc2.metric("Mediana — esperada (h)", f"{pred_median:.1f}")
        rc3.metric("P90 — pessimista (h)", f"{pred_q90:.1f}")

        fig_g = go.Figure(go.Indicator(
            mode="gauge+number",
            value=pred_median,
            title={"text": "TEstadia previsto (h)"},
            gauge={
                "axis": {"range": [0, max(pred_q90 * 1.3, 200)]},
                "bar": {"color": "#1f77b4"},
                "steps": [
                    {"range": [0, pred_q10],          "color": "lightgreen"},
                    {"range": [pred_q10, pred_q90],   "color": "lightyellow"},
                    {"range": [pred_q90, pred_q90 * 1.3], "color": "lightcoral"},
                ],
                "threshold": {"line": {"color": "red", "width": 4}, "value": pred_q90}
            }
        ))
        st.plotly_chart(fig_g, width="stretch")

    except (ImportError, ModuleNotFoundError) as e:
        st.warning(
            f"⚠️ Pacote de ML não disponível neste ambiente: `{e}`\n\n"
            "Os gráficos de performance e SHAP acima estão disponíveis. "
            "Para usar a predição interativa, acesse o projeto localmente."
        )
    except Exception as e:
        st.error(f"Erro ao carregar ou executar o modelo: {e}")
        st.info(
            "Possível causa: os arquivos `.pkl` foram gerados com uma versão diferente "
            "de numpy/xgboost. Retreine o modelo localmente e faça upload novamente com "
            "`python pipeline/04_treinar.py && python pipeline/05_upload_hub.py --camadas model`."
        )
