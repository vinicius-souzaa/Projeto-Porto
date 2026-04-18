"""Página 9 — Catálogo & Pipeline: metadados, linhagem e quality score."""

import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from util.data import load_catalog

st.title("📚 Catálogo de Dados & Pipeline")

@st.cache_data(ttl=3600, show_spinner="Carregando catálogo...")
def _load():
    return load_catalog()

try:
    cat = _load()
except FileNotFoundError as e:
    st.error(str(e))
    st.info("Execute: `python pipeline/catalog.py` após o pipeline completo.")
    st.stop()

# ── Quality score global ──────────────────────────────────────────────────────
qs = cat.get("quality_summary", {})
c1, c2, c3, c4 = st.columns(4)
c1.metric("Quality Score médio", f"{qs.get('score_medio', 0):.0f}/100")
c2.metric("Tabelas catalogadas", qs.get("n_tabelas", 0))
c3.metric("Score mínimo", f"{qs.get('score_minimo', 0):.0f}/100")
c4.metric("Score máximo", f"{qs.get('score_maximo', 0):.0f}/100")

gerado = cat.get("gerado_em", "N/A")
st.caption(f"Catálogo gerado em: {gerado}")

st.divider()

# ── Pipeline visual ───────────────────────────────────────────────────────────
st.subheader("Pipeline de dados — Medallion Architecture")
st.markdown("""
```
 TXT Brutos (ANTAQ)
       │
       ▼
 01_converter.py ──► Silver (Parquet por tipo/ano)
       │                   Partições: silver/{tipo}/ano={ano}/data.parquet
       ▼
 02_agregar.py ───► Gold (tabelas analíticas)
       │                   gold/atracacao_master.parquet
       │                   gold/carga_por_atracacao.parquet
       │                   gold/taxa_ocupacao_anual.parquet
       │                   gold/paralisacoes_por_atracacao.parquet
       │                   gold/carga_hidrovia_anual.parquet
       │                   gold/porto_perfil.parquet
       ▼
 03_features.py ──► Feature Store
       │                   features/features.parquet
       │                   features/encoders_map.json
       ▼
 04_treinar.py ───► Modelos ML
       │                   model/model.pkl  (XGBoost TEstadia)
       │                   model/model_operacao.pkl  (XGBoost TOperacao)
       │                   model/model_q10.pkl / model_q90.pkl  (quantile)
       │                   model/model_lgbm.pkl  (LightGBM)
       │                   model/shap_importance.parquet
       │                   model/model_card.json
       ▼
 05_upload_hub.py ► HuggingFace Hub (vinicius-souza/antaq)
       │
       ▼
 catalog.py ──────► gold/catalog.json  (esta página)
```
""")

st.divider()

# ── Camada Silver ─────────────────────────────────────────────────────────────
st.subheader("Camada Silver")
silver = cat.get("silver", [])
if silver:
    silver_df = pd.DataFrame([{
        "Tipo":        s["tipo"],
        "Anos":        f"{s['ano_min']}–{s['ano_max']}",
        "Partições":   s["n_particoes"],
        "Colunas":     s["n_colunas"],
        "Tamanho (MB)":s["tamanho_mb"],
    } for s in silver])
    st.dataframe(silver_df, use_container_width=True, hide_index=True)

    tipo_sel = st.selectbox("Inspecionar tipo Silver", [s["tipo"] for s in silver])
    entry_s = next(s for s in silver if s["tipo"] == tipo_sel)
    col_df = pd.DataFrame(entry_s["colunas"])
    st.dataframe(
        col_df.style.background_gradient(subset=["nulos_pct"], cmap="Reds"),
        use_container_width=True, hide_index=True,
    )

st.divider()

# ── Camada Gold ───────────────────────────────────────────────────────────────
st.subheader("Camada Gold")
gold = cat.get("gold", [])
if gold:
    gold_df = pd.DataFrame([{
        "Tabela":       g["nome"],
        "Linhas":       f"{g['n_linhas']:,}",
        "Colunas":      g["n_colunas"],
        "Tamanho (MB)": g["tamanho_mb"],
        "Período":      f"{g.get('ano_min','?')}–{g.get('ano_max','?')}",
    } for g in gold])
    st.dataframe(gold_df, use_container_width=True, hide_index=True)

    nome_sel = st.selectbox("Inspecionar tabela Gold", [g["nome"] for g in gold])
    entry_g = next(g for g in gold if g["nome"] == nome_sel)
    col_df2 = pd.DataFrame(entry_g["colunas"])
    st.dataframe(
        col_df2.style.background_gradient(subset=["nulos_pct"], cmap="Reds"),
        use_container_width=True, hide_index=True,
    )

# ── Quality score por tabela ──────────────────────────────────────────────────
st.subheader("Quality score por tabela")
all_entries = silver + gold
scores = []
for e in all_entries:
    null_rates = [c["nulos_pct"] for c in e.get("colunas", [])]
    if null_rates:
        avg = sum(null_rates) / len(null_rates)
        score = max(0, int(100 * (1 - avg * 2)))
        nome = e.get("tipo") or e.get("nome")
        camada = e.get("camada", "?")
        scores.append({"Tabela": nome, "Camada": camada, "Score": score})

if scores:
    score_df = pd.DataFrame(scores).sort_values("Score")
    fig = px.bar(score_df, x="Score", y="Tabela", orientation="h",
                 color="Camada", title="Quality score por tabela (0–100)",
                 labels={"Score": "Score", "Tabela": ""},
                 color_discrete_map={"silver": "#aac", "gold": "#fa0"})
    fig.add_vline(x=80, line_dash="dash", annotation_text="Meta 80")
    st.plotly_chart(fig, use_container_width=True)
