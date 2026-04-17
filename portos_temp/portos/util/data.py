import pandas as pd
import numpy as np
import streamlit as st
import joblib
import os

# ── PATH HELPER ───────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def _p(fname: str) -> str:
    return os.path.join(_ROOT, fname)

# ── ENCODING FIX ──────────────────────────────────────────────
def fix_enc(s):
    if pd.isna(s): return s
    try:
        return str(s).encode('latin-1').decode('utf-8')
    except Exception:
        return str(s)

def fix_cols_encoding(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(fix_enc)
    return df

# ── LOADERS ───────────────────────────────────────────────────

@st.cache_data(show_spinner="Carregando atracações...")
def load_master() -> pd.DataFrame:
    """Master dataset: Atracacao JOIN TemposAtracacao JOIN carga_por_atracacao"""

    atrac = pd.read_parquet(_p("Atracacao.parquet"))
    tempos = pd.read_parquet(_p("TemposAtracacao.parquet"))

    # Fix tempo columns that came as string
    for col in ["TEsperaAtracacao", "TEsperaInicioOp", "TEsperaDesatracacao"]:
        tempos[col] = pd.to_numeric(
            tempos[col].astype(str).str.replace(",", ".", regex=False),
            errors="coerce",
        )

    carga = pd.read_parquet(_p("carga_por_atracacao.parquet"))
    carga["IDAtracacao"] = pd.to_numeric(carga["IDAtracacao"], errors="coerce")

    df = atrac.merge(tempos, on="IDAtracacao", how="left")
    df = df.merge(carga,    on="IDAtracacao", how="left")

    # Fix encoding
    str_cols = [
        "Porto Atracação", "Complexo Portuário",
        "Tipo de Operação", "Tipo de Navegação da Atracação",
        "Tipo da Autoridade Portuária", "Região Geográfica",
        "UF", "Mes", "Município", "natureza_top", "sentido_top",
    ]
    df = fix_cols_encoding(df, str_cols)

    # Parse dates
    for col in ["Data Atracação", "Data Chegada", "Data Desatracação",
                "Data Início Operação", "Data Término Operação"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y %H:%M:%S",
                                     errors="coerce")

    # Derived columns
    df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce").astype("Int64")
    df["peso_total"]  = df["peso_total"].fillna(0)
    df["teu_total"]   = df["teu_total"].fillna(0)
    df["natureza_top"] = df["natureza_top"].fillna("Sem Carga").apply(fix_enc)
    df["sentido_top"]  = df["sentido_top"].fillna("Não Informado").apply(fix_enc)

    return df


@st.cache_data(show_spinner="Carregando hidrovias...")
def load_hidrovia() -> pd.DataFrame:
    df = pd.read_parquet(_p("carga_hidrovia_anual.parquet"))
    df = fix_cols_encoding(df, ["Hidrovia", "Região Geográfica", "UF"])
    df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce").astype("Int64")
    df["tonelagem_total"] = pd.to_numeric(df["tonelagem_total"], errors="coerce")
    return df


@st.cache_resource
def load_model():
    """Carrega o XGBoost treinado + metadados + encoders"""
    model    = joblib.load(_p("model.pkl"))
    meta     = joblib.load(_p("model_meta.pkl"))
    encoders = joblib.load(_p("encoders.pkl"))
    return model, meta, encoders


@st.cache_data
def load_shap():
    imp = pd.read_parquet(_p("shap_importance.parquet"))
    return imp


# ── AGGREGATIONS (cached) ─────────────────────────────────────

@st.cache_data
def agg_anual(df: pd.DataFrame) -> pd.DataFrame:
    return (df.groupby("Ano")
              .agg(
                  n_atracacoes  = ("IDAtracacao",     "count"),
                  peso_total    = ("peso_total",       "sum"),
                  teu_total     = ("teu_total",        "sum"),
                  estadia_media = ("TEstadia",         "mean"),
                  espera_media  = ("TEsperaAtracacao", "mean"),
                  op_media      = ("TOperacao",        "mean"),
              )
              .reset_index())


@st.cache_data
def agg_porto(df: pd.DataFrame, min_atrac: int = 100) -> pd.DataFrame:
    grp = (df.groupby("Porto Atracação")
             .agg(
                 n_atracacoes  = ("IDAtracacao",     "count"),
                 peso_total    = ("peso_total",       "sum"),
                 teu_total     = ("teu_total",        "sum"),
                 estadia_media = ("TEstadia",         "mean"),
                 espera_media  = ("TEsperaAtracacao", "mean"),
                 op_media      = ("TOperacao",        "mean"),
                 uf            = ("UF",               lambda x: x.mode()[0] if len(x)>0 else ""),
                 regiao        = ("Região Geográfica",lambda x: x.mode()[0] if len(x)>0 else ""),
             )
             .reset_index())
    return grp[grp["n_atracacoes"] >= min_atrac].copy()


@st.cache_data
def agg_nav_anual(df: pd.DataFrame) -> pd.DataFrame:
    return (df.groupby(["Ano", "Tipo de Navegação da Atracação"])
              .agg(n = ("IDAtracacao", "count"), peso = ("peso_total", "sum"))
              .reset_index())


@st.cache_data
def agg_carga_anual(df: pd.DataFrame) -> pd.DataFrame:
    return (df.groupby(["Ano", "natureza_top"])
              .agg(n = ("IDAtracacao", "count"), peso = ("peso_total", "sum"))
              .reset_index())


@st.cache_data
def agg_mensal(df: pd.DataFrame) -> pd.DataFrame:
    from util.constants import MESES_ORDER
    grp = (df.groupby("Mes")
             .agg(
                 n_atracacoes  = ("IDAtracacao",     "count"),
                 estadia_media = ("TEstadia",         "mean"),
                 espera_media  = ("TEsperaAtracacao", "mean"),
             )
             .reset_index())
    grp["Mes_ord"] = grp["Mes"].map({m: i for i, m in enumerate(MESES_ORDER)})
    return grp.sort_values("Mes_ord")


@st.cache_data
def get_summary(df: pd.DataFrame) -> dict:
    last_year = int(df["Ano"].max())
    prev_year = last_year - 1
    cur  = df[df["Ano"] == last_year]
    prev = df[df["Ano"] == prev_year]

    def pct(a, b):
        return (a / b - 1) * 100 if b > 0 else 0

    return {
        "last_year":        last_year,
        "n_atracacoes":     len(df),
        "n_atracacoes_ano": len(cur),
        "n_atracacoes_prev":len(prev),
        "pct_atrac":        pct(len(cur), len(prev)),
        "peso_total":       df["peso_total"].sum(),
        "peso_ano":         cur["peso_total"].sum(),
        "n_portos":         df["Porto Atracação"].nunique(),
        "estadia_media":    df["TEstadia"].mean(),
        "espera_media":     df["TEsperaAtracacao"].mean(),
        "top_porto":        df.groupby("Porto Atracação").size().idxmax(),
        "top_nav":          df["Tipo de Navegação da Atracação"].value_counts().idxmax(),
    }
