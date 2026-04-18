"""
pipeline/03_features.py — Feature Store
=========================================
Constrói a feature store para o modelo ML a partir das tabelas Gold.

Features geradas por atracação:
  Estáticas (do evento):
    - tipo_navegacao, tipo_operacao, natureza_carga, sentido
    - mes, trimestre, dia_semana (da data de atracação)
    - regiao, uf

  Contextuais (do porto):
    - estadia_media_porto_3a   — média dos últimos 3 anos no porto
    - operacao_media_porto_3a  — idem para TOperacao
    - espera_media_porto_3a    — idem para TEsperaAtracacao
    - n_atracacoes_porto_ano   — volume do porto naquele ano
    - taxa_ocupacao_porto      — taxa de ocupação média do berço (quando disponível)

  De carga:
    - peso_total, teu_total, peso_cont
    - flag_conteiner, flag_carga_pesada (peso > p75)

  De paralisação:
    - n_paralisacoes, tempo_paralisacao (histórico do porto)

Targets:
    - TEstadia    — tempo total de estadia (horas)
    - TOperacao   — tempo de operação (horas)

Saída:
    features/features.parquet — uma linha por atracação, pronta para treino

Uso:
    python pipeline/03_features.py
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import GOLD, FEATURES, LOGS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _load_gold(nome: str) -> pd.DataFrame:
    path = GOLD / f"{nome}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Gold '{nome}.parquet' não encontrado. Execute 02_agregar.py primeiro.")
    df = pd.read_parquet(path, engine="pyarrow")
    log.info("  Carregado: %s (%d linhas)", nome, len(df))
    return df


def _features_temporais(df: pd.DataFrame) -> pd.DataFrame:
    """Extrai features de data a partir de 'Data Atracação'."""
    col = "Data Atracação"
    if col not in df.columns:
        log.warning("  Coluna '%s' não encontrada — features temporais puladas", col)
        df["mes"]         = np.nan
        df["trimestre"]   = np.nan
        df["dia_semana"]  = np.nan
        return df

    datas = pd.to_datetime(df[col], format="%d/%m/%Y %H:%M:%S", errors="coerce")
    df["mes"]        = datas.dt.month.astype("Int64")
    df["trimestre"]  = datas.dt.quarter.astype("Int64")
    df["dia_semana"] = datas.dt.dayofweek.astype("Int64")  # 0=segunda
    return df


def _media_historica_porto(master: pd.DataFrame,
                            col_target: str,
                            janela_anos: int = 3) -> pd.DataFrame:
    """
    Para cada (porto, ano), calcula a média do col_target
    nos 'janela_anos' anos anteriores (exclusive o ano atual).
    Evita data leakage: nunca usa o ano corrente no cálculo.
    """
    porto_col = "Porto Atracação"
    if porto_col not in master.columns or col_target not in master.columns:
        return master

    anual = (
        master.groupby([porto_col, "Ano"])[col_target]
        .mean()
        .reset_index()
        .rename(columns={col_target: f"_media_{col_target}"})
    )

    resultado = []
    for porto, grp in anual.groupby(porto_col):
        grp = grp.sort_values("Ano")
        medias = []
        for i, row in grp.iterrows():
            anos_passados = grp[grp["Ano"] < row["Ano"]].tail(janela_anos)
            media = anos_passados[f"_media_{col_target}"].mean() if len(anos_passados) > 0 else np.nan
            medias.append(media)
        grp[f"{col_target}_media_{janela_anos}a"] = medias
        resultado.append(grp[[porto_col, "Ano", f"{col_target}_media_{janela_anos}a"]])

    historico = pd.concat(resultado, ignore_index=True)
    master = master.merge(historico, on=[porto_col, "Ano"], how="left")
    return master


def _features_porto(master: pd.DataFrame) -> pd.DataFrame:
    """Acrescenta features contextuais do porto sem data leakage."""
    log.info("  Calculando médias históricas por porto (3 anos)...")

    for col in ["TEstadia", "TOperacao", "TEsperaAtracacao"]:
        if col in master.columns:
            master = _media_historica_porto(master, col, janela_anos=3)

    # Volume do porto no ano
    if "Porto Atracação" in master.columns:
        vol = (
            master.groupby(["Porto Atracação", "Ano"])["IDAtracacao"]
            .count()
            .reset_index()
            .rename(columns={"IDAtracacao": "n_atracacoes_porto_ano"})
        )
        master = master.merge(vol, on=["Porto Atracação", "Ano"], how="left")

    return master


def _features_taxa_ocupacao(master: pd.DataFrame) -> pd.DataFrame:
    """Junta taxa de ocupação média do porto/ano (quando disponível)."""
    taxa_path = GOLD / "taxa_ocupacao_anual.parquet"
    if not taxa_path.exists():
        log.info("  taxa_ocupacao_anual não disponível — pulando")
        return master

    taxa = pd.read_parquet(taxa_path, engine="pyarrow")
    taxa["Ano"] = pd.to_numeric(taxa.get("Ano", None), errors="coerce").astype("Int64")

    # Detecta coluna principal de taxa
    num_cols = taxa.select_dtypes(include="float64").columns.tolist()
    porto_col = next((c for c in taxa.columns if "Porto" in c or "Complexo" in c), None)

    if not porto_col or not num_cols:
        return master

    taxa_agg = taxa.groupby([porto_col, "Ano"])[num_cols[0]].mean().reset_index()
    taxa_agg = taxa_agg.rename(columns={porto_col: "Porto Atracação",
                                        num_cols[0]: "taxa_ocupacao_media"})

    if "Porto Atracação" in master.columns:
        master = master.merge(taxa_agg, on=["Porto Atracação", "Ano"], how="left")

    return master


def _features_paralisacao(master: pd.DataFrame) -> pd.DataFrame:
    """Junta features de paralisação histórica por porto/ano."""
    par_path = GOLD / "paralisacoes_por_atracacao.parquet"
    if not par_path.exists():
        log.info("  paralisacoes_por_atracacao não disponível — pulando")
        return master

    par = pd.read_parquet(par_path, engine="pyarrow")
    par["IDAtracacao"] = pd.to_numeric(par["IDAtracacao"], errors="coerce")
    master = master.merge(par, on="IDAtracacao", how="left")
    master["n_paralisacoes"]    = master["n_paralisacoes"].fillna(0).astype("Int64")
    master["tempo_paralisacao"] = master["tempo_paralisacao"].fillna(0)
    return master


def _features_carga(master: pd.DataFrame) -> pd.DataFrame:
    """Deriva features de carga."""
    master["flag_conteiner"] = (master.get("teu_total", pd.Series(0)) > 0).astype(int)

    if "peso_total" in master.columns:
        p75 = master["peso_total"].quantile(0.75)
        master["flag_carga_pesada"] = (master["peso_total"] > p75).astype(int)
    else:
        master["flag_carga_pesada"] = 0

    return master


def _encode_categoricas(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Label-encode colunas categóricas e salva mapeamento para uso no modelo."""
    encoders = {}
    for col in cols:
        if col not in df.columns:
            continue
        df[col] = df[col].fillna("Desconhecido").astype(str)
        cats = pd.Categorical(df[col])
        df[col + "_cod"] = cats.codes
        encoders[col] = dict(enumerate(cats.categories))
    return df, encoders


def main():
    inicio = datetime.now(timezone.utc)
    log.info("Pipeline ANTAQ Feature Store")

    master = _load_gold("atracacao_master")

    log.info("Extraindo features temporais...")
    master = _features_temporais(master)

    log.info("Calculando features contextuais de porto...")
    master = _features_porto(master)

    log.info("Juntando taxa de ocupação...")
    master = _features_taxa_ocupacao(master)

    log.info("Juntando paralisações...")
    master = _features_paralisacao(master)

    log.info("Derivando features de carga...")
    master = _features_carga(master)

    # Colunas categóricas para encoding
    cat_cols = [
        "Tipo de Navegação da Atracação",
        "Tipo de Operação",
        "natureza_top",
        "sentido_top",
        "Região Geográfica",
        "UF",
    ]
    log.info("Codificando categóricas: %s", cat_cols)
    master, encoders = _encode_categoricas(master, cat_cols)

    # Feature store final: seleciona colunas relevantes
    feature_cols = [
        "IDAtracacao", "Ano", "mes", "trimestre", "dia_semana",
        "Porto Atracação", "Região Geográfica", "UF",
        "peso_total", "teu_total", "peso_cont",
        "flag_conteiner", "flag_carga_pesada",
        "n_paralisacoes", "tempo_paralisacao",
        "n_atracacoes_porto_ano",
        "taxa_ocupacao_media",
        "TEstadia_media_3a", "TOperacao_media_3a", "TEsperaAtracacao_media_3a",
        # Códigos categóricos
        *[c + "_cod" for c in cat_cols if c + "_cod" in master.columns],
        # Targets
        "TEstadia", "TOperacao",
    ]

    # Mantém apenas as que existem
    feature_cols = [c for c in feature_cols if c in master.columns]
    features = master[feature_cols].copy()

    # Remove linhas sem targets
    n_antes = len(features)
    features = features.dropna(subset=["TEstadia", "TOperacao"], how="all")
    log.info("  Removidas %d linhas sem target (de %d)", n_antes - len(features), n_antes)

    # Salva feature store
    out = FEATURES / "features.parquet"
    features.to_parquet(out, index=False, compression="zstd", engine="pyarrow")
    mb = out.stat().st_size / 1e6
    log.info("Feature store: %d linhas | %d colunas | %.1f MB", len(features), len(features.columns), mb)

    # Salva mapeamento dos encoders (para uso no dashboard)
    import json
    enc_path = FEATURES / "encoders_map.json"
    enc_path.write_text(json.dumps(encoders, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Encoders salvos: %s", enc_path)

    fim = datetime.now(timezone.utc)
    log.info("Concluído em %.0fs", (fim - inicio).total_seconds())


if __name__ == "__main__":
    main()
