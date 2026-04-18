"""
pipeline/02_agregar.py — Silver → Gold
========================================
Lê Parquet Silver e produz tabelas analíticas Gold prontas para o dashboard.

Tabelas geradas:
  gold/atracacao_master.parquet    — base principal (Atracacao + Tempos + Carga)
  gold/carga_por_atracacao.parquet — carga agregada por atracação
  gold/taxa_ocupacao_anual.parquet — taxa de ocupação anual por porto
  gold/paralisacoes_por_atracacao.parquet — paralisações por atracação
  gold/carga_hidrovia_anual.parquet — movimentação anual em hidrovias
  gold/porto_perfil.parquet        — perfil operacional por porto (para clustering)

Uso:
  python pipeline/02_agregar.py
  python pipeline/02_agregar.py --tabelas atracacao_master carga_por_atracacao
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import SILVER, GOLD, LOGS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── DuckDB helper ─────────────────────────────────────────────────────────────

def _silver(tipo: str) -> str:
    """Retorna glob path para todas as partições Silver de um tipo."""
    return str(SILVER / tipo / "ano=*" / "data.parquet")


def _read_silver(tipo: str) -> pd.DataFrame:
    """Lê todas as partições Silver de um tipo via DuckDB."""
    glob = _silver(tipo)
    path = SILVER / tipo
    if not path.exists() or not list(path.glob("ano=*/data.parquet")):
        raise FileNotFoundError(f"Silver não encontrado para tipo '{tipo}'. Execute 01_converter.py primeiro.")
    log.info("  Lendo Silver: %s", tipo)
    df = duckdb.execute(
        f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true, union_by_name=true)"
    ).df()
    log.info("    → %d linhas | %d colunas", len(df), len(df.columns))
    return df


def _save_gold(df: pd.DataFrame, nome: str) -> Path:
    """Salva DataFrame como Gold Parquet com metadados."""
    out = GOLD / f"{nome}.parquet"
    df.to_parquet(out, index=False, compression="zstd", engine="pyarrow")
    mb = out.stat().st_size / 1e6
    log.info("  Salvo: %s (%.1f MB, %d linhas)", out.name, mb, len(df))
    return out


# ── Tabelas Gold ──────────────────────────────────────────────────────────────

def _detect_col(real_cols: list[str], candidates: list[str]) -> str | None:
    """Retorna o primeiro nome de coluna candidato que exista no schema real."""
    for c in candidates:
        if c in real_cols:
            return c
    return None


def build_carga_por_atracacao() -> pd.DataFrame:
    """Agrega Carga + Carga_Conteinerizada por IDAtracacao — GROUP BY no DuckDB."""
    log.info("Construindo: carga_por_atracacao")

    path_carga = SILVER / "Carga"
    if not path_carga.exists() or not list(path_carga.glob("ano=*/data.parquet")):
        raise FileNotFoundError("Silver Carga não encontrado.")

    glob_carga = str(path_carga / "ano=*" / "data.parquet")

    # Descobre colunas reais do Silver (schema pode variar entre anos)
    real_cols = duckdb.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{glob_carga}', "
        f"hive_partitioning=true, union_by_name=true) LIMIT 0"
    ).df()["column_name"].tolist()
    log.info("  Colunas Carga detectadas: %s", real_cols)

    peso_col    = _detect_col(real_cols, ["VLPesoCargaBruta", "Peso Bruto (t)", "VLPeso"])
    teu_col     = _detect_col(real_cols, ["TEU", "QTCarga"])
    natureza_col = _detect_col(real_cols, ["CDNaturezaCarga", "STNaturezaCarga",
                                            "Natureza da Carga", "NaturezaCarga"])
    sentido_col  = _detect_col(real_cols, ["Sentido", "SentidoCarga", "CDSentido"])

    peso_expr    = f'SUM(TRY_CAST("{peso_col}" AS DOUBLE))'    if peso_col    else "0.0"
    teu_expr     = f'SUM(TRY_CAST("{teu_col}" AS DOUBLE))'     if teu_col     else "0.0"
    natureza_expr = f'MODE("{natureza_col}")'                  if natureza_col else "NULL"
    sentido_expr  = f'MODE("{sentido_col}")'                   if sentido_col  else "NULL"

    grp = duckdb.execute(f"""
        SELECT
            TRY_CAST(IDAtracacao AS DOUBLE) AS IDAtracacao,
            {peso_expr}                     AS peso_total,
            {teu_expr}                      AS teu_total,
            COUNT(*)                        AS n_cargas,
            {natureza_expr}                 AS natureza_top,
            {sentido_expr}                  AS sentido_top
        FROM read_parquet('{glob_carga}', hive_partitioning=true, union_by_name=true)
        WHERE IDAtracacao IS NOT NULL
        GROUP BY TRY_CAST(IDAtracacao AS DOUBLE)
    """).df()

    # Peso conteinerizado: soma VLPesoCargaBruta onde TEU > 0 (evita JOIN de 100M linhas)
    if teu_col and peso_col:
        cont_grp = duckdb.execute(f"""
            SELECT
                TRY_CAST(IDAtracacao AS DOUBLE) AS IDAtracacao,
                SUM(CASE WHEN TRY_CAST("{teu_col}" AS DOUBLE) > 0
                         THEN TRY_CAST("{peso_col}" AS DOUBLE) ELSE 0 END) AS peso_cont
            FROM read_parquet('{glob_carga}', hive_partitioning=true, union_by_name=true)
            WHERE IDAtracacao IS NOT NULL
            GROUP BY TRY_CAST(IDAtracacao AS DOUBLE)
        """).df()
        grp = grp.merge(cont_grp, on="IDAtracacao", how="left")
        grp["peso_cont"] = grp["peso_cont"].fillna(0)
    else:
        grp["peso_cont"] = 0.0

    log.info("    → carga_por_atracacao: %d atracações", len(grp))
    return grp


def build_atracacao_master() -> pd.DataFrame:
    """
    Tabela principal: Atracacao JOIN TemposAtracacao JOIN carga_por_atracacao.
    É a base de todas as análises operacionais.
    """
    log.info("Construindo: atracacao_master")

    atrac  = _read_silver("Atracacao")
    tempos = _read_silver("TemposAtracacao")

    atrac["IDAtracacao"]  = pd.to_numeric(atrac["IDAtracacao"], errors="coerce")
    tempos["IDAtracacao"] = pd.to_numeric(tempos["IDAtracacao"], errors="coerce")

    # Colunas de tempo: garante float64
    for col in ["TEsperaAtracacao", "TEsperaInicioOp", "TOperacao",
                "TEsperaDesatracacao", "TAtracado", "TEstadia"]:
        if col in tempos.columns:
            tempos[col] = pd.to_numeric(tempos[col], errors="coerce")

    master = atrac.merge(tempos, on="IDAtracacao", how="left")

    # Carga agregada
    carga_grp = build_carga_por_atracacao()
    master = master.merge(carga_grp, on="IDAtracacao", how="left")
    master["peso_total"] = master["peso_total"].fillna(0)
    master["teu_total"]  = master["teu_total"].fillna(0)

    # Ano como inteiro
    master["Ano"] = pd.to_numeric(master["Ano"], errors="coerce").astype("Int64")

    log.info("  Master: %d linhas | %d colunas", len(master), len(master.columns))
    return master


def build_taxa_ocupacao_anual() -> pd.DataFrame:
    """Taxa de ocupação dos berços por porto e ano (2020–2026)."""
    log.info("Construindo: taxa_ocupacao_anual")
    try:
        taxa = _read_silver("TaxaOcupacao")
    except FileNotFoundError:
        log.warning("  TaxaOcupacao Silver não encontrado — pulando")
        return pd.DataFrame()

    log.info("  Colunas disponíveis: %s", taxa.columns.tolist())

    # Ano: aceita "Ano", "ano", "AnoTaxaOcupacao" etc.
    ano_col = _detect_col(taxa.columns.tolist(),
                          ["Ano", "ano", "AnoTaxaOcupacao", "ANO"])
    if ano_col is None:
        log.warning("  Coluna de ano não encontrada em TaxaOcupacao")
        return pd.DataFrame()
    taxa["Ano"] = pd.to_numeric(taxa[ano_col], errors="coerce")
    taxa = taxa[taxa["Ano"].notna()].copy()
    taxa["Ano"] = taxa["Ano"].astype(int)

    # Porto/Complexo: aceita qualquer coluna com "Porto", "Complexo", "Instalacao" no nome
    all_cols = taxa.columns.tolist()
    porto_col = _detect_col(all_cols, ["Porto Atracação", "Complexo Portuário",
                                        "NomeComplexo", "Instalacao", "Porto",
                                        "IDComplexo", "Complexo"])
    if porto_col is None:
        # Última tentativa: qualquer coluna string não-ano
        str_cols = taxa.select_dtypes(include="object").columns.tolist()
        porto_col = str_cols[0] if str_cols else None

    num_cols = taxa.select_dtypes(include="number").columns.tolist()
    num_cols = [c for c in num_cols if c != "Ano" and "ID" not in c and "Dia" not in c]
    log.info("  Colunas numéricas: %s | Porto col: %s", num_cols, porto_col)

    if not num_cols:
        log.warning("  Nenhuma coluna numérica útil encontrada")
        return pd.DataFrame()

    id_cols = ["Ano"] + ([porto_col] if porto_col else [])
    agg_dict = {col: "mean" for col in num_cols if col not in id_cols}
    if not agg_dict:
        return pd.DataFrame()

    grp = taxa.groupby(id_cols).agg(agg_dict).reset_index()
    # Normaliza para 0–1 se os valores forem em minutos (>1000 = provavelmente minutos/dia)
    for col in num_cols:
        if col in grp.columns and grp[col].median() > 100:
            grp[col] = (grp[col] / 1440).clip(0, 1)  # minutos → fração do dia
    return grp


def build_paralisacoes_por_atracacao() -> pd.DataFrame:
    """Paralisações agregadas por atracação."""
    log.info("Construindo: paralisacoes_por_atracacao")
    try:
        par = _read_silver("TemposAtracacaoParalisacao")
    except FileNotFoundError:
        log.warning("  TemposAtracacaoParalisacao Silver não encontrado — pulando")
        return pd.DataFrame()

    par["IDAtracacao"] = pd.to_numeric(par["IDAtracacao"], errors="coerce")
    if "TParalisacao" not in par.columns:
        par["TParalisacao"] = 0.0
    par["TParalisacao"] = pd.to_numeric(par["TParalisacao"], errors="coerce").fillna(0)

    grp = par.groupby("IDAtracacao").agg(
        n_paralisacoes    = ("IDAtracacao", "count"),
        tempo_paralisacao = ("TParalisacao", "sum"),
    ).reset_index()

    return grp


def build_carga_hidrovia_anual() -> pd.DataFrame:
    """Movimentação anual em hidrovias."""
    log.info("Construindo: carga_hidrovia_anual")
    hidrovia = _read_silver("Carga_Hidrovia")

    log.info("  Colunas disponíveis: %s", hidrovia.columns.tolist())

    # Ano: "Ano", "ano" (partição hive), "AnoMovimento" etc.
    ano_col = _detect_col(hidrovia.columns.tolist(), ["Ano", "ano", "AnoMovimento"])
    if ano_col is None:
        log.warning("  Coluna de ano não encontrada em Carga_Hidrovia")
        return pd.DataFrame()
    hidrovia["Ano"] = pd.to_numeric(hidrovia[ano_col], errors="coerce")
    hidrovia = hidrovia[hidrovia["Ano"].notna()].copy()
    hidrovia["Ano"] = hidrovia["Ano"].astype(int)

    # Detecta coluna de valor
    all_cols = hidrovia.columns.tolist()
    val_col = _detect_col(all_cols, ["ValorMovimentado", "VLMovimentado",
                                      "Peso Bruto (t)", "VLPeso", "QTMovimentado"])
    if val_col is None:
        num_candidates = hidrovia.select_dtypes(include="number").columns.tolist()
        val_col = next((c for c in num_candidates
                        if "ID" not in c and "Ano" not in c and "ano" not in c), None)

    if val_col is None:
        log.warning("  Coluna de valor não encontrada em Carga_Hidrovia — pulando")
        return pd.DataFrame()

    log.info("  Coluna de valor detectada: %s", val_col)
    hidrovia[val_col] = pd.to_numeric(hidrovia[val_col], errors="coerce").fillna(0)

    # Colunas de agrupamento — aceita o que existir
    id_candidates = ["Hidrovia", "hidrovia", "NomeHidrovia",
                     "Região Geográfica", "UF", "Ano"]
    id_cols = [c for c in id_candidates if c in all_cols]
    if "Ano" not in id_cols:
        id_cols.append("Ano")

    grp = hidrovia.groupby(id_cols).agg(
        tonelagem_total=(val_col, "sum"),
        n_registros    =(val_col, "count"),
    ).reset_index()
    return grp


def build_porto_perfil(master: pd.DataFrame) -> pd.DataFrame:
    """
    Perfil operacional por porto — base para clustering.
    Inclui médias históricas de tempos, volume, mix de carga.
    """
    log.info("Construindo: porto_perfil")
    if "Porto Atracação" not in master.columns:
        log.warning("  Coluna 'Porto Atracação' não encontrada — pulando porto_perfil")
        return pd.DataFrame()

    grp = master.groupby("Porto Atracação").agg(
        n_atracacoes       = ("IDAtracacao",     "count"),
        estadia_media      = ("TEstadia",         "mean"),
        operacao_media     = ("TOperacao",        "mean"),
        espera_media       = ("TEsperaAtracacao", "mean"),
        peso_medio         = ("peso_total",       "mean"),
        teu_medio          = ("teu_total",        "mean"),
        pct_conteiner      = ("teu_total",        lambda x: (x > 0).mean()),
        uf                 = ("UF",               lambda x: x.mode().iloc[0] if len(x) > 0 else None),
        regiao             = ("Região Geográfica",lambda x: x.mode().iloc[0] if len(x) > 0 else None),
        ano_min            = ("Ano",              "min"),
        ano_max            = ("Ano",              "max"),
    ).reset_index()

    return grp[grp["n_atracacoes"] >= 50].copy()


# ── Main ──────────────────────────────────────────────────────────────────────

TABELAS_DISPONIVEIS = [
    "atracacao_master",
    "carga_por_atracacao",
    "taxa_ocupacao_anual",
    "paralisacoes_por_atracacao",
    "carga_hidrovia_anual",
    "porto_perfil",
]


def main():
    parser = argparse.ArgumentParser(description="ANTAQ Silver → Gold aggregator")
    parser.add_argument("--tabelas", nargs="+", default=TABELAS_DISPONIVEIS,
                        help="Tabelas a gerar (padrão: todas)")
    args = parser.parse_args()

    inicio = datetime.now(timezone.utc)
    log.info("Pipeline ANTAQ Silver → Gold")
    log.info("Tabelas: %s", args.tabelas)

    run_log = {"inicio": inicio.isoformat(), "tabelas": {}, "erros": []}

    # Constrói master primeiro pois outras tabelas o utilizam
    master = None
    if "atracacao_master" in args.tabelas or "porto_perfil" in args.tabelas:
        try:
            master = build_atracacao_master()
            if "atracacao_master" in args.tabelas:
                out = _save_gold(master, "atracacao_master")
                run_log["tabelas"]["atracacao_master"] = {
                    "linhas": len(master), "mb": round(out.stat().st_size / 1e6, 2)
                }
        except Exception as e:
            log.error("atracacao_master: %s", e)
            run_log["erros"].append(f"atracacao_master: {e}")

    builds = {
        "carga_por_atracacao":         build_carga_por_atracacao,
        "taxa_ocupacao_anual":         build_taxa_ocupacao_anual,
        "paralisacoes_por_atracacao":  build_paralisacoes_por_atracacao,
        "carga_hidrovia_anual":        build_carga_hidrovia_anual,
    }

    for nome, fn in builds.items():
        if nome not in args.tabelas:
            continue
        try:
            df = fn()
            if not df.empty:
                out = _save_gold(df, nome)
                run_log["tabelas"][nome] = {
                    "linhas": len(df), "mb": round(out.stat().st_size / 1e6, 2)
                }
        except Exception as e:
            log.error("%s: %s", nome, e)
            run_log["erros"].append(f"{nome}: {e}")

    if "porto_perfil" in args.tabelas and master is not None:
        try:
            perfil = build_porto_perfil(master)
            if not perfil.empty:
                out = _save_gold(perfil, "porto_perfil")
                run_log["tabelas"]["porto_perfil"] = {
                    "linhas": len(perfil), "mb": round(out.stat().st_size / 1e6, 2)
                }
        except Exception as e:
            log.error("porto_perfil: %s", e)
            run_log["erros"].append(f"porto_perfil: {e}")

    fim = datetime.now(timezone.utc)
    run_log["fim"] = fim.isoformat()
    run_log["duracao_s"] = round((fim - inicio).total_seconds(), 1)

    log_path = LOGS / f"02_agregar_{inicio.strftime('%Y%m%d_%H%M%S')}.json"
    log_path.write_text(json.dumps(run_log, indent=2, ensure_ascii=False), encoding="utf-8")

    log.info("=" * 60)
    log.info("CONCLUÍDO em %.0fs | %d tabelas | %d erros",
             run_log["duracao_s"], len(run_log["tabelas"]), len(run_log["erros"]))
    log.info("Log: %s", log_path)

    if run_log["erros"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
