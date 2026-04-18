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
    df = duckdb.execute(f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true)").df()
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

def build_carga_por_atracacao() -> pd.DataFrame:
    """Agrega Carga + Carga_Conteinerizada por IDAtracacao."""
    log.info("Construindo: carga_por_atracacao")

    carga = _read_silver("Carga")
    carga["IDAtracacao"] = pd.to_numeric(carga["IDAtracacao"], errors="coerce")
    carga["VLPesoCargaBruta"] = pd.to_numeric(carga.get("VLPesoCargaBruta", 0), errors="coerce").fillna(0)
    carga["TEU"] = pd.to_numeric(carga.get("TEU", 0), errors="coerce").fillna(0)

    grp = carga.groupby("IDAtracacao").agg(
        peso_total   = ("VLPesoCargaBruta", "sum"),
        teu_total    = ("TEU", "sum"),
        n_cargas     = ("IDAtracacao", "count"),
        natureza_top = ("CDNaturezaCarga",
                        lambda x: x.mode().iloc[0] if len(x) > 0 else None),
        sentido_top  = ("Sentido",
                        lambda x: x.mode().iloc[0] if len(x) > 0 else None),
    ).reset_index()

    # Acrescenta peso conteinerizado
    try:
        cont = _read_silver("Carga_Conteinerizada")
        cont["IDAtracacao"] = pd.to_numeric(cont["IDAtracacao"], errors="coerce")
        cont["VLPesoCargaConteinerizada"] = pd.to_numeric(
            cont.get("VLPesoCargaConteinerizada", 0), errors="coerce"
        ).fillna(0)
        cont_grp = cont.groupby("IDAtracacao").agg(
            peso_cont=("VLPesoCargaConteinerizada", "sum")
        ).reset_index()
        grp = grp.merge(cont_grp, on="IDAtracacao", how="left")
        grp["peso_cont"] = grp["peso_cont"].fillna(0)
    except FileNotFoundError:
        grp["peso_cont"] = 0.0

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

    taxa["Ano"] = pd.to_numeric(taxa.get("Ano", None), errors="coerce").astype("Int64")

    # Detecta colunas numéricas disponíveis
    num_cols = taxa.select_dtypes(include="float64").columns.tolist()
    log.info("  Colunas numéricas detectadas: %s", num_cols)

    # Agrupamento flexível por porto e ano
    id_cols = [c for c in ["Porto Atracação", "Complexo Portuário", "Ano"] if c in taxa.columns]
    if not id_cols:
        log.warning("  Colunas de agrupamento não encontradas em TaxaOcupacao")
        return taxa

    agg_dict = {col: "mean" for col in num_cols if col not in id_cols}
    if not agg_dict:
        return taxa

    grp = taxa.groupby(id_cols).agg(agg_dict).reset_index()
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
    par["TParalisacao"] = pd.to_numeric(par.get("TParalisacao", 0), errors="coerce").fillna(0)

    grp = par.groupby("IDAtracacao").agg(
        n_paralisacoes    = ("IDAtracacao", "count"),
        tempo_paralisacao = ("TParalisacao", "sum"),
    ).reset_index()

    return grp


def build_carga_hidrovia_anual() -> pd.DataFrame:
    """Movimentação anual em hidrovias."""
    log.info("Construindo: carga_hidrovia_anual")
    hidrovia = _read_silver("Carga_Hidrovia")
    hidrovia["Ano"] = pd.to_numeric(hidrovia.get("Ano", None), errors="coerce").astype("Int64")
    hidrovia["ValorMovimentado"] = pd.to_numeric(
        hidrovia.get("ValorMovimentado", 0), errors="coerce"
    ).fillna(0)

    id_cols = [c for c in ["Hidrovia", "Região Geográfica", "UF", "Ano"] if c in hidrovia.columns]
    grp = hidrovia.groupby(id_cols).agg(
        tonelagem_total=("ValorMovimentado", "sum"),
        n_registros    =("ValorMovimentado", "count"),
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
