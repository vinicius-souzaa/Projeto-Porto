"""
pipeline/01_converter.py — Bronze → Silver
===========================================
Lê TXTs brutos de Dados/{ano}/ e grava Parquet em parquet/silver/{tipo}/ano={ano}/

Características:
  - Schema evolution: 3 eras (2010-2019 / 2020-2022 / 2023-2026)
  - Encoding: latin-1, BOM removido, separador ponto-e-vírgula
  - Numéricas: vírgula decimal → float64 (conhecidas + auto-detecção)
  - Idempotente: pula anos já convertidos (use --force para reprocessar)
  - Quality score 0-100 por arquivo gravado no log de execução
  - Data lineage: registra arquivo de origem em cada partição

Uso:
  python pipeline/01_converter.py
  python pipeline/01_converter.py --force
  python pipeline/01_converter.py --tipos Atracacao TemposAtracacao
  python pipeline/01_converter.py --anos 2023 2024 2025 2026
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import DADOS, SILVER, LOGS, TIPOS, NUMERICAS, ANO_MIN, ANO_MAX

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _fix_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Remove BOM e re-codifica nomes de colunas de latin-1 para utf-8."""
    df.columns = (
        df.columns
        .str.replace("ï»¿", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.encode("latin-1", errors="replace")
        .str.decode("utf-8", errors="replace")
        .str.strip()
    )
    return df


def _fix_numericas(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Converte colunas para float64 tratando vírgula como separador decimal."""
    for col in cols:
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(
            df[col].astype(str).str.strip().str.replace(",", ".", regex=False),
            errors="coerce",
        ).astype("float64")
    return df


def _autodetect_numericas(df: pd.DataFrame, conhecidas: list[str]) -> list[str]:
    """
    Detecta colunas numéricas adicionais não listadas em NUMERICAS.
    Critério: >80% dos valores não-nulos convertem para número após troca de vírgula.
    """
    extras = []
    for col in df.columns:
        if col in conhecidas:
            continue
        sample = df[col].dropna().head(200).astype(str).str.replace(",", ".", regex=False)
        converted = pd.to_numeric(sample, errors="coerce")
        if len(converted) > 0 and converted.notna().mean() > 0.8:
            extras.append(col)
    return extras


def _quality_score(df: pd.DataFrame, tipo: str, ano: int) -> dict:
    """
    Calcula score de qualidade 0-100 para um DataFrame.
    Retorna dict com score e detalhes dos checks.
    """
    checks = {}

    # 1. Linhas suficientes (mínimo esperado por tipo)
    min_linhas = {"Atracacao": 1000, "Carga": 5000, "TemposAtracacao": 1000}
    minimo = min_linhas.get(tipo, 100)
    checks["min_linhas"] = len(df) >= minimo

    # 2. Sem colunas 100% nulas
    cols_todas_nulas = [c for c in df.columns if df[c].isna().all()]
    checks["sem_colunas_nulas"] = len(cols_todas_nulas) == 0

    # 3. Coluna Ano presente e consistente
    if "Ano" in df.columns:
        anos_unicos = pd.to_numeric(df["Ano"], errors="coerce").dropna().unique()
        checks["ano_consistente"] = all(int(a) == ano for a in anos_unicos if pd.notna(a))
    else:
        checks["ano_consistente"] = None  # não aplicável

    # 4. Taxa de nulos < 50% nas colunas numéricas
    num_cols = df.select_dtypes(include="float64").columns
    if len(num_cols) > 0:
        taxa_media = df[num_cols].isna().mean().mean()
        checks["nulos_numericos_ok"] = taxa_media < 0.5
    else:
        checks["nulos_numericos_ok"] = None

    # Score: média dos checks aplicáveis
    aplicaveis = [v for v in checks.values() if v is not None]
    score = int(100 * sum(aplicaveis) / len(aplicaveis)) if aplicaveis else 0

    return {"score": score, "checks": checks, "n_linhas": len(df), "n_colunas": len(df.columns)}


def _already_done(tipo: str, ano: int) -> bool:
    """Verifica se a partição já existe no Silver."""
    path = SILVER / tipo / f"ano={ano}" / "data.parquet"
    return path.exists()


def _read_txt(arquivo: Path, tipo: str) -> pd.DataFrame | None:
    """Lê um TXT ANTAQ e aplica todas as correções de schema."""
    try:
        df = pd.read_csv(
            arquivo,
            sep=";",
            encoding="latin-1",
            low_memory=False,
            on_bad_lines="skip",
            dtype=str,
        )
    except Exception as e:
        log.error("  Erro ao ler %s: %s", arquivo.name, e)
        return None

    if df.empty:
        log.warning("  Arquivo vazio: %s", arquivo.name)
        return None

    df = _fix_cols(df)

    # Numéricas conhecidas
    conhecidas = NUMERICAS.get(tipo, [])
    df = _fix_numericas(df, conhecidas)

    # Auto-detecção de numéricas extras (para TaxaOcupacao, CargaAreas, etc.)
    extras = _autodetect_numericas(df, conhecidas)
    if extras:
        log.info("    Auto-detectadas numéricas: %s", extras)
        df = _fix_numericas(df, extras)

    # Strings: strip e None para nulos
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip().replace({"nan": None, "": None})

    return df


def _write_partition(df: pd.DataFrame, tipo: str, ano: int, fonte: str) -> Path:
    """
    Grava DataFrame como Parquet particionado em silver/{tipo}/ano={ano}/data.parquet.
    Adiciona metadados de lineage.
    """
    out_dir = SILVER / tipo / f"ano={ano}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    # Garante object → string para Arrow
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype("string")

    table = pa.Table.from_pandas(df, preserve_index=False)

    # Metadados de lineage
    meta = {
        b"antaq.tipo": tipo.encode(),
        b"antaq.ano": str(ano).encode(),
        b"antaq.fonte": fonte.encode(),
        b"antaq.convertido_em": datetime.now(timezone.utc).isoformat().encode(),
        b"antaq.n_linhas": str(len(df)).encode(),
    }
    table = table.replace_schema_metadata({**table.schema.metadata, **meta})

    pq.write_table(table, out_path, compression="zstd")
    return out_path


# ── Processamento principal ───────────────────────────────────────────────────

def converter_tipo(tipo: str, anos: list[int], force: bool) -> dict:
    """
    Converte todos os anos de um tipo para Parquet Silver.
    Retorna sumário com métricas de qualidade.
    """
    log.info("=" * 60)
    log.info("TIPO: %s", tipo)
    log.info("=" * 60)

    nome_fn = TIPOS[tipo]
    sumario = {"tipo": tipo, "anos": {}, "erros": []}

    pulados = 0
    processados = 0

    for ano in anos:
        arquivo = DADOS / str(ano) / nome_fn(ano)

        if not arquivo.exists():
            log.debug("  %d: arquivo não encontrado — %s", ano, arquivo.name)
            continue

        if not force and _already_done(tipo, ano):
            log.info("  %d: já convertido — pulando (use --force para reprocessar)", ano)
            pulados += 1
            continue

        log.info("  %d: lendo %s (%.1f MB)", ano, arquivo.name,
                 arquivo.stat().st_size / 1e6)

        df = _read_txt(arquivo, tipo)
        if df is None:
            sumario["erros"].append(f"{ano}: falha na leitura")
            continue

        quality = _quality_score(df, tipo, ano)
        out_path = _write_partition(df, tipo, ano, str(arquivo))

        pq_mb = out_path.stat().st_size / 1e6
        log.info("    → %d linhas | %d colunas | %.1f MB | score=%d/100",
                 len(df), len(df.columns), pq_mb, quality["score"])

        if quality["score"] < 60:
            log.warning("    ATENÇÃO: quality score baixo (%d/100) — checks: %s",
                        quality["score"], quality["checks"])

        sumario["anos"][ano] = {**quality, "parquet_mb": round(pq_mb, 2)}
        processados += 1
        del df

    log.info("  Resultado: %d processados, %d pulados, %d erros",
             processados, pulados, len(sumario["erros"]))
    return sumario


def main():
    parser = argparse.ArgumentParser(description="ANTAQ Bronze → Silver converter")
    parser.add_argument("--tipos", nargs="+", default=list(TIPOS.keys()),
                        help="Tipos a processar (padrão: todos)")
    parser.add_argument("--anos", nargs="+", type=int,
                        default=list(range(ANO_MIN, ANO_MAX + 1)),
                        help="Anos a processar (padrão: todos)")
    parser.add_argument("--force", action="store_true",
                        help="Reprocessa mesmo que já exista")
    args = parser.parse_args()

    # Valida tipos
    tipos_invalidos = [t for t in args.tipos if t not in TIPOS]
    if tipos_invalidos:
        log.error("Tipos inválidos: %s. Disponíveis: %s", tipos_invalidos, list(TIPOS.keys()))
        sys.exit(1)

    inicio = datetime.now(timezone.utc)
    log.info("Pipeline ANTAQ Bronze → Silver")
    log.info("Tipos: %s", args.tipos)
    log.info("Anos:  %s–%s", min(args.anos), max(args.anos))
    log.info("Force: %s", args.force)
    log.info("Dados: %s", DADOS)
    log.info("Silver: %s", SILVER)

    run_log = {
        "inicio": inicio.isoformat(),
        "tipos": args.tipos,
        "anos": args.anos,
        "force": args.force,
        "sumarios": [],
    }

    for tipo in args.tipos:
        sumario = converter_tipo(tipo, args.anos, args.force)
        run_log["sumarios"].append(sumario)

    fim = datetime.now(timezone.utc)
    run_log["fim"] = fim.isoformat()
    run_log["duracao_s"] = round((fim - inicio).total_seconds(), 1)

    # Salva log de execução
    log_path = LOGS / f"01_converter_{inicio.strftime('%Y%m%d_%H%M%S')}.json"
    log_path.write_text(json.dumps(run_log, indent=2, ensure_ascii=False), encoding="utf-8")

    # Resumo final
    total_erros = sum(len(s["erros"]) for s in run_log["sumarios"])
    total_anos  = sum(len(s["anos"]) for s in run_log["sumarios"])
    log.info("=" * 60)
    log.info("CONCLUÍDO em %.0fs | %d partições geradas | %d erros",
             run_log["duracao_s"], total_anos, total_erros)
    log.info("Log salvo: %s", log_path)

    if total_erros > 0:
        log.warning("Erros encontrados — revise o log: %s", log_path)
        sys.exit(1)


if __name__ == "__main__":
    main()
