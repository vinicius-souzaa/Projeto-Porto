"""
pipeline/catalog.py — Data Catalog Automático
===============================================
Inspeciona as camadas Silver e Gold e gera um catálogo JSON
com metadados de cada tabela: colunas, tipos, cobertura temporal,
taxa de nulos e quality score.

Saída:
  parquet/gold/catalog.json  — consumido pelo dashboard (página Catálogo)

Uso:
  python pipeline/catalog.py
"""

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


def _null_rate(s: pd.Series) -> float:
    return round(float(s.isna().mean()), 4)


def _catalog_silver_tipo(tipo: str) -> dict | None:
    """Gera catálogo de um tipo Silver lendo amostra de todos os anos."""
    tipo_dir = SILVER / tipo
    partitions = sorted(tipo_dir.glob("ano=*/data.parquet")) if tipo_dir.exists() else []

    if not partitions:
        return None

    anos = sorted(int(p.parent.name.split("=")[1]) for p in partitions)

    # Amostra rápida via DuckDB
    glob = str(tipo_dir / "ano=*" / "data.parquet")
    try:
        df = duckdb.execute(
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning=true) LIMIT 5000"
        ).df()
    except Exception as e:
        log.warning("  [%s] erro ao ler amostra: %s", tipo, e)
        return None

    total_mb = sum(p.stat().st_size for p in partitions) / 1e6

    colunas = []
    for col in df.columns:
        colunas.append({
            "nome":       col,
            "tipo":       str(df[col].dtype),
            "nulos_pct":  _null_rate(df[col]),
            "exemplo":    str(df[col].dropna().iloc[0]) if df[col].notna().any() else None,
        })

    return {
        "tipo":        tipo,
        "camada":      "silver",
        "anos":        anos,
        "ano_min":     anos[0],
        "ano_max":     anos[-1],
        "n_particoes": len(partitions),
        "tamanho_mb":  round(total_mb, 1),
        "n_colunas":   len(df.columns),
        "colunas":     colunas,
        "inspecionado_em": datetime.now(timezone.utc).isoformat(),
    }


def _catalog_gold_tabela(nome: str) -> dict | None:
    """Gera catálogo de uma tabela Gold."""
    path = GOLD / f"{nome}.parquet"
    if not path.exists():
        return None

    try:
        df = duckdb.execute(
            f"SELECT * FROM read_parquet('{path}') LIMIT 10000"
        ).df()
        total_rows = duckdb.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path}')"
        ).fetchone()[0]
    except Exception as e:
        log.warning("  [%s] erro ao ler: %s", nome, e)
        return None

    mb = path.stat().st_size / 1e6

    colunas = []
    for col in df.columns:
        entry = {
            "nome":      col,
            "tipo":      str(df[col].dtype),
            "nulos_pct": _null_rate(df[col]),
        }
        if pd.api.types.is_numeric_dtype(df[col]):
            entry["min"] = round(float(df[col].min()), 2) if df[col].notna().any() else None
            entry["max"] = round(float(df[col].max()), 2) if df[col].notna().any() else None
            entry["media"] = round(float(df[col].mean()), 2) if df[col].notna().any() else None
        else:
            top = df[col].value_counts().head(3).index.tolist()
            entry["top_valores"] = [str(v) for v in top]
        colunas.append(entry)

    # Cobertura temporal
    ano_min = ano_max = None
    for col in ["Ano", "ano", "_ano"]:
        if col in df.columns:
            anos = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(anos) > 0:
                ano_min, ano_max = int(anos.min()), int(anos.max())
            break

    return {
        "nome":         nome,
        "camada":       "gold",
        "n_linhas":     total_rows,
        "tamanho_mb":   round(mb, 1),
        "n_colunas":    len(df.columns),
        "ano_min":      ano_min,
        "ano_max":      ano_max,
        "colunas":      colunas,
        "inspecionado_em": datetime.now(timezone.utc).isoformat(),
    }


def _quality_summary(catalog: list[dict]) -> dict:
    """Calcula score global de qualidade do dataset."""
    scores = []
    for entry in catalog:
        if not entry:
            continue
        null_rates = [c["nulos_pct"] for c in entry.get("colunas", [])]
        if null_rates:
            avg_null = sum(null_rates) / len(null_rates)
            score = max(0, int(100 * (1 - avg_null * 2)))
            scores.append(score)

    return {
        "score_medio":  round(sum(scores) / len(scores), 1) if scores else 0,
        "n_tabelas":    len([e for e in catalog if e]),
        "score_minimo": min(scores) if scores else 0,
        "score_maximo": max(scores) if scores else 0,
    }


def main():
    log.info("=" * 60)
    log.info("ANTAQ Data Catalog")
    log.info("Silver: %s", SILVER)
    log.info("Gold:   %s", GOLD)
    log.info("=" * 60)

    catalog = {"gerado_em": datetime.now(timezone.utc).isoformat(), "silver": [], "gold": []}

    # ── Silver ────────────────────────────────────────────────────────────────
    log.info("Inspecionando Silver...")
    if SILVER.exists():
        for tipo_dir in sorted(SILVER.iterdir()):
            if tipo_dir.is_dir():
                log.info("  %s", tipo_dir.name)
                entry = _catalog_silver_tipo(tipo_dir.name)
                if entry:
                    catalog["silver"].append(entry)
                    log.info("    → %d anos | %d colunas | %.1f MB",
                             len(entry["anos"]), entry["n_colunas"], entry["tamanho_mb"])

    # ── Gold ──────────────────────────────────────────────────────────────────
    log.info("Inspecionando Gold...")
    gold_tabelas = [
        "atracacao_master", "carga_por_atracacao", "taxa_ocupacao_anual",
        "paralisacoes_por_atracacao", "carga_hidrovia_anual", "porto_perfil",
    ]
    for nome in gold_tabelas:
        log.info("  %s", nome)
        entry = _catalog_gold_tabela(nome)
        if entry:
            catalog["gold"].append(entry)
            log.info("    → %d linhas | %d colunas | %.1f MB",
                     entry["n_linhas"], entry["n_colunas"], entry["tamanho_mb"])

    # ── Quality summary ───────────────────────────────────────────────────────
    catalog["quality_summary"] = _quality_summary(catalog["silver"] + catalog["gold"])

    # ── Salva ─────────────────────────────────────────────────────────────────
    out = GOLD / "catalog.json"
    out.write_text(json.dumps(catalog, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("=" * 60)
    log.info("Catálogo salvo: %s", out)
    log.info("  Silver: %d tipos | Gold: %d tabelas",
             len(catalog["silver"]), len(catalog["gold"]))
    log.info("  Quality score médio: %.0f/100",
             catalog["quality_summary"]["score_medio"])


if __name__ == "__main__":
    main()
