"""
pipeline/07_enrich_weather.py — Enriquecimento climático via INMET
===================================================================
Baixa dados históricos de estações meteorológicas do INMET para as
cidades dos principais portos brasileiros e salva em:
  parquet/features/weather_porto.parquet

Campos por (porto, data):
  wind_speed_ms   : velocidade do vento (m/s)
  wind_dir_deg    : direção do vento (graus)
  precipitation_mm: precipitação acumulada (mm)
  temp_c          : temperatura (°C)
  humidity_pct    : umidade relativa (%)
  visibility_km   : visibilidade (km) — quando disponível

API INMET (gratuita, sem autenticação):
  https://apitempo.inmet.gov.br/estacao/dados/{inicio}/{fim}/{codEstacao}

Uso:
  python pipeline/07_enrich_weather.py                    # todos os portos, 2010-2026
  python pipeline/07_enrich_weather.py --anos 2023 2024
  python pipeline/07_enrich_weather.py --portos SANTOS PARANAGUÁ
  python pipeline/07_enrich_weather.py --dry-run
"""

import argparse
import logging
import sys
import time
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import FEATURES, ANO_MIN, ANO_MAX

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT = FEATURES / "weather_porto.parquet"

# ── Estações INMET por porto ────────────────────────────────────────────────────
# Código INMET (OMM) → estação mais próxima de cada porto
# Consulte: https://portal.inmet.gov.br/noticias/estações-automaticas
PORTO_ESTACOES = {
    "SANTOS":          ("A740", "São Paulo - Mirante"),   # mais próxima de Santos
    "PARANAGUÁ":       ("A832", "Paranaguá"),
    "ITAJAÍ":          ("A852", "Itajaí"),
    "RIO DE JANEIRO":  ("A636", "Rio de Janeiro - Marambaia"),
    "SUAPE":           ("A301", "Recife"),
    "ITAQUI":          ("A204", "São Luís"),
    "VILA DO CONDE":   ("A201", "Belém"),
    "PECÉM":           ("A305", "Fortaleza"),
    "SALVADOR":        ("A401", "Salvador"),
    "MANAUS":          ("A101", "Manaus"),
    "PORTO ALEGRE":    ("A801", "Porto Alegre"),
    "FORTALEZA":       ("A305", "Fortaleza"),
    "RECIFE":          ("A301", "Recife"),
    "BELÉM":           ("A201", "Belém"),
    "VITÓRIA":         ("A612", "Vitória"),
}

INMET_API = "https://apitempo.inmet.gov.br"

# Mapeamento de campos da API INMET → nomes limpos
FIELD_MAP = {
    "VEN_VEL":  "wind_speed_ms",
    "VEN_DIR":  "wind_dir_deg",
    "CHUVA":    "precipitation_mm",
    "TEM_INS":  "temp_c",
    "UMD_INS":  "humidity_pct",
    "VIS_HORIZ":"visibility_km",
}


# ── Download INMET ─────────────────────────────────────────────────────────────

def _fetch_station_year(station_code: str, ano: int) -> pd.DataFrame | None:
    """Baixa dados diários de uma estação para um ano inteiro."""
    inicio = f"{ano}-01-01"
    fim    = f"{ano}-12-31"
    url    = f"{INMET_API}/estacao/dados/{inicio}/{fim}/{station_code}"

    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code == 404:
            log.debug("  Estação %s não tem dados para %d", station_code, ano)
            return None
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        log.warning("  Erro ao buscar %s/%d: %s", station_code, ano, e)
        return None
    except Exception as e:
        log.warning("  Resposta inválida %s/%d: %s", station_code, ano, e)
        return None

    if not data:
        return None

    rows = []
    for entry in data:
        row = {"data": entry.get("DT_MEDICAO")}
        for api_key, clean_key in FIELD_MAP.items():
            val = entry.get(api_key)
            row[clean_key] = pd.to_numeric(val, errors="coerce") if val not in (None, "", "-9999") else None
        rows.append(row)

    df = pd.DataFrame(rows)
    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    return df


def _aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega dados horários → médias/somas diárias."""
    if df is None or df.empty:
        return pd.DataFrame()

    agg = df.groupby("data").agg(
        wind_speed_ms   =("wind_speed_ms",    "mean"),
        wind_dir_deg    =("wind_dir_deg",     "mean"),
        precipitation_mm=("precipitation_mm", "sum"),
        temp_c          =("temp_c",           "mean"),
        humidity_pct    =("humidity_pct",     "mean"),
        visibility_km   =("visibility_km",    "mean"),
    ).reset_index()

    return agg


# ── Processamento principal ────────────────────────────────────────────────────

def enrich_weather(portos: list[str], anos: list[int], dry_run: bool) -> pd.DataFrame:
    all_records = []

    for porto in portos:
        if porto not in PORTO_ESTACOES:
            log.warning("Porto desconhecido: %s — pulando", porto)
            continue

        station_code, station_name = PORTO_ESTACOES[porto]
        log.info("Porto: %-20s | Estação: %s (%s)", porto, station_code, station_name)

        for ano in anos:
            if dry_run:
                log.info("  [DRY-RUN] %d: %s → %s", ano, porto, station_code)
                continue

            log.info("  Baixando %d...", ano)
            raw = _fetch_station_year(station_code, ano)
            daily = _aggregate_daily(raw)

            if daily.empty:
                log.warning("  Sem dados para %s/%d", porto, ano)
                continue

            daily["porto"]  = porto
            daily["ano"]    = ano
            all_records.append(daily)
            log.info("  ✓ %d dias | vento_médio=%.1f m/s | chuva_total=%.0f mm",
                     len(daily),
                     daily["wind_speed_ms"].mean() if not daily["wind_speed_ms"].isna().all() else 0,
                     daily["precipitation_mm"].sum() if not daily["precipitation_mm"].isna().all() else 0)

            time.sleep(0.5)  # respeita rate limit

    if not all_records:
        return pd.DataFrame()

    return pd.concat(all_records, ignore_index=True)


# ── Save ───────────────────────────────────────────────────────────────────────

def _save(df: pd.DataFrame):
    FEATURES.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUTPUT, compression="zstd")
    log.info("Salvo: %s (%.1f MB | %d linhas)", OUTPUT, OUTPUT.stat().st_size / 1e6, len(df))


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enriquecimento climático INMET")
    parser.add_argument("--portos", nargs="+", default=list(PORTO_ESTACOES.keys()),
                        help="Portos a processar (padrão: todos)")
    parser.add_argument("--anos", nargs="+", type=int,
                        default=list(range(ANO_MIN, ANO_MAX + 1)),
                        help="Anos (padrão: 2010–2026)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Lista o que seria baixado, sem fazer requisições")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Enriquecimento climático INMET")
    log.info("Portos: %s", args.portos)
    log.info("Anos:   %s–%s", min(args.anos), max(args.anos))
    log.info("=" * 60)

    df = enrich_weather(args.portos, args.anos, args.dry_run)

    if df.empty:
        log.info("Nenhum dado coletado.")
        return

    _save(df)

    # Resumo
    log.info("=" * 60)
    log.info("Cobertura: %d portos | %d anos | %d linhas",
             df["porto"].nunique(), df["ano"].nunique(), len(df))
    log.info("Campos: %s", list(df.columns))


if __name__ == "__main__":
    main()
