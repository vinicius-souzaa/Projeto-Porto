"""
pipeline/06_enrich_imo.py — Enriquecimento via N° do IMO → Equasis
===================================================================
Para cada IMO único na camada Silver (Atracacao), consulta o Equasis
e salva características estáticas do navio em parquet/features/ships_imo.parquet

Fonte: https://www.equasis.org  (gratuito, requer cadastro)

Campos obtidos por IMO:
  ship_type    : tipo do navio (Bulk Carrier, Tanker, Container Ship, etc.)
  ship_dwt     : Dead Weight Tonnage (capacidade de carga em t)
  ship_gt      : Gross Tonnage
  ship_loa_m   : comprimento total (metros)
  ship_beam_m  : boca (metros)
  ship_built   : ano de construção
  ship_flag    : bandeira (país de registro)

Pré-requisitos:
  pip install requests beautifulsoup4
  Crie conta em https://www.equasis.org (gratuito)

Uso:
  python pipeline/06_enrich_imo.py --email seu@email.com --password suasenha
  python pipeline/06_enrich_imo.py --email seu@email.com --password suasenha --limit 500
  python pipeline/06_enrich_imo.py --dry-run   # lista IMOs sem buscar
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import SILVER, FEATURES, LOGS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT = FEATURES / "ships_imo.parquet"

# Mapeamento Equasis → campos limpos
EQUASIS_FIELDS = {
    "Type of ship":    "ship_type",
    "Gross Tonnage":   "ship_gt",
    "Deadweight":      "ship_dwt",
    "Length Overall":  "ship_loa_m",
    "Breadth Moulded": "ship_beam_m",
    "Year of build":   "ship_built",
    "Flag":            "ship_flag",
}


# ── Coleta de IMOs do Silver ───────────────────────────────────────────────────

def _collect_imos() -> list[int]:
    """Lê todos os arquivos Atracacao Silver e retorna IMOs únicos válidos."""
    imos = set()
    glob = str(SILVER / "Atracacao" / "**" / "data.parquet")

    for pq_file in sorted((SILVER / "Atracacao").glob("*/data.parquet")):
        try:
            df = pd.read_parquet(pq_file, columns=["N° do IMO"])
            valid = pd.to_numeric(df["N° do IMO"], errors="coerce").dropna()
            valid = valid[valid > 1_000_000].astype(int)  # IMO válido: 7 dígitos
            imos.update(valid.tolist())
        except Exception as e:
            log.warning("Erro ao ler %s: %s", pq_file, e)

    log.info("IMOs únicos encontrados no Silver: %d", len(imos))
    return sorted(imos)


# ── Consulta Equasis ───────────────────────────────────────────────────────────

def _equasis_session(email: str, password: str):
    """Cria sessão autenticada no Equasis."""
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError:
        log.error("Execute: pip install requests beautifulsoup4")
        sys.exit(1)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; research/1.0)",
    })

    # Login
    login_url = "https://www.equasis.org/EquasisWeb/authen/AuthenUser.do"
    payload = {
        "j_email":    email,
        "j_password": password,
        "submit":     "Login",
    }
    resp = session.post(login_url, data=payload, timeout=30)
    resp.raise_for_status()

    if "logout" not in resp.text.lower() and "sign out" not in resp.text.lower():
        log.error("Login no Equasis falhou — verifique e-mail/senha")
        sys.exit(1)

    log.info("Login no Equasis OK")
    return session


def _fetch_ship(session, imo: int) -> dict | None:
    """Busca características de um navio pelo IMO no Equasis."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        sys.exit(1)

    url = f"https://www.equasis.org/EquasisWeb/restricted/ShipInfo.do?P_IMO={imo}"
    try:
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.warning("  IMO %d: erro HTTP — %s", imo, e)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extrai tabela de características técnicas
    result = {"imo": imo}
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True)
            value = cells[1].get_text(strip=True)
            if label in EQUASIS_FIELDS:
                field = EQUASIS_FIELDS[label]
                result[field] = value

    if len(result) <= 1:  # só IMO, nenhum campo encontrado
        return None

    # Normaliza campos numéricos
    for num_field in ["ship_gt", "ship_dwt", "ship_loa_m", "ship_beam_m", "ship_built"]:
        if num_field in result:
            result[num_field] = pd.to_numeric(
                str(result[num_field]).replace(",", "").replace(" ", ""),
                errors="coerce"
            )

    return result


# ── Merge com cache existente ──────────────────────────────────────────────────

def _load_existing() -> pd.DataFrame:
    if OUTPUT.exists():
        df = pd.read_parquet(OUTPUT)
        log.info("Cache existente: %d IMOs já consultados", len(df))
        return df
    return pd.DataFrame(columns=["imo"])


def _save(records: list[dict]):
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUTPUT, compression="zstd")
    log.info("Salvo: %d registros em %s", len(df), OUTPUT)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enriquecimento IMO → Equasis")
    parser.add_argument("--email",    required=False, help="E-mail do Equasis")
    parser.add_argument("--password", required=False, help="Senha do Equasis")
    parser.add_argument("--limit",    type=int, default=0,
                        help="Máximo de IMOs a consultar nesta execução (0 = todos)")
    parser.add_argument("--sleep",    type=float, default=1.5,
                        help="Segundos entre requisições (padrão: 1.5)")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Lista IMOs sem consultar")
    args = parser.parse_args()

    all_imos  = _collect_imos()
    existing  = _load_existing()
    done_imos = set(existing["imo"].tolist()) if "imo" in existing.columns else set()

    pending = [i for i in all_imos if i not in done_imos]
    log.info("IMOs pendentes: %d (de %d total)", len(pending), len(all_imos))

    if args.dry_run:
        log.info("=== DRY-RUN — primeiros 20 IMOs pendentes ===")
        for imo in pending[:20]:
            log.info("  IMO %d", imo)
        return

    if not args.email or not args.password:
        log.error("--email e --password são obrigatórios para consulta real")
        log.info("Exemplo: python pipeline/06_enrich_imo.py --email x@x.com --password abc")
        log.info("Cadastro gratuito: https://www.equasis.org")
        sys.exit(1)

    if args.limit > 0:
        pending = pending[: args.limit]
        log.info("Limitando a %d IMOs nesta execução", args.limit)

    session = _equasis_session(args.email, args.password)

    records = existing.to_dict("records")
    ok = err = 0

    for i, imo in enumerate(pending, 1):
        log.info("[%d/%d] IMO %d", i, len(pending), imo)
        data = _fetch_ship(session, imo)
        if data:
            records.append(data)
            ok += 1
            log.info("  ✓ %s | DWT=%s | LOA=%s m",
                     data.get("ship_type", "?"),
                     data.get("ship_dwt", "?"),
                     data.get("ship_loa_m", "?"))
        else:
            log.warning("  ✗ sem dados")
            err += 1

        # Salva a cada 50 registros (checkpoint)
        if i % 50 == 0:
            _save(records)

        time.sleep(args.sleep)

    _save(records)
    log.info("Concluído: %d OK | %d erros | %d total no cache", ok, err, len(records))


if __name__ == "__main__":
    main()
