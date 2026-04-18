"""
pipeline/05_upload_hub.py — Upload para HuggingFace Hub
=========================================================
Publica as camadas Silver, Gold, Features e Model no dataset
HuggingFace: vinicius-souza/antaq

Estrutura no Hub:
  silver/{tipo}/ano={ano}/data.parquet
  gold/{nome}.parquet
  features/features.parquet
  features/encoders_map.json
  model/model.pkl
  model/model_operacao.pkl
  model/model_q10.pkl
  model/model_q90.pkl
  model/model_lgbm.pkl
  model/model_meta.pkl
  model/model_card.json
  model/shap_importance.parquet
  model/shap_values.parquet
  model/encoders.pkl

Pré-requisitos:
  pip install huggingface_hub
  huggingface-cli login      ← só na primeira vez

Uso:
  python pipeline/05_upload_hub.py               # sobe tudo
  python pipeline/05_upload_hub.py --camadas silver gold
  python pipeline/05_upload_hub.py --dry-run     # lista o que seria enviado

Nota: usa upload_folder() (um commit por camada) para evitar o rate limit
de 128 commits/hora do HuggingFace Hub.
"""

import argparse
import logging
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import SILVER, GOLD, FEATURES, MODEL, LOGS, HF_REPO, HF_REPO_TYPE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _get_api():
    try:
        from huggingface_hub import HfApi
        return HfApi()
    except ImportError:
        log.error("huggingface_hub não instalado. Execute: pip install huggingface_hub")
        sys.exit(1)


def _ensure_repo(api):
    """Cria o dataset repo se não existir."""
    try:
        api.repo_info(repo_id=HF_REPO, repo_type=HF_REPO_TYPE)
        log.info("Repo existente: huggingface.co/datasets/%s", HF_REPO)
    except Exception:
        log.info("Criando repo: %s ...", HF_REPO)
        api.create_repo(repo_id=HF_REPO, repo_type=HF_REPO_TYPE, private=False)
        log.info("Repo criado: huggingface.co/datasets/%s", HF_REPO)


def _count_files(folder: Path, pattern: str = "**/*") -> tuple[int, float]:
    """Retorna (n_arquivos, total_mb) de uma pasta."""
    files = [f for f in folder.glob(pattern) if f.is_file()]
    total_mb = sum(f.stat().st_size for f in files) / 1e6
    return len(files), total_mb


def upload_silver(api, dry_run: bool) -> dict:
    """Sobe toda a camada Silver em um único commit usando upload_folder()."""
    log.info("── SILVER ──")
    stats = {"ok": 0, "erro": 0}

    if not SILVER.exists():
        log.warning("Silver não encontrado: %s", SILVER)
        return stats

    n_files, total_mb = _count_files(SILVER, "**/*.parquet")
    log.info("  %d arquivos Parquet | %.1f MB total", n_files, total_mb)

    if dry_run:
        for tipo_dir in sorted(SILVER.iterdir()):
            if not tipo_dir.is_dir():
                continue
            for part_dir in sorted(tipo_dir.glob("ano=*")):
                pq = part_dir / "data.parquet"
                if pq.exists():
                    mb = pq.stat().st_size / 1e6
                    remote = f"silver/{tipo_dir.name}/{part_dir.name}/data.parquet"
                    log.info("  [DRY-RUN] %s → %s (%.1f MB)", pq.name, remote, mb)
                    stats["ok"] += 1
        return stats

    log.info("  Enviando Silver como um único commit...")
    try:
        api.upload_folder(
            folder_path=str(SILVER),
            path_in_repo="silver",
            repo_id=HF_REPO,
            repo_type=HF_REPO_TYPE,
            commit_message="Upload Silver layer (all tipos/anos)",
        )
        stats["ok"] = n_files
        log.info("  ✓ Silver: %d arquivos enviados (%.1f MB)", n_files, total_mb)
    except Exception as e:
        log.error("  ERRO ao enviar Silver: %s", e)
        stats["erro"] = n_files

    return stats


def upload_gold(api, dry_run: bool) -> dict:
    """Sobe todos os arquivos Gold em um único commit."""
    log.info("── GOLD ──")
    stats = {"ok": 0, "erro": 0}

    if not GOLD.exists():
        log.warning("Gold não encontrado: %s", GOLD)
        return stats

    pq_files = sorted(GOLD.glob("*.parquet"))
    if not pq_files:
        log.warning("Nenhum arquivo Parquet em Gold: %s", GOLD)
        return stats

    total_mb = sum(f.stat().st_size for f in pq_files) / 1e6
    log.info("  %d arquivos | %.1f MB total", len(pq_files), total_mb)

    if dry_run:
        for pq in pq_files:
            mb = pq.stat().st_size / 1e6
            log.info("  [DRY-RUN] %s → gold/%s (%.1f MB)", pq.name, pq.name, mb)
            stats["ok"] += 1
        return stats

    log.info("  Enviando Gold como um único commit...")
    try:
        api.upload_folder(
            folder_path=str(GOLD),
            path_in_repo="gold",
            repo_id=HF_REPO,
            repo_type=HF_REPO_TYPE,
            commit_message="Upload Gold layer (aggregated tables)",
        )
        stats["ok"] = len(pq_files)
        log.info("  ✓ Gold: %d arquivos enviados (%.1f MB)", len(pq_files), total_mb)
    except Exception as e:
        log.error("  ERRO ao enviar Gold: %s", e)
        stats["erro"] = len(pq_files)

    return stats


def upload_features(api, dry_run: bool) -> dict:
    """Sobe feature store e encoders em um único commit."""
    log.info("── FEATURES ──")
    stats = {"ok": 0, "erro": 0}

    if not FEATURES.exists():
        log.warning("Features não encontrado: %s", FEATURES)
        return stats

    expected = ["features.parquet", "encoders_map.json"]
    found = [FEATURES / f for f in expected if (FEATURES / f).exists()]

    if not found:
        log.warning("Nenhum arquivo de features encontrado em: %s", FEATURES)
        return stats

    total_mb = sum(f.stat().st_size for f in found) / 1e6
    log.info("  %d arquivos | %.1f MB total", len(found), total_mb)

    if dry_run:
        for f in found:
            mb = f.stat().st_size / 1e6
            log.info("  [DRY-RUN] %s → features/%s (%.1f MB)", f.name, f.name, mb)
            stats["ok"] += 1
        return stats

    log.info("  Enviando Features como um único commit...")
    try:
        api.upload_folder(
            folder_path=str(FEATURES),
            path_in_repo="features",
            repo_id=HF_REPO,
            repo_type=HF_REPO_TYPE,
            commit_message="Upload Features layer (feature store + encoders)",
            # ignora subpastas desnecessárias
            ignore_patterns=["*.tmp", "__pycache__/**"],
        )
        stats["ok"] = len(found)
        log.info("  ✓ Features: %d arquivos enviados (%.1f MB)", len(found), total_mb)
    except Exception as e:
        log.error("  ERRO ao enviar Features: %s", e)
        stats["erro"] = len(found)

    return stats


def upload_model(api, dry_run: bool) -> dict:
    """Sobe todos os artefatos do modelo em um único commit."""
    log.info("── MODEL ──")
    stats = {"ok": 0, "erro": 0}

    if not MODEL.exists():
        log.warning("Model não encontrado: %s", MODEL)
        return stats

    model_files_expected = [
        "model.pkl",
        "model_operacao.pkl",
        "model_q10.pkl",
        "model_q90.pkl",
        "model_lgbm.pkl",
        "model_meta.pkl",
        "model_card.json",
        "shap_importance.parquet",
        "shap_values.parquet",
        "encoders.pkl",
    ]

    found = [MODEL / f for f in model_files_expected if (MODEL / f).exists()]

    if not found:
        log.warning("Nenhum artefato de modelo encontrado em: %s", MODEL)
        return stats

    total_mb = sum(f.stat().st_size for f in found) / 1e6
    log.info("  %d arquivos | %.1f MB total", len(found), total_mb)

    if dry_run:
        for f in found:
            mb = f.stat().st_size / 1e6
            log.info("  [DRY-RUN] %s → model/%s (%.1f MB)", f.name, f.name, mb)
            stats["ok"] += 1
        return stats

    # Cria pasta temporária com apenas os arquivos esperados
    # (evita subir arquivos indesejados que possam estar em MODEL/)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_model = Path(tmpdir) / "model"
        tmp_model.mkdir()
        for f in found:
            shutil.copy2(f, tmp_model / f.name)

        log.info("  Enviando Model como um único commit...")
        try:
            api.upload_folder(
                folder_path=str(tmp_model),
                path_in_repo="model",
                repo_id=HF_REPO,
                repo_type=HF_REPO_TYPE,
                commit_message="Upload Model layer (artifacts + SHAP + model card)",
            )
            stats["ok"] = len(found)
            log.info("  ✓ Model: %d arquivos enviados (%.1f MB)", len(found), total_mb)
        except Exception as e:
            log.error("  ERRO ao enviar Model: %s", e)
            stats["erro"] = len(found)

    return stats


def upload_readme(api, dry_run: bool):
    """Gera e sobe o README do dataset."""
    log.info("── README ──")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    readme = f"""---
license: cc-by-4.0
task_categories:
  - tabular-regression
  - time-series-forecasting
language:
  - pt
tags:
  - ports
  - brazil
  - antaq
  - logistics
  - shipping
  - data-engineering
  - medallion-architecture
size_categories:
  - 10M<n<100M
---

# ANTAQ — Portos do Brasil (2010–2026)

Dataset construído a partir dos dados abertos da **ANTAQ** (Agência Nacional de Transportes
Aquaviários), cobrindo todas as atracações, cargas e operações nos portos brasileiros de 2010 a 2026.

## Estrutura (Medallion Architecture)

```
silver/   → Parquet por tipo e ano (Bronze → Silver)
gold/     → Tabelas analíticas agregadas (Silver → Gold)
features/ → Feature store para ML
model/    → Modelos treinados + SHAP + model card
```

## Tabelas Silver (12 tipos)

| Tipo | Descrição | Disponível desde |
|------|-----------|-----------------|
| Atracacao | Eventos de atracação | 2010 |
| TemposAtracacao | Tempos operacionais | 2010 |
| TemposAtracacaoParalisacao | Paralisações | 2010 |
| Carga | Movimentação de cargas | 2010 |
| Carga_Conteinerizada | Cargas em contêineres | 2010 |
| Carga_Hidrovia | Movimentação em hidrovias | 2010 |
| Carga_Regiao | Cargas por região geográfica | 2010 |
| Carga_Rio | Cargas em rios | 2010 |
| TaxaOcupacao | Taxa de ocupação dos berços | 2020 |
| TaxaOcupacaoComCarga | Taxa de ocupação com carga | 2020 |
| TaxaOcupacaoTOAtracacao | Taxa de ocupação por atracação | 2020 |
| CargaAreas | Cargas por área portuária | 2023 |

## Tabelas Gold

| Tabela | Descrição |
|--------|-----------|
| atracacao_master | Tabela principal: atracação + tempos + carga |
| carga_por_atracacao | Carga agregada por atracação |
| taxa_ocupacao_anual | Taxa de ocupação anual por porto |
| paralisacoes_por_atracacao | Paralisações por atracação |
| carga_hidrovia_anual | Movimentação anual em hidrovias |
| porto_perfil | Perfil operacional por porto |

## Modelo Preditivo

- **Target 1**: `TEstadia` — tempo total de estadia (horas)
- **Target 2**: `TOperacao` — tempo de operação (horas)
- **Algoritmos**: XGBoost + LightGBM + Quantile Regression (P10/P90)
- **Validação**: TimeSeriesSplit (5 folds, sem data leakage temporal)
- **Explicabilidade**: SHAP values por feature

## Pipeline

```bash
python pipeline/01_converter.py   # TXT → Silver
python pipeline/02_agregar.py     # Silver → Gold
python pipeline/03_features.py    # Gold → Features
python pipeline/04_treinar.py     # Treina modelos
python pipeline/05_upload_hub.py  # Sobe tudo aqui
```

## Fonte

Dados públicos da ANTAQ: https://web3.antaq.gov.br/ea/

Atualizado em: {now}
"""

    readme_path = Path(tempfile.gettempdir()) / "antaq_README.md"
    readme_path.write_text(readme, encoding="utf-8")

    if dry_run:
        log.info("  [DRY-RUN] README.md seria enviado")
        return

    try:
        api.upload_file(
            path_or_fileobj=str(readme_path),
            path_in_repo="README.md",
            repo_id=HF_REPO,
            repo_type=HF_REPO_TYPE,
            commit_message="Update README.md",
        )
        log.info("  ✓ README.md enviado")
    except Exception as e:
        log.error("  ERRO ao enviar README: %s", e)


def main():
    parser = argparse.ArgumentParser(
        description="Upload ANTAQ → HuggingFace Hub (batch por camada)"
    )
    parser.add_argument(
        "--camadas",
        nargs="+",
        choices=["silver", "gold", "features", "model", "readme"],
        default=["silver", "gold", "features", "model", "readme"],
        help="Camadas a enviar (padrão: todas)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas lista o que seria enviado, sem fazer upload",
    )
    args = parser.parse_args()

    log.info("=" * 65)
    log.info("ANTAQ → HuggingFace Hub  [modo: upload_folder por camada]")
    log.info("Repo: huggingface.co/datasets/%s", HF_REPO)
    log.info("Camadas: %s", args.camadas)
    log.info("Dry-run: %s", args.dry_run)
    log.info("=" * 65)

    api = _get_api()

    if not args.dry_run:
        _ensure_repo(api)

    stats_total = {"ok": 0, "erro": 0}

    dispatch = {
        "silver":   lambda: upload_silver(api, args.dry_run),
        "gold":     lambda: upload_gold(api, args.dry_run),
        "features": lambda: upload_features(api, args.dry_run),
        "model":    lambda: upload_model(api, args.dry_run),
        "readme":   lambda: upload_readme(api, args.dry_run) or {},
    }

    for camada in args.camadas:
        result = dispatch[camada]() or {}
        stats_total["ok"]   += result.get("ok", 0)
        stats_total["erro"] += result.get("erro", 0)

    log.info("=" * 65)
    log.info("CONCLUÍDO: %d arquivos | %d erros",
             stats_total["ok"], stats_total["erro"])
    log.info("Dataset: https://huggingface.co/datasets/%s", HF_REPO)

    if stats_total["erro"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
