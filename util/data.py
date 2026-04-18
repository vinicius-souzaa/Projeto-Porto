"""
util/data.py — Data access layer
=================================
Downloads Gold/Model artifacts from HuggingFace Hub (vinicius-souza/antaq)
with local file-system caching. All parquet reads go through DuckDB so
there is no pyarrow/cmake dependency on the host.
"""

import json
import logging
import os
from pathlib import Path

import duckdb
import pandas as pd

HF_REPO      = "vinicius-souza/antaq"
HF_REPO_TYPE = "dataset"

# Local cache: ~/.cache/antaq  (or ANTAQ_CACHE env var)
_CACHE = Path(os.environ.get("ANTAQ_CACHE", Path.home() / ".cache" / "antaq"))

log = logging.getLogger(__name__)


# ── HF download helper ────────────────────────────────────────────────────────

def _hf_path(remote: str) -> Path:
    """Return local cached path for a remote HF file, downloading if needed."""
    local = _CACHE / remote
    if local.exists():
        return local

    local.parent.mkdir(parents=True, exist_ok=True)

    try:
        from huggingface_hub import hf_hub_download
        tmp = hf_hub_download(
            repo_id=HF_REPO,
            repo_type=HF_REPO_TYPE,
            filename=remote,
            local_dir=str(_CACHE),
        )
        return Path(tmp)
    except Exception as e:
        log.error("Falha ao baixar %s do HF Hub: %s", remote, e)
        raise FileNotFoundError(
            f"Arquivo '{remote}' não encontrado localmente nem no HF Hub.\n"
            "Execute o pipeline (01→05) e faça upload com 05_upload_hub.py."
        ) from e


def _pq(remote: str, limit: int | None = None) -> pd.DataFrame:
    """Read a remote parquet file via DuckDB (no pyarrow needed)."""
    path = str(_hf_path(remote))
    sql = f"SELECT * FROM read_parquet('{path}')"
    if limit:
        sql += f" LIMIT {limit}"
    return duckdb.execute(sql).df()


def _json(remote: str) -> dict:
    path = _hf_path(remote)
    return json.loads(path.read_text(encoding="utf-8"))


# ── Gold tables ───────────────────────────────────────────────────────────────

def load_atracacao_master(cols: list[str] | None = None) -> pd.DataFrame:
    df = _pq("gold/atracacao_master.parquet")
    return df[cols] if cols else df


def load_carga_por_atracacao() -> pd.DataFrame:
    return _pq("gold/carga_por_atracacao.parquet")


def load_taxa_ocupacao_anual() -> pd.DataFrame:
    return _pq("gold/taxa_ocupacao_anual.parquet")


def load_paralisacoes_por_atracacao() -> pd.DataFrame:
    return _pq("gold/paralisacoes_por_atracacao.parquet")


def load_carga_hidrovia_anual() -> pd.DataFrame:
    return _pq("gold/carga_hidrovia_anual.parquet")


def load_porto_perfil() -> pd.DataFrame:
    return _pq("gold/porto_perfil.parquet")


def load_catalog() -> dict:
    return _json("gold/catalog.json")


# ── Model artifacts ───────────────────────────────────────────────────────────

def load_model():
    import joblib
    return joblib.load(str(_hf_path("model/model.pkl")))


def load_model_operacao():
    import joblib
    return joblib.load(str(_hf_path("model/model_operacao.pkl")))


def load_model_quantile(quantile: str = "q10"):
    """quantile: 'q10' or 'q90'"""
    import joblib
    return joblib.load(str(_hf_path(f"model/model_{quantile}.pkl")))


def load_model_meta() -> dict:
    return _json("model/model_card.json")


def load_shap_importance() -> pd.DataFrame:
    return _pq("model/shap_importance.parquet")


def load_shap_values() -> pd.DataFrame:
    return _pq("model/shap_values.parquet")


def load_encoders() -> dict:
    import joblib
    return joblib.load(str(_hf_path("model/encoders.pkl")))


def load_encoders_map() -> dict:
    return _json("features/encoders_map.json")
