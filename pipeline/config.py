"""
pipeline/config.py — Configuração central do pipeline ANTAQ.
Todos os caminhos e constantes em um único lugar.
Override via variáveis de ambiente para rodar em outro diretório.
"""
import os
from pathlib import Path

# ── Raiz do projeto ───────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent

# ── Camadas de dados ──────────────────────────────────────────────────────────
DADOS    = Path(os.environ.get("ANTAQ_DADOS",    str(ROOT / "Dados")))
SILVER   = Path(os.environ.get("ANTAQ_SILVER",   str(ROOT / "parquet" / "silver")))
GOLD     = Path(os.environ.get("ANTAQ_GOLD",     str(ROOT / "parquet" / "gold")))
FEATURES = Path(os.environ.get("ANTAQ_FEATURES", str(ROOT / "parquet" / "features")))
MODEL    = Path(os.environ.get("ANTAQ_MODEL",    str(ROOT / "parquet" / "model")))
LOGS     = Path(os.environ.get("ANTAQ_LOGS",     str(ROOT / "pipeline" / "logs")))

# ── HuggingFace Hub ───────────────────────────────────────────────────────────
HF_REPO      = "vinicius-souza/antaq"
HF_REPO_TYPE = "dataset"

# ── Anos disponíveis ──────────────────────────────────────────────────────────
ANO_MIN = 2010
ANO_MAX = 2026

# ── Tipos de arquivo por era ──────────────────────────────────────────────────
# Era 1: 2010-2019 (8 tipos)
# Era 2: 2020-2022 (+3 tipos de taxa de ocupação)
# Era 3: 2023-2026 (+1 CargaAreas)
TIPOS = {
    "Atracacao":                  lambda a: f"{a}Atracacao.txt",
    "TemposAtracacao":            lambda a: f"{a}TemposAtracacao.txt",
    "TemposAtracacaoParalisacao": lambda a: f"{a}TemposAtracacaoParalisacao.txt",
    "Carga":                      lambda a: f"{a}Carga.txt",
    "Carga_Conteinerizada":       lambda a: f"{a}Carga_Conteinerizada.txt",
    "Carga_Rio":                  lambda a: f"{a}Carga_Rio.txt",
    "Carga_Hidrovia":             lambda a: f"{a}Carga_Hidrovia.txt",
    "Carga_Regiao":               lambda a: f"{a}Carga_Regiao.txt",
    "TaxaOcupacao":               lambda a: f"{a}TaxaOcupacao.txt",          # era 2+
    "TaxaOcupacaoComCarga":       lambda a: f"{a}TaxaOcupacaoComCarga.txt",  # era 2+
    "TaxaOcupacaoTOAtracacao":    lambda a: f"{a}TaxaOcupacaoTOAtracacao.txt", # era 2+
    "CargaAreas":                 lambda a: f"{a}CargaAreas.txt",            # era 3+
}

# Colunas numéricas conhecidas por tipo (vírgula decimal → float64)
# Tipos sem entrada: detecção automática
NUMERICAS = {
    "Atracacao": [],
    "TemposAtracacao": [
        "TEsperaAtracacao", "TEsperaInicioOp", "TOperacao",
        "TEsperaDesatracacao", "TAtracado", "TEstadia",
    ],
    "TemposAtracacaoParalisacao": ["TParalisacao"],
    "Carga":              ["TEU", "QTCarga", "VLPesoCargaBruta"],
    "Carga_Conteinerizada": ["VLPesoCargaConteinerizada"],
    "Carga_Rio":          ["ValorMovimentado"],
    "Carga_Hidrovia":     ["ValorMovimentado"],
    "Carga_Regiao":       ["ValorMovimentado"],
    "TaxaOcupacao":       [],
    "TaxaOcupacaoComCarga": [],
    "TaxaOcupacaoTOAtracacao": [],
    "CargaAreas":         [],
}

# ── Cria diretórios ───────────────────────────────────────────────────────────
for _d in [SILVER, GOLD, FEATURES, MODEL, LOGS]:
    _d.mkdir(parents=True, exist_ok=True)
