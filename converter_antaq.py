# converter_antaq.py
import logging
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

_HERE = Path(__file__).resolve().parent
PASTA_RAW    = Path(os.environ.get("ANTAQ_RAW",    str(_HERE / "raw")))
PASTA_OUTPUT = Path(os.environ.get("ANTAQ_PARQUET", str(_HERE / "parquet")))

PASTA_OUTPUT.mkdir(exist_ok=True)

TIPOS = {
    'Atracacao':            lambda ano: f"{ano}Atracacao.txt",
    'TemposAtracacao':      lambda ano: f"{ano}TemposAtracacao.txt",
    'Carga':                lambda ano: f"{ano}Carga.txt",
    'Carga_Conteinerizada': lambda ano: f"{ano}Carga_Conteinerizada.txt",
    'Carga_Rio':            lambda ano: f"{ano}Carga_Rio.txt",
    'Carga_Hidrovia':       lambda ano: f"{ano}Carga_Hidrovia.txt",
    'Carga_Regiao':         lambda ano: f"{ano}Carga_Regiao.txt",
}

# Todas as colunas numéricas forçadas como float64 — evita conflito int/float entre anos
NUMERICAS = {
    'Atracacao':            [],
    'TemposAtracacao':      ['TEsperaAtracacao','TEsperaInicioOp','TOperacao',
                             'TEsperaDesatracacao','TAtracado','TEstadia'],
    'Carga':                ['TEU','QTCarga','VLPesoCargaBruta'],
    'Carga_Conteinerizada': ['VLPesoCargaConteinerizada'],
    'Carga_Rio':            ['ValorMovimentado'],
    'Carga_Hidrovia':       ['ValorMovimentado'],
    'Carga_Regiao':         ['ValorMovimentado'],
}

def fix_cols(df):
    df.columns = (df.columns
        .str.replace('ï»¿', '', regex=False)
        .str.replace('\ufeff', '', regex=False)
        .str.encode('latin-1').str.decode('utf-8', errors='replace'))
    return df

def fix_numericas(df, colunas):
    """Converte para float64 — nunca int — evita truncamento entre anos."""
    for col in colunas:
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(
            df[col].astype(str).str.strip().str.replace(',', '.', regex=False),
            errors='coerce'
        ).astype('float64')  # força float64 explicitamente
    return df

def df_para_arrow(df, schema_ref=None):
    """
    Converte DataFrame para PyArrow Table.
    - Colunas object → string
    - Se schema_ref fornecido, alinha e faz cast para garantir tipos iguais
    """
    # Força object → string pura
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).replace({'nan': None, 'None': None, '<NA>': None})

    table = pa.Table.from_pandas(df, preserve_index=False)

    if schema_ref is not None and table.schema != schema_ref:
        # Alinha colunas e faz cast de tipos
        table = table.cast(schema_ref)

    return table

# ── DIAGNÓSTICO ───────────────────────────────────────────────
log.info("=" * 60)
log.info("DIAGNÓSTICO")
log.info("=" * 60)

anos = sorted(set(
    f.name[:4]
    for f in PASTA_RAW.glob("*.txt")
    if f.name[:4].isdigit()
))

if not anos:
    log.error("Nenhum arquivo encontrado em: %s", PASTA_RAW)
    exit(1)

log.info("Anos detectados: %s–%s (%d anos)", anos[0], anos[-1], len(anos))
log.info("Arquivos por tipo:")
for tipo, nome_fn in TIPOS.items():
    n = sum(1 for ano in anos if (PASTA_RAW / nome_fn(ano)).exists())
    log.info("  %-30s %3d anos", tipo, n)

# ── CONVERSÃO ─────────────────────────────────────────────────
log.info("=" * 60)
log.info("CONVERTENDO PARA PARQUET")
log.info("=" * 60)

ja_prontos = {f.stem for f in PASTA_OUTPUT.glob("*.parquet")}
if ja_prontos:
    log.info("Já convertidos (pulando): %s", sorted(ja_prontos))

total_txt_mb = 0
total_pq_mb  = 0

for tipo, nome_fn in TIPOS.items():
    out_path = PASTA_OUTPUT / f"{tipo}.parquet"

    if tipo in ja_prontos:
        total_pq_mb  += out_path.stat().st_size / 1e6
        for ano in anos:
            arq = PASTA_RAW / nome_fn(ano)
            if arq.exists():
                total_txt_mb += arq.stat().st_size / 1e6
        continue

    log.info("Processando: %s", tipo)

    writer  = None
    schema  = None
    n_total = 0
    erros   = []

    # Passo 1: determina o schema lendo TODOS os anos com float64 forçado
    log.info("  Determinando schema...")
    schema_dfs = []
    for ano in anos:
        arquivo = PASTA_RAW / nome_fn(ano)
        if not arquivo.exists():
            continue
        try:
            df_s = pd.read_csv(
                arquivo, sep=';', encoding='latin-1',
                low_memory=False, on_bad_lines='skip',
                dtype=str, nrows=100
            )
            df_s = fix_cols(df_s)
            df_s = fix_numericas(df_s, NUMERICAS[tipo])
            for col in df_s.select_dtypes(include='object').columns:
                df_s[col] = df_s[col].astype(str)
            schema_dfs.append(df_s)
        except Exception as e:
            log.warning("  Schema ano %s ignorado: %s", ano, e)

    if not schema_dfs:
        log.warning("  Nenhum dado para determinar schema — pulando %s", tipo)
        continue

    # Schema unificado: usa o supertipo (float supera int, string supera tudo)
    df_schema = pd.concat(schema_dfs, ignore_index=True)
    for col in df_schema.select_dtypes(include='object').columns:
        df_schema[col] = df_schema[col].astype(str)
    schema = pa.Table.from_pandas(df_schema, preserve_index=False).schema
    log.info("  Schema definido (%d colunas)", len(schema))

    # Passo 2: escreve ano por ano usando o schema fixo — writer fechado no finally
    try:
        for ano in anos:
            arquivo = PASTA_RAW / nome_fn(ano)
            if not arquivo.exists():
                continue

            txt_mb = arquivo.stat().st_size / 1e6
            total_txt_mb += txt_mb

            try:
                df = pd.read_csv(
                    arquivo, sep=';', encoding='latin-1',
                    low_memory=False, on_bad_lines='skip',
                    dtype=str
                )
                df = fix_cols(df)
                df = fix_numericas(df, NUMERICAS[tipo])

                table = df_para_arrow(df, schema_ref=schema)

                if writer is None:
                    writer = pq.ParquetWriter(out_path, schema=schema, compression='zstd')

                writer.write_table(table)
                n_total += len(df)
                log.info("  %s: %9d linhas | %6.1f MB", ano, len(df), txt_mb)
                del df, table

            except Exception as e:
                erros.append(f"{ano}: {e}")
                log.error("  %s: erro — %s", ano, e)
    finally:
        if writer:
            writer.close()

    if n_total > 0:
        pq_mb = out_path.stat().st_size / 1e6
        total_pq_mb += pq_mb
        log.info("  TOTAL: %9d linhas → %.1f MB", n_total, pq_mb)
        if erros:
            log.warning("  %d ano(s) com erro: %s", len(erros), erros)
    else:
        log.warning("  Nenhum dado carregado para %s", tipo)

# ── RESUMO ────────────────────────────────────────────────────
log.info("=" * 60)
log.info("RESUMO FINAL")
log.info("=" * 60)
log.info("TXT lido:        %.1f GB", total_txt_mb / 1e3)
log.info("Parquet gerado:  %.0f MB", total_pq_mb)
if total_txt_mb > 0:
    log.info("Reducao:         %.0f%%", (1 - total_pq_mb / total_txt_mb) * 100)
log.info("Pasta:           %s", PASTA_OUTPUT)
log.info("Arquivos gerados:")
for f in sorted(PASTA_OUTPUT.glob("*.parquet")):
    mb = f.stat().st_size / 1e6
    log.info("  %-35s %6.1f MB", f.name, mb)
log.info("Conversao concluida.")
