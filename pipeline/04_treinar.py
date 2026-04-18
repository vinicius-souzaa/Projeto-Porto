"""
pipeline/04_treinar.py — Treinamento ML
=========================================
Treina modelos para prever TEstadia e TOperacao (em horas).

Algoritmos:
  - XGBoost (principal)
  - LightGBM (comparação)
  - Quantile Regression (P10/P50/P90) com XGBoost — intervalos de confiança

Técnicas:
  - TimeSeriesSplit (sem data leakage temporal)
  - SHAP values para explicabilidade
  - Model card automático com métricas por subgrupo (porto, navegação, ano)
  - Salva modelo vencedor + comparação + shap + metadata

Saída em model/:
  model_xgb.pkl          — modelo XGBoost (target: TEstadia)
  model_lgbm.pkl         — modelo LightGBM (target: TEstadia)
  model_operacao.pkl     — XGBoost (target: TOperacao)
  model_q10.pkl / model_q90.pkl — quantile regression P10 e P90
  model_meta.pkl         — metadata, métricas, comparação
  encoders.pkl           — encoders das categóricas
  shap_importance.parquet
  shap_values.parquet
  model_card.json        — card completo com métricas por subgrupo

Uso:
  python pipeline/04_treinar.py
  python pipeline/04_treinar.py --target TOperacao
  python pipeline/04_treinar.py --skip-lgbm
"""

import argparse
import json
import logging
import pickle
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import shap
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import FEATURES, MODEL, LOGS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuração dos modelos ──────────────────────────────────────────────────

XGB_PARAMS = dict(
    n_estimators=600,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_weight=5,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42,
    n_jobs=-1,
    tree_method="hist",
)

LGBM_PARAMS = dict(
    n_estimators=600,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_samples=20,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42,
    n_jobs=-1,
    verbose=-1,
)

# Features que entram no modelo (excluindo IDs e targets)
FEATURE_EXCLUDE = {
    "IDAtracacao", "Porto Atracação", "Região Geográfica", "UF",
    "TEstadia", "TOperacao",
}


def _load_features() -> pd.DataFrame:
    path = FEATURES / "features.parquet"
    if not path.exists():
        raise FileNotFoundError("features.parquet não encontrado. Execute 03_features.py primeiro.")
    df = pd.read_parquet(path, engine="pyarrow")
    log.info("Features carregadas: %d linhas | %d colunas", len(df), len(df.columns))
    return df


def _prepare(df: pd.DataFrame, target: str) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    """Prepara X e y removendo nulos no target e selecionando features."""
    df = df.dropna(subset=[target]).copy()
    # Aplica transformação log1p ao target (normaliza distribuição skewed)
    y = np.log1p(df[target].clip(lower=0))

    feat_cols = [c for c in df.columns if c not in FEATURE_EXCLUDE and c != target]
    # Remove colunas com > 80% nulos
    feat_cols = [c for c in feat_cols if df[c].isna().mean() < 0.8]

    X = df[feat_cols].copy()
    # Preenche nulos com mediana
    for col in X.select_dtypes(include="number").columns:
        X[col] = X[col].fillna(X[col].median())

    log.info("  Target: %s | %d amostras | %d features", target, len(y), len(feat_cols))
    return X, y, feat_cols


def _metrics(y_true: np.ndarray, y_pred: np.ndarray, label: str = "") -> dict:
    """Calcula métricas na escala original (desfaz log1p)."""
    y_true_orig = np.expm1(y_true)
    y_pred_orig = np.expm1(y_pred)
    mae  = mean_absolute_error(y_true_orig, y_pred_orig)
    rmse = np.sqrt(mean_squared_error(y_true_orig, y_pred_orig))
    r2   = r2_score(y_true_orig, y_pred_orig)
    mape = np.mean(np.abs((y_true_orig - y_pred_orig) / (y_true_orig + 1e-6))) * 100

    m = {"MAE": round(mae, 2), "RMSE": round(rmse, 2), "R2": round(r2, 4), "MAPE_%": round(mape, 2)}
    if label:
        log.info("  %s → MAE=%.1fh | RMSE=%.1fh | R²=%.3f | MAPE=%.1f%%",
                 label, mae, rmse, r2, mape)
    return m


def _cv_temporal(model, X: pd.DataFrame, y: pd.Series,
                 df_orig: pd.DataFrame) -> dict:
    """
    Cross-validation temporal com TimeSeriesSplit.
    Ordena por Ano para garantir que o futuro nunca vaza para o treino.
    """
    if "Ano" in df_orig.columns:
        order = df_orig.loc[X.index, "Ano"].argsort().values
        X = X.iloc[order]
        y = y.iloc[order]

    tscv = TimeSeriesSplit(n_splits=5)
    scores = []
    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]
        model.fit(X_tr, y_tr, eval_set=[(X_val, y_val)], verbose=False)
        pred = model.predict(X_val)
        m = _metrics(y_val.values, pred)
        scores.append(m)
        log.info("    Fold %d: MAE=%.1fh R²=%.3f", fold + 1, m["MAE"], m["R2"])

    cv_result = {k: round(np.mean([s[k] for s in scores]), 4) for k in scores[0]}
    log.info("  CV médio: MAE=%.1fh | R²=%.3f", cv_result["MAE"], cv_result["R2"])
    return cv_result


def _metrics_por_subgrupo(model, X: pd.DataFrame, y_true: pd.Series,
                           df_orig: pd.DataFrame, col: str) -> dict:
    """Métricas segmentadas por subgrupo (porto, navegação, etc.)."""
    if col not in df_orig.columns:
        return {}
    pred = model.predict(X)
    subgrupos = df_orig.loc[X.index, col].fillna("Desconhecido")
    resultado = {}
    for grupo in subgrupos.unique():
        mask = subgrupos == grupo
        if mask.sum() < 30:
            continue
        m = _metrics(y_true[mask].values, pred[mask])
        resultado[str(grupo)] = m
    return resultado


def _treinar_quantile(X_train: pd.DataFrame, y_train: pd.Series,
                       X_test: pd.DataFrame, y_test: pd.Series,
                       quantile: float) -> XGBRegressor:
    """Treina modelo de regressão quantílica para intervalos de confiança."""
    model = XGBRegressor(
        **{**XGB_PARAMS, "objective": "reg:quantileerror",
           "quantile_alpha": quantile, "n_estimators": 400},
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    return model


def _calc_shap(model, X: pd.DataFrame, feat_cols: list[str],
               n_sample: int = 5000) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calcula SHAP values e retorna importance + sample de valores."""
    log.info("  Calculando SHAP values (amostra de %d)...", n_sample)
    X_sample = X.sample(min(n_sample, len(X)), random_state=42)
    explainer = shap.TreeExplainer(model)
    shap_vals  = explainer.shap_values(X_sample)

    importance = pd.DataFrame({
        "feature":    feat_cols,
        "shap_mean":  np.abs(shap_vals).mean(axis=0),
        "shap_std":   np.abs(shap_vals).std(axis=0),
    }).sort_values("shap_mean", ascending=False)

    shap_df = pd.DataFrame(shap_vals, columns=feat_cols)
    return importance, shap_df


def treinar_target(df: pd.DataFrame, target: str,
                   skip_lgbm: bool = False, skip_cv: bool = False) -> dict:
    """Treina XGBoost (+ LightGBM opcional) para um target."""
    log.info("=" * 60)
    log.info("TARGET: %s", target)
    log.info("=" * 60)

    X, y, feat_cols = _prepare(df, target)

    # Split temporal: 80% treino, 20% teste
    split = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split], X.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]
    log.info("  Treino: %d | Teste: %d", len(X_train), len(X_test))

    result = {"target": target, "n_amostras": len(X), "feat_cols": feat_cols}

    # ── XGBoost ──────────────────────────────────────────────────────────────
    log.info("Treinando XGBoost...")
    xgb = XGBRegressor(**XGB_PARAMS)
    xgb.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    pred_xgb = xgb.predict(X_test)
    result["xgb_test"] = _metrics(y_test.values, pred_xgb, "XGB test")
    if not skip_cv:
        result["xgb_cv"] = _cv_temporal(
            XGBRegressor(**XGB_PARAMS), X, y, df
        )
    else:
        log.info("  CV pulado (--skip-cv)")
        result["xgb_cv"] = {}

    # ── LightGBM ─────────────────────────────────────────────────────────────
    if not skip_lgbm:
        try:
            from lightgbm import LGBMRegressor
            log.info("Treinando LightGBM...")
            lgbm = LGBMRegressor(**LGBM_PARAMS)
            lgbm.fit(X_train, y_train,
                     eval_set=[(X_test, y_test)], callbacks=[])
            pred_lgbm = lgbm.predict(X_test)
            result["lgbm_test"] = _metrics(y_test.values, pred_lgbm, "LGBM test")
            result["lgbm_model"] = lgbm
        except ImportError:
            log.warning("  LightGBM não instalado — pulando comparação")

    # ── Quantile regression ───────────────────────────────────────────────────
    log.info("Treinando modelos quantílicos (P10/P90)...")
    q10 = _treinar_quantile(X_train, y_train, X_test, y_test, 0.10)
    q90 = _treinar_quantile(X_train, y_train, X_test, y_test, 0.90)
    result["q10_model"] = q10
    result["q90_model"] = q90

    # ── Métricas por subgrupo ─────────────────────────────────────────────────
    log.info("Calculando métricas por subgrupo...")
    result["metricas_porto"] = _metrics_por_subgrupo(
        xgb, X_test, y_test, df, "Porto Atracação"
    )
    result["metricas_navegacao"] = _metrics_por_subgrupo(
        xgb, X_test, y_test, df, "Tipo de Navegação da Atracação"
    )
    result["metricas_ano"] = _metrics_por_subgrupo(xgb, X_test, y_test, df, "Ano")

    result["xgb_model"] = xgb
    result["feat_cols"]  = feat_cols
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["TEstadia", "TOperacao", "ambos"],
                        default="ambos")
    parser.add_argument("--skip-lgbm", action="store_true",
                        help="Pula treinamento LightGBM")
    parser.add_argument("--skip-cv", action="store_true",
                        help="Pula cross-validation (mais rápido)")
    args = parser.parse_args()

    inicio = datetime.now(timezone.utc)
    df = _load_features()

    targets = (["TEstadia", "TOperacao"] if args.target == "ambos"
               else [args.target])

    resultados = {}
    for t in targets:
        resultados[t] = treinar_target(df, t, skip_lgbm=args.skip_lgbm,
                                       skip_cv=args.skip_cv)

    # ── Salva modelos ─────────────────────────────────────────────────────────
    log.info("Salvando modelos...")

    # Modelo principal (TEstadia XGBoost) — usado no dashboard
    r_est = resultados.get("TEstadia", {})
    if "xgb_model" in r_est:
        with open(MODEL / "model.pkl", "wb") as f:
            pickle.dump(r_est["xgb_model"], f)
        with open(MODEL / "model_q10.pkl", "wb") as f:
            pickle.dump(r_est["q10_model"], f)
        with open(MODEL / "model_q90.pkl", "wb") as f:
            pickle.dump(r_est["q90_model"], f)

        # SHAP
        shap_imp, shap_df = _calc_shap(
            r_est["xgb_model"],
            df[r_est["feat_cols"]].fillna(0),
            r_est["feat_cols"],
        )
        shap_imp.to_parquet(MODEL / "shap_importance.parquet", index=False,
                            compression="zstd", engine="pyarrow")
        shap_df.to_parquet(MODEL / "shap_values.parquet", index=False,
                           compression="zstd", engine="pyarrow")
        log.info("  SHAP salvo: %d features", len(shap_imp))

    if "TOperacao" in resultados and "xgb_model" in resultados["TOperacao"]:
        with open(MODEL / "model_operacao.pkl", "wb") as f:
            pickle.dump(resultados["TOperacao"]["xgb_model"], f)

    if "lgbm_model" in r_est:
        with open(MODEL / "model_lgbm.pkl", "wb") as f:
            pickle.dump(r_est["lgbm_model"], f)

    # ── Model card ────────────────────────────────────────────────────────────
    fim = datetime.now(timezone.utc)
    card = {
        "treinado_em":    inicio.isoformat(),
        "duracao_s":      round((fim - inicio).total_seconds(), 1),
        "dataset_linhas": len(df),
        "algoritmos":     ["XGBoost", "LightGBM"] if not args.skip_lgbm else ["XGBoost"],
        "targets":        targets,
        "metricas": {
            t: {
                "xgb_test": resultados[t].get("xgb_test"),
                "xgb_cv":   resultados[t].get("xgb_cv"),
                "lgbm_test":resultados[t].get("lgbm_test"),
            }
            for t in targets
        },
        "metricas_por_subgrupo": {
            t: {
                "porto":     resultados[t].get("metricas_porto", {}),
                "navegacao": resultados[t].get("metricas_navegacao", {}),
                "ano":       resultados[t].get("metricas_ano", {}),
            }
            for t in targets
        },
        "n_features": len(r_est.get("feat_cols", [])),
        "features":   r_est.get("feat_cols", []),
    }

    card_path = MODEL / "model_card.json"
    card_path.write_text(json.dumps(card, indent=2, ensure_ascii=False), encoding="utf-8")

    # model_meta.pkl para o dashboard
    import joblib
    meta = {
        "training_date": inicio.isoformat(),
        "n_amostras":    len(df),
        "features":      r_est.get("feat_cols", []),
        "metricas":      card["metricas"],
        "targets":       targets,
    }
    joblib.dump(meta, MODEL / "model_meta.pkl")

    log.info("=" * 60)
    log.info("CONCLUÍDO em %.0fs", card["duracao_s"])
    for t in targets:
        m = card["metricas"][t]["xgb_test"] or {}
        log.info("  %s → MAE=%.1fh R²=%.3f", t, m.get("MAE", 0), m.get("R2", 0))
    log.info("Model card: %s", card_path)


if __name__ == "__main__":
    main()
