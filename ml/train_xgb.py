"""UrbanFlow — Sprint 3 : modèle XGBoost + comparaison à la baseline (voir §6.1–6.4).

Premier VRAI modèle : arbres de décision boostés (gradient boosting). On entraîne un
modèle PAR HORIZON sur les features <= t, on évalue sur le MÊME split temporel que la
baseline (common.py), et on affiche le gain vs persistance. Un modèle qui ne bat pas
nettement la baseline ne mérite pas la prod (§6.1).

Sérialisation : ml/models/xgb_<cible>.json (format natif XGBoost, rechargé par predict.py).

Lancement :
  python ml/train_xgb.py                                       # dataset réel
  python ml/train_xgb.py --data ml/data/dataset_synth.parquet    # smoke-test
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
from xgboost import XGBRegressor

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import FEATURE_COLS, HORIZONS, mae_rmse, temporal_split   # noqa: E402

DATA_DEFAULT = Path(__file__).resolve().parent / "data" / "dataset.parquet"
MODELS_DIR = Path(__file__).resolve().parent / "models"

# Hyperparamètres raisonnables (non optimisés — tuning = étape ultérieure).
XGB_PARAMS = dict(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,          # échantillonnage de lignes -> régularisation
    colsample_bytree=0.8,   # échantillonnage de colonnes -> régularisation
    n_jobs=-1,
    random_state=0,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=str(DATA_DEFAULT))
    a = ap.parse_args()

    df = pd.read_parquet(a.data)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[XGB] dataset : {len(df):,} lignes | {df['ts'].min()} -> {df['ts'].max()}\n")

    header = f"{'horizon':<8}{'MAE_base':>10}{'MAE_xgb':>10}{'RMSE_base':>11}{'RMSE_xgb':>10}{'gain_MAE':>10}"
    print(header)
    print("-" * len(header))

    for label, col in HORIZONS:
        d = df.dropna(subset=[col])
        train, test, _ = temporal_split(d)
        if train.empty or test.empty:
            print(f"{label:<8}  (train/test vide — données insuffisantes)")
            continue

        # Entraînement sur le PASSÉ uniquement (train), features <= t.
        model = XGBRegressor(**XGB_PARAMS)
        model.fit(train[FEATURE_COLS], train[col])

        # Évaluation sur le FUTUR (test), comparée à la persistance sur le même test.
        pred = model.predict(test[FEATURE_COLS])
        mae_x, rmse_x = mae_rmse(test[col], pred)
        mae_b, rmse_b = mae_rmse(test[col], test["bikes"])   # baseline persistance
        gain = 100 * (mae_b - mae_x) / mae_b                 # % d'amélioration du MAE

        print(f"{label:<8}{mae_b:>10.3f}{mae_x:>10.3f}{rmse_b:>11.3f}{rmse_x:>10.3f}{gain:>9.1f}%")

        # Sérialisation du modèle (format natif XGBoost).
        out = MODELS_DIR / f"xgb_{col}.json"
        model.save_model(out)
        print(f"         -> modèle sauvegardé : {out}")

    print("\n[XGB] gain_MAE > 0 = XGBoost bat la persistance. Négatif/nul = inutile (§6.1).")


if __name__ == "__main__":
    main()
