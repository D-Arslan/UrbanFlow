"""UrbanFlow — Sprint 3 : XGBoost v2 — cible DELTA + objectif L1 (voir §6.8).

Correctif du paradoxe MAE/RMSE (§6.8) : la v1 (cible absolue, objectif quadratique)
ne battait pas la persistance sur le MAE. Deux changements :

  1) CIBLE DELTA : on prédit le CHANGEMENT Δ = bikes(t+h) − bikes(t), pas la valeur
     absolue. La persistance devient « Δ = 0 » ; le modèle se concentre sur les écarts
     (le vrai signal). Identité clé : bikes(t+h) − (bikes(t)+Δ̂) = Δ_vrai − Δ̂, donc le
     MAE sur le delta = MAE sur l'absolu → comparaison à la MÊME baseline (§6.8).

  2) OBJECTIF L1 : `reg:absoluteerror` optimise le MAE (métrique évaluée), au lieu de
     `reg:squarederror` qui vise le RMSE.

Reconstruction bornée : bikes(t+h) ≈ clip(bikes(t) + Δ̂, 0, capacité).

Sérialisation : ml/models/xgb_delta_<cible>.json.

Lancement :
  python ml/train_xgb_delta.py                                      # dataset réel
  python ml/train_xgb_delta.py --data ml/data/dataset_synth.parquet   # smoke-test
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from xgboost import XGBRegressor

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import FEATURE_COLS, HORIZONS, mae_rmse, temporal_split   # noqa: E402

DATA_DEFAULT = Path(__file__).resolve().parent / "data" / "dataset.parquet"
MODELS_DIR = Path(__file__).resolve().parent / "models"

# Mêmes hyperparamètres que la v1, SEUL l'objectif change (L1 au lieu de L2).
XGB_PARAMS = dict(
    objective="reg:absoluteerror",   # <-- optimise le MAE (cause n°1 du paradoxe, §6.8)
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    n_jobs=-1,
    random_state=0,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=str(DATA_DEFAULT))
    a = ap.parse_args()

    df = pd.read_parquet(a.data)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[XGB-d] dataset : {len(df):,} lignes | {df['ts'].min()} -> {df['ts'].max()}\n")

    header = (f"{'horizon':<8}{'MAE_base':>10}{'MAE_dL1':>10}"
              f"{'RMSE_base':>11}{'RMSE_dL1':>10}{'gain_MAE':>10}")
    print(header)
    print("-" * len(header))

    for label, col in HORIZONS:
        d = df.dropna(subset=[col]).copy()
        # CIBLE DELTA = valeur future − état courant.
        d["delta"] = d[col] - d["bikes"]

        train, test, _ = temporal_split(d)
        if train.empty or test.empty:
            print(f"{label:<8}  (train/test vide — données insuffisantes)")
            continue

        # Entraînement sur le PASSÉ, features <= t -> prédit le DELTA.
        model = XGBRegressor(**XGB_PARAMS)
        model.fit(train[FEATURE_COLS], train["delta"])

        # Reconstruction ABSOLUE bornée à [0, capacité], puis évaluation sur l'absolu.
        delta_hat = model.predict(test[FEATURE_COLS])
        pred_abs = np.clip(test["bikes"] + delta_hat, 0, test["capacity"])

        mae_x, rmse_x = mae_rmse(test[col], pred_abs)
        mae_b, rmse_b = mae_rmse(test[col], test["bikes"])     # persistance = Δ=0
        gain = 100 * (mae_b - mae_x) / mae_b

        print(f"{label:<8}{mae_b:>10.3f}{mae_x:>10.3f}"
              f"{rmse_b:>11.3f}{rmse_x:>10.3f}{gain:>9.1f}%")

        # DIAGNOSTIC : où le modèle apporte-t-il de la valeur ? Sur le sous-ensemble
        # des cas qui ont RÉELLEMENT bougé (|Δ_vrai| >= 1), là où la persistance échoue.
        moved = (test[col] - test["bikes"]).abs() >= 1
        if moved.any():
            mae_bm, _ = mae_rmse(test[col][moved], test["bikes"][moved])
            mae_xm, _ = mae_rmse(test[col][moved], pred_abs[moved])
            gain_m = 100 * (mae_bm - mae_xm) / mae_bm
            pct = 100 * moved.mean()
            print(f"  └─ cas 'bougé' (|Δ|>=1, {pct:4.1f}% du test) : "
                  f"MAE_base={mae_bm:.3f}  MAE_dL1={mae_xm:.3f}  gain={gain_m:+.1f}%")

        out = MODELS_DIR / f"xgb_delta_{col}.json"
        model.save_model(out)
        print(f"         -> modèle sauvegardé : {out}")

    print("\n[XGB-d] gain_MAE > 0 = on bat ENFIN la persistance sur le MAE (§6.8).")


if __name__ == "__main__":
    main()
