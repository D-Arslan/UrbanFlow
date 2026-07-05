"""UrbanFlow — Sprint 3 : INFÉRENCE (recharge le modèle sérialisé et prédit).

Preuve que la sérialisation est exploitable : on recharge les modèles XGBoost sauvegardés
(train_xgb.py) et on prédit la disponibilité future à partir de l'ÉTAT LE PLUS RÉCENT connu
de chaque station (dernière ligne de features par station).

Note : ici on illustre sur le dataset (lignes historiques -> on peut afficher la vérité à
côté). En production, on construirait les mêmes features <= t depuis la grille temps réel
(sans exiger de cible), puis on appellerait model.predict().

Lancement :
  python ml/predict.py                                       # dataset réel
  python ml/predict.py --data ml/data/dataset_synth.parquet    # smoke-test
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
from xgboost import XGBRegressor

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import FEATURE_COLS, HORIZONS   # noqa: E402

DATA_DEFAULT = Path(__file__).resolve().parent / "data" / "dataset.parquet"
MODELS_DIR = Path(__file__).resolve().parent / "models"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=str(DATA_DEFAULT))
    ap.add_argument("--n", type=int, default=8, help="nb de stations à afficher")
    a = ap.parse_args()

    df = pd.read_parquet(a.data)
    # État le plus récent connu par station (dernière ligne de features de chaque station).
    latest = df.sort_values("ts").groupby("station_id").tail(1).copy()

    out = latest[["station_id", "ts", "bikes"]].copy()
    out = out.rename(columns={"bikes": "bikes_now"})

    for label, col in HORIZONS:
        model_path = MODELS_DIR / f"xgb_{col}.json"
        if not model_path.exists():
            print(f"[PREDICT] modèle manquant : {model_path} (lance train_xgb.py d'abord).")
            return
        model = XGBRegressor()
        model.load_model(model_path)                      # rechargement du modèle sérialisé
        out[f"pred_{label}"] = model.predict(latest[FEATURE_COLS]).round(1)

    print(f"[PREDICT] prédictions depuis l'état le plus récent ({latest['ts'].max()}) :\n")
    print(out.head(a.n).to_string(index=False))


if __name__ == "__main__":
    main()
