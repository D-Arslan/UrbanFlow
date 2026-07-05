"""UrbanFlow — Sprint 3 : BASELINE de persistance + métriques (voir §6.1, §6.2, §6.4).

Baseline de persistance : « dans 15/30 min, ce sera comme MAINTENANT » -> la prédiction
est simplement `bikes` (l'état courant à t). C'est l'ÉTALON obligatoire : tout modèle devra
la battre nettement pour mériter d'exister (§6.1). Split temporel + embargo dans common.py.

Lancement :
  python ml/train_baseline.py                                     # dataset réel
  python ml/train_baseline.py --data ml/data/dataset_synth.parquet  # smoke-test
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))   # pour importer common
from common import HORIZONS, mae_rmse, temporal_split       # noqa: E402

DATA_DEFAULT = Path(__file__).resolve().parent / "data" / "dataset.parquet"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=str(DATA_DEFAULT))
    a = ap.parse_args()

    df = pd.read_parquet(a.data)
    print(f"[BASELINE] dataset : {len(df):,} lignes | "
          f"{df['ts'].min()} -> {df['ts'].max()}\n")

    print(f"{'horizon':<8}{'n_test':>10}{'MAE':>10}{'RMSE':>10}")
    print("-" * 38)
    for label, col in HORIZONS:
        d = df.dropna(subset=[col])                  # lignes où la cible existe vraiment
        if d.empty:
            print(f"{label:<8}{'0':>10}{'-':>10}{'-':>10}")
            continue
        train, test, _ = temporal_split(d)
        if test.empty:
            print(f"{label:<8}{'0':>10}{'-':>10}{'-':>10}  (test vide)")
            continue

        # Persistance : la prédiction EST l'état courant `bikes` (aucun apprentissage).
        mae, rmse = mae_rmse(test[col], test["bikes"])
        print(f"{label:<8}{len(test):>10,}{mae:>10.3f}{rmse:>10.3f}")

    print("\n[BASELINE] Rappel : un modele n'a de valeur que s'il BAT ces chiffres (SS6.1).")


if __name__ == "__main__":
    main()
