"""UrbanFlow — Sprint 3 : générateur de GRILLE SYNTHÉTIQUE (outil de test uniquement).

But : produire une grille au MÊME schéma que build_grid.py, mais avec plusieurs jours de
données plausibles, pour VALIDER l'aval (build_dataset -> baseline -> modèles) sans attendre
la collecte réelle. Données FACTICES : les métriques obtenues n'ont aucune valeur métier.

Modèle simple mais réaliste : chaque station a une capacité fixe et un cycle journalier
(pendulaire) + du bruit -> forte autocorrélation à 15 min (la persistance sera un bon
étalon), mais un signal horaire/jour que XGBoost peut exploiter. On perce ~5 % de trous.

Sortie : ml/data/grid_synth/part-0.parquet
"""
from pathlib import Path

import numpy as np
import pandas as pd

OUT_DIR = Path(__file__).resolve().parent / "data" / "grid_synth"

N_STATIONS = 15
DAYS = 4
FREQ = "5min"
STEPS_PER_DAY = 24 * 12                 # 288 bins de 5 min par jour
GAP_RATE = 0.05                         # ~5 % de bins manquants (trous)


def main() -> None:
    rng = np.random.default_rng(42)     # graine fixe -> reproductible
    start = pd.Timestamp("2026-06-20 00:00:00")
    periods = DAYS * STEPS_PER_DAY
    idx = pd.date_range(start, periods=periods, freq=FREQ)
    t = np.arange(periods)

    frames = []
    for s in range(N_STATIONS):
        cap = int(rng.integers(20, 40))                 # capacité de la station
        phase = rng.uniform(0, 2 * np.pi)               # décalage du cycle journalier
        daily = np.sin(2 * np.pi * t / STEPS_PER_DAY + phase)   # pendulaire sur 24 h
        bikes = cap / 2 + 0.4 * cap * daily + rng.normal(0, 2, periods)  # signal + bruit
        bikes = np.clip(np.round(bikes), 0, cap).astype(int)
        docks = (cap - bikes).astype(int)

        keep = rng.random(periods) > GAP_RATE           # perce des trous aléatoires
        df = pd.DataFrame({
            "station_id": 1000 + s,
            "station_code": str(1000 + s),
            "bin_epoch": (idx.astype("int64") // 10**9),
            "ts": idx,
            "bikes": bikes,
            "docks": docks,
        })[keep]
        frames.append(df)

    grid = pd.concat(frames, ignore_index=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    grid.to_parquet(OUT_DIR / "part-0.parquet", index=False)
    print(f"[SYNTH] grille synthétique : {len(grid):,} points, {N_STATIONS} stations, "
          f"{DAYS} jours -> {OUT_DIR}")


if __name__ == "__main__":
    main()
