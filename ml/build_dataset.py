"""UrbanFlow — Sprint 3 : build_dataset (pandas = feature engineering, voir §6.5).

Entrée  : ml/data/grid  (Parquet écrit par build_grid.py — 1 point par station et par
          bin de 5 min : station_id, station_code, ts, bikes, docks).
Sortie  : ml/data/dataset.parquet  (table prête au modeling : features <= t + cibles t+h).

Ce script matérialise les règles de learning.md §6.3–6.4 :
  - GRILLE RÉGULIÈRE par station (reindex 5 min) : les bins manquants (station muette)
    apparaissent en trou -> détectés, pas inventés.
  - FEATURES <= t uniquement (forward-fill BORNÉ par une tolérance) : aucune info du futur.
  - CIBLE décalée t+15 / t+30 construite depuis la série OBSERVÉE (jamais forward-fillée) :
    si le bin futur n'a pas été réellement mesuré, la cible est NaN et la ligne est jetée
    (on n'entraîne pas sur une réponse fabriquée).
  - Décalage NÉGATIF = passé (feature), POSITIF = futur (cible). Le signe fait la fuite.

Lancement (hôte, venv actif) :
  python ml/build_dataset.py
"""
from pathlib import Path

import numpy as np
import pandas as pd

# --- Chemins -----------------------------------------------------------------
BASE = Path(__file__).resolve().parent / "data"
GRID_DIR = BASE / "grid"
OUT_PATH = BASE / "dataset.parquet"

# --- Paramètres de la grille & des horizons ----------------------------------
FREQ = "5min"            # pas de la grille
STEP_MIN = 5             # minutes par pas
H1, H2 = 15, 30          # horizons de prédiction (minutes)
S1, S2 = H1 // STEP_MIN, H2 // STEP_MIN   # -> 3 et 6 pas
LAGS = [1, 2, 3]         # décalages passés : t-5, t-10, t-15 min
ROLL = 6                 # fenêtre glissante = 6 pas = 30 min
FILL_LIMIT = 2           # tolérance forward-fill features : <= 2 pas (10 min) de retard


def build_station(sid: int, g: pd.DataFrame) -> pd.DataFrame:
    """Construit features + cibles pour UNE station, sur sa grille régulière."""
    # 1) GRILLE RÉGULIÈRE : on réindexe sur un pas de 5 min continu (min..max observés).
    #    Les bins sans mesure deviennent NaN -> ce sont les TROUS.
    g = g.sort_values("ts").set_index("ts")
    idx = pd.date_range(g.index.min(), g.index.max(), freq=FREQ)
    g = g.reindex(idx)

    # 2) SÉRIE OBSERVÉE vs FORWARD-FILLÉE.
    #    - features : dernier état connu <= t, forward-fill BORNÉ (tolérance FILL_LIMIT).
    #    - cible    : série BRUTE observée (aucun ffill) -> exige une vraie mesure à t+h.
    bikes_raw = g["bikes"]                       # NaN là où non observé (pour la cible)
    bikes_ff = bikes_raw.ffill(limit=FILL_LIMIT)  # état connu <= t (pour les features)
    docks_ff = g["docks"].ffill(limit=FILL_LIMIT)

    out = pd.DataFrame(index=idx)
    out["station_id"] = sid
    out["ts"] = idx

    # 3) FEATURES <= t (état courant) ----------------------------------------
    out["bikes"] = bikes_ff
    out["docks"] = docks_ff
    out["capacity"] = bikes_ff + docks_ff
    out["fill_ratio"] = bikes_ff / out["capacity"].replace(0, np.nan)

    # Lags (strictement passés) : shift POSITIF = on regarde en arrière.
    for L in LAGS:
        out[f"bikes_lag{L * STEP_MIN}"] = bikes_ff.shift(L)

    # Dynamique passée : moyenne/écart-type sur la fenêtre finissant à t (donc <= t).
    out["bikes_roll_mean30"] = bikes_ff.rolling(ROLL, min_periods=ROLL).mean()
    out["bikes_roll_std30"] = bikes_ff.rolling(ROLL, min_periods=ROLL).std()

    # Calendaire (déductible de t, donc licite).
    out["hour"] = idx.hour
    out["dow"] = idx.dayofweek
    out["is_weekend"] = (idx.dayofweek >= 5).astype(int)

    # 4) CIBLES t+h (shift NÉGATIF = futur) depuis la série OBSERVÉE brute.
    #    NaN si le bin futur n'a pas été réellement mesuré -> ligne jetée plus bas.
    out["target_15"] = bikes_raw.shift(-S1)
    out["target_30"] = bikes_raw.shift(-S2)
    return out


FEATURE_COLS = [
    "bikes", "docks", "capacity", "fill_ratio",
    "bikes_lag5", "bikes_lag10", "bikes_lag15",
    "bikes_roll_mean30", "bikes_roll_std30",
    "hour", "dow", "is_weekend",
]


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description="Construit le dataset ML depuis une grille.")
    ap.add_argument("--grid", default=str(GRID_DIR), help="dossier Parquet de la grille")
    ap.add_argument("--out", default=str(OUT_PATH), help="fichier Parquet de sortie")
    a = ap.parse_args()
    out_path = Path(a.out)

    grid = pd.read_parquet(a.grid)       # lit tous les part-*.parquet du dossier
    print(f"[DATASET] grille lue : {len(grid):,} points, "
          f"{grid['station_id'].nunique():,} stations.")

    # Construction station par station puis concaténation.
    frames = [build_station(sid, g) for sid, g in grid.groupby("station_id")]
    data = pd.concat(frames, ignore_index=True)

    # 5) FILTRAGE anti-leakage / anti-trou :
    #    - toutes les features doivent être présentes (assez d'historique <= t) ;
    #    - au moins une cible réellement observée (sinon ligne inutile).
    before = len(data)
    data = data.dropna(subset=FEATURE_COLS)
    data = data[data["target_15"].notna() | data["target_30"].notna()]
    data = data.sort_values(["ts", "station_id"]).reset_index(drop=True)
    print(f"[DATASET] {before:,} lignes brutes -> {len(data):,} lignes valides "
          f"(features complètes + cible observée).")

    if len(data):
        print(f"[DATASET] plage temporelle : {data['ts'].min()} -> {data['ts'].max()}")
        print(f"[DATASET] cible t+15 dispo : {data['target_15'].notna().sum():,} | "
              f"t+30 dispo : {data['target_30'].notna().sum():,}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    data.to_parquet(out_path, index=False)
    print(f"[DATASET] écrit dans {out_path}")


if __name__ == "__main__":
    main()
