"""Tests de la couche ML — les règles anti-leakage, SANS données réelles.

Ce sont les garanties qui portent l'argument scientifique du projet (learning.md §6.3–6.4) :
  - split CHRONOLOGIQUE avec embargo (jamais de futur dans le train) ;
  - cible construite depuis la série OBSERVÉE (jamais forward-fillée) ;
  - forward-fill des features BORNÉ (un trou long ne devient pas une valeur inventée) ;
  - lags strictement PASSÉS.
Chaque test construit un petit DataFrame synthétique : rapide, exécutable en CI (pandas +
scikit-learn seulement, pas de xgboost/torch).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ml"))   # scripts ml/ = modules
import build_dataset  # noqa: E402
import common  # noqa: E402


def _series(n: int, start: str = "2026-07-01 08:00") -> pd.DataFrame:
    """n points de grille 5 min pour une station, bikes = 0..n-1 (valeur = index du pas)."""
    ts = pd.date_range(start, periods=n, freq="5min")
    return pd.DataFrame({"ts": ts, "bikes": np.arange(n, dtype=float), "docks": 10.0})


def test_temporal_split_is_chronological_with_embargo():
    """Train = passé, test = futur, et une bande de GAP_MIN minutes vide entre les deux."""
    df = _series(200).assign(station_id=1)
    train, test, cut = common.temporal_split(df, test_fraction=0.2, gap_min=120)

    assert train["ts"].max() < test["ts"].min()                    # aucun chevauchement
    assert train["ts"].max() <= cut - pd.Timedelta(minutes=120)    # embargo respecté
    assert test["ts"].min() > cut
    assert len(train) + len(test) < len(df)                        # la bande d'embargo est jetée
    assert abs(len(test) / len(df) - 0.2) < 0.02                   # ~20 % du temps en test


def test_target_is_nan_when_future_bin_not_observed():
    """Un trou à t+15 -> cible NaN, même si les features à t sont forward-fillées."""
    g = _series(20)
    g = g[g["ts"] != g["ts"].iloc[10]]            # on retire le bin n°10 (station muette)
    out = build_dataset.build_station(1, g)
    t = out.index[7]                              # t + 15 min = 3 pas -> bin n°10 (manquant)
    assert np.isnan(out.loc[t, "target_15"])
    assert not np.isnan(out.loc[t, "bikes"])      # les features, elles, existent
    # Le bin manquant a bien ses features forward-fillées (1 pas <= FILL_LIMIT)…
    assert out.loc[out.index[10], "bikes"] == 9.0
    # …mais il ne fabrique jamais de cible : la cible de t-15 (bin 7) reste NaN ci-dessus.


def test_forward_fill_is_bounded():
    """Après FILL_LIMIT pas sans mesure, les features redeviennent NaN (trou détecté)."""
    g = _series(30)
    gap = g["ts"].iloc[10:15]                     # 5 bins consécutifs sans mesure
    g = g[~g["ts"].isin(gap)]
    out = build_dataset.build_station(1, g)
    filled = out.loc[gap, "bikes"].tolist()
    limit = build_dataset.FILL_LIMIT
    assert filled[:limit] == [9.0] * limit        # tolérance : on reporte le dernier état
    assert all(np.isnan(v) for v in filled[limit:])   # au-delà : NaN, pas une valeur inventée


def test_lags_and_targets_look_in_the_right_direction():
    """bikes_lag5(t) = bikes(t-5) (passé) ; target_15(t) = bikes(t+15) (futur)."""
    out = build_dataset.build_station(1, _series(30))
    t = out.index[10]                             # bikes == index du pas -> valeur 10
    assert out.loc[t, "bikes"] == 10
    assert out.loc[t, "bikes_lag5"] == 9          # 1 pas en arrière
    assert out.loc[t, "bikes_lag15"] == 7         # 3 pas en arrière
    assert out.loc[t, "target_15"] == 13          # 3 pas en avant
    assert np.isnan(out.loc[t, "target_120"])   # 24 pas en avant = hors série -> NaN
    # Liste de features partagée entre build_dataset et common (un seul contrat).
    assert build_dataset.FEATURE_COLS == common.FEATURE_COLS
