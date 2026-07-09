"""UrbanFlow — Sprint 3 : utilitaires partagés (split temporel, features, métriques).

Centralisé pour GARANTIR que la baseline et tous les modèles utilisent EXACTEMENT le même
découpage train/test et la même liste de features -> comparaison honnête (§6.4).
"""
import pandas as pd
from sklearn.metrics import mean_absolute_error, root_mean_squared_error

# Découpage temporel (identique partout)
TEST_FRACTION = 0.2       # derniers 20 % du temps en test
GAP_MIN = 120             # embargo entre train et test >= plus grand horizon (t+120)
                          # (élargi pour l'expérience horizons longs : une cible du train
                          #  ne doit jamais déborder dans la période de test -> anti-leakage)

# Horizons de prédiction : (libellé, colonne cible)
# t+15/t+30 : court terme (persistance quasi-optimale) ; t+60/t+120 : long terme (le modèle
# doit y prendre l'avantage — c'est là qu'on teste l'utilité réelle du modèle).
HORIZONS = [
    ("t+15", "target_15"),
    ("t+30", "target_30"),
    ("t+60", "target_60"),
    ("t+120", "target_120"),
]

# Features <= t (aucune info du futur). Doit rester alignée sur build_dataset.py.
FEATURE_COLS = [
    "bikes", "docks", "capacity", "fill_ratio",
    "bikes_lag5", "bikes_lag10", "bikes_lag15",
    "bikes_roll_mean30", "bikes_roll_std30",
    "hour", "dow", "is_weekend",
]


def temporal_split(df: pd.DataFrame, test_fraction: float = TEST_FRACTION,
                   gap_min: int = GAP_MIN):
    """Découpe CHRONOLOGIQUE (§6.4) : train = passé, test = futur, séparés par un embargo.

    - `cut` = quantile temporel (1 - test_fraction) des timestamps ;
    - train = lignes <= cut - embargo (la bande d'embargo évite qu'une cible du train
      tombe dans la période de test) ;
    - test = lignes strictement après cut.
    """
    df = df.sort_values("ts")
    cut = df["ts"].quantile(1 - test_fraction)
    gap = pd.Timedelta(f"{gap_min}min")
    train = df[df["ts"] <= cut - gap]
    test = df[df["ts"] > cut]
    return train, test, cut


def mae_rmse(y_true, y_pred):
    """Renvoie (MAE, RMSE) — §6.2."""
    return mean_absolute_error(y_true, y_pred), root_mean_squared_error(y_true, y_pred)
