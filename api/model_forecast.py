"""Service des prédictions XGBoost multi-horizon — DÉMO sur données historiques.

Honnêteté (voir learning.md §8.3) : la base chaude ne stocke PAS le vecteur de features
(lags/rolling). On sert donc les prédictions XGBoost à partir du DERNIER état connu de
chaque station dans le dataset du Sprint 3 (features <= t). C'est une démonstration de la
couche de service ML, pas une inférence temps réel.

IMPORTANT : les imports lourds (xgboost, lecture Parquet) sont faits en LAZY (dans la
fonction), pas au niveau module -> importer api.main (tests/CI) ne tire pas xgboost, absent
des dépendances allégées de la CI (requirements-dev.txt).
"""
from functools import lru_cache
from pathlib import Path

# Horizons servis (mêmes colonnes cibles que l'entraînement).
MODEL_HORIZONS = [
    ("t+15", "target_15"), ("t+30", "target_30"),
    ("t+60", "target_60"), ("t+120", "target_120"),
]
# Features <= t, dans l'ORDRE d'entraînement (doit rester aligné sur build_dataset.py).
FEATURE_COLS = [
    "bikes", "docks", "capacity", "fill_ratio",
    "bikes_lag5", "bikes_lag10", "bikes_lag15",
    "bikes_roll_mean30", "bikes_roll_std30",
    "hour", "dow", "is_weekend",
]

_ML = Path(__file__).resolve().parent.parent / "ml"
DATA_PATH = _ML / "data" / "dataset.parquet"
MODELS_DIR = _ML / "models"


@lru_cache(maxsize=1)
def _load():
    """Charge (une seule fois) le dernier état par station + les 4 modèles XGBoost.

    lru_cache = singleton paresseux : le coût (lecture Parquet + désérialisation) n'est payé
    qu'au PREMIER appel de l'endpoint, puis mémorisé pour toute la vie du process.
    """
    import pandas as pd  # lazy (absent de la CI allégée)
    from xgboost import XGBRegressor  # lazy

    df = pd.read_parquet(DATA_PATH, columns=["station_id", "ts", *FEATURE_COLS])
    # Dernière ligne de features connue par station (état le plus récent <= t).
    latest = df.sort_values("ts").groupby("station_id").tail(1).set_index("station_id")

    models = {}
    for label, col in MODEL_HORIZONS:
        m = XGBRegressor()
        m.load_model(MODELS_DIR / f"xgb_{col}.json")
        models[label] = m
    return latest, models


def predict_station(station_id: int) -> dict | None:
    """Prédictions XGBoost t+15/30/60/120 pour une station, ou None si absente du dataset."""
    latest, models = _load()
    if station_id not in latest.index:
        return None
    row = latest.loc[[station_id]]                       # DataFrame d'UNE ligne
    preds = {label: round(float(m.predict(row[FEATURE_COLS])[0]), 1)
             for label, m in models.items()}
    return {
        "station_id": station_id,
        "as_of": str(row["ts"].iloc[0]),                 # timestamp du vecteur de features
        "bikes_ref": round(float(row["bikes"].iloc[0]), 1),  # état courant = persistance
        "preds": preds,
    }
