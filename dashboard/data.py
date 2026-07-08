"""Dashboard — couche DONNÉES (pure, sans Streamlit -> testable en headless).

Rôle : appeler l'API FastAPI (HTTP) et joindre les coordonnées statiques du référentiel
`station_information.json` (lat/lon/nom/capacité) sur `station_id`. Aucune logique d'UI ici.
"""
import json
import os
from pathlib import Path

import httpx
import pandas as pd

# URL de base de l'API (surchargée par la variable d'env API_URL en déploiement).
API_URL = os.environ.get("API_URL", "http://localhost:8000")
# Référentiel statique des stations (lat/lon/nom/capacité), généré depuis le flux GBFS.
REFERENCE_FILE = Path(__file__).resolve().parent / "stations_information.json"


def load_reference() -> dict[int, dict]:
    """Charge le référentiel et l'indexe par station_id (jointure O(1) ensuite)."""
    stations = json.loads(REFERENCE_FILE.read_text(encoding="utf-8"))
    return {s["station_id"]: s for s in stations}


def get_health(api_url: str = API_URL) -> dict:
    """État de l'API (/health). Renvoie un statut 'down' si l'API est injoignable."""
    try:
        return httpx.get(f"{api_url}/health", timeout=5).json()
    except httpx.HTTPError:
        return {"status": "unreachable", "database": "down"}


def get_stations(api_url: str = API_URL) -> list[dict]:
    """Toutes les stations et leur disponibilité courante (GET /stations)."""
    r = httpx.get(f"{api_url}/stations", timeout=15)
    r.raise_for_status()
    return r.json()


def get_forecast(station_id: int, api_url: str = API_URL) -> dict:
    """Prédiction t+15/t+30 d'une station (GET /stations/{id}/forecast)."""
    r = httpx.get(f"{api_url}/stations/{station_id}/forecast", timeout=10)
    r.raise_for_status()
    return r.json()


def build_map_df(stations: list[dict], reference: dict[int, dict]) -> pd.DataFrame:
    """Joint dispo courante + coordonnées -> DataFrame prêt pour la carte.

    - jointure sur station_id ; les stations sans coordonnées (absentes du référentiel)
      sont écartées (impossible à placer sur la carte) ;
    - `fill_ratio` = vélos / capacité (borné [0,1]) pour la couleur ;
    - `color` = dégradé rouge (station vide) -> vert (station pleine de vélos).
    """
    rows = []
    for s in stations:
        ref = reference.get(s["station_id"])
        if ref is None or ref.get("lat") is None:
            continue                                    # pas de coordonnées -> non plaçable
        bikes = s["bikes_available"] or 0
        capacity = ref.get("capacity") or 0
        fill = min(bikes / capacity, 1.0) if capacity else 0.0
        rows.append({
            "station_id": s["station_id"],
            "name": ref["name"],
            "station_code": s["station_code"],
            "bikes_available": bikes,
            "docks_available": s["docks_available"],
            "capacity": capacity,
            "lat": ref["lat"],
            "lon": ref["lon"],
            "fill_ratio": round(fill, 2),
            # couleur RGB pour pydeck : rouge (peu de vélos) -> vert (beaucoup)
            "color": [int(255 * (1 - fill)), int(180 * fill) + 40, 60],
        })
    return pd.DataFrame(rows)
