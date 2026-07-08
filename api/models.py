"""Schémas Pydantic : la FORME des réponses de l'API (validation + sérialisation + doc).

FastAPI valide chaque réponse contre ces modèles avant de l'envoyer : si une clé manque
ou a le mauvais type, l'erreur est levée côté serveur (contrat d'API explicite).
"""
from datetime import datetime

from pydantic import BaseModel


class Station(BaseModel):
    """État chaud d'une station (une ligne de station_availability_current)."""
    station_id: int
    station_code: str | None            # code lisible (ex. "16107") ; peut être nul
    window_end: datetime | None         # fin de la dernière fenêtre 5 min agrégée
    bikes_available: float | None       # vélos dispo (moyenne sur la fenêtre)
    docks_available: float | None       # bornes libres (moyenne sur la fenêtre)
    n_observations: int | None          # nb de mesures agrégées dans la fenêtre
    updated_at: datetime                # horodatage du dernier upsert Spark


class Forecast(BaseModel):
    """Prédiction de disponibilité à t+15/t+30 pour une station."""
    station_id: int
    bikes_now: float | None              # état courant (point de départ de la prédiction)
    pred_t15: float | None               # vélos dispo prédits à +15 min
    pred_t30: float | None               # vélos dispo prédits à +30 min
    method: str                          # méthode utilisée (ex. "persistence") — transparence


class Health(BaseModel):
    """Réponse de /health : l'API répond-elle et la base est-elle joignable ?"""
    status: str                          # "ok" si tout va bien
    database: str                        # "up" ou "down"
