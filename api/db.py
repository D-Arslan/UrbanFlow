"""Couche d'accès aux données : toutes les requêtes SQL vers PostgreSQL vivent ici.

Choix : une connexion COURTE par requête (psycopg.connect dans un `with`). Simple et
robuste pour le trafic d'un projet portfolio ; en prod on mettrait un pool (psycopg_pool).
"""
import psycopg
from psycopg.rows import dict_row

from api.config import DB_CONNINFO

# Colonnes exposées, avec alias SQL alignés sur les champs du modèle Pydantic `Station`.
_SELECT_COLS = """
    station_id,
    station_code,
    window_end,
    avg_bikes_available AS bikes_available,
    avg_docks_available AS docks_available,
    n_observations,
    updated_at
"""


def ping() -> bool:
    """La base est-elle joignable ? (SELECT 1) — utilisé par /health."""
    try:
        with psycopg.connect(DB_CONNINFO, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone() is not None
    except psycopg.Error:
        return False


def fetch_all_stations() -> list[dict]:
    """Toutes les stations, triées par id (liste pour GET /stations)."""
    with psycopg.connect(DB_CONNINFO, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {_SELECT_COLS} FROM station_availability_current ORDER BY station_id"
            )
            return cur.fetchall()


def fetch_station(station_id: int) -> dict | None:
    """Une station par son id, ou None si absente (GET /stations/{id})."""
    with psycopg.connect(DB_CONNINFO, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {_SELECT_COLS} FROM station_availability_current WHERE station_id = %s",
                (station_id,),                       # requête paramétrée (anti-injection)
            )
            return cur.fetchone()
