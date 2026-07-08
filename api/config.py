"""Configuration de l'API : charge le .env et construit la chaîne de connexion PostgreSQL.

L'API tourne sur l'HÔTE (hors Docker) : les variables d'environnement du docker-compose
ne sont donc PAS présentes -> on lit explicitement le .env à la racine du projet.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Racine du projet = dossier parent de api/ . On y charge le .env (gitignoré).
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Valeurs par défaut = celles d'un dev local (docker-compose publie 5432 sur localhost).
POSTGRES_USER = os.environ.get("POSTGRES_USER", "urbanflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "urbanflow")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")

# "conninfo" au format libpq (mots-clés espacés) attendu par psycopg.connect().
DB_CONNINFO = (
    f"host={POSTGRES_HOST} port={POSTGRES_PORT} "
    f"dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
)
