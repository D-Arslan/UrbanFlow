"""Tests de l'API FastAPI — SANS base réelle.

Principe : on remplace (`monkeypatch`) les fonctions de la couche `api.db` par des doubles
de test. On teste ainsi le CONTRAT de l'API (routes, sérialisation Pydantic, codes HTTP,
logique du predictor) en isolation -> rapide et exécutable partout (y compris en CI).
"""
from fastapi.testclient import TestClient

from api import db, main

client = TestClient(main.app)

# Une station factice, au format renvoyé par db.fetch_station (clés = champs du modèle).
FAKE_STATION = {
    "station_id": 42,
    "station_code": "0042",
    "window_end": "2026-06-26T11:45:00",
    "bikes_available": 10.0,
    "docks_available": 5.0,
    "n_observations": 3,
    "updated_at": "2026-06-26T11:45:28",
}


def test_health_ok(monkeypatch):
    monkeypatch.setattr(db, "ping", lambda: True)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "database": "up"}


def test_health_degraded(monkeypatch):
    monkeypatch.setattr(db, "ping", lambda: False)      # base injoignable
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "degraded", "database": "down"}


def test_list_stations(monkeypatch):
    monkeypatch.setattr(db, "fetch_all_stations", lambda: [FAKE_STATION])
    r = client.get("/stations")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    assert body[0]["station_id"] == 42
    assert body[0]["bikes_available"] == 10.0


def test_get_station_found(monkeypatch):
    monkeypatch.setattr(db, "fetch_station", lambda sid: FAKE_STATION)
    r = client.get("/stations/42")
    assert r.status_code == 200
    assert r.json()["station_code"] == "0042"


def test_get_station_404(monkeypatch):
    monkeypatch.setattr(db, "fetch_station", lambda sid: None)   # station absente
    r = client.get("/stations/999999")
    assert r.status_code == 404


def test_forecast_is_persistence(monkeypatch):
    monkeypatch.setattr(db, "fetch_station", lambda sid: FAKE_STATION)
    r = client.get("/stations/42/forecast")
    assert r.status_code == 200
    body = r.json()
    # Persistance : les deux horizons valent l'état courant.
    assert body["bikes_now"] == 10.0
    assert body["pred_t15"] == 10.0
    assert body["pred_t30"] == 10.0
    assert body["method"] == "persistence"


def test_forecast_404(monkeypatch):
    monkeypatch.setattr(db, "fetch_station", lambda sid: None)
    r = client.get("/stations/999999/forecast")
    assert r.status_code == 404
