"""Point d'entrée de l'API FastAPI — UrbanFlow.

Lancement (depuis la racine du projet, .venv activé) :
    uvicorn api.main:app --reload
Doc interactive : http://localhost:8000/docs
"""
from fastapi import FastAPI, HTTPException

from api import db, model_forecast, predictor
from api.models import Forecast, Health, ModelForecast, Station

app = FastAPI(
    title="UrbanFlow API",
    version="0.1.0",
    description="Disponibilité Vélib' (état chaud PostgreSQL alimenté par Spark). "
    "/forecast = persistance (t+15/t+30) ; /forecast_model = XGBoost t+15..t+120 "
    "sur le dernier état du dataset historique (démo, 503 si les artefacts ML sont absents).",
)


@app.get("/health", response_model=Health)
def health() -> Health:
    """Sonde de vivacité : l'API répond et la base est-elle joignable ?"""
    up = db.ping()
    return Health(status="ok" if up else "degraded", database="up" if up else "down")


@app.get("/stations", response_model=list[Station])
def list_stations() -> list[dict]:
    """Toutes les stations avec leur dernière disponibilité connue."""
    return db.fetch_all_stations()


@app.get("/stations/{station_id}", response_model=Station)
def get_station(station_id: int) -> dict:
    """Détail d'une station par son id (404 si inconnue)."""
    row = db.fetch_station(station_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"station {station_id} inconnue")
    return row


@app.get("/stations/{station_id}/forecast", response_model=Forecast)
def forecast_station(station_id: int) -> Forecast:
    """Prédiction t+15/t+30 pour une station (persistance ; 404 si inconnue)."""
    row = db.fetch_station(station_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"station {station_id} inconnue")
    bikes_now = row["bikes_available"]
    preds = predictor.predict(bikes_now)
    return Forecast(
        station_id=station_id,
        bikes_now=bikes_now,
        pred_t15=preds["t+15"],
        pred_t30=preds["t+30"],
        method=predictor.METHOD,
    )


@app.get("/stations/{station_id}/forecast_model", response_model=ModelForecast)
def forecast_model(station_id: int) -> ModelForecast:
    """Prévision XGBoost t+15/30/60/120 (démo sur le dataset historique).

    404 si la station est absente du dataset ; 503 si dataset/modèles ne sont pas sur le
    disque (clone frais : ils sont gitignorés et se produisent via ml/train_xgb.py).
    """
    try:
        r = model_forecast.predict_station(station_id)
    except FileNotFoundError as e:                       # inclut ModelArtifactsMissing
        raise HTTPException(status_code=503, detail=str(e)) from e
    if r is None:
        raise HTTPException(status_code=404, detail=f"station {station_id} absente du dataset")
    p = r["preds"]
    return ModelForecast(
        station_id=station_id,
        as_of=r["as_of"],
        bikes_ref=r["bikes_ref"],
        method="xgboost",
        pred_t15=p["t+15"],
        pred_t30=p["t+30"],
        pred_t60=p["t+60"],
        pred_t120=p["t+120"],
    )
