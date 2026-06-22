# UrbanFlow 🚲

Pipeline de données **temps réel** sur la mobilité urbaine francilienne
(Vélib' / IDFM) : ingestion → streaming → traitement → ML → visualisation.

> Projet portfolio — Data Science / Systèmes Distribués.

## Architecture cible

```
API Vélib'/IDFM
      │  (poller Python)
      ▼
   Kafka  ──────────────┐
      │                 │
      ▼                 ▼
Spark Structured   (autres consumers)
   Streaming
      │
      ├──► PostgreSQL   (état chaud, dernier état des stations)
      └──► Parquet/MinIO (historique)
              │
              ▼
           ML (prédiction disponibilité t+15/30 min)
              │
              ▼
        FastAPI + Streamlit (API + dashboard)
```

## Stack technique

| Couche | Techno |
|--------|--------|
| Ingestion | Python (`requests`) |
| Streaming | Apache Kafka |
| Traitement | Spark Structured Streaming |
| Stockage chaud | PostgreSQL |
| Stockage froid | Parquet / MinIO |
| ML | scikit-learn (V1) |
| Service | FastAPI + Streamlit |
| Orchestration | Docker Compose |
| CI | GitHub Actions |

## Avancement (par sprints)

- [x] **Sprint 1** — Repo + Docker Compose (Kafka KRaft + PostgreSQL) + poller + consumer de test ✅
- [ ] **Sprint 2** — Spark Structured Streaming + stockage
- [ ] **Sprint 3** — ML (prédiction de disponibilité)
- [ ] **Sprint 4** — FastAPI + Streamlit + CI

## Démarrage rapide

```powershell
# 1. Infra (Kafka + PostgreSQL)
copy .env.example .env          # puis ajuster les valeurs
docker compose up -d
docker compose ps               # verifier que tout est "healthy"

# 2. Environnement Python
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 3. Lancer le poller (producer) — Ctrl+C pour arreter
python ingestion\poller.py

# 4. Dans un autre terminal : lire ce qui a ete publie
python consumer\test_consumer.py
```

## Documentation

Les concepts, définitions et choix d'architecture sont consignés dans
[`learning.md`](./learning.md) (base du rapport final).
