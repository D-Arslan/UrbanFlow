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

- [ ] **Sprint 1** — Repo + Docker Compose (Kafka + PostgreSQL) + poller + consumer de test
- [ ] **Sprint 2** — Spark Structured Streaming + stockage
- [ ] **Sprint 3** — ML (prédiction de disponibilité)
- [ ] **Sprint 4** — FastAPI + Streamlit + CI

## Démarrage rapide

> ⚠️ Sera complété au fur et à mesure du Sprint 1.

```bash
# à venir : docker compose up -d
```

## Documentation

Les concepts, définitions et choix d'architecture sont consignés dans
[`learning.md`](./learning.md) (base du rapport final).
