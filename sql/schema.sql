-- UrbanFlow — Sprint 2 : schéma de l'état chaud (PostgreSQL).
-- Table "état courant" : UNE ligne par station, mise à jour par upsert depuis Spark.
-- (L'historique complet, lui, part en Parquet sur MinIO — voir le job Spark.)

CREATE TABLE IF NOT EXISTS station_availability_current (
    station_id          BIGINT PRIMARY KEY,        -- clé d'upsert : 1 ligne / station
    station_code        TEXT,                       -- code lisible (ex. "16107")
    window_start        TIMESTAMP,                  -- début de la dernière fenêtre 5 min
    window_end          TIMESTAMP,                  -- fin de cette fenêtre
    avg_bikes_available DOUBLE PRECISION,           -- vélos dispo (moyenne sur la fenêtre)
    avg_docks_available DOUBLE PRECISION,           -- bornes libres (moyenne sur la fenêtre)
    n_observations      BIGINT,                     -- nb de mesures agrégées
    updated_at          TIMESTAMP NOT NULL DEFAULT now()  -- horodatage du dernier upsert
);
