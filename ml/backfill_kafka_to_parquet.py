"""UrbanFlow — Sprint 3 : BACKFILL batch Kafka -> Parquet (couche de collecte).

Idée (voir learning.md §6.6). Le poller alimente Kafka en continu ; Kafka PERSISTE
sur disque (rétention 20 jours). Plutôt que de faire tourner un job STREAMING fragile
pendant des jours, on lit Kafka en BATCH, à la demande, du début (earliest) à la fin
(latest), et on écrit un jeu de mesures propre en Parquet sur MinIO. C'est robuste
(pas de checkpoint, pas de job zombie) et RÉUTILISABLE : on relancera ce script le jour
où assez de données seront accumulées pour construire le vrai dataset ML.

Sortie : s3a://urbanflow/ml/measures  (écrasée à chaque run -> IDEMPOTENT),
partitionnée par date d'ingestion. Une ligne = un relevé d'une station à un cycle de
poll (clé temporelle = ingested_at, l'horloge fiable).

Lancement (le conteneur Spark doit tourner, jars passés en --jars, voir README/notes) :
  docker compose exec spark /opt/spark/bin/spark-submit --jars <...kafka...,...s3...> \
    /opt/spark/work-dir/ml/backfill_kafka_to_parquet.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# --- Kafka (listener INTERNAL, réseau Docker) --------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = "velib.stations.raw"

# --- MinIO / S3 --------------------------------------------------------------
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "urbanflow")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "")
MEASURES_PATH = "s3a://urbanflow/ml/measures"     # jeu de mesures propre pour le ML

# --- Schéma d'un message station (aligné sur le poller, ingested_at inclus) ---
STATION_SCHEMA = StructType([
    StructField("station_id", LongType()),
    StructField("stationCode", StringType()),
    StructField("num_bikes_available", IntegerType()),
    StructField("num_docks_available", IntegerType()),
    StructField("is_installed", IntegerType()),
    StructField("last_reported", LongType()),        # sparse (change à chaque état) — gardé pour info
    StructField("ingested_at", LongType()),          # epoch s : HORLOGE ML (heure de capture)
])


def build_spark() -> SparkSession:
    """Session Spark configurée pour écrire sur MinIO (S3A). Lecture Kafka batch."""
    return (
        SparkSession.builder
        .appName("urbanflow-backfill")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1) LECTURE BATCH de tout le topic (earliest -> latest). `read` (pas readStream)
    #    = une passe finie sur le buffer Kafka actuel, sans checkpoint ni streaming.
    raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # 2) DÉSÉRIALISATION value(bytes) -> JSON -> colonnes typées.
    parsed = (
        raw
        .select(from_json(col("value").cast("string"), STATION_SCHEMA).alias("s"))
        .select("s.*")
    )

    # 3) VALIDATION (stateless) : lignes exploitables + stations opérationnelles.
    #    On EXIGE ingested_at non nul (messages d'avant l'ajout du champ -> écartés).
    measures = (
        parsed
        .filter(col("station_id").isNotNull())
        .filter(col("ingested_at").isNotNull())
        .filter(col("is_installed") == 1)
        .select(
            "station_id",
            "stationCode",
            "num_bikes_available",
            "num_docks_available",
            "last_reported",
            "ingested_at",
        )
        # colonne de partition = jour d'ingestion (horloge fiable, pas last_reported)
        .withColumn("ingest_date", to_date(col("ingested_at").cast("timestamp")))
    )

    n = measures.count()
    print(f"\n[BACKFILL] {n:,} mesures valides (avec ingested_at) prêtes à écrire.")

    # 4) ÉCRITURE Parquet sur MinIO. `overwrite` -> le jeu est reconstruit à chaque run
    #    (idempotent, pas de doublons contrairement à un append répété).
    (
        measures.write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(MEASURES_PATH)
    )
    print(f"[BACKFILL] écrit dans {MEASURES_PATH} (partitionné par ingest_date).")

    spark.stop()


if __name__ == "__main__":
    main()
