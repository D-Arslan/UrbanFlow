"""UrbanFlow — Spark Structured Streaming (Sprint 2).

ÉTAPE 1 : lire le topic Kafka `velib.stations.raw`, désérialiser le JSON, dériver
l'event-time.
ÉTAPE 2 : NETTOYER le flux (rejet des lignes invalides, stations opérationnelles,
déduplication avec watermark).
ÉTAPE 3 : AGRÉGER en fenêtres temporelles de 5 min (disponibilité moyenne/station).
ÉTAPE 4 : écrire l'ÉTAT COURANT dans PostgreSQL par UPSERT (foreachBatch + table
de staging + INSERT ... ON CONFLICT DO UPDATE).
ÉTAPE 5 (actuelle) : écrire l'HISTORIQUE granulaire en Parquet sur MinIO (s3a://),
partitionné par date. Deux requêtes streaming tournent en parallèle (état chaud
PostgreSQL + historique froid Parquet).

Lancement (depuis l'hôte) :
  docker compose exec spark /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /opt/spark/work-dir/streaming_job.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, to_date
from pyspark.sql.functions import round as round_
from pyspark.sql.functions import window
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# --- Configuration (injectée par docker-compose via l'environnement) ---------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")  # listener INTERNAL
TOPIC = "velib.stations.raw"

# --- PostgreSQL (état chaud) -------------------------------------------------
PG_DB = os.environ.get("POSTGRES_DB", "urbanflow")
PG_USER = os.environ.get("POSTGRES_USER", "urbanflow")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
PG_URL = f"jdbc:postgresql://postgres:5432/{PG_DB}"   # "postgres" = nom de service Docker
PG_TABLE = "station_availability_current"             # table finale (état courant)
STAGING_TABLE = "staging_availability"                # table tampon (1 batch à la fois)
CHECKPOINT_PG = "/opt/spark/checkpoints/postgres"     # progression du stream (volume)

# Upsert : on fusionne le staging dans la table finale.
#  - DISTINCT ON (station_id) ... ORDER BY window_start DESC : si une station a
#    plusieurs fenêtres dans le batch, on ne garde que la PLUS RÉCENTE ;
#  - WHERE EXCLUDED.window_start >= cur.window_start : garde-fou anti-régression
#    (une vieille fenêtre n'écrase jamais une plus récente déjà stockée).
UPSERT_SQL = f"""
INSERT INTO {PG_TABLE} AS cur (
    station_id, station_code, window_start, window_end,
    avg_bikes_available, avg_docks_available, n_observations, updated_at
)
SELECT DISTINCT ON (station_id)
    station_id, station_code, window_start, window_end,
    avg_bikes_available, avg_docks_available, n_observations, now()
FROM {STAGING_TABLE}
ORDER BY station_id, window_start DESC
ON CONFLICT (station_id) DO UPDATE SET
    station_code        = EXCLUDED.station_code,
    window_start        = EXCLUDED.window_start,
    window_end          = EXCLUDED.window_end,
    avg_bikes_available = EXCLUDED.avg_bikes_available,
    avg_docks_available = EXCLUDED.avg_docks_available,
    n_observations      = EXCLUDED.n_observations,
    updated_at          = now()
WHERE EXCLUDED.window_start >= cur.window_start;
"""

# --- MinIO / S3 (stockage froid : historique Parquet) ------------------------
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "urbanflow")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "")
PARQUET_PATH = "s3a://urbanflow/history/stations"     # bucket urbanflow, préfixe history
CHECKPOINT_PARQUET = "/opt/spark/checkpoints/parquet"

# --- Schéma d'un message station (sous-ensemble utile du GBFS Vélib') --------
# Spark NE DEVINE PAS le JSON : on déclare explicitement les champs et leurs types.
# Les champs absents d'un message arriveront à NULL (mode permissif par défaut).
STATION_SCHEMA = StructType([
    StructField("station_id", LongType()),         # identifiant numérique de station
    StructField("stationCode", StringType()),      # code lisible (ex. "16107")
    StructField("num_bikes_available", IntegerType()),
    StructField("num_docks_available", IntegerType()),
    StructField("is_installed", IntegerType()),     # 0/1
    StructField("is_renting", IntegerType()),       # 0/1
    StructField("is_returning", IntegerType()),     # 0/1
    StructField("last_reported", LongType()),       # timestamp Unix (secondes) -> EVENT TIME
])


def build_spark() -> SparkSession:
    """Crée la session Spark (point d'entrée de toute application Spark)."""
    return (
        SparkSession.builder
        .appName("urbanflow-streaming")
        # micro-batchs raisonnables sur une machine de dev :
        .config("spark.sql.shuffle.partitions", "4")
        # --- Accès MinIO via le connecteur S3A (compatible S3) ---
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        # MinIO impose le "path-style" (bucket dans le chemin, pas en sous-domaine) :
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # on est en http (pas https) en local :
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    """Ouvre un flux de lecture sur le topic Kafka. Renvoie un DataFrame streaming
    au schéma FIXE imposé par la source Kafka (key, value, topic, offset...)."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")   # on ne lit que les NOUVEAUX messages
        .load()
    )


def parse_stations(raw_df):
    """Désérialise la colonne binaire `value` (octets) -> JSON -> colonnes typées,
    puis dérive l'event-time à partir de `last_reported` (secondes Unix)."""
    return (
        raw_df
        # value est en octets : on le caste en texte, puis on parse le JSON.
        .select(from_json(col("value").cast("string"), STATION_SCHEMA).alias("s"))
        .select("s.*")                                   # aplatit la struct en colonnes
        # un LONG de secondes casté en timestamp est interprété comme epoch -> date réelle
        .withColumn("event_time", col("last_reported").cast("timestamp"))
    )


def validate_stations(parsed_df):
    """Nettoyage SANS ÉTAT (stateless) :
      1. rejette les lignes invalides (station_id / event_time à NULL) ;
      2. ne garde que les stations OPÉRATIONNELLES (is_installed = 1).
    N'introduit aucun state store -> réutilisable directement pour l'historique."""
    return (
        parsed_df
        .filter(col("station_id").isNotNull() & col("event_time").isNotNull())
        .filter(col("is_installed") == 1)
    )


def deduplicate_stations(validated_df):
    """Déduplication AVEC ÉTAT (stateful) : le poller republie tout l'instantané
    toutes les 60 s, donc une même paire (station_id, last_reported) peut arriver
    plusieurs fois. Le watermark borne l'état de déduplication (sinon mémoire
    infinie). À n'utiliser que sur la branche d'AGRÉGATION."""
    return (
        validated_df
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["station_id", "last_reported"])
    )


WINDOW_DURATION = "5 minutes"        # taille des fenêtres tumbling (event-time)


def aggregate_availability(cleaned_df):
    """Agrège la disponibilité PAR STATION et PAR FENÊTRE de 5 min.

    Le watermark hérité de clean_stations permet à Spark de fermer les fenêtres
    passées (et d'en libérer l'état). `window(event_time, ...)` découpe le temps
    en tranches tumbling de 5 min, calées sur l'EVENT-TIME (pas l'heure de calcul).
    """
    return (
        cleaned_df
        .groupBy(
            window(col("event_time"), WINDOW_DURATION),   # fenêtre de 5 min
            col("station_id"),
            col("stationCode"),                            # 1:1 avec station_id
        )
        .agg(
            round_(avg("num_bikes_available"), 1).alias("avg_bikes_available"),
            round_(avg("num_docks_available"), 1).alias("avg_docks_available"),
            count("*").alias("n_observations"),            # nb de mesures dans la fenêtre
        )
        # on aplatit la struct `window` en deux colonnes lisibles
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("station_id"),
            col("stationCode").alias("station_code"),
            col("avg_bikes_available"),
            col("avg_docks_available"),
            col("n_observations"),
        )
    )


def upsert_to_postgres(batch_df, batch_id: int) -> None:
    """foreachBatch : appelé pour CHAQUE micro-batch avec un DataFrame batch
    classique. JDBC natif ne sait pas faire d'upsert -> on passe par un staging
    puis une requête INSERT ... ON CONFLICT DO UPDATE."""
    # 1) écrit le micro-batch dans la table de staging (on l'écrase à chaque fois)
    (
        batch_df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", STAGING_TABLE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    # 2) fusionne staging -> table finale via une connexion JDBC (pont py4j vers la JVM)
    jvm = batch_df.sparkSession._jvm
    conn = jvm.java.sql.DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD)
    try:
        stmt = conn.createStatement()
        stmt.execute(UPSERT_SQL)
        stmt.close()
    finally:
        conn.close()


def start_postgres_sink(availability_df):
    """ÉTAPE 4 : état courant -> PostgreSQL (upsert via foreachBatch, mode update)."""
    return (
        availability_df.writeStream
        .outputMode("update")     # on émet les fenêtres modifiées à chaque batch
        .option("checkpointLocation", CHECKPOINT_PG)
        .foreachBatch(upsert_to_postgres)
        .start()
    )


def start_parquet_sink(validated_df):
    """ÉTAPE 5 : historique granulaire -> Parquet sur MinIO (append, partitionné).

    On écrit le flux VALIDÉ brut (sans état, pas l'agrégat) : matière première
    immuable pour le ML. `append` émet chaque observation immédiatement ;
    `partitionBy(event_date)` crée un dossier par jour (lectures futures ciblées)."""
    history = validated_df.withColumn("event_date", to_date(col("event_time")))
    return (
        history.writeStream
        .format("parquet")
        .option("path", PARQUET_PATH)
        .option("checkpointLocation", CHECKPOINT_PARQUET)
        .partitionBy("event_date")
        .outputMode("append")
        .start()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")   # réduit le bruit des logs Spark

    raw = read_kafka_stream(spark)
    stations = parse_stations(raw)
    validated = validate_stations(stations)        # ÉTAPE 2a : filtres (sans état)
    cleaned = deduplicate_stations(validated)      # ÉTAPE 2b : déduplication (avec état)
    availability = aggregate_availability(cleaned)  # ÉTAPE 3 : fenêtres de 5 min

    # Deux requêtes streaming EN PARALLÈLE, sans nœud stateful partagé :
    start_postgres_sink(availability)             # ÉTAPE 4 : PostgreSQL (agrégat dédupliqué)
    start_parquet_sink(validated)                 # ÉTAPE 5 : Parquet/MinIO (historique brut)

    # on attend la fin de N'IMPORTE LAQUELLE des requêtes (sinon le driver sort)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
