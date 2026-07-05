"""UrbanFlow — Sprint 3 : build_grid (Spark = I/O + réduction, voir learning.md §6.5).

Rôle. Lire le jeu de mesures brut (`ml/measures` sur MinIO, une ligne par station par
cycle de poll, horloge = ingested_at) et le RÉGULARISER sur une grille temporelle de
5 min par station : pour chaque (station, bin de 5 min), on ne garde que la mesure la
plus FRAÎCHE du bin. Cela réduit le volume et pose une grille quasi régulière.

Ce script NE calcule PAS les features/cibles (ça, c'est le travail de pandas côté hôte,
dans build_dataset.py) : ici on se contente de la partie que Spark fait de mieux — lire
le stockage objet à l'échelle et réduire le volume. Le handoff se fait par un FICHIER
Parquet écrit en LOCAL (dossier monté ./ml -> conteneur), car l'image Spark n'embarque
pas pandas (§6.5).

Sortie : ml/data/grid (Parquet local, écrasé à chaque run). Colonnes :
  station_id, station_code, bin_epoch (s), ts (timestamp du point de grille),
  bikes, docks.

Lancement :
  docker compose exec spark /opt/spark/bin/spark-submit --jars <...s3...> \
    /opt/spark/work-dir/ml/build_grid.py
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, floor, row_number

# --- MinIO / S3 (source) -----------------------------------------------------
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "urbanflow")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "")
MEASURES_PATH = "s3a://urbanflow/ml/measures"

# --- Sortie LOCALE (dossier monté ./ml -> visible depuis l'hôte) -------------
GRID_PATH = "file:///opt/spark/work-dir/ml/data/grid"

BIN_SECONDS = 5 * 60      # granularité de la grille : 5 minutes (horizons t+15=3, t+30=6 pas)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("urbanflow-build-grid")
        .config("spark.sql.shuffle.partitions", "8")   # petit shuffle (machine de dev)
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

    measures = spark.read.parquet(MEASURES_PATH)

    # 1) BIN de 5 min : on tronque ingested_at (epoch s) au multiple de 300 s inférieur.
    #    Ex. 10:07:32 -> bin 10:05:00. floor(ts/300)*300 = début du bin.
    binned = measures.withColumn(
        "bin_epoch",
        (floor(col("ingested_at") / BIN_SECONDS) * BIN_SECONDS).cast("long"),
    )

    # 2) MESURE LA PLUS FRAÎCHE par (station, bin) : on classe chaque ligne du bin par
    #    ingested_at décroissant et on garde la 1re (row_number = 1). Un bin peut contenir
    #    ~5 relevés (poll chaque minute) ; on retient l'état le plus récent du bin.
    w = Window.partitionBy("station_id", "bin_epoch").orderBy(col("ingested_at").desc())
    grid = (
        binned
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select(
            col("station_id"),
            col("stationCode").alias("station_code"),
            col("bin_epoch"),
            col("bin_epoch").cast("timestamp").alias("ts"),   # point de grille (timestamp)
            col("num_bikes_available").alias("bikes"),
            col("num_docks_available").alias("docks"),
        )
    )

    n = grid.count()
    n_stations = grid.select("station_id").distinct().count()
    print(f"\n[GRID] {n:,} points de grille | {n_stations:,} stations distinctes.")

    # 3) ÉCRITURE LOCALE. coalesce(1) : jeu compact -> un seul fichier, relu facilement
    #    par pandas côté hôte. `overwrite` -> reconstruit à chaque run (idempotent).
    grid.coalesce(1).write.mode("overwrite").parquet(GRID_PATH)
    print(f"[GRID] écrit dans {GRID_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
