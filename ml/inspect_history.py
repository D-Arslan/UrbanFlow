"""UrbanFlow — Sprint 3, sous-incrément 1a : INSPECTION de l'historique Parquet.

But : AVANT de construire le dataset ML, on REGARDE les données pour dimensionner
la grille temporelle et la tolérance aux trous (voir learning.md §6.5).

Ce script ne produit AUCUN dataset : il lit l'historique froid (MinIO/s3a) et
imprime des statistiques de cadrage :
  - volume brut vs vraies mesures distinctes (mesure des doublons du poller) ;
  - couverture temporelle (jours, plage de dates) ;
  - nombre de stations et de mesures par station ;
  - PROFIL DES TROUS : distribution des écarts entre mesures consécutives d'une
    même station (médiane, p90, p99, max) -> dimensionne grille & tolérance.

Lancement (depuis l'hôte, le conteneur Spark doit tourner) :
  docker compose exec spark /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    /opt/spark/work-dir/ml/inspect_history.py
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    countDistinct,
    expr,
    lag,
    max as max_,
    min as min_,
    unix_timestamp,
)

# --- MinIO / S3 (mêmes paramètres que le job streaming, voir streaming_job.py) ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "urbanflow")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "")
HISTORY_PATH = "s3a://urbanflow/history/stations"   # écrit par le sink Parquet du Sprint 2


def build_spark() -> SparkSession:
    """Session Spark configurée pour lire MinIO via le connecteur S3A.
    Identique à la config du job streaming, MOINS les options de streaming :
    ici on fait un simple READ BATCH d'un répertoire Parquet."""
    return (
        SparkSession.builder
        .appName("urbanflow-inspect-history")
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

    # 1) LECTURE BATCH de tout l'historique Parquet (toutes les partitions event_date).
    raw = spark.read.parquet(HISTORY_PATH)
    raw_count = raw.count()

    # 2) VRAIES MESURES distinctes : le poller republie tout l'instantané chaque
    #    minute, mais last_reported ne change QUE si la station change d'état.
    #    -> on déduplique sur (station_id, last_reported) pour isoler les mesures réelles.
    measures = raw.dropDuplicates(["station_id", "last_reported"])
    measures_count = measures.count()

    print("\n========== VOLUME ==========")
    print(f"Lignes brutes (1 par station par poll)   : {raw_count:,}")
    print(f"Mesures distinctes (station, last_reported): {measures_count:,}")
    ratio = raw_count / measures_count if measures_count else 0
    print(f"Facteur de duplication (brut / distinct)  : {ratio:.1f}x")

    # 3) COUVERTURE TEMPORELLE : plage d'event_time + nombre de jours couverts.
    print("\n========== COUVERTURE TEMPORELLE ==========")
    (measures
        .select(
            min_("event_time").alias("premier_event"),
            max_("event_time").alias("dernier_event"),
            countDistinct("event_date").alias("nb_jours"),
            countDistinct("station_id").alias("nb_stations"),
        )
        .show(truncate=False))

    # 4) MESURES PAR STATION : combien de points réels a-t-on par station ?
    #    (renseigne sur la densité de la série de chaque station).
    print("\n========== MESURES PAR STATION (distribution) ==========")
    per_station = measures.groupBy("station_id").count()
    (per_station
        .selectExpr(
            "min(count)  as min_par_station",
            "percentile_approx(count, 0.5)  as mediane",
            "percentile_approx(count, 0.9)  as p90",
            "max(count)  as max_par_station",
            "avg(count)  as moyenne",
        )
        .show(truncate=False))

    # 5) PROFIL DES TROUS : pour chaque station, écart (en secondes) entre deux
    #    mesures consécutives (ordonnées par last_reported). C'est LA statistique
    #    qui dimensionne la grille (pas régulier) et la tolérance au forward-fill.
    print("\n========== PROFIL DES TROUS entre mesures consécutives (secondes) ==========")
    w = Window.partitionBy("station_id").orderBy("last_reported")
    gaps = (measures
        .withColumn("prev_last_reported", lag("last_reported").over(w))
        .withColumn("gap_s", col("last_reported") - col("prev_last_reported"))
        .filter(col("gap_s").isNotNull()))
    (gaps
        .selectExpr(
            "percentile_approx(gap_s, 0.5)  as mediane_s",
            "percentile_approx(gap_s, 0.9)  as p90_s",
            "percentile_approx(gap_s, 0.99) as p99_s",
            "max(gap_s) as max_s",
        )
        .show(truncate=False))

    # 6) PART DES TROUS > 15 min : si une station reste muette > horizon, le
    #    forward-fill produirait une valeur périmée -> on devra jeter ces points.
    total_gaps = gaps.count()
    big_gaps = gaps.filter(col("gap_s") > 15 * 60).count()
    print("\n========== TROUS > 15 min ==========")
    pct = 100 * big_gaps / total_gaps if total_gaps else 0
    print(f"Écarts > 15 min : {big_gaps:,} / {total_gaps:,} ({pct:.2f} %)")

    spark.stop()


if __name__ == "__main__":
    main()
