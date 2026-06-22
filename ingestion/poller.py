"""UrbanFlow — poller Vélib' (Sprint 1).

Étape 3b : interroge le flux GBFS `station_status` en boucle et publie chaque
station dans Kafka. Boucle résiliente : une erreur ponctuelle ne tue pas le poller.
"""
import json
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Configuration ---------------------------------------------------------
STATION_STATUS_URL = (
    "https://velib-metropole-opendata.smovengo.cloud"
    "/opendata/Velib_Metropole/station_status.json"
)
KAFKA_BOOTSTRAP = "localhost:9092"      # adresse "advertised" annoncée par le broker
TOPIC = "velib.stations.raw"            # topic de destination
POLL_INTERVAL_SECONDS = 60             # fréquence d'interrogation de l'API


def fetch_station_status() -> list[dict]:
    """Interroge l'API GBFS et renvoie la liste des statuts de stations."""
    response = requests.get(STATION_STATUS_URL, timeout=15)
    response.raise_for_status()
    return response.json()["data"]["stations"]


def build_producer() -> KafkaProducer:
    """Crée le producer Kafka avec sérialisation JSON -> octets."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # value : dict Python -> texte JSON -> octets UTF-8
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # key : station_id -> texte -> octets (decide la partition de destination)
        key_serializer=lambda k: str(k).encode("utf-8"),
    )


def publish_stations(producer: KafkaProducer, stations: list[dict]) -> int:
    """Publie chaque station dans le topic, clé = station_id. Renvoie le nb publié."""
    for station in stations:
        producer.send(TOPIC, key=station["station_id"], value=station)
    producer.flush()                    # force l'envoi + attend la confirmation broker
    return len(stations)


def main() -> None:
    producer = build_producer()
    print(f"Poller demarre | topic='{TOPIC}' | intervalle={POLL_INTERVAL_SECONDS}s "
          f"| Ctrl+C pour arreter")
    try:
        while True:
            try:
                stations = fetch_station_status()
                count = publish_stations(producer, stations)
                print(f"[OK] {count} stations publiees dans '{TOPIC}'.")
            except requests.RequestException as exc:
                # erreur cote API (timeout, 5xx...) : on log et on reessaiera
                print(f"[WARN] echec API : {exc!r} -> nouvel essai au prochain cycle.")
            except KafkaError as exc:
                # erreur cote Kafka : idem, on ne meurt pas
                print(f"[WARN] echec Kafka : {exc!r} -> nouvel essai au prochain cycle.")
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nArret demande (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()                # fermeture propre du producer
        print("Producer ferme proprement.")


if __name__ == "__main__":
    main()
