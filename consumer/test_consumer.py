"""UrbanFlow — consumer de test (Sprint 1, étape 4).

Lit le topic `velib.stations.raw` et affiche quelques messages.
But : prouver que la donnee circule de bout en bout (API -> poller -> Kafka -> ici).
"""
import json

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "velib.stations.raw"
MAX_MESSAGES = 5                        # on s'arrete apres 5 messages (consumer de test)


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="urbanflow-test-consumer",     # identifie ce groupe (memorise l'offset)
        auto_offset_reset="earliest",           # 1re lecture : depuis le debut du topic
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        consumer_timeout_ms=10000,              # stoppe si plus rien apres 10s (pratique en test)
    )
    print(f"Consumer demarre sur '{TOPIC}'. Lecture de {MAX_MESSAGES} messages max...\n")

    count = 0
    for message in consumer:
        count += 1
        station = message.value
        print(
            f"#{count} | partition={message.partition} offset={message.offset} "
            f"key={message.key}\n"
            f"     station {station['station_id']} : "
            f"{station['num_bikes_available']} velos / {station['num_docks_available']} bornes"
        )
        if count >= MAX_MESSAGES:
            break

    consumer.close()
    print(f"\n{count} message(s) lu(s). Consumer ferme.")


if __name__ == "__main__":
    main()
