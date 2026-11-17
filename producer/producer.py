import json
import time

from kafka import KafkaProducer

# ---- Configuration -----------------------------------------------------------
KAFKA_BROKER = "kafka:29092"
TOPIC = "user-clicks"


def create_producer():
    """Retry until the Kafka broker is reachable, then return a producer."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("[producer] Connected to Kafka broker.")
            return producer
        except Exception as exc:
            print(f"[producer] Waiting for Kafka to be ready... ({exc})")
            time.sleep(3)


def main():
    producer = create_producer()
    event_id = 1

    print("[producer] Starting to publish user-clicks events...")
    while True:
        payload = {
            "event_id": event_id,
            "action": "click",
            "platform": "web",
        }

        producer.send(TOPIC, payload)
        producer.flush()

        print(f"[producer] Sent to '{TOPIC}': {payload}")
        event_id += 1
        time.sleep(2)


if __name__ == "__main__":
    main()
