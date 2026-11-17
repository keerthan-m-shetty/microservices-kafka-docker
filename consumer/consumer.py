import json
import time

from kafka import KafkaConsumer

# ---- Configuration -----------------------------------------------------------
KAFKA_BROKER = "kafka:29092"
TOPIC = "user-clicks"


def create_consumer():
    """Retry until the Kafka broker is reachable, then return a consumer."""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="consumer-group-1",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            print("[consumer] Connected to Kafka broker.")
            return consumer
        except Exception as exc:
            print(f"[consumer] Waiting for Kafka to be ready... ({exc})")
            time.sleep(3)


def main():
    consumer = create_consumer()
    print(f"[consumer] Listening to '{TOPIC}' for messages...")

    for message in consumer:
        print(f"[consumer] Received: {message.value}")


if __name__ == "__main__":
    main()
