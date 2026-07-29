"""
Kafka Producer

Publishes AI SaaS events to Kafka.
"""

from streaming import KafkaProducer

from generators.simulator import simulate_user_journey
from streaming.kafka_config import (
    DEFAULT_TOPIC,
    producer_config,
)
from streaming.serializer import serialize_event


# ==========================================================
# Kafka Producer Factory
# ==========================================================

def create_producer() -> KafkaProducer:
    """
    Create and return a Kafka producer.
    """

    config = producer_config()

    producer = KafkaProducer(
        **config,
        value_serializer=serialize_event,
    )

    return producer


# ==========================================================
# Publish Events
# ==========================================================

def publish_events():
    """
    Publish all simulated events to Kafka.
    """

    producer = create_producer()

    print("=" * 80)
    print("Kafka Producer Started")
    print("=" * 80)

    try:

        for event in simulate_user_journey():

            future = producer.send(
                DEFAULT_TOPIC,
                value=event,
            )

            metadata = future.get(timeout=10)

            print(
                f"[SENT] "
                f"Topic={metadata.topic} | "
                f"Partition={metadata.partition} | "
                f"Offset={metadata.offset} | "
                f"Event={event['event_type']}"
            )

    finally:

        producer.flush()
        producer.close()

        print("-" * 80)
        print("Producer Closed")
        print("=" * 80)


# ==========================================================
# Main
# ==========================================================

if __name__ == "__main__":

    publish_events()