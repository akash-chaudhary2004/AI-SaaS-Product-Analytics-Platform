"""
Kafka Configuration

Centralized Kafka configuration for the
AI SaaS Product Analytics Platform.

Both the Kafka Producer and Spark Consumer
will use these settings.
"""

from streaming.topics import AI_PRODUCT_EVENTS


# ==========================================================
# Kafka Broker Configuration
# ==========================================================

# Running Kafka locally via Docker Compose.
BOOTSTRAP_SERVERS = [
    "localhost:9092",
]

# ==========================================================
# Client Configuration
# ==========================================================

CLIENT_ID = "ai-saas-product-analytics"

# ==========================================================
# Topic Configuration
# ==========================================================

DEFAULT_TOPIC = AI_PRODUCT_EVENTS

# ==========================================================
# Producer Configuration
# ==========================================================

ACKS = "all"

RETRIES = 5

RETRY_BACKOFF_MS = 1000

REQUEST_TIMEOUT_MS = 30000

DELIVERY_TIMEOUT_MS = 120000

MAX_IN_FLIGHT_REQUESTS = 5

ENABLE_IDEMPOTENCE = True

LINGER_MS = 10

BATCH_SIZE = 16384

COMPRESSION_TYPE = "gzip"

# ==========================================================
# Consumer Configuration
# ==========================================================

GROUP_ID = "spark-bronze-consumer"

AUTO_OFFSET_RESET = "earliest"

ENABLE_AUTO_COMMIT = True

# ==========================================================
# Helper Function
# ==========================================================

def producer_config() -> dict:
    """
    Return Kafka producer configuration.
    """

    return {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "client_id": CLIENT_ID,
        "acks": ACKS,
        "retries": RETRIES,
        "retry_backoff_ms": RETRY_BACKOFF_MS,
        "request_timeout_ms": REQUEST_TIMEOUT_MS,
        "delivery_timeout_ms": DELIVERY_TIMEOUT_MS,
        "max_in_flight_requests_per_connection": MAX_IN_FLIGHT_REQUESTS,
        "enable_idempotence": ENABLE_IDEMPOTENCE,
        "linger_ms": LINGER_MS,
        "batch_size": BATCH_SIZE,
        "compression_type": COMPRESSION_TYPE,
    }


def consumer_config() -> dict:
    """
    Return Kafka consumer configuration.
    """

    return {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "group_id": GROUP_ID,
        "auto_offset_reset": AUTO_OFFSET_RESET,
        "enable_auto_commit": ENABLE_AUTO_COMMIT,
    }


# ==========================================================
# Local Test
# ==========================================================

if __name__ == "__main__":

    from pprint import pprint

    print("=" * 60)
    print("Kafka Producer Configuration")
    print("=" * 60)

    pprint(producer_config())

    print()

    print("=" * 60)
    print("Kafka Consumer Configuration")
    print("=" * 60)

    pprint(consumer_config())