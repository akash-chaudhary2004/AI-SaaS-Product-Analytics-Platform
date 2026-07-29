"""
Kafka Serializer

Serializes Python dictionaries into JSON bytes
for Kafka messages.
"""

import json
from typing import Any


# ==========================================================
# Serializer
# ==========================================================

def serialize_event(event: dict) -> bytes:
    """
    Convert an event dictionary into UTF-8 encoded JSON bytes.
    """

    return json.dumps(
        event,
        ensure_ascii=False,
        separators=(",", ":")
    ).encode("utf-8")


def deserialize_event(message: bytes) -> dict:
    """
    Convert Kafka message bytes back into a Python dictionary.
    """

    return json.loads(
        message.decode("utf-8")
    )


# ==========================================================
# Local Test
# ==========================================================

if __name__ == "__main__":

    sample_event = {
        "event_type": "user_login",
        "user_id": "usr_12345678",
        "session_id": "ses_12345678",
    }

    print("=" * 60)
    print("Original Event")
    print("=" * 60)
    print(sample_event)

    encoded = serialize_event(sample_event)

    print("\nSerialized Bytes")
    print("=" * 60)
    print(encoded)

    decoded = deserialize_event(encoded)

    print("\nDecoded Event")
    print("=" * 60)
    print(decoded)