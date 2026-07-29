"""
AI SaaS Session Generator

Creates one login session for an existing user.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

from generators.schemas import (
    DEVICE_TYPES,
    BROWSERS,
    OPERATING_SYSTEMS,
)


def generate_session_id() -> str:
    """Generate unique session id."""
    return f"ses_{uuid.uuid4().hex[:8]}"


def generate_ip_address() -> str:
    """Generate a fake IPv4 address."""

    return ".".join(
        str(random.randint(1, 255))
        for _ in range(4)
    )


def generate_session(user: dict) -> dict:
    """
    Create one session for an existing user.
    """

    login_time = datetime.now(timezone.utc)

    duration_minutes = random.randint(5, 90)

    logout_time = login_time + timedelta(
        minutes=duration_minutes
    )

    session = {

        "session_id": generate_session_id(),

        "user_id": user["user_id"],

        "country": user["country"],

        "timezone": user["timezone"],

        "subscription_plan": user["subscription_plan"],

        "preferred_model": user["preferred_model"],

        "device_type": random.choice(
            DEVICE_TYPES
        ),

        "browser": random.choice(
            BROWSERS
        ),

        "operating_system": random.choice(
            OPERATING_SYSTEMS
        ),

        "ip_address": generate_ip_address(),

        "login_time": login_time.isoformat(),

        "logout_time": logout_time.isoformat(),

        "session_duration_minutes": duration_minutes,
    }

    return session


if __name__ == "__main__":

    from pprint import pprint
    from generators.user_generator import generate_user

    user = generate_user()

    session = generate_session(user)

    pprint(session)