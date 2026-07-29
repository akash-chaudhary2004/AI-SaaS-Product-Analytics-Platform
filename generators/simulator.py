"""
AI SaaS User Simulator

This module simulates a complete user journey
through an AI SaaS application.

Flow:
User
    ↓
Session
    ↓
Event Generator
    ↓
Event Stream

Later this stream will be sent directly to Kafka.
"""

import random
import time
from pprint import pprint

from generators.user_generator import generate_user
from generators.session_generator import generate_session
from generators.event_generator import generate_event


# ==========================================================
# Fixed Event Flow
# ==========================================================

EVENT_FLOW = [
    "user_login",
    "chat_started",
    "prompt_submitted",
    "response_generated",
    "prompt_submitted",
    "response_generated",
    "feedback_submitted",
    "user_logout",
]


# ==========================================================
# Event Stream Generator
# ==========================================================

def simulate_user_journey():
    """
    Simulate one complete AI SaaS user journey.

    Returns
    -------
    generator
        Generates one complete event at a time.
    """

    user = generate_user()

    session = generate_session(user)

    print("=" * 80)
    print("NEW USER")
    pprint(user)

    print("\nNEW SESSION")
    pprint(session)

    print("\nSTREAMING EVENTS")
    print("-" * 80)

    for step, event_name in enumerate(EVENT_FLOW, start=1):

        event = generate_event(
            event_name,
            user,
            session
        )

        print(
            f"\n[{step}] {event_name.upper()}"
        )

        pprint(event)

        # This yield is the important change.
        # Kafka Producer will consume these events later.
        yield event

        time.sleep(
            random.uniform(0.5, 1.5)
        )

    print("-" * 80)
    print("SESSION COMPLETED")
    print("=" * 80)


# ==========================================================
# Local Test
# ==========================================================

if __name__ == "__main__":

    for _ in simulate_user_journey():
        pass