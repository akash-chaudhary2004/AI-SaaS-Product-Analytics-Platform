"""
AI SaaS User Simulator

This module simulates a complete AI SaaS user journey.

Flow:

User
    ↓
Session
    ↓
Login
    ↓
Chat Started
    ↓
1–5 Prompt / Response pairs
    ↓
(Optional) Feedback
    ↓
Logout

Each event is yielded one-by-one so that the Kafka
Producer can stream events in real time.
"""

import random
import time
from pprint import pprint

from generators.user_generator import generate_user
from generators.session_generator import generate_session
from generators.event_generator import generate_event


# ==========================================================
# User Journey Simulator
# ==========================================================

def simulate_user_journey():
    """
    Simulate one complete AI SaaS user journey.

    Yields
    ------
    dict
        One event at a time.
    """

    # ------------------------------------------------------
    # Create User & Session
    # ------------------------------------------------------

    user = generate_user()
    session = generate_session(user)

    print("=" * 80)
    print("NEW USER")
    pprint(user)

    print("\nNEW SESSION")
    pprint(session)

    print("\nSTREAMING EVENTS")
    print("-" * 80)

    step = 1

    # ======================================================
    # USER LOGIN
    # ======================================================

    event = generate_event(
        "user_login",
        user,
        session
    )

    print(f"\n[{step}] USER_LOGIN")
    pprint(event)

    yield event

    step += 1
    time.sleep(random.uniform(0.5, 1.5))

    # ======================================================
    # CHAT STARTED
    # ======================================================

    event = generate_event(
        "chat_started",
        user,
        session
    )

    print(f"\n[{step}] CHAT_STARTED")
    pprint(event)

    yield event

    step += 1
    time.sleep(random.uniform(0.5, 1.5))

    # ======================================================
    # RANDOM PROMPTS
    # ======================================================

    prompt_count = random.randint(1, 5)

    for _ in range(prompt_count):

        # ----------------------------------------------
        # Prompt Submitted
        # ----------------------------------------------

        event = generate_event(
            "prompt_submitted",
            user,
            session
        )

        print(f"\n[{step}] PROMPT_SUBMITTED")
        pprint(event)

        yield event

        step += 1
        time.sleep(random.uniform(0.5, 1.5))

        # ----------------------------------------------
        # Response Generated
        # ----------------------------------------------

        event = generate_event(
            "response_generated",
            user,
            session
        )

        print(f"\n[{step}] RESPONSE_GENERATED")
        pprint(event)

        yield event

        step += 1
        time.sleep(random.uniform(0.5, 1.5))

    # ======================================================
    # OPTIONAL FEEDBACK
    # ======================================================

    if random.random() < 0.8:

        event = generate_event(
            "feedback_submitted",
            user,
            session
        )

        print(f"\n[{step}] FEEDBACK_SUBMITTED")
        pprint(event)

        yield event

        step += 1
        time.sleep(random.uniform(0.5, 1.5))

    # ======================================================
    # USER LOGOUT
    # ======================================================

    event = generate_event(
        "user_logout",
        user,
        session
    )

    print(f"\n[{step}] USER_LOGOUT")
    pprint(event)

    yield event

    print("-" * 80)
    print("SESSION COMPLETED")
    print("=" * 80)


# ==========================================================
# Local Test
# ==========================================================

if __name__ == "__main__":

    for _ in simulate_user_journey():
        pass