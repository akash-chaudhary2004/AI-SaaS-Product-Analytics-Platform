"""
AI SaaS User Simulator

This module simulates a complete user journey
through an AI SaaS application.

Flow:
User
    ↓
Session
    ↓
Login
    ↓
Chat Started
    ↓
Prompt Submitted
    ↓
Response Generated
    ↓
Prompt Submitted
    ↓
Response Generated
    ↓
Feedback Submitted
    ↓
Logout
"""

import random
import time
from pprint import pprint

from generators.user_generator import generate_user
from generators.session_generator import generate_session


# =====================================================
# Fixed Event Flow
# =====================================================

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


# =====================================================
# User Journey
# =====================================================

def simulate_user_journey():
    """
    Simulate one complete user session.
    """

    # Create user
    user = generate_user()

    # Create session
    session = generate_session(user)

    print("=" * 70)
    print("NEW USER")
    pprint(user)

    print("\nNEW SESSION")
    pprint(session)

    print("\nEVENT FLOW")
    print("-" * 70)

    for step, event in enumerate(EVENT_FLOW, start=1):

        print(
            f"[{step}] "
            f"{event.upper()} | "
            f"User={user['user_id']} | "
            f"Session={session['session_id']}"
        )

        # Small delay to simulate real activity
        time.sleep(random.uniform(0.5, 1.5))

    print("-" * 70)
    print("SESSION COMPLETED")
    print("=" * 70)


# =====================================================
# Main
# =====================================================

if __name__ == "__main__":

    simulate_user_journey()