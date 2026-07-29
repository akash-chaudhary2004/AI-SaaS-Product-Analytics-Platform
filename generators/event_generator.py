"""
AI SaaS Event Generator

Generates realistic events for an AI SaaS platform.

Each event contains:
- Common metadata
- Event-specific information

This module will later be used by:
- Kafka Producer
- Spark Streaming
- Bronze Layer
"""

import random
import uuid
from datetime import datetime, timezone

from generators.schemas import (
    AI_MODELS,
    PROMPT_CATEGORIES,
    FEEDBACK_OPTIONS,
)

# ==========================================================
# Prompt Library
# ==========================================================

PROMPT_LIBRARY = {
    "Coding": [
        "Write a Python function to reverse a list.",
        "Explain Kafka in simple words.",
        "Difference between Spark and Hadoop.",
        "Create a REST API using FastAPI."
    ],

    "Data Engineering": [
        "Explain ETL vs ELT.",
        "What is Delta Lake?",
        "How does Spark Structured Streaming work?",
        "Explain Apache Airflow DAGs."
    ],

    "Marketing": [
        "Write a product launch email.",
        "Generate an Instagram caption.",
        "Create a LinkedIn post for an AI product.",
        "Write SEO keywords for an analytics platform."
    ],

    "Business": [
        "Prepare a SWOT analysis.",
        "Explain Porter's Five Forces.",
        "Create a business proposal.",
        "Generate quarterly KPI summary."
    ],

    "General": [
        "Summarise this article.",
        "Translate this paragraph.",
        "Improve my writing.",
        "Explain machine learning."
    ]
}

# ==========================================================
# Base Event
# ==========================================================

def create_base_event(user, session, event_type):
    """
    Common fields shared by every event.
    """

    return {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,

        "user_id": user["user_id"],
        "session_id": session["session_id"],

        "country": user["country"],
        "subscription_plan": user["subscription_plan"],

        "model_name": user["preferred_model"]
    }

# ==========================================================
# Login
# ==========================================================

def generate_login_event(user, session):

    event = create_base_event(
        user,
        session,
        "user_login"
    )

    event.update({
        "browser": session["browser"],
        "device_type": session["device_type"],
        "operating_system": session["operating_system"],
        "ip_address": session["ip_address"]
    })

    return event

# ==========================================================
# Chat Started
# ==========================================================

def generate_chat_started_event(user, session):

    event = create_base_event(
        user,
        session,
        "chat_started"
    )

    event["chat_id"] = f"chat_{uuid.uuid4().hex[:10]}"

    return event

# ==========================================================
# Prompt
# ==========================================================

def generate_prompt_event(user, session):

    category = random.choice(list(PROMPT_LIBRARY.keys()))

    prompt = random.choice(
        PROMPT_LIBRARY[category]
    )

    event = create_base_event(
        user,
        session,
        "prompt_submitted"
    )

    event.update({
        "prompt_category": category,
        "prompt_text": prompt
    })

    return event

# ==========================================================
# Response
# ==========================================================

def generate_response_event(user, session):

    prompt_tokens = random.randint(40, 500)

    completion_tokens = random.randint(
        80,
        1000
    )

    latency = random.randint(
        300,
        4000
    )

    event = create_base_event(
        user,
        session,
        "response_generated"
    )

    event.update({

        "latency_ms": latency,

        "prompt_tokens": prompt_tokens,

        "completion_tokens": completion_tokens,

        "total_tokens":
            prompt_tokens + completion_tokens,

        "response_cost":
            round(
                (prompt_tokens + completion_tokens)
                * 0.00002,
                5
            ),

        "success": random.random() > 0.03
    })

    return event

# ==========================================================
# Feedback
# ==========================================================

def generate_feedback_event(user, session):

    event = create_base_event(
        user,
        session,
        "feedback_submitted"
    )

    event.update({

        "feedback": random.choice(
            FEEDBACK_OPTIONS
        ),

        "rating":
            random.randint(1, 5)

    })

    return event

# ==========================================================
# Logout
# ==========================================================

def generate_logout_event(user, session):

    event = create_base_event(
        user,
        session,
        "user_logout"
    )

    event["session_duration_minutes"] = session[
        "session_duration_minutes"
    ]

    return event


# ==========================================================
# Factory
# ==========================================================

def generate_event(event_type, user, session):

    generators = {

        "user_login":
            generate_login_event,

        "chat_started":
            generate_chat_started_event,

        "prompt_submitted":
            generate_prompt_event,

        "response_generated":
            generate_response_event,

        "feedback_submitted":
            generate_feedback_event,

        "user_logout":
            generate_logout_event
    }

    if event_type not in generators:
        raise ValueError(f"Unsupported event type: {event_type}")

    return generators[event_type](
        user,
        session
    )


# ==========================================================
# Testing
# ==========================================================

if __name__ == "__main__":

    from pprint import pprint

    from generators.user_generator import generate_user
    from generators.session_generator import generate_session

    user = generate_user()
    session = generate_session(user)

    event_types = [

        "user_login",
        "chat_started",
        "prompt_submitted",
        "response_generated",
        "feedback_submitted",
        "user_logout"
    ]

    for event_type in event_types:

        print("\n" + "=" * 80)
        pprint(
            generate_event(
                event_type,
                user,
                session
            )
        )