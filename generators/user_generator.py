"""
AI SaaS User Generator.

This module creates synthetic users for the
AI SaaS Product Analytics Platform.

A generated user represents an account on an AI product
such as ChatGPT, Claude, Gemini, or a similar SaaS platform.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

from generators.schemas import (
    AI_MODELS,
    DEVICE_TYPES,
    SUBSCRIPTION_PLANS,
)


# ==========================================================
# Configuration
# ==========================================================

# Using weighted probabilities makes the simulated customer
# base more realistic than choosing every plan equally.
SUBSCRIPTION_WEIGHTS = [0.65, 0.22, 0.08, 0.05]

COUNTRY_PROFILES = [
    {
        "country": "India",
        "timezone": "Asia/Kolkata",
        "language": "English",
    },
    {
        "country": "United States",
        "timezone": "America/New_York",
        "language": "English",
    },
    {
        "country": "United Kingdom",
        "timezone": "Europe/London",
        "language": "English",
    },
    {
        "country": "Germany",
        "timezone": "Europe/Berlin",
        "language": "German",
    },
    {
        "country": "Canada",
        "timezone": "America/Toronto",
        "language": "English",
    },
    {
        "country": "Australia",
        "timezone": "Australia/Sydney",
        "language": "English",
    },
    {
        "country": "Singapore",
        "timezone": "Asia/Singapore",
        "language": "English",
    },
]


# ==========================================================
# Helper Functions
# ==========================================================

def generate_user_id() -> str:
    """
    Generate a unique user ID.

    Example:
        usr_a8f73c21
    """
    return f"usr_{uuid.uuid4().hex[:8]}"


def generate_signup_date() -> str:
    """
    Generate a signup timestamp from within the last two years.
    """
    now = datetime.now(timezone.utc)

    days_ago = random.randint(0, 730)

    signup_date = now - timedelta(days=days_ago)

    return signup_date.isoformat()


def choose_subscription_plan() -> str:
    """
    Select a subscription plan using weighted probabilities.

    Most simulated users are Free users, while fewer users
    belong to paid or Enterprise plans.
    """
    return random.choices(
        SUBSCRIPTION_PLANS,
        weights=SUBSCRIPTION_WEIGHTS,
        k=1,
    )[0]


def choose_preferred_model(subscription_plan: str) -> str:
    """
    Choose a preferred AI model.

    Paid users have a higher probability of using
    more advanced models.
    """

    if subscription_plan == "Free":
        return random.choice(AI_MODELS)

    if subscription_plan == "Plus":
        return random.choices(
            AI_MODELS,
            weights=[0.45, 0.25, 0.20, 0.10],
            k=1,
        )[0]

    if subscription_plan == "Pro":
        return random.choices(
            AI_MODELS,
            weights=[0.55, 0.25, 0.15, 0.05],
            k=1,
        )[0]

    # Enterprise
    return random.choices(
        AI_MODELS,
        weights=[0.50, 0.30, 0.15, 0.05],
        k=1,
    )[0]


# ==========================================================
# Main User Generator
# ==========================================================

def generate_user() -> dict:
    """
    Generate one synthetic AI SaaS user.
    """

    subscription_plan = choose_subscription_plan()

    location = random.choice(COUNTRY_PROFILES)

    user = {
        "user_id": generate_user_id(),

        "signup_date": generate_signup_date(),

        "subscription_plan": subscription_plan,

        "country": location["country"],

        "timezone": location["timezone"],

        "language": location["language"],

        "preferred_model": choose_preferred_model(
            subscription_plan
        ),

        "device_preference": random.choice(
            DEVICE_TYPES
        ),
    }

    return user


# ==========================================================
# Local Test
# ==========================================================

if __name__ == "__main__":

    from pprint import pprint

    user = generate_user()

    pprint(user)