"""
AI SaaS Product Analytics Platform
Realistic Synthetic Event Producer

Generates meaningful AI SaaS product telemetry and publishes
events continuously to Kafka.

Kafka Topic:
    ai-product-events

The generated data represents realistic relationships between:

User
  -> Subscription
  -> Session
  -> Chat
  -> Prompt
  -> Response
  -> Feedback
  -> Billing

Modes:
    DEV_USERS=10
    DEV_USERS=100
    DEV_USERS=1000

Environment variables:

    KAFKA_BOOTSTRAP_SERVERS
    KAFKA_TOPIC
    SIMULATION_USERS
    SIMULATION_SESSIONS
    SIMULATION_DELAY
    CONTINUOUS_MODE
"""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from faker import Faker
from kafka import KafkaProducer


# ==============================================================================
# Configuration
# ==============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ai-product-events",
)

SIMULATION_USERS = int(
    os.getenv("SIMULATION_USERS", "10")
)

SIMULATION_SESSIONS = int(
    os.getenv("SIMULATION_SESSIONS", "20")
)

SIMULATION_DELAY = float(
    os.getenv("SIMULATION_DELAY", "0.15")
)

CONTINUOUS_MODE = os.getenv(
    "CONTINUOUS_MODE",
    "false",
).lower() == "true"


# ==============================================================================
# Faker
# ==============================================================================

fake = Faker()

random.seed()


# ==============================================================================
# Business Configuration
# ==============================================================================

COUNTRIES = {
    "India": {
        "timezone": "Asia/Kolkata",
        "currency": "INR",
        "language": "English",
        "region": "Asia",
        "cities": [
            "Delhi",
            "Mumbai",
            "Bengaluru",
            "Hyderabad",
            "Pune",
        ],
    },
    "United States": {
        "timezone": "America/New_York",
        "currency": "USD",
        "language": "English",
        "region": "North America",
        "cities": [
            "New York",
            "Boston",
            "Chicago",
            "Austin",
            "Seattle",
        ],
    },
    "United Kingdom": {
        "timezone": "Europe/London",
        "currency": "GBP",
        "language": "English",
        "region": "Europe",
        "cities": [
            "London",
            "Manchester",
            "Birmingham",
            "Leeds",
        ],
    },
    "Germany": {
        "timezone": "Europe/Berlin",
        "currency": "EUR",
        "language": "German",
        "region": "Europe",
        "cities": [
            "Berlin",
            "Munich",
            "Hamburg",
            "Frankfurt",
        ],
    },
    "Australia": {
        "timezone": "Australia/Sydney",
        "currency": "AUD",
        "language": "English",
        "region": "Oceania",
        "cities": [
            "Sydney",
            "Melbourne",
            "Brisbane",
            "Perth",
        ],
    },
    "Canada": {
        "timezone": "America/Toronto",
        "currency": "CAD",
        "language": "English",
        "region": "North America",
        "cities": [
            "Toronto",
            "Vancouver",
            "Montreal",
            "Ottawa",
        ],
    },
}


PLANS = {
    "Free": {
        "monthly_price": 0.0,
        "currency": "USD",
        "max_requests": 25,
    },
    "Plus": {
        "monthly_price": 20.0,
        "currency": "USD",
        "max_requests": 250,
    },
    "Pro": {
        "monthly_price": 50.0,
        "currency": "USD",
        "max_requests": 1000,
    },
    "Enterprise": {
        "monthly_price": 250.0,
        "currency": "USD",
        "max_requests": 10000,
    },
}


PLAN_WEIGHTS = {
    "Free": 0.55,
    "Plus": 0.25,
    "Pro": 0.15,
    "Enterprise": 0.05,
}


MODEL_CONFIG = {
    "GPT-5": {
        "provider": "OpenAI",
        "base_latency": 850,
        "cost_per_1k_tokens": 0.020,
    },
    "Claude-4": {
        "provider": "Anthropic",
        "base_latency": 950,
        "cost_per_1k_tokens": 0.018,
    },
    "Gemini-2.5": {
        "provider": "Google",
        "base_latency": 700,
        "cost_per_1k_tokens": 0.012,
    },
    "Llama-4": {
        "provider": "Meta",
        "base_latency": 1100,
        "cost_per_1k_tokens": 0.008,
    },
    "Mistral-Large": {
        "provider": "Mistral AI",
        "base_latency": 1000,
        "cost_per_1k_tokens": 0.010,
    },
}


MODEL_WEIGHTS = {
    "GPT-5": 0.35,
    "Claude-4": 0.20,
    "Gemini-2.5": 0.20,
    "Llama-4": 0.15,
    "Mistral-Large": 0.10,
}


PROMPT_CATEGORIES = {
    "Coding": [
        "Debug a Python API and explain the issue.",
        "Create a SQL query for customer retention.",
        "Build a data pipeline using PySpark.",
        "Explain this Python function.",
    ],
    "Marketing": [
        "Create a LinkedIn post for an AI product.",
        "Write a product launch campaign.",
        "Create an email marketing campaign.",
        "Generate SEO content for a SaaS product.",
    ],
    "Business": [
        "Analyze monthly revenue performance.",
        "Create a business strategy for SaaS growth.",
        "Summarize the quarterly business performance.",
        "Identify the most important business KPIs.",
    ],
    "Analytics": [
        "Analyze customer churn trends.",
        "Calculate conversion rate from this dataset.",
        "Explain the customer retention pattern.",
        "Create an executive analytics summary.",
    ],
    "Education": [
        "Explain machine learning in simple terms.",
        "Teach me SQL joins with examples.",
        "Explain Spark architecture.",
        "Create a learning plan for data engineering.",
    ],
    "Research": [
        "Summarize recent AI technology trends.",
        "Compare different LLM architectures.",
        "Explain the advantages of vector databases.",
        "Research AI SaaS market opportunities.",
    ],
}


FEEDBACK_REASONS = {
    "thumbs_up": [
        "helpful",
        "accurate",
        "clear",
        "fast",
    ],
    "thumbs_down": [
        "incorrect",
        "too_slow",
        "irrelevant",
        "incomplete",
    ],
}


DEVICES = [
    "Desktop",
    "Mobile",
    "Tablet",
]


BROWSERS = [
    "Chrome",
    "Firefox",
    "Safari",
    "Edge",
]


OPERATING_SYSTEMS = [
    "Windows",
    "macOS",
    "Linux",
    "Android",
    "iOS",
]


INDUSTRIES = [
    "Technology",
    "Finance",
    "Healthcare",
    "Education",
    "Retail",
    "Manufacturing",
    "Marketing",
    "Consulting",
]


COMPANY_SIZES = [
    "Startup",
    "Small Business",
    "Mid-Market",
    "Enterprise",
]


JOB_ROLES = [
    "Data Analyst",
    "Software Engineer",
    "Product Manager",
    "Marketing Manager",
    "Business Analyst",
    "Founder",
    "Student",
]


PROMPT_LANGUAGES = [
    "English",
    "Hindi",
    "German",
    "French",
    "Spanish",
]


# ==============================================================================
# Helpers
# ==============================================================================

def generate_id(prefix: str) -> str:
    """Generate a readable unique identifier."""

    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def utc_now() -> datetime:
    """Return current UTC timestamp."""

    return datetime.now(timezone.utc)


def iso_timestamp(value: datetime | None = None) -> str:
    """Convert datetime to ISO-8601 string."""

    value = value or utc_now()

    return value.isoformat()


def choose_plan() -> str:
    """Select a subscription plan using realistic weights."""

    return random.choices(
        list(PLAN_WEIGHTS.keys()),
        weights=list(PLAN_WEIGHTS.values()),
        k=1,
    )[0]


def choose_model() -> str:
    """Select AI model according to realistic usage distribution."""

    return random.choices(
        list(MODEL_WEIGHTS.keys()),
        weights=list(MODEL_WEIGHTS.values()),
        k=1,
    )[0]


def choose_country() -> str:
    """Select customer country."""

    return random.choice(
        list(COUNTRIES.keys())
    )


def calculate_prompt_complexity(
    prompt_length: int,
    category: str,
) -> str:
    """Classify prompt complexity."""

    if category == "Coding":

        if prompt_length > 250:
            return "High"

        if prompt_length > 120:
            return "Medium"

        return "Low"

    if prompt_length > 300:
        return "High"

    if prompt_length > 150:
        return "Medium"

    return "Low"


def calculate_cost(
    total_tokens: int,
    model_name: str,
) -> float:
    """Estimate model inference cost."""

    model = MODEL_CONFIG[model_name]

    cost = (
        total_tokens / 1000
    ) * model["cost_per_1k_tokens"]

    return round(cost, 5)


def send_event(
    producer: KafkaProducer,
    event: dict[str, Any],
    event_number: int | None = None,
) -> None:
    """Publish one event to Kafka."""

    producer.send(
        KAFKA_TOPIC,
        value=event,
    )

    if event_number is not None:

        print(
            f"[{event_number}] "
            f"{event['event_type'].upper():22} "
            f"user={event.get('user_id', '-'):<16} "
            f"model={event.get('model_name', '-'):<15}"
        )

    time.sleep(SIMULATION_DELAY)


# ==============================================================================
# User Generation
# ==============================================================================

def create_user() -> dict[str, Any]:
    """Create a realistic SaaS customer."""

    country = choose_country()

    location = COUNTRIES[country]

    plan = choose_plan()

    signup_date = (
        utc_now()
        - timedelta(
            days=random.randint(1, 730)
        )
    )

    user = {
        "user_id": generate_id("usr"),
        "event_type": "user_created",
        "timestamp": iso_timestamp(),
        "signup_timestamp": iso_timestamp(signup_date),
        "country": country,
        "region": location["region"],
        "city": random.choice(
            location["cities"]
        ),
        "timezone": location["timezone"],
        "language": location["language"],
        "currency": location["currency"],
        "industry": random.choice(INDUSTRIES),
        "company_size": random.choice(COMPANY_SIZES),
        "job_role": random.choice(JOB_ROLES),
        "device_preference": random.choice(DEVICES),
        "subscription_plan": plan,
        "plan_price": PLANS[plan]["monthly_price"],
    }

    return user


# ==============================================================================
# Subscription
# ==============================================================================

def create_subscription(
    user: dict[str, Any],
) -> dict[str, Any]:
    """Create one subscription lifecycle event per user."""

    plan = user["subscription_plan"]

    trial_days = (
        14
        if plan != "Free"
        else 0
    )

    trial_start = (
        utc_now()
        - timedelta(
            days=random.randint(1, 180)
        )
    )

    trial_end = (
        trial_start
        + timedelta(days=trial_days)
        if trial_days
        else None
    )

    status = (
        random.choices(
            [
                "active",
                "cancelled",
                "past_due",
            ],
            weights=[
                0.90,
                0.07,
                0.03,
            ],
            k=1,
        )[0]
        if plan != "Free"
        else "active"
    )

    return {
        "subscription_id": generate_id("sub"),
        "event_id": generate_id("evt"),
        "event_type": "subscription_created",
        "timestamp": iso_timestamp(),
        "user_id": user["user_id"],
        "subscription_plan": plan,
        "monthly_price": PLANS[plan]["monthly_price"],
        "currency": PLANS[plan]["currency"],
        "billing_cycle": "monthly",
        "trial_start": iso_timestamp(trial_start),
        "trial_end": (
            iso_timestamp(trial_end)
            if trial_end
            else None
        ),
        "subscription_status": status,
    }


# ==============================================================================
# Session
# ==============================================================================

def create_session(
    user: dict[str, Any],
) -> dict[str, Any]:
    """Create a realistic user session."""

    duration = random.randint(
        5,
        90,
    )

    login_time = utc_now()

    logout_time = (
        login_time
        + timedelta(
            minutes=duration
        )
    )

    return {
        "session_id": generate_id("ses"),
        "user_id": user["user_id"],
        "event_type": "session_started",
        "timestamp": iso_timestamp(login_time),
        "login_time": iso_timestamp(login_time),
        "logout_time": iso_timestamp(logout_time),
        "session_duration_minutes": duration,
        "country": user["country"],
        "device_type": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "operating_system": random.choice(
            OPERATING_SYSTEMS
        ),
        "ip_address": fake.ipv4_public(),
        "subscription_plan": user["subscription_plan"],
        "model_name": choose_model(),
    }


# ==============================================================================
# Prompt
# ==============================================================================

def create_prompt(
    user: dict[str, Any],
    session: dict[str, Any],
) -> dict[str, Any]:
    """Create a meaningful prompt event."""

    category = random.choice(
        list(PROMPT_CATEGORIES.keys())
    )

    prompt_text = random.choice(
        PROMPT_CATEGORIES[category]
    )

    language = random.choice(
        PROMPT_LANGUAGES
    )

    prompt_length = len(prompt_text)

    return {
        "prompt_id": generate_id("prm"),
        "event_id": generate_id("evt"),
        "event_type": "prompt_submitted",
        "timestamp": iso_timestamp(),
        "user_id": user["user_id"],
        "session_id": session["session_id"],
        "chat_id": generate_id("chat"),
        "country": user["country"],
        "subscription_plan": user["subscription_plan"],
        "model_name": session["model_name"],
        "prompt_category": category,
        "prompt_text": prompt_text,
        "prompt_language": language,
        "prompt_length": prompt_length,
        "prompt_complexity": calculate_prompt_complexity(
            prompt_length,
            category,
        ),
        "contains_code": category == "Coding",
        "contains_image": (
            random.random() < 0.08
        ),
    }


# ==============================================================================
# Response
# ==============================================================================

def create_response(
    user: dict[str, Any],
    session: dict[str, Any],
    prompt: dict[str, Any],
) -> dict[str, Any]:
    """Create model response telemetry."""

    model_name = prompt["model_name"]

    model = MODEL_CONFIG[
        model_name
    ]

    complexity_multiplier = {
        "Low": 0.8,
        "Medium": 1.2,
        "High": 1.8,
    }[
        prompt["prompt_complexity"]
    ]

    prompt_tokens = random.randint(
        80,
        600,
    )

    completion_tokens = random.randint(
        100,
        1200,
    )

    total_tokens = (
        prompt_tokens
        + completion_tokens
    )

    queue_time = random.randint(
        30,
        250,
    )

    inference_time = int(
        model["base_latency"]
        * complexity_multiplier
        * random.uniform(
            0.75,
            1.35,
        )
    )

    render_time = random.randint(
        20,
        100,
    )

    latency = (
        queue_time
        + inference_time
        + render_time
    )

    success = random.random() < 0.96

    retry_count = (
        random.choices(
            [0, 1, 2],
            weights=[
                0.94,
                0.05,
                0.01,
            ],
            k=1,
        )[0]
        if not success
        else 0
    )

    cache_hit = (
        random.random() < 0.18
    )

    response_cost = calculate_cost(
        total_tokens,
        model_name,
    )

    finish_reason = (
        "stop"
        if success
        else random.choice(
            [
                "timeout",
                "server_error",
                "rate_limit",
            ]
        )
    )

    return {
        "event_id": generate_id("evt"),
        "event_type": "response_generated",
        "timestamp": iso_timestamp(),
        "user_id": user["user_id"],
        "session_id": session["session_id"],
        "chat_id": prompt["chat_id"],
        "prompt_id": prompt["prompt_id"],
        "country": user["country"],
        "subscription_plan": user["subscription_plan"],
        "model_provider": model["provider"],
        "model_name": model_name,
        "prompt_category": prompt["prompt_category"],
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "queue_time_ms": queue_time,
        "inference_time_ms": inference_time,
        "render_time_ms": render_time,
        "latency_ms": latency,
        "response_cost": response_cost,
        "success": success,
        "retry_count": retry_count,
        "cache_hit": cache_hit,
        "finish_reason": finish_reason,
        "temperature": round(
            random.uniform(
                0.1,
                1.0,
            ),
            2,
        ),
        "max_tokens": random.choice(
            [
                512,
                1024,
                2048,
                4096,
            ]
        ),
    }


# ==============================================================================
# Feedback
# ==============================================================================

def create_feedback(
    user: dict[str, Any],
    session: dict[str, Any],
    prompt: dict[str, Any],
    response: dict[str, Any],
) -> dict[str, Any]:
    """Create feedback associated with a response."""

    positive = (
        response["success"]
        and random.random() < 0.82
    )

    feedback_type = (
        "thumbs_up"
        if positive
        else "thumbs_down"
    )

    rating = (
        random.choices(
            [4, 5],
            weights=[
                0.30,
                0.70,
            ],
            k=1,
        )[0]
        if positive
        else random.choices(
            [1, 2, 3],
            weights=[
                0.20,
                0.55,
                0.25,
            ],
            k=1,
        )[0]
    )

    return {
        "feedback_id": generate_id("fb"),
        "event_id": generate_id("evt"),
        "event_type": "feedback_submitted",
        "timestamp": iso_timestamp(),
        "user_id": user["user_id"],
        "session_id": session["session_id"],
        "chat_id": prompt["chat_id"],
        "prompt_id": prompt["prompt_id"],
        "country": user["country"],
        "subscription_plan": user["subscription_plan"],
        "model_name": response["model_name"],
        "feedback": feedback_type,
        "rating": rating,
        "feedback_reason": random.choice(
            FEEDBACK_REASONS[
                feedback_type
            ]
        ),
    }


# ==============================================================================
# Billing
# ==============================================================================

def create_billing_event(
    user: dict[str, Any],
    subscription: dict[str, Any],
) -> dict[str, Any] | None:
    """Create one billing event linked to the user's subscription."""

    plan = user["subscription_plan"]

    if plan == "Free":
        return None

    payment_success = (
        random.random() < 0.97
    )

    amount = PLANS[plan][
        "monthly_price"
    ]

    return {
        "invoice_id": generate_id("inv"),
        "payment_id": generate_id("pay"),
        "event_id": generate_id("evt"),
        "event_type": (
            "payment_success"
            if payment_success
            else "payment_failed"
        ),
        "timestamp": iso_timestamp(),

        # User attribution
        "user_id": user["user_id"],

        # Subscription attribution
        "subscription_id": subscription[
            "subscription_id"
        ],
        "subscription_plan": plan,

        # Payment details
        "amount": amount,
        "currency": PLANS[plan]["currency"],
        "payment_status": (
            "paid"
            if payment_success
            else "failed"
        ),
    }


# ==============================================================================
# User Logout
# ==============================================================================

def create_logout_event(
    user: dict[str, Any],
    session: dict[str, Any],
) -> dict[str, Any]:
    """Create session logout event."""

    return {
        "event_id": generate_id("evt"),
        "event_type": "user_logout",
        "timestamp": iso_timestamp(),
        "user_id": user["user_id"],
        "session_id": session["session_id"],
        "country": user["country"],
        "subscription_plan": user["subscription_plan"],
        "model_name": session["model_name"],
        "session_duration_minutes": session[
            "session_duration_minutes"
        ],
    }


# ==============================================================================
# User Session Simulation
# ==============================================================================

def simulate_user_session(
    producer: KafkaProducer,
    user: dict[str, Any],
    subscription: dict[str, Any],
    session_number: int,
) -> None:
    """Generate one realistic user session."""

    print()
    print("=" * 80)
    print(
        f"USER SESSION #{session_number}"
    )
    print("=" * 80)

    print(
        f"User       : {user['user_id']}"
    )

    print(
        f"Country    : {user['country']}"
    )

    print(
        f"Plan       : {user['subscription_plan']}"
    )

    print(
        f"Industry   : {user['industry']}"
    )

    # ==========================================================================
    # Session Start
    # ==========================================================================

    session = create_session(
        user
    )

    session["subscription_id"] = (
        subscription["subscription_id"]
    )

    send_event(
        producer,
        session,
    )

    # ==========================================================================
    # Login
    # ==========================================================================

    login_event = {
        **session,
        "event_id": generate_id("evt"),
        "event_type": "user_login",
        "timestamp": iso_timestamp(),
    }

    send_event(
        producer,
        login_event,
    )

    # ==========================================================================
    # Number of Prompts
    # ==========================================================================

    plan = user["subscription_plan"]

    prompt_count = {
        "Free": random.randint(1, 4),
        "Plus": random.randint(2, 7),
        "Pro": random.randint(4, 10),
        "Enterprise": random.randint(6, 15),
    }[plan]

    print(
        f"Generating {prompt_count} "
        f"prompt/response pairs..."
    )

    for _ in range(prompt_count):

        # ----------------------------------------------------------------------
        # Chat
        # ----------------------------------------------------------------------

        chat_event = {
            "event_id": generate_id("evt"),
            "event_type": "chat_started",
            "timestamp": iso_timestamp(),
            "user_id": user["user_id"],
            "session_id": session["session_id"],
            "subscription_id": subscription[
                "subscription_id"
            ],
            "chat_id": generate_id("chat"),
            "country": user["country"],
            "subscription_plan": user[
                "subscription_plan"
            ],
            "model_name": session[
                "model_name"
            ],
        }

        send_event(
            producer,
            chat_event,
        )

        # ----------------------------------------------------------------------
        # Prompt
        # ----------------------------------------------------------------------

        prompt = create_prompt(
            user,
            session,
        )

        prompt["chat_id"] = (
            chat_event["chat_id"]
        )

        prompt["subscription_id"] = (
            subscription["subscription_id"]
        )

        send_event(
            producer,
            prompt,
        )

        # ----------------------------------------------------------------------
        # Response
        # ----------------------------------------------------------------------

        response = create_response(
            user,
            session,
            prompt,
        )

        response["subscription_id"] = (
            subscription["subscription_id"]
        )

        send_event(
            producer,
            response,
        )

        # ----------------------------------------------------------------------
        # Feedback
        # ----------------------------------------------------------------------

        if random.random() < 0.35:

            feedback = create_feedback(
                user,
                session,
                prompt,
                response,
            )

            feedback["subscription_id"] = (
                subscription["subscription_id"]
            )

            send_event(
                producer,
                feedback,
            )

    # ==========================================================================
    # Logout
    # ==========================================================================

    logout = create_logout_event(
        user,
        session,
    )

    logout["subscription_id"] = (
        subscription["subscription_id"]
    )

    send_event(
        producer,
        logout,
    )

    print(
        f"SESSION COMPLETED | "
        f"user={user['user_id']} | "
        f"plan={user['subscription_plan']} | "
        f"subscription={subscription['subscription_id']}"
    )


# ==============================================================================
# Kafka Producer
# ==============================================================================

def create_producer() -> KafkaProducer:
    """Create Kafka producer."""

    print("=" * 80)
    print("Connecting to Kafka...")
    print("=" * 80)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
        batch_size=32768,
        compression_type="gzip",
    )

    print(
        "Kafka connection established."
    )

    print(
        f"Kafka Broker : "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Kafka Topic  : "
        f"{KAFKA_TOPIC}"
    )

    return producer


# ==============================================================================
# Main Simulation
# ==============================================================================

def run_simulation(
    producer: KafkaProducer,
    number_of_users: int,
    number_of_sessions: int,
) -> None:
    """Run one simulation batch."""

    print()
    print("=" * 80)
    print(
        "STARTING AI SAAS DATA SIMULATION"
    )
    print("=" * 80)

    print(
        f"Users planned    : "
        f"{number_of_users}"
    )

    print(
        f"Sessions planned : "
        f"{number_of_sessions}"
    )

    users = [
        create_user()
        for _ in range(number_of_users)
    ]

    event_count = 0

    for user in users:

        # ======================================================================
        # USER CREATED
        # ======================================================================

        send_event(
            producer,
            user,
        )

        # ======================================================================
        # ONE SUBSCRIPTION PER USER
        # ======================================================================

        subscription = create_subscription(
            user
        )

        send_event(
            producer,
            subscription,
        )

        # Keep the relationship available
        # for all downstream session events.
        user["subscription_id"] = (
            subscription["subscription_id"]
        )

        user["subscription_status"] = (
            subscription[
                "subscription_status"
            ]
        )

        # ======================================================================
        # SESSIONS
        # ======================================================================

        sessions_for_user = max(
            1,
            round(
                number_of_sessions
                / number_of_users
            ),
        )

        for _ in range(
            sessions_for_user
        ):

            event_count += 1

            simulate_user_session(
                producer,
                user,
                subscription,
                event_count,
            )

        # ======================================================================
        # BILLING
        # ======================================================================
        #
        # Billing is generated once per user
        # in this simulation batch.
        #
        # This prevents multiple payments from
        # being created simply because a user
        # has multiple sessions.
        # ======================================================================

        if random.random() < 0.20:

            billing = create_billing_event(
                user,
                subscription,
            )

            if billing:

                send_event(
                    producer,
                    billing,
                )

    producer.flush()

    print()
    print("=" * 80)
    print(
        "SIMULATION BATCH COMPLETED"
    )
    print("=" * 80)


# ==============================================================================
# Main
# ==============================================================================

def main() -> None:

    producer = None

    try:

        producer = create_producer()

        print(
            f"Publishing to topic: "
            f"{KAFKA_TOPIC}"
        )

        if CONTINUOUS_MODE:

            print()
            print(
                "CONTINUOUS MODE ENABLED"
            )

            print(
                "Producer will continuously "
                "generate fresh events."
            )

            print(
                "Press Ctrl+C to stop."
            )

            print()

            while True:

                run_simulation(
                    producer,
                    SIMULATION_USERS,
                    SIMULATION_SESSIONS,
                )

                # Small pause between batches.
                time.sleep(2)

        else:

            run_simulation(
                producer,
                SIMULATION_USERS,
                SIMULATION_SESSIONS,
            )

    except KeyboardInterrupt:

        print()

        print(
            "Stopping producer..."
        )

    except Exception as error:

        print()
        print("=" * 80)
        print(
            "PRODUCER ERROR"
        )
        print("=" * 80)

        print(error)

        raise

    finally:

        if producer:

            producer.flush()

            producer.close()

            print()
            print("=" * 80)
            print(
                "Kafka Producer Closed"
            )
            print("=" * 80)


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":
    main()