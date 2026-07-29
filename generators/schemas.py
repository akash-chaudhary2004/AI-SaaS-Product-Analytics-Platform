"""
Schema definitions and constants for the AI SaaS Product Analytics Platform.

This file contains reusable lookup values used by the event generator
and future streaming components.
"""

# ==========================================================
# EVENT TYPES
# ==========================================================

EVENT_TYPES = [
    "user_login",
    "chat_started",
    "prompt_submitted",
    "model_selected",
    "response_generated",
    "token_usage",
    "billing_completed",
    "feedback_submitted",
    "subscription_upgraded",
    "user_logout",
]

# ==========================================================
# SUBSCRIPTION PLANS
# ==========================================================

SUBSCRIPTION_PLANS = [
    "Free",
    "Plus",
    "Pro",
    "Enterprise",
]

# ==========================================================
# AI MODELS
# ==========================================================

AI_MODELS = [
    "GPT-5",
    "Claude",
    "Gemini",
    "Llama",
]

# ==========================================================
# PROMPT CATEGORIES
# ==========================================================

PROMPT_CATEGORIES = [
    "Coding",
    "Writing",
    "Marketing",
    "Finance",
    "Education",
    "Research",
]

# ==========================================================
# DEVICE TYPES
# ==========================================================

DEVICE_TYPES = [
    "Desktop",
    "Mobile",
    "Tablet",
]

# ==========================================================
# OPERATING SYSTEMS
# ==========================================================

OPERATING_SYSTEMS = [
    "Windows",
    "macOS",
    "Linux",
    "Android",
    "iOS",
]

# ==========================================================
# BROWSERS
# ==========================================================

BROWSERS = [
    "Chrome",
    "Edge",
    "Firefox",
    "Safari",
]

# ==========================================================
# FEEDBACK
# ==========================================================

FEEDBACK_OPTIONS = [
    "thumbs_up",
    "thumbs_down",
]

# ==========================================================
# COUNTRIES
# ==========================================================

COUNTRIES = [
    "India",
    "United States",
    "United Kingdom",
    "Germany",
    "Canada",
    "Australia",
    "Singapore",
]