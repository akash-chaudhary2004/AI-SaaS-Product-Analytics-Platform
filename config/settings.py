"""
Configuration for the AI SaaS Product Analytics Platform.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"

DEFAULT_RECORDS = 10000
DEFAULT_OUTPUT_FILE = RAW_DIR / "ai_events.csv"
