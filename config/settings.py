"""
Central settings module.
Reads from .env (or environment variables) and exposes typed Path objects.
"""
from pathlib import Path
from dotenv import load_dotenv
import os

# Project root = the directory that contains config/, api/, data/, etc.
PROJECT_ROOT = Path(__file__).parent.parent

load_dotenv(PROJECT_ROOT / ".env", override=False)


def _resolve(env_key: str, default: str) -> Path:
    raw = os.getenv(env_key, default)
    p = Path(raw)
    return p if p.is_absolute() else PROJECT_ROOT / p


BRONZE_PATH: Path = _resolve("BRONZE_PATH", "data/bronze")
SILVER_PATH: Path = _resolve("SILVER_PATH", "data/silver")
GOLD_PATH:   Path = _resolve("GOLD_PATH",   "data/gold")
DUCKDB_PATH: Path = _resolve("DUCKDB_PATH", "data/gold/statsbomb.duckdb")

API_HOST: str = os.getenv("API_HOST", "127.0.0.1")
API_PORT: int = int(os.getenv("API_PORT", "8000"))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
