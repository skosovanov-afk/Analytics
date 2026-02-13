from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


@lru_cache(maxsize=1)
def _load_env_file() -> None:
    # Load project-local .env once (if present).
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env", override=False)


def get_secret_key() -> str:
    _load_env_file()
    # Dev-only: allow override via env var.
    return os.environ.get("PRODUCT_SECRET_KEY", "dev-secret-key-change-me")


def get_database_url() -> str:
    _load_env_file()
    # Use local sqlite file by default.
    return os.environ.get("PRODUCT_DATABASE_URL", "sqlite+aiosqlite:///./product.db")


def get_supabase_url() -> str:
    _load_env_file()
    return os.environ.get("SUPABASE_URL", "")


def get_supabase_service_role_key() -> str:
    _load_env_file()
    return os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

