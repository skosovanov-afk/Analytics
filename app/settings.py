from __future__ import annotations

import os
import secrets
import warnings
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
    key = os.environ.get("PRODUCT_SECRET_KEY", "")
    if not key:
        warnings.warn(
            "PRODUCT_SECRET_KEY not set - generating random key. "
            "Set PRODUCT_SECRET_KEY env var in production!",
            stacklevel=2,
        )
        return secrets.token_urlsafe(32)
    return key


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


def get_smartlead_api_key() -> str:
    _load_env_file()
    return os.environ.get("SMARTLEAD_API_KEY", "")


def get_smartlead_base_url() -> str:
    _load_env_file()
    return os.environ.get("SMARTLEAD_BASE_URL", "https://server.smartlead.ai")

