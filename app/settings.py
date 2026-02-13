from __future__ import annotations

import os


def get_secret_key() -> str:
    # Dev-only: allow override via env var.
    return os.environ.get("PRODUCT_SECRET_KEY", "dev-secret-key-change-me")


def get_database_url() -> str:
    # Use local sqlite file by default.
    return os.environ.get("PRODUCT_DATABASE_URL", "sqlite+aiosqlite:///./product.db")

