from __future__ import annotations

from functools import lru_cache
from typing import Any

from supabase import Client, create_client

from app.settings import get_supabase_service_role_key, get_supabase_url


@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    url = get_supabase_url().strip()
    key = get_supabase_service_role_key().strip()
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    return create_client(url, key)


def insert_row(table: str, record: dict[str, Any]) -> list[dict[str, Any]]:
    client = get_supabase_client()
    response = client.table(table).insert(record).execute()
    return response.data or []
