from __future__ import annotations

from typing import Any

from supabase import Client, create_client

from app.settings import get_supabase_service_role_key, get_supabase_url

_client: Client | None = None


def get_supabase_client() -> Client:
    global _client
    if _client is None:
        url = get_supabase_url().strip()
        key = get_supabase_service_role_key().strip()
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
        _client = create_client(url, key)
    return _client


def reset_supabase_client() -> None:
    """Call to force a fresh client on next access."""
    global _client
    _client = None


def insert_row(table: str, record: dict[str, Any]) -> list[dict[str, Any]]:
    client = get_supabase_client()
    response = client.table(table).insert(record).execute()
    return response.data or []


def select_rows(
    table: str,
    limit: int | None = None,
    order_by: str | None = None,
    desc: bool = False,
) -> list[dict[str, Any]]:
    client = get_supabase_client()
    query = client.table(table).select("*")
    if order_by:
        query = query.order(order_by, desc=desc)
    if limit is not None:
        query = query.limit(limit)
    response = query.execute()
    return response.data or []
