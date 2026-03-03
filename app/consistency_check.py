from __future__ import annotations

from typing import Any

import httpx

from app.settings import (
    get_smartlead_api_key,
    get_smartlead_base_url,
    get_supabase_service_role_key,
    get_supabase_url,
)


def _sl_get(client: httpx.Client, base_url: str, api_key: str, path: str) -> Any:
    sep = "&" if "?" in path else "?"
    url = f"{base_url.rstrip('/')}{path}{sep}api_key={api_key}"
    resp = client.get(url, headers={"accept": "application/json"})
    resp.raise_for_status()
    return resp.json()


def _to_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "leads", "campaigns"):
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def _supabase_count(
    client: httpx.Client,
    supabase_url: str,
    service_key: str,
    table: str,
    *,
    filters: dict[str, str] | None = None,
) -> int:
    params: dict[str, str] = {"select": "*", "limit": "1"}
    if filters:
        params.update(filters)
    resp = client.get(
        f"{supabase_url.rstrip('/')}/rest/v1/{table}",
        params=params,
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Accept": "application/json",
            "Prefer": "count=exact",
        },
    )
    resp.raise_for_status()
    cr = resp.headers.get("Content-Range", "*/0")
    try:
        return int(cr.split("/")[-1])
    except Exception:
        return 0


def run_consistency_check(include_lead_rows: bool = True) -> dict[str, Any]:
    smartlead_key = get_smartlead_api_key().strip()
    smartlead_base = get_smartlead_base_url().strip() or "https://server.smartlead.ai"
    supabase_url = get_supabase_url().strip()
    service_key = get_supabase_service_role_key().strip()

    if not smartlead_key:
        raise RuntimeError("SMARTLEAD_API_KEY is not configured")
    if not supabase_url or not service_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")

    with httpx.Client(timeout=60.0) as client:
        campaigns = _to_list(_sl_get(client, smartlead_base, smartlead_key, "/api/v1/campaigns"))

        live_sent = 0
        live_reply = 0
        live_lead_rows = 0
        empty_campaigns = 0

        for c in campaigns:
            cid = c.get("id")
            if cid is None:
                continue
            analytics = _sl_get(client, smartlead_base, smartlead_key, f"/api/v1/campaigns/{cid}/analytics")
            live_sent += int(analytics.get("sent_count") or 0)
            live_reply += int(analytics.get("reply_count") or 0)

            if include_lead_rows:
                page_size = 100
                offset = 0
                campaign_rows = 0
                while True:
                    rows = _to_list(
                        _sl_get(
                            client,
                            smartlead_base,
                            smartlead_key,
                            f"/api/v1/campaigns/{cid}/leads?limit={page_size}&offset={offset}",
                        )
                    )
                    if not rows:
                        break
                    campaign_rows += len(rows)
                    if len(rows) < page_size:
                        break
                    offset += len(rows)
                live_lead_rows += campaign_rows
                if campaign_rows == 0:
                    empty_campaigns += 1

        supabase_leads = _supabase_count(client, supabase_url, service_key, "smartlead_leads")
        supabase_sent = _supabase_count(
            client, supabase_url, service_key, "smartlead_events", filters={"event_type": "eq.sent"}
        )
        supabase_reply = _supabase_count(
            client, supabase_url, service_key, "smartlead_events", filters={"event_type": "eq.reply"}
        )

    checks = {
        "sent_match": supabase_sent == live_sent,
        "reply_match": supabase_reply == live_reply,
    }
    if include_lead_rows:
        checks["lead_rows_match"] = supabase_leads == live_lead_rows

    return {
        "ok": all(checks.values()),
        "mode": "full" if include_lead_rows else "quick",
        "checks": checks,
        "smartlead": {
            "campaigns": len(campaigns),
            "empty_campaigns": empty_campaigns if include_lead_rows else None,
            "lead_rows_sum": live_lead_rows if include_lead_rows else None,
            "sent_sum": live_sent,
            "reply_sum": live_reply,
        },
        "supabase": {
            "smartlead_leads_rows": supabase_leads,
            "smartlead_events_sent": supabase_sent,
            "smartlead_events_reply": supabase_reply,
        },
        "diff": {
            "lead_rows": (supabase_leads - live_lead_rows) if include_lead_rows else None,
            "sent": supabase_sent - live_sent,
            "reply": supabase_reply - live_reply,
        },
    }


def fetch_smartlead_campaign_analytics(limit: int | None = None) -> dict[str, Any]:
    smartlead_key = get_smartlead_api_key().strip()
    smartlead_base = get_smartlead_base_url().strip() or "https://server.smartlead.ai"
    if not smartlead_key:
        raise RuntimeError("SMARTLEAD_API_KEY is not configured")

    with httpx.Client(timeout=60.0) as client:
        campaigns = _to_list(_sl_get(client, smartlead_base, smartlead_key, "/api/v1/campaigns"))
        out: list[dict[str, Any]] = []
        sent_sum = 0
        reply_sum = 0
        total_sum = 0

        for c in campaigns:
            cid = c.get("id")
            if cid is None:
                continue
            analytics = _sl_get(client, smartlead_base, smartlead_key, f"/api/v1/campaigns/{cid}/analytics")
            sent = int(analytics.get("sent_count") or 0)
            reply = int(analytics.get("reply_count") or 0)
            total = int(analytics.get("total_count") or 0)
            sent_sum += sent
            reply_sum += reply
            total_sum += total
            out.append(
                {
                    "campaign_id": cid,
                    "campaign_name": c.get("name"),
                    "status": c.get("status"),
                    "sent_total": sent,
                    "reply_total": reply,
                    "total_leads": total,
                    "reply_rate_pct": round((reply / sent) * 100, 2) if sent else 0.0,
                }
            )

    out.sort(key=lambda x: int(x.get("sent_total") or 0), reverse=True)
    if limit is not None:
        out = out[: max(1, limit)]

    return {
        "sent_total": sent_sum,
        "reply_total": reply_sum,
        "total_leads": total_sum,
        "reply_rate_pct": round((reply_sum / sent_sum) * 100, 2) if sent_sum else 0.0,
        "campaigns_count": len(campaigns),
        "campaigns": out,
    }
