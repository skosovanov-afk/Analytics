from __future__ import annotations

import argparse
import datetime as dt
import os
from typing import Any

import requests

from app.supabase_client import get_supabase_client


def _smartlead_get(base_url: str, api_key: str, path: str) -> Any:
    sep = "&" if "?" in path else "?"
    url = f"{base_url}{path}{sep}api_key={api_key}"
    res = requests.get(url, headers={"accept": "application/json"}, timeout=60)
    res.raise_for_status()
    return res.json()


def _to_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "leads", "campaigns"):
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def _pick_email(lead: dict[str, Any], row: dict[str, Any]) -> str | None:
    for key in ("email", "work_email", "personal_email", "contact_email"):
        val = lead.get(key)
        if val:
            return str(val).strip().lower()
    for key in ("email", "lead_email", "contact_email"):
        val = row.get(key)
        if val:
            return str(val).strip().lower()
    return None


def _pick_lead_id(lead: dict[str, Any], row: dict[str, Any]) -> int | None:
    for key in ("id", "lead_id"):
        val = lead.get(key)
        if val is not None:
            try:
                return int(str(val))
            except ValueError:
                pass
    for key in ("lead_id", "id"):
        val = row.get(key)
        if val is not None:
            try:
                return int(str(val))
            except ValueError:
                pass
    return None


def _chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def run(max_campaigns: int | None, campaign_limit: int, lead_page_size: int) -> None:
    api_key = (os.environ.get("SMARTLEAD_API_KEY") or "").strip()
    base_url = (os.environ.get("SMARTLEAD_BASE_URL") or "https://server.smartlead.ai").strip().rstrip("/")
    if not api_key:
        raise RuntimeError("SMARTLEAD_API_KEY is required")

    supabase = get_supabase_client()

    campaigns = _to_list(_smartlead_get(base_url, api_key, "/api/v1/campaigns"))
    if max_campaigns is not None:
        campaigns = campaigns[: max(0, max_campaigns)]

    print(f"campaigns_found={len(campaigns)}")

    total_rows = 0
    total_upserts = 0
    processed_campaigns = 0

    for c in campaigns:
        campaign_id = c.get("id")
        if campaign_id is None:
            continue
        campaign_id = int(str(campaign_id))
        campaign_name = c.get("name")

        offset = 0
        rows_for_campaign = 0
        payload_to_upsert: list[dict[str, Any]] = []

        while True:
            data = _smartlead_get(
                base_url,
                api_key,
                f"/api/v1/campaigns/{campaign_id}/leads?limit={lead_page_size}&offset={offset}",
            )
            rows = _to_list(data)
            if not rows:
                break

            for row in rows:
                lead = row.get("lead") if isinstance(row.get("lead"), dict) else {}
                lead_id = _pick_lead_id(lead, row)
                email = _pick_email(lead, row)
                if lead_id is None:
                    # We upsert by (campaign_id, lead_id); skip rows without lead_id.
                    continue

                rec = {
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "lead_id": lead_id,
                    "email": email,
                    "first_name": lead.get("first_name") or lead.get("name") or row.get("first_name"),
                    "last_name": lead.get("last_name") or row.get("last_name"),
                    "company": lead.get("company") or row.get("company"),
                    "linkedin": lead.get("linkedin") or lead.get("linkedin_url") or row.get("linkedin"),
                    "lead_status": row.get("status") or lead.get("status"),
                    "lead_category_id": row.get("lead_category_id"),
                    "is_unsubscribed": lead.get("is_unsubscribed"),
                    "emails_sent_count": lead.get("emails_sent_count"),
                    "last_email_date": lead.get("last_email_date"),
                    "created_at_source": row.get("created_at") or lead.get("created_at"),
                    "updated_at_source": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "raw_payload": row,
                    "synced_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                }
                payload_to_upsert.append(rec)
                rows_for_campaign += 1

            if len(rows) < lead_page_size:
                break
            offset += len(rows)

        if payload_to_upsert:
            # Upsert by (campaign_id, lead_id) - no delete needed, no data loss on crash.
            for chunk in _chunked(payload_to_upsert, campaign_limit):
                supabase.table("smartlead_leads").upsert(
                    chunk, on_conflict="campaign_id,lead_id"
                ).execute()
                total_upserts += len(chunk)

        total_rows += rows_for_campaign
        processed_campaigns += 1
        print(
            f"campaign_id={campaign_id} leads_seen={rows_for_campaign} "
            f"upserted_running={total_upserts}"
        )

    # Store service checkpoint.
    try:
        # Some sync_state schemas use INTEGER value type; store unix timestamp there.
        supabase.table("sync_state").upsert(
            {"key": "smartlead_leads_last_full_sync_ts", "value": int(dt.datetime.now(dt.timezone.utc).timestamp())},
            on_conflict="key",
        ).execute()
    except Exception:
        # Non-fatal for one-off backfill runs.
        pass

    print(
        "done "
        f"campaigns_processed={processed_campaigns} "
        f"leads_seen_total={total_rows} "
        f"upserts_total={total_upserts}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill smartlead_leads from SmartLead API")
    parser.add_argument("--max-campaigns", type=int, default=None, help="Limit number of campaigns for test runs")
    parser.add_argument("--upsert-batch-size", type=int, default=500, help="Supabase upsert chunk size")
    parser.add_argument("--lead-page-size", type=int, default=100, help="SmartLead leads page size")
    args = parser.parse_args()

    run(
        max_campaigns=args.max_campaigns,
        campaign_limit=max(1, min(2000, args.upsert_batch_size)),
        lead_page_size=max(1, min(500, args.lead_page_size)),
    )


if __name__ == "__main__":
    main()
