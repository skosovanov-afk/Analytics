from __future__ import annotations

import argparse
import datetime as dt
import os
from collections import defaultdict
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


def _chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _safe_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(str(v))
    except Exception:
        return None


def _safe_iso(v: Any) -> str | None:
    if not v:
        return None
    s = str(v).strip()
    return s or None


def _load_campaign_email_to_lead_id(supabase) -> dict[int, dict[str, int]]:
    mapping: dict[int, dict[str, int]] = defaultdict(dict)
    offset = 0
    limit = 1000
    while True:
        rows = (
            supabase.table("smartlead_leads")
            .select("campaign_id,email,lead_id")
            .order("id")
            .range(offset, offset + limit - 1)
            .execute()
            .data
            or []
        )
        if not rows:
            break
        for row in rows:
            cid = _safe_int(row.get("campaign_id"))
            lid = _safe_int(row.get("lead_id"))
            email = str(row.get("email") or "").strip().lower()
            if cid is None or lid is None or not email:
                continue
            mapping[cid][email] = lid
        if len(rows) < limit:
            break
        offset += len(rows)
    return mapping


def _rebuild_daily_stats(supabase) -> None:
    # Pull events and aggregate in Python (portable, no SQL RPC requirement).
    agg: dict[tuple[str, int, int], dict[str, Any]] = {}
    offset = 0
    limit = 1000

    while True:
        rows = (
            supabase.table("smartlead_events")
            .select(
                "campaign_id,campaign_name,sequence_number,event_type,occurred_at,open_count,click_count,email"
            )
            .order("id")
            .range(offset, offset + limit - 1)
            .execute()
            .data
            or []
        )
        if not rows:
            break

        for r in rows:
            occurred_at = str(r.get("occurred_at") or "")
            if not occurred_at:
                continue
            date = occurred_at[:10]
            cid = _safe_int(r.get("campaign_id"))
            if cid is None:
                continue
            touch = _safe_int(r.get("sequence_number")) or 0
            key = (date, cid, touch)
            slot = agg.get(key)
            if slot is None:
                slot = {
                    "date": date,
                    "campaign_id": cid,
                    "campaign_name": r.get("campaign_name"),
                    "touch_number": touch,
                    "sent_count": 0,
                    "reply_count": 0,
                    "open_count": 0,
                    "click_count": 0,
                    "emails": set(),
                    "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                }
                agg[key] = slot
            event_type = str(r.get("event_type") or "").lower()
            if event_type == "sent":
                slot["sent_count"] += 1
            elif event_type == "reply":
                slot["reply_count"] += 1
            slot["open_count"] += _safe_int(r.get("open_count")) or 0
            slot["click_count"] += _safe_int(r.get("click_count")) or 0
            email = str(r.get("email") or "").strip().lower()
            if email:
                slot["emails"].add(email)

        if len(rows) < limit:
            break
        offset += len(rows)

    # Group payload by campaign_id for per-campaign upsert (crash-safe).
    by_campaign: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for v in agg.values():
        row = {
            "date": v["date"],
            "campaign_id": v["campaign_id"],
            "campaign_name": v["campaign_name"],
            "touch_number": v["touch_number"],
            "sent_count": v["sent_count"],
            "reply_count": v["reply_count"],
            "open_count": v["open_count"],
            "click_count": v["click_count"],
            "unique_leads_count": len(v["emails"]),
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        by_campaign[v["campaign_id"]].append(row)

    for cid, rows in by_campaign.items():
        supabase.table("smartlead_stats_daily").delete().eq("campaign_id", cid).execute()
        for chunk in _chunked(rows, 500):
            supabase.table("smartlead_stats_daily").insert(chunk).execute()


def run(max_campaigns: int | None, page_size: int, insert_batch: int) -> None:
    api_key = (os.environ.get("SMARTLEAD_API_KEY") or "").strip()
    base_url = (os.environ.get("SMARTLEAD_BASE_URL") or "https://server.smartlead.ai").strip().rstrip("/")
    if not api_key:
        raise RuntimeError("SMARTLEAD_API_KEY is required")

    supabase = get_supabase_client()
    campaign_email_to_lead = _load_campaign_email_to_lead_id(supabase)

    campaigns = _to_list(_smartlead_get(base_url, api_key, "/api/v1/campaigns"))
    if max_campaigns is not None:
        campaigns = campaigns[: max(0, max_campaigns)]

    total_inserted = 0
    processed_campaigns = 0
    sent_total = 0
    reply_total = 0

    print(f"campaigns_found={len(campaigns)}")

    for c in campaigns:
        cid = _safe_int(c.get("id"))
        if cid is None:
            continue
        cname = c.get("name")
        email_map = campaign_email_to_lead.get(cid, {})

        offset = 0
        rows_seen = 0
        to_insert: list[dict[str, Any]] = []

        while True:
            stats = _to_list(
                _smartlead_get(base_url, api_key, f"/api/v1/campaigns/{cid}/statistics?limit={page_size}&offset={offset}")
            )
            if not stats:
                break
            rows_seen += len(stats)

            for s in stats:
                email = str(s.get("lead_email") or "").strip().lower()
                lead_id = email_map.get(email)
                seq = _safe_int(s.get("sequence_number"))
                stats_id = str(s.get("stats_id") or "").strip() or None
                subject = s.get("email_subject")
                body = s.get("email_message")
                open_count = _safe_int(s.get("open_count")) or 0
                click_count = _safe_int(s.get("click_count")) or 0
                is_unsub = bool(s.get("is_unsubscribed")) if s.get("is_unsubscribed") is not None else None
                is_bounced = bool(s.get("is_bounced")) if s.get("is_bounced") is not None else None

                sent_time = _safe_iso(s.get("sent_time"))
                if sent_time:
                    to_insert.append(
                        {
                            "campaign_id": cid,
                            "campaign_name": cname,
                            "lead_id": lead_id,
                            "email": email or None,
                            "event_type": "sent",
                            "sequence_number": seq,
                            "occurred_at": sent_time,
                            "stats_id": stats_id,
                            "message_id": None,
                            "subject": subject,
                            "message_body": body,
                            "from_email": None,
                            "to_email": email or None,
                            "open_count": open_count,
                            "click_count": click_count,
                            "is_unsubscribed": is_unsub,
                            "is_bounced": is_bounced,
                            "raw_payload": s,
                            "synced_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                        }
                    )
                    sent_total += 1

                reply_time = _safe_iso(s.get("reply_time"))
                if reply_time:
                    to_insert.append(
                        {
                            "campaign_id": cid,
                            "campaign_name": cname,
                            "lead_id": lead_id,
                            "email": email or None,
                            "event_type": "reply",
                            "sequence_number": seq,
                            "occurred_at": reply_time,
                            "stats_id": stats_id,
                            "message_id": None,
                            "subject": subject,
                            "message_body": body,
                            "from_email": email or None,
                            "to_email": None,
                            "open_count": 0,
                            "click_count": 0,
                            "is_unsubscribed": is_unsub,
                            "is_bounced": is_bounced,
                            "raw_payload": s,
                            "synced_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                        }
                    )
                    reply_total += 1

            if len(stats) < page_size:
                break
            offset += len(stats)

        # Delete then insert per-campaign; only delete once we have data ready.
        if to_insert:
            supabase.table("smartlead_events").delete().eq("campaign_id", cid).execute()
            for chunk in _chunked(to_insert, insert_batch):
                supabase.table("smartlead_events").insert(chunk).execute()
                total_inserted += len(chunk)

        processed_campaigns += 1
        print(
            f"campaign_id={cid} stats_rows={rows_seen} events_inserted={len(to_insert)} "
            f"inserted_running={total_inserted}"
        )

    _rebuild_daily_stats(supabase)
    try:
        supabase.table("sync_state").upsert(
            {"key": "smartlead_events_last_full_sync_ts", "value": int(dt.datetime.now(dt.timezone.utc).timestamp())},
            on_conflict="key",
        ).execute()
    except Exception:
        pass

    print(
        "done "
        f"campaigns_processed={processed_campaigns} "
        f"events_inserted_total={total_inserted} "
        f"sent_total={sent_total} "
        f"reply_total={reply_total}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill smartlead_events and smartlead_stats_daily")
    parser.add_argument("--max-campaigns", type=int, default=None, help="Limit campaigns for test runs")
    parser.add_argument("--page-size", type=int, default=100, help="SmartLead statistics page size")
    parser.add_argument("--insert-batch-size", type=int, default=500, help="Supabase insert chunk size")
    args = parser.parse_args()

    run(
        max_campaigns=args.max_campaigns,
        page_size=max(1, min(500, args.page_size)),
        insert_batch=max(1, min(2000, args.insert_batch_size)),
    )


if __name__ == "__main__":
    main()
