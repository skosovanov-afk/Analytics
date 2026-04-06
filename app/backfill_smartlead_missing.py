from __future__ import annotations

"""
Backfill missing Smartlead campaigns into smartlead_events and smartlead_stats_daily.

Unlike backfill_smartlead_events.py (which syncs all *active* campaigns and
rebuilds the whole stats table from scratch), this script:

  1. Fetches ALL campaigns from Smartlead, including archived/deleted ones, by
     trying the status=all variant first.
  2. Identifies which campaign IDs are NOT yet in smartlead_stats_daily.
  3. Processes only those missing campaigns (or a force-list supplied via
     --campaign-names), aggregating events into daily stats.
  4. Upserts rows into smartlead_stats_daily using on_conflict — it never
     deletes existing rows for other campaigns.

Usage examples:
    # Process only campaigns not already in DB
    python -m app.backfill_smartlead_missing

    # Force-reprocess specific campaigns by name (comma-separated)
    python -m app.backfill_smartlead_missing \
        --campaign-names "SBC Conference,Cross-Border Corporate Payments,Telecom"

    # Dry run — print what would be upserted, write nothing
    python -m app.backfill_smartlead_missing --dry-run

    # Adjust pagination / batch sizes
    python -m app.backfill_smartlead_missing --page-size 200 --insert-batch-size 500
"""

import argparse
import datetime as dt
import os
from collections import defaultdict
from typing import Any

import requests

from app.supabase_client import get_supabase_client


# ---------------------------------------------------------------------------
# Low-level helpers (copied from backfill_smartlead_events.py so this script
# is fully self-contained and can be run independently)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Campaign discovery
# ---------------------------------------------------------------------------


def _fetch_all_campaigns(base_url: str, api_key: str) -> list[dict[str, Any]]:
    """Try to fetch all campaigns including archived ones.

    Smartlead does not always honour status=all, so we try both endpoints and
    merge results (deduplicating by id).
    """
    seen_ids: set[int] = set()
    merged: list[dict[str, Any]] = []

    def _add(rows: list[dict[str, Any]]) -> None:
        for row in rows:
            cid = _safe_int(row.get("id"))
            if cid is not None and cid not in seen_ids:
                seen_ids.add(cid)
                merged.append(row)

    # Attempt 1: status=all (may return archived campaigns)
    try:
        rows = _to_list(_smartlead_get(base_url, api_key, "/api/v1/campaigns?status=all"))
        _add(rows)
        print(f"  /campaigns?status=all  -> {len(rows)} rows")
    except Exception as exc:
        print(f"  /campaigns?status=all  -> error: {exc}")

    # Attempt 2: default active endpoint
    try:
        rows = _to_list(_smartlead_get(base_url, api_key, "/api/v1/campaigns"))
        _add(rows)
        print(f"  /campaigns (default)   -> {len(rows)} rows (after dedup: {len(merged)} total)")
    except Exception as exc:
        print(f"  /campaigns (default)   -> error: {exc}")

    return merged


# ---------------------------------------------------------------------------
# Existing DB state
# ---------------------------------------------------------------------------


def _existing_campaign_ids(supabase) -> set[int]:
    """Return campaign_ids that already have rows in smartlead_stats_daily."""
    existing: set[int] = set()
    offset = 0
    limit = 1000
    while True:
        rows = (
            supabase.table("smartlead_stats_daily")
            .select("campaign_id")
            .order("campaign_id")
            .range(offset, offset + limit - 1)
            .execute()
            .data
            or []
        )
        if not rows:
            break
        for row in rows:
            cid = _safe_int(row.get("campaign_id"))
            if cid is not None:
                existing.add(cid)
        if len(rows) < limit:
            break
        offset += len(rows)
    return existing


# ---------------------------------------------------------------------------
# Per-campaign processing
# ---------------------------------------------------------------------------


def _fetch_campaign_stats(
    base_url: str,
    api_key: str,
    cid: int,
    cname: str,
    page_size: int,
) -> tuple[list[dict[str, Any]], int, int]:
    """Fetch all statistics rows for a single campaign.

    Returns (raw_events_list, sent_count, reply_count).
    """
    events: list[dict[str, Any]] = []
    sent_count = 0
    reply_count = 0
    offset = 0

    while True:
        stats = _to_list(
            _smartlead_get(
                base_url,
                api_key,
                f"/api/v1/campaigns/{cid}/statistics?limit={page_size}&offset={offset}",
            )
        )
        if not stats:
            break

        for s in stats:
            email = str(s.get("lead_email") or "").strip().lower()
            seq = _safe_int(s.get("sequence_number"))
            stats_id = str(s.get("stats_id") or "").strip() or None
            subject = s.get("email_subject")
            body = s.get("email_message")
            open_count = _safe_int(s.get("open_count")) or 0
            click_count = _safe_int(s.get("click_count")) or 0
            is_unsub = bool(s.get("is_unsubscribed")) if s.get("is_unsubscribed") is not None else None
            is_bounced = bool(s.get("is_bounced")) if s.get("is_bounced") is not None else None
            synced_at = dt.datetime.now(dt.timezone.utc).isoformat()

            sent_time = _safe_iso(s.get("sent_time"))
            if sent_time:
                events.append(
                    {
                        "campaign_id": cid,
                        "campaign_name": cname,
                        "lead_id": None,
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
                        "synced_at": synced_at,
                    }
                )
                sent_count += 1

            reply_time = _safe_iso(s.get("reply_time"))
            if reply_time:
                events.append(
                    {
                        "campaign_id": cid,
                        "campaign_name": cname,
                        "lead_id": None,
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
                        "synced_at": synced_at,
                    }
                )
                reply_count += 1

        if len(stats) < page_size:
            break
        offset += len(stats)

    return events, sent_count, reply_count


def _build_daily_stats_from_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate a flat list of event dicts into smartlead_stats_daily rows."""
    agg: dict[tuple[str, int, int], dict[str, Any]] = {}

    for r in events:
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

    rows: list[dict[str, Any]] = []
    for v in agg.values():
        rows.append(
            {
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
        )
    return rows


# ---------------------------------------------------------------------------
# Main run logic
# ---------------------------------------------------------------------------


def run(
    campaign_names: list[str],
    page_size: int,
    insert_batch: int,
    dry_run: bool,
) -> None:
    api_key = (os.environ.get("SMARTLEAD_API_KEY") or "").strip()
    base_url = (
        os.environ.get("SMARTLEAD_BASE_URL") or "https://server.smartlead.ai"
    ).strip().rstrip("/")

    if not api_key:
        raise RuntimeError("SMARTLEAD_API_KEY environment variable is required")

    supabase = get_supabase_client()

    # --- 1. Fetch all campaigns from Smartlead ---
    print("Fetching campaigns from Smartlead...")
    all_campaigns = _fetch_all_campaigns(base_url, api_key)
    print(f"Total unique campaigns found: {len(all_campaigns)}")

    # --- 2. Determine which campaigns to process ---
    existing_ids = _existing_campaign_ids(supabase)
    print(f"Campaign IDs already in smartlead_stats_daily: {len(existing_ids)}")

    # Build lookup: normalised name -> campaign dict
    name_to_campaign: dict[str, dict[str, Any]] = {}
    for c in all_campaigns:
        name = str(c.get("name") or "").strip()
        if name:
            name_to_campaign[name.lower()] = c

    to_process: list[dict[str, Any]] = []

    if campaign_names:
        # Force-process named campaigns regardless of whether they are in DB
        force_names_lower = {n.strip().lower() for n in campaign_names if n.strip()}
        matched: set[str] = set()
        for c in all_campaigns:
            name = str(c.get("name") or "").strip()
            if name.lower() in force_names_lower:
                to_process.append(c)
                matched.add(name.lower())
        missing_names = force_names_lower - matched
        if missing_names:
            print(
                f"WARNING: These --campaign-names were not found in Smartlead: "
                + ", ".join(sorted(missing_names))
            )
        print(
            f"Force-processing {len(to_process)} campaign(s) by name "
            f"(--campaign-names flag)"
        )
    else:
        # Auto-detect: campaigns not yet in smartlead_stats_daily
        for c in all_campaigns:
            cid = _safe_int(c.get("id"))
            if cid is not None and cid not in existing_ids:
                to_process.append(c)
        print(f"Campaigns missing from smartlead_stats_daily: {len(to_process)}")
        for c in to_process:
            print(f"  - [{c.get('id')}] {c.get('name')}  status={c.get('status')}")

    if not to_process:
        print("Nothing to do — all campaigns are already present in the DB.")
        return

    # --- 3. Process each campaign ---
    grand_sent = 0
    grand_reply = 0
    grand_events = 0
    grand_daily_rows = 0

    for c in to_process:
        cid = _safe_int(c.get("id"))
        if cid is None:
            continue
        cname = str(c.get("name") or "").strip()

        print(f"\nProcessing campaign [{cid}] '{cname}' ...")

        events, sent_count, reply_count = _fetch_campaign_stats(
            base_url, api_key, cid, cname, page_size
        )
        daily_rows = _build_daily_stats_from_events(events)

        print(
            f"  stats_api_rows fetched, events built: sent={sent_count} "
            f"reply={reply_count} total_events={len(events)} "
            f"daily_agg_rows={len(daily_rows)}"
        )

        if dry_run:
            print(f"  [dry-run] Would insert {len(events)} events and upsert {len(daily_rows)} daily rows — skipping writes.")
            grand_sent += sent_count
            grand_reply += reply_count
            grand_events += len(events)
            grand_daily_rows += len(daily_rows)
            continue

        # Insert raw events (delete existing first to avoid duplicates on re-run).
        # Only delete after we have fetched data, so a crash doesn't wipe events
        # without replacement.
        if events:
            try:
                supabase.table("smartlead_events").delete().eq("campaign_id", cid).execute()
                for chunk in _chunked(events, insert_batch):
                    supabase.table("smartlead_events").insert(chunk).execute()
                print(f"  Inserted {len(events)} events into smartlead_events")
            except Exception as exc:
                print(f"  ERROR inserting events for campaign {cid}: {exc}")
                # Continue with other campaigns instead of aborting entirely

        # Upsert daily stats — safe for other campaigns
        if daily_rows:
            for chunk in _chunked(daily_rows, insert_batch):
                supabase.table("smartlead_stats_daily").upsert(
                    chunk, on_conflict="date,campaign_id,touch_number"
                ).execute()
            print(f"  Upserted {len(daily_rows)} rows into smartlead_stats_daily")

        grand_sent += sent_count
        grand_reply += reply_count
        grand_events += len(events)
        grand_daily_rows += len(daily_rows)

    # --- 4. Summary ---
    print(
        f"\ndone "
        f"campaigns_processed={len(to_process)} "
        f"total_sent={grand_sent} "
        f"total_reply={grand_reply} "
        f"total_events={grand_events} "
        f"total_daily_rows_upserted={grand_daily_rows}"
        + (" [dry-run, nothing written]" if dry_run else "")
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill missing Smartlead campaigns into smartlead_events and "
            "smartlead_stats_daily without touching existing campaign data."
        )
    )
    parser.add_argument(
        "--campaign-names",
        type=str,
        default=None,
        help=(
            "Comma-separated campaign names to force-process even if already in DB. "
            'Example: "SBC Conference,Telecom"'
        ),
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Smartlead statistics page size (default: 100)",
    )
    parser.add_argument(
        "--insert-batch-size",
        type=int,
        default=500,
        help="Supabase insert/upsert chunk size (default: 500)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without actually writing to the DB",
    )
    args = parser.parse_args()

    names: list[str] = []
    if args.campaign_names:
        names = [n.strip() for n in args.campaign_names.split(",") if n.strip()]

    run(
        campaign_names=names,
        page_size=max(1, min(500, args.page_size)),
        insert_batch=max(1, min(2000, args.insert_batch_size)),
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
