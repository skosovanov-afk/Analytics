from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any

import httpx
from dotenv import load_dotenv


def _to_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "leads", "campaigns"):
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def _sl_get(client: httpx.Client, base_url: str, api_key: str, path: str) -> Any:
    sep = "&" if "?" in path else "?"
    url = f"{base_url.rstrip('/')}{path}{sep}api_key={api_key}"
    attempts = 0
    while True:
        attempts += 1
        res = client.get(url, headers={"accept": "application/json"})
        if res.status_code in (429, 500, 502, 503, 504):
            if attempts >= 8:
                res.raise_for_status()
            retry_after = res.headers.get("retry-after")
            if retry_after and retry_after.isdigit():
                sleep_s = min(30, max(1, int(retry_after)))
            else:
                sleep_s = min(30, 2 ** min(attempts, 5))
            time.sleep(sleep_s)
            continue
        res.raise_for_status()
        return res.json()


def _sb_get_rows(client: httpx.Client, supabase_url: str, service_key: str) -> list[dict[str, Any]]:
    # Pull all rows from the pre-aggregated view.
    rows: list[dict[str, Any]] = []
    offset = 0
    page = 1000
    while True:
        res = client.get(
            f"{supabase_url.rstrip('/')}/rest/v1/v_smartlead_by_campaign",
            params={
                "select": "*",
                "order": "campaign_id.asc",
                "limit": str(page),
                "offset": str(offset),
            },
            headers={
                "apikey": service_key,
                "Authorization": f"Bearer {service_key}",
                "Accept": "application/json",
            },
        )
        res.raise_for_status()
        batch = res.json() or []
        if not isinstance(batch, list) or not batch:
            break
        rows.extend([r for r in batch if isinstance(r, dict)])
        if len(batch) < page:
            break
        offset += len(batch)
    return rows


def _as_int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _pick_total_field(row: dict[str, Any]) -> int:
    for key in ("total_leads", "total_count", "leads_total", "unique_sent_leads"):
        if key in row:
            return _as_int(row.get(key))
    return 0


def _pick_email_from_lead_row(row: dict[str, Any]) -> str | None:
    lead = row.get("lead") if isinstance(row.get("lead"), dict) else {}
    for v in (
        lead.get("email"),
        lead.get("work_email"),
        lead.get("personal_email"),
        lead.get("contact_email"),
        row.get("email"),
        row.get("lead_email"),
        row.get("contact_email"),
    ):
        if not v:
            continue
        e = str(v).strip().lower()
        if e and "@" in e:
            return e
    return None


def _sl_campaign_emails(
    client: httpx.Client, base_url: str, api_key: str, campaign_id: int, page_size: int = 100
) -> set[str]:
    out: set[str] = set()
    offset = 0
    while True:
        payload = _sl_get(
            client,
            base_url,
            api_key,
            f"/api/v1/campaigns/{campaign_id}/leads?limit={page_size}&offset={offset}",
        )
        rows = _to_list(payload)
        if not rows:
            break
        for row in rows:
            email = _pick_email_from_lead_row(row)
            if email:
                out.add(email)
        if len(rows) < page_size:
            break
        offset += len(rows)
    return out


def _sb_campaign_emails(
    client: httpx.Client, supabase_url: str, service_key: str, campaign_id: int, page_size: int = 1000
) -> set[str]:
    out: set[str] = set()
    offset = 0
    while True:
        res = client.get(
            f"{supabase_url.rstrip('/')}/rest/v1/smartlead_leads",
            params={
                "select": "email",
                "campaign_id": f"eq.{campaign_id}",
                "limit": str(page_size),
                "offset": str(offset),
                "order": "id.asc",
            },
            headers={
                "apikey": service_key,
                "Authorization": f"Bearer {service_key}",
                "Accept": "application/json",
            },
        )
        res.raise_for_status()
        batch = res.json() or []
        if not isinstance(batch, list) or not batch:
            break
        for row in batch:
            if not isinstance(row, dict):
                continue
            v = row.get("email")
            if not v:
                continue
            email = str(v).strip().lower()
            if email and "@" in email:
                out.add(email)
        if len(batch) < page_size:
            break
        offset += len(batch)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare SmartLead campaign analytics vs Supabase v_smartlead_by_campaign")
    parser.add_argument(
        "--mode",
        choices=("analytics", "emails"),
        default="analytics",
        help="analytics: compare campaign counters, emails: compare unique emails per campaign",
    )
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout seconds")
    parser.add_argument("--strict", action="store_true", help="Exit 1 if any campaign mismatch exists")
    parser.add_argument(
        "--email-sample",
        type=int,
        default=5,
        help="How many example missing/extra emails to print per mismatched campaign in emails mode",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=5,
        help="Print progress every N campaigns in emails mode (0 to disable)",
    )
    args = parser.parse_args()

    load_dotenv()

    smartlead_key = (os.environ.get("SMARTLEAD_API_KEY") or "").strip()
    smartlead_base = (os.environ.get("SMARTLEAD_BASE_URL") or "https://server.smartlead.ai").strip()
    supabase_url = (os.environ.get("SUPABASE_URL") or "").strip()
    supabase_service_key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()

    missing = []
    if not smartlead_key:
        missing.append("SMARTLEAD_API_KEY")
    if not supabase_url:
        missing.append("SUPABASE_URL")
    if not supabase_service_key:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        print(f"Missing required env vars: {', '.join(missing)}")
        return 2

    with httpx.Client(timeout=args.timeout) as client:
        campaigns = _to_list(_sl_get(client, smartlead_base, smartlead_key, "/api/v1/campaigns"))

        if args.mode == "emails":
            campaigns_sorted: list[tuple[int, str | None]] = []
            for c in campaigns:
                cid_raw = c.get("id")
                if cid_raw is None:
                    continue
                try:
                    cid = int(cid_raw)
                except Exception:
                    continue
                campaigns_sorted.append((cid, c.get("name")))

            campaigns_sorted.sort(key=lambda x: x[0])

            mismatches = 0
            missing_in_supabase_total = 0
            extra_in_supabase_total = 0
            sl_total_unique = 0
            sb_total_unique = 0

            total_campaigns = len(campaigns_sorted)
            for idx, (cid, cname) in enumerate(campaigns_sorted, start=1):
                if args.progress_every > 0 and (idx == 1 or idx % args.progress_every == 0 or idx == total_campaigns):
                    print(f"[PROGRESS] {idx}/{total_campaigns} campaign_id={cid}")
                sl_emails = _sl_campaign_emails(client, smartlead_base, smartlead_key, cid)
                sb_emails = _sb_campaign_emails(client, supabase_url, supabase_service_key, cid)

                sl_total_unique += len(sl_emails)
                sb_total_unique += len(sb_emails)

                missing_in_supabase = sorted(sl_emails - sb_emails)
                extra_in_supabase = sorted(sb_emails - sl_emails)
                if missing_in_supabase or extra_in_supabase:
                    mismatches += 1
                    missing_in_supabase_total += len(missing_in_supabase)
                    extra_in_supabase_total += len(extra_in_supabase)
                    print(
                        f"[EMAIL_DIFF] campaign_id={cid} "
                        f"sl_unique={len(sl_emails)} sb_unique={len(sb_emails)} "
                        f"missing_in_supabase={len(missing_in_supabase)} "
                        f"extra_in_supabase={len(extra_in_supabase)} "
                        f"name={cname}"
                    )
                    if args.email_sample > 0 and missing_in_supabase:
                        print(f"  missing_sample={missing_in_supabase[: args.email_sample]}")
                    if args.email_sample > 0 and extra_in_supabase:
                        print(f"  extra_sample={extra_in_supabase[: args.email_sample]}")

            print("\n=== SUMMARY (emails) ===")
            print(f"campaigns_checked={len(campaigns_sorted)}")
            print(f"campaigns_with_email_diff={mismatches}")
            print(f"missing_in_supabase_total={missing_in_supabase_total}")
            print(f"extra_in_supabase_total={extra_in_supabase_total}")
            print(f"smartlead_unique_sum={sl_total_unique}")
            print(f"supabase_unique_sum={sb_total_unique}")
            print(f"unique_sum_diff={sb_total_unique - sl_total_unique}")

            if args.strict and mismatches:
                return 1
            return 0

        smartlead_map: dict[int, dict[str, Any]] = {}
        for c in campaigns:
            cid_raw = c.get("id")
            if cid_raw is None:
                continue
            try:
                cid = int(cid_raw)
            except Exception:
                continue
            analytics = _sl_get(client, smartlead_base, smartlead_key, f"/api/v1/campaigns/{cid}/analytics")
            smartlead_map[cid] = {
                "campaign_name": c.get("name"),
                "sent": _as_int(analytics.get("sent_count")),
                "reply": _as_int(analytics.get("reply_count")),
                "total": _as_int(analytics.get("total_count")),
            }

        supabase_rows = _sb_get_rows(client, supabase_url, supabase_service_key)
        supabase_map: dict[int, dict[str, Any]] = {}
        for r in supabase_rows:
            cid = _as_int(r.get("campaign_id"))
            if cid <= 0:
                continue
            supabase_map[cid] = {
                "campaign_name": r.get("campaign_name"),
                "sent": _as_int(r.get("sent_total")),
                "reply": _as_int(r.get("reply_total")),
                "total": _pick_total_field(r),
            }

    all_ids = sorted(set(smartlead_map.keys()) | set(supabase_map.keys()))
    mismatches = 0
    missing_in_supabase = 0
    missing_in_smartlead = 0

    sl_sent_sum = sl_reply_sum = sl_total_sum = 0
    sb_sent_sum = sb_reply_sum = sb_total_sum = 0

    for cid in all_ids:
        sl = smartlead_map.get(cid)
        sb = supabase_map.get(cid)
        if sl:
            sl_sent_sum += sl["sent"]
            sl_reply_sum += sl["reply"]
            sl_total_sum += sl["total"]
        if sb:
            sb_sent_sum += sb["sent"]
            sb_reply_sum += sb["reply"]
            sb_total_sum += sb["total"]

        if sl is None:
            missing_in_smartlead += 1
            print(f"[ONLY_SUPABASE] campaign_id={cid} sb={sb}")
            continue
        if sb is None:
            missing_in_supabase += 1
            print(f"[ONLY_SMARTLEAD] campaign_id={cid} sl={sl}")
            continue

        sent_ok = sl["sent"] == sb["sent"]
        reply_ok = sl["reply"] == sb["reply"]
        total_ok = sl["total"] == sb["total"]
        if not (sent_ok and reply_ok and total_ok):
            mismatches += 1
            print(
                f"[DIFF] campaign_id={cid} "
                f"sent sl={sl['sent']} sb={sb['sent']} | "
                f"reply sl={sl['reply']} sb={sb['reply']} | "
                f"total sl={sl['total']} sb={sb['total']} | "
                f"name={sl.get('campaign_name') or sb.get('campaign_name')}"
            )

    print("\n=== SUMMARY ===")
    print(f"campaigns_smartlead={len(smartlead_map)}")
    print(f"campaigns_supabase={len(supabase_map)}")
    print(f"missing_in_supabase={missing_in_supabase}")
    print(f"missing_in_smartlead={missing_in_smartlead}")
    print(f"campaign_value_mismatches={mismatches}")
    print("--- totals ---")
    print(f"sent:  smartlead={sl_sent_sum} supabase={sb_sent_sum} diff={sb_sent_sum - sl_sent_sum}")
    print(f"reply: smartlead={sl_reply_sum} supabase={sb_reply_sum} diff={sb_reply_sum - sl_reply_sum}")
    print(f"total: smartlead={sl_total_sum} supabase={sb_total_sum} diff={sb_total_sum - sl_total_sum}")

    if args.strict and (mismatches or missing_in_supabase or missing_in_smartlead):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
