#!/usr/bin/env python3
import argparse
import json
import math
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional

import requests


def as_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


def as_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def as_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        t = v.strip().lower()
        if t == "true":
            return True
        if t == "false":
            return False
    return None


def parse_json_lenient(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return json.loads(text, strict=False)


def list_from_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            return [x for x in payload["results"] if isinstance(x, dict)]
        if isinstance(payload.get("data"), list):
            return [x for x in payload["data"] if isinstance(x, dict)]
    return []


def prune_payload(v: Any) -> Any:
    if isinstance(v, dict):
        out: Dict[str, Any] = {}
        for k, val in v.items():
            if k == "image_base64":
                continue
            out[k] = prune_payload(val)
        return out
    if isinstance(v, list):
        return [prune_payload(x) for x in v]
    return v


def _pick_campaign_instance_from_contact(contact_obj: Dict[str, Any]) -> Optional[int]:
    rows = contact_obj.get("campaigninstancecontacts_set")
    if not isinstance(rows, list):
        return None
    candidates: List[Dict[str, Any]] = [x for x in rows if isinstance(x, dict)]
    if not candidates:
        return None

    def _score(x: Dict[str, Any]) -> tuple:
        active = bool(x.get("campaign_instance_active"))
        updated = as_text(x.get("updated")) or ""
        created = as_text(x.get("created")) or ""
        rid = as_int(x.get("id")) or 0
        return (1 if active else 0, updated, created, rid)

    best = sorted(candidates, key=_score, reverse=True)[0]
    return as_int(best.get("campaign_instance"))


def _pick_primary_campaign_contact(contact_obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rows = contact_obj.get("campaigninstancecontacts_set")
    if not isinstance(rows, list):
        return None
    candidates: List[Dict[str, Any]] = [x for x in rows if isinstance(x, dict)]
    if not candidates:
        return None

    def _score(x: Dict[str, Any]) -> tuple:
        active = bool(x.get("campaign_instance_active"))
        updated = as_text(x.get("updated")) or ""
        created = as_text(x.get("created")) or ""
        rid = as_int(x.get("id")) or 0
        return (1 if active else 0, updated, created, rid)

    return sorted(candidates, key=_score, reverse=True)[0]


def _extract_urls_domains(text: Optional[str]) -> Dict[str, List[str]]:
    if not text:
        return {"urls": [], "domains": []}
    urls = []
    seen_urls = set()
    for m in re.finditer(r"https?://[^\s<>\"')\]]+", text, flags=re.IGNORECASE):
        raw = (m.group(0) or "").strip()
        cleaned = re.sub(r"[),.;!?]+$", "", raw)
        if cleaned and cleaned not in seen_urls:
            seen_urls.add(cleaned)
            urls.append(cleaned)
    domains = []
    seen_domains = set()
    for u in urls:
        m = re.match(r"^https?://([^/?#]+)", u, flags=re.IGNORECASE)
        if not m:
            continue
        d = (m.group(1) or "").strip().lower()
        if d and d not in seen_domains:
            seen_domains.add(d)
            domains.append(d)
    return {"urls": urls, "domains": domains}


class ExpandiClient:
    def __init__(
        self,
        base_url: str,
        base_path: str,
        api_key: str,
        api_secret: str,
        login: str = "",
        password: str = "",
        token_paths: Optional[List[str]] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.base_path = base_path.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret
        self.login = (login or "").strip()
        self.password = (password or "").strip()
        self.token_paths = token_paths or ["/token/", "/api/token/", "/auth/token/", "/jwt/token/"]
        self._token_lock = threading.Lock()
        self.jwt_token: Optional[str] = None
        self.s = requests.Session()
        self.s.headers.update({"accept": "application/json", "content-type": "application/json"})

    def _url(self, path: str) -> str:
        p = path if path.startswith("/") else f"/{path}"
        if p.startswith(self.base_path + "/") or p == self.base_path:
            return f"{self.base_url}{p}"
        return f"{self.base_url}{self.base_path}{p}"

    def _auth_headers(self, force_refresh_token: bool = False) -> Dict[str, str]:
        h: Dict[str, str] = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        if self.api_key:
            h["key"] = self.api_key
            h["X-API-Key"] = self.api_key
        if self.api_secret:
            h["secret"] = self.api_secret
            h["X-API-Secret"] = self.api_secret
        with self._token_lock:
            if force_refresh_token:
                self.jwt_token = None
            if self.jwt_token is None:
                self.jwt_token = self.try_fetch_token()
            token = self.jwt_token
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def _pick_token(self, payload: Any) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        for k in ("access", "token", "jwt", "access_token"):
            v = payload.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        data = payload.get("data")
        if isinstance(data, dict):
            for k in ("access", "token", "jwt", "access_token"):
                v = data.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
        return None

    def try_fetch_token(self) -> Optional[str]:
        payloads: List[Dict[str, Any]] = []
        if self.login and self.password:
            payloads.extend(
                [
                    {"username": self.login, "password": self.password},
                    {"login": self.login, "password": self.password},
                    {"email": self.login, "password": self.password},
                ]
            )
        if self.api_key and self.api_secret:
            payloads.extend(
                [
                    {"username": self.api_key, "password": self.api_secret},
                    {"api_key": self.api_key, "api_secret": self.api_secret},
                    {"key": self.api_key, "secret": self.api_secret},
                ]
            )
        if not payloads:
            return None

        for pth in self.token_paths:
            path = pth if pth.startswith("/") else f"/{pth}"
            url = f"{self.base_url}{path}"
            for payload in payloads:
                try:
                    r = self.s.post(url, data=json.dumps(payload), timeout=25)
                    if not r.ok:
                        continue
                    parsed = parse_json_lenient(r.text) if r.text else {}
                    tok = self._pick_token(parsed)
                    if tok:
                        return tok
                except Exception:
                    continue
        return None

    def get_json(self, path_or_url: str, timeout: int = 60, tries: int = 5) -> Any:
        last = None
        for i in range(tries):
            try:
                if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
                    url = path_or_url
                else:
                    url = self._url(path_or_url)
                # First try with cached token + API key/secret headers.
                r = self.s.get(url, headers=self._auth_headers(), timeout=timeout)
                if r.status_code in (401, 403):
                    # Re-fetch token once and retry quickly.
                    r = self.s.get(url, headers=self._auth_headers(force_refresh_token=True), timeout=timeout)
                if r.status_code >= 500 or r.status_code == 429:
                    body = (r.text or "").strip().replace("\n", " ")
                    last = RuntimeError(f"HTTP {r.status_code}: {body[:300]}")
                    time.sleep(0.5 * (i + 1))
                    continue
                r.raise_for_status()
                return parse_json_lenient(r.text)
            except Exception as e:
                last = e
                time.sleep(0.5 * (i + 1))
        raise RuntimeError(f"Expandi GET failed for {path_or_url}: {last}")

    def paginate_pages(self, path: str, max_pages: Optional[int] = None):
        url: Optional[str] = path
        pages = 0
        seen = set()
        while url:
            if url in seen:
                break
            seen.add(url)
            payload = self.get_json(url)
            rows = list_from_payload(payload)
            pages += 1
            total_count = as_int(payload.get("count")) if isinstance(payload, dict) else None
            yield rows, pages, total_count, url
            if max_pages and pages >= max_pages:
                break
            nxt = payload.get("next") if isinstance(payload, dict) else None
            url = nxt if isinstance(nxt, str) and nxt.strip() else None

    def paginate(self, path: str, max_pages: Optional[int] = None) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for rows, _pages, _total, _url in self.paginate_pages(path, max_pages=max_pages):
            out.extend(rows)
        return out


class SupabaseRest:
    def __init__(self, url: str, service_key: str):
        self.url = url.rstrip("/")
        self.key = service_key
        self.s = requests.Session()
        self.s.headers.update(
            {
                "apikey": self.key,
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json",
            }
        )

    def upsert_rows(self, table: str, rows: List[Dict[str, Any]], on_conflict: str = "id", chunk_size: int = 500) -> int:
        if not rows:
            return 0
        total = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            params = f"?on_conflict={on_conflict}"
            last_err: Optional[str] = None
            ok = False
            for attempt in range(1, 6):
                try:
                    r = self.s.post(
                        f"{self.url}/rest/v1/{table}{params}",
                        headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
                        data=json.dumps(chunk, ensure_ascii=False),
                        timeout=180,
                    )
                    if r.ok:
                        ok = True
                        break
                    last_err = f"HTTP {r.status_code} {r.text[:500]}"
                except Exception as e:
                    last_err = str(e)
                time.sleep(0.8 * attempt)
            if not ok:
                raise RuntimeError(f"Supabase upsert failed {table}: {last_err}")
            total += len(chunk)
        return total

    def fetch_ids(self, table: str, id_col: str = "id", page_size: int = 1000) -> List[int]:
        out: List[int] = []
        offset = 0
        while True:
            r = self.s.get(
                f"{self.url}/rest/v1/{table}?select={id_col}&order={id_col}.asc&limit={page_size}&offset={offset}",
                timeout=120,
            )
            if not r.ok:
                raise RuntimeError(f"Supabase fetch ids failed {table}: {r.status_code} {r.text[:500]}")
            rows = r.json()
            if not isinstance(rows, list) or not rows:
                break
            for row in rows:
                v = as_int(row.get(id_col))
                if v is not None:
                    out.append(v)
            if len(rows) < page_size:
                break
            offset += page_size
        return out

    def fetch_rows(
        self,
        table: str,
        select: str,
        where: str = "",
        order: str = "",
        page_size: int = 1000,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        offset = 0
        while True:
            q = f"select={select}&limit={page_size}&offset={offset}"
            if where:
                q += f"&{where}"
            if order:
                q += f"&order={order}"
            r = self.s.get(f"{self.url}/rest/v1/{table}?{q}", timeout=120)
            if not r.ok:
                raise RuntimeError(f"Supabase fetch rows failed {table}: {r.status_code} {r.text[:500]}")
            rows = r.json()
            if not isinstance(rows, list) or not rows:
                break
            out.extend([x for x in rows if isinstance(x, dict)])
            if len(rows) < page_size:
                break
            offset += page_size
        return out


def map_account(r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rid = as_int(r.get("id"))
    if rid is None:
        return None
    return {
        "id": rid,
        "workspace_id": as_int(r.get("workspace_id")),
        "name": as_text(r.get("name")),
        "login": as_text(r.get("login")),
        "headline": as_text(r.get("headline")),
        "job_title": as_text(r.get("job_title")),
        "image_base64": None,
        "li_account_user_id": as_int(r.get("li_account_user_id")),
        "li_account_user_role_id": as_int(r.get("li_account_user_role_id")),
        "li_account_user_role_name": as_text(r.get("li_account_user_role_name")),
        "raw_payload": prune_payload(r),
    }


def map_campaign(r: Dict[str, Any], li_account_id: int) -> Optional[Dict[str, Any]]:
    rid = as_int(r.get("id"))
    if rid is None:
        return None
    return {
        "id": rid,
        "li_account_id": as_int(r.get("li_account")) or li_account_id,
        "campaign_id": as_int(r.get("campaign_id")) or as_int(r.get("campaign")),
        "name": as_text(r.get("name")),
        "campaign_type": as_int(r.get("campaign_type")),
        "active": as_bool(r.get("active")),
        "archived": as_bool(r.get("archived")),
        "step_count": as_int(r.get("step_count")),
        "first_action_action_type": as_int(r.get("first_action_action_type")),
        "nr_contacts_total": as_int(r.get("nr_contacts_total")),
        "campaign_status": as_text(r.get("campaign_status")),
        "stats": r.get("stats"),
        "raw_payload": prune_payload(r),
    }


def map_messenger(r: Dict[str, Any], li_account_id: int) -> Optional[Dict[str, Any]]:
    rid = as_int(r.get("id"))
    if rid is None:
        return None
    c = r.get("contact") if isinstance(r.get("contact"), dict) else {}
    campaign_contact = _pick_primary_campaign_contact(c) or {}
    contact_info = campaign_contact.get("contact_information") if isinstance(campaign_contact.get("contact_information"), dict) else {}
    campaign_instance_id = as_int(r.get("campaign_instance_id")) or as_int(r.get("campaign_instance"))
    if campaign_instance_id is None:
        campaign_instance_id = _pick_campaign_instance_from_contact(c)
    return {
        "id": rid,
        "li_account_id": as_int(r.get("li_account")) or li_account_id,
        "contact_id": as_int(c.get("id")) or as_int(r.get("contact")),
        "contact_profile_link": as_text(c.get("profile_link")) or as_text(contact_info.get("profile_link")),
        "contact_profile_link_sn": as_text(c.get("profile_link_sn")) or as_text(contact_info.get("profile_link_sn")),
        "contact_public_identifier": as_text(c.get("public_identifier")) or as_text(contact_info.get("public_identifier")),
        "contact_entity_urn": as_text(c.get("entity_urn")) or as_text(contact_info.get("entity_urn")),
        "contact_email": as_text(c.get("email")) or as_text(contact_info.get("email")),
        "contact_phone": as_text(c.get("phone")) or as_text(contact_info.get("phone")),
        "contact_address": as_text(c.get("address")) or as_text(contact_info.get("address")),
        "contact_name": as_text(c.get("name")) or as_text(contact_info.get("name")),
        "contact_job_title": as_text(c.get("job_title")) or as_text(contact_info.get("job_title")),
        "contact_company_name": as_text(c.get("company_name")) or as_text(contact_info.get("company_name")),
        "contact_status": as_int(r.get("contact_status")),
        "conversation_status": as_int(r.get("conversation_status")),
        "last_message_id": as_int(r.get("last_message")),
        "has_new_messages": as_bool(r.get("has_new_messages")),
        "last_datetime": as_text(r.get("last_datetime")),
        "connected_at": as_text(r.get("connected_at")),
        "invited_at": as_text(r.get("invited_at")),
        "is_blacklisted": as_bool(r.get("is_blacklisted")),
        "reason_failed": as_int(r.get("reason_failed")),
        "campaign_instance_id": campaign_instance_id,
        "campaign_id": as_int(r.get("campaign_id")) or as_int(r.get("campaign")),
        "campaign_name": as_text(r.get("campaign_name")) or as_text(campaign_contact.get("campaign_instance_name")),
        "campaign_contact_status": as_int(campaign_contact.get("status")),
        "campaign_running_status": as_int(campaign_contact.get("campaign_running_status")),
        "last_action_id": as_int(campaign_contact.get("last_action")),
        "nr_steps_before_responding": as_int(campaign_contact.get("nr_steps_before_responding")),
        "first_outbound_at": as_text(r.get("first_outbound_at")),
        "first_inbound_at": as_text(r.get("first_inbound_at")),
        "replied_at": as_text(r.get("replied_at")),
        "is_replied": as_bool(r.get("is_replied")),
        "raw_payload": prune_payload(r),
    }


def map_message(
    r: Dict[str, Any],
    messenger_id: int,
    messenger_meta: Optional[Dict[str, Optional[int]]] = None,
) -> Optional[Dict[str, Any]]:
    rid = as_int(r.get("id"))
    if rid is None:
        return None
    send_at = as_text(r.get("send_datetime"))
    recv_at = as_text(r.get("received_datetime"))
    body = as_text(r.get("body"))
    extracted = _extract_urls_domains(body)
    direction = None
    if send_at and not recv_at:
        direction = "outbound"
    elif recv_at and not send_at:
        direction = "inbound"
    return {
        "id": rid,
        "messenger_id": as_int(r.get("messenger")) or messenger_id,
        "li_account_id": as_int(r.get("li_account")),
        "created_at_source": as_text(r.get("created")),
        "updated_at_source": as_text(r.get("updated")),
        "send_datetime": send_at,
        "received_datetime": recv_at,
        "event_datetime": recv_at or send_at or as_text(r.get("created")),
        "body": body,
        "status": as_int(r.get("status")),
        "send_by": as_text(r.get("send_by")),
        "send_by_id": as_int(r.get("send_by_id")),
        "direction": direction,
        "is_outbound": bool(send_at),
        "is_inbound": bool(recv_at),
        "flag_direct": as_bool(r.get("flag_direct")),
        "flag_mobile": as_bool(r.get("flag_mobile")),
        "flag_open_inmail": as_bool(r.get("flag_open_inmail")),
        "inmail": as_bool(r.get("inmail")),
        "inmail_type": as_int(r.get("inmail_type")),
        "inmail_accepted": as_bool(r.get("inmail_accepted")),
        "reason_failed": as_int(r.get("reason_failed")),
        "attachment": as_text(r.get("attachment")),
        "attachment_size": as_int(r.get("attachment_size")),
        "has_attachment": (as_text(r.get("attachment")) is not None) or (as_int(r.get("attachment_size")) is not None),
        "extracted_urls": extracted["urls"],
        "extracted_domains": extracted["domains"],
        "campaign_instance_id": (as_int(r.get("campaign_instance_id")) or as_int(r.get("campaign_instance")) or (messenger_meta or {}).get("campaign_instance_id")),
        "campaign_id": (as_int(r.get("campaign_id")) or as_int(r.get("campaign")) or (messenger_meta or {}).get("campaign_id")),
        "campaign_step_id": as_int(r.get("campaign_step_id")) or as_int(r.get("step_id")) or as_int(r.get("action_id")),
        "raw_payload": prune_payload(r),
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Direct import Expandi -> Supabase")
    p.add_argument("--messages-workers", type=int, default=16)
    p.add_argument("--sb-chunk-size", type=int, default=500)
    p.add_argument("--messages-batch-upsert", type=int, default=1000)
    p.add_argument("--skip-accounts-upsert", action="store_true")
    p.add_argument("--only-messages", action="store_true")
    p.add_argument("--include-non-campaign-messages", action="store_true")
    args = p.parse_args()

    sb_url = (os.getenv("SUPABASE_URL") or "").strip()
    sb_key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    ex_base = (os.getenv("EXPANDI_BASE_URL") or "https://api.liaufa.com").strip()
    ex_path = (os.getenv("EXPANDI_BASE_PATH") or "/api/v1/open-api/v2").strip()
    ex_key = (os.getenv("EXPANDI_API_KEY") or "").strip()
    ex_secret = (os.getenv("EXPANDI_API_SECRET") or "").strip()
    ex_login = (os.getenv("EXPANDI_LOGIN") or "").strip()
    ex_password = (os.getenv("EXPANDI_PASSWORD") or "").strip()
    ex_token_paths_raw = (os.getenv("EXPANDI_TOKEN_PATHS") or "/token/,/api/token/,/auth/token/,/jwt/token/").strip()
    ex_token_paths = [x.strip() for x in ex_token_paths_raw.split(",") if x.strip()]

    missing = []
    if not sb_url:
        missing.append("SUPABASE_URL")
    if not sb_key:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    has_api_pair = bool(ex_key and ex_secret)
    has_login_pair = bool(ex_login and ex_password)
    if not has_api_pair and not has_login_pair:
        missing.append("EXPANDI_API_KEY+EXPANDI_API_SECRET or EXPANDI_LOGIN+EXPANDI_PASSWORD")
    if missing:
        print("Missing env:", ", ".join(missing), file=sys.stderr)
        return 2

    exp = ExpandiClient(
        ex_base,
        ex_path,
        ex_key,
        ex_secret,
        login=ex_login,
        password=ex_password,
        token_paths=ex_token_paths,
    )
    sb = SupabaseRest(sb_url, sb_key)

    campaign_only = not args.include_non_campaign_messages

    messenger_meta_map: Dict[int, Dict[str, Optional[int]]] = {}
    if args.only_messages:
        print("[messages-only] Loading messenger IDs from Supabase...", flush=True)
        where = "campaign_instance_id=not.is.null" if campaign_only else ""
        rows = sb.fetch_rows(
            "expandi_messengers",
            "id,campaign_instance_id,campaign_id",
            where=where,
            order="id.asc",
            page_size=1000,
        )
        messenger_ids = []
        for row in rows:
            mid = as_int(row.get("id"))
            if mid is None:
                continue
            messenger_ids.append(mid)
            messenger_meta_map[mid] = {
                "campaign_instance_id": as_int(row.get("campaign_instance_id")),
                "campaign_id": as_int(row.get("campaign_id")),
            }
        print(f"messenger_ids from supabase: {len(messenger_ids)}", flush=True)
        accounts = []
        campaigns_all = 0
        messengers_all = []
    else:
        print("[1/4] Loading accounts from Expandi...", flush=True)
        accounts_raw = exp.paginate("/li_accounts/")
        accounts = [x for x in (map_account(r) for r in accounts_raw) if x]
        if args.skip_accounts_upsert:
            print(f"accounts: {len(accounts)} (upsert skipped)", flush=True)
        else:
            sb.upsert_rows("expandi_accounts", accounts, on_conflict="id", chunk_size=args.sb_chunk_size)
            print(f"accounts: {len(accounts)}", flush=True)

        account_ids = [a["id"] for a in accounts]

        print("[2/4] Loading campaign instances...", flush=True)
        campaigns_all = 0
        for aid in account_ids:
            account_count = 0
            for rows, page, total_count, _url in exp.paginate_pages(f"/li_accounts/{aid}/campaign_instances/"):
                mapped = [x for x in (map_campaign(r, aid) for r in rows) if x]
                if mapped:
                    sb.upsert_rows("expandi_campaign_instances", mapped, on_conflict="id", chunk_size=args.sb_chunk_size)
                account_count += len(mapped)
                campaigns_all += len(mapped)
                if page == 1 or page % 20 == 0:
                    tc = total_count if total_count is not None else "?"
                    print(f"  li_account={aid} campaigns page={page} loaded={account_count}/{tc}", flush=True)
            print(f"  li_account={aid} campaigns done={account_count}", flush=True)
        print(f"campaign_instances total: {campaigns_all}", flush=True)

        print("[3/4] Loading messengers...", flush=True)
        messengers_all: List[Dict[str, Any]] = []
        for aid in account_ids:
            account_count = 0
            skipped_no_campaign = 0
            for rows, page, total_count, _url in exp.paginate_pages(f"/li_accounts/{aid}/messengers/"):
                mapped_all = [x for x in (map_messenger(r, aid) for r in rows) if x]
                if campaign_only:
                    mapped = [x for x in mapped_all if as_int(x.get("campaign_instance_id")) is not None]
                    skipped_no_campaign += max(0, len(mapped_all) - len(mapped))
                else:
                    mapped = mapped_all
                if mapped:
                    sb.upsert_rows("expandi_messengers", mapped, on_conflict="id", chunk_size=args.sb_chunk_size)
                    messengers_all.extend(mapped)
                account_count += len(mapped)
                if page == 1 or page % 25 == 0:
                    tc = total_count if total_count is not None else "?"
                    print(f"  li_account={aid} messengers page={page} loaded={account_count}/{tc}", flush=True)
            if campaign_only:
                print(f"  li_account={aid} messengers done={account_count} skipped_no_campaign={skipped_no_campaign}", flush=True)
            else:
                print(f"  li_account={aid} messengers done={account_count}", flush=True)
        print(f"messengers total: {len(messengers_all)}", flush=True)
        messenger_ids = [m["id"] for m in messengers_all]
        messenger_meta_map = {
            int(m["id"]): {
                "campaign_instance_id": as_int(m.get("campaign_instance_id")),
                "campaign_id": as_int(m.get("campaign_id")),
            }
            for m in messengers_all
            if as_int(m.get("id")) is not None
        }

    print("[4/4] Loading messages...", flush=True)

    def fetch_messages(mid: int) -> List[Dict[str, Any]]:
        rows = exp.paginate(f"/li_accounts/messengers/{mid}/messages/")
        meta = messenger_meta_map.get(mid)
        mapped = [x for x in (map_message(r, mid, meta) for r in rows) if x]
        if campaign_only:
            mapped = [x for x in mapped if as_int(x.get("campaign_instance_id")) is not None]
        return mapped

    inserted = 0
    msgs_total = 0
    non_empty_threads = 0
    pending: List[Dict[str, Any]] = []
    processed = 0
    started = time.time()

    with ThreadPoolExecutor(max_workers=max(1, args.messages_workers)) as pool:
        futures = {pool.submit(fetch_messages, mid): mid for mid in messenger_ids}
        for fut in as_completed(futures):
            mid = futures[fut]
            processed += 1
            try:
                msgs = fut.result()
            except Exception as e:
                print(f"  warn: messenger {mid} failed: {e}", flush=True)
                msgs = []
            if msgs:
                non_empty_threads += 1
                msgs_total += len(msgs)
                pending.extend(msgs)
            if len(pending) >= args.messages_batch_upsert:
                inserted += sb.upsert_rows(
                    "expandi_messages", pending, on_conflict="id", chunk_size=args.sb_chunk_size
                )
                pending.clear()
            if processed % 500 == 0:
                elapsed = time.time() - started
                print(
                    f"  progress threads={processed}/{len(messenger_ids)} messages_seen={msgs_total} upserted={inserted} non_empty_threads={non_empty_threads} elapsed={elapsed:.1f}s",
                    flush=True,
                )

    if pending:
        inserted += sb.upsert_rows("expandi_messages", pending, on_conflict="id", chunk_size=args.sb_chunk_size)
        pending.clear()

    print("DONE", flush=True)
    print(f"accounts={len(accounts)}", flush=True)
    campaign_total = campaigns_all if isinstance(campaigns_all, int) else len(campaigns_all)
    print(f"campaign_instances={campaign_total}", flush=True)
    messenger_total = len(messengers_all) if isinstance(messengers_all, list) else 0
    print(f"messengers={messenger_total}", flush=True)
    print(f"messages_seen={msgs_total}", flush=True)
    print(f"messages_upserted={inserted}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
