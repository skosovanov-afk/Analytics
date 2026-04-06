/* eslint-disable no-console */

type Json = Record<string, unknown>;

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")?.trim() || "";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")?.trim() || "";

const EXPANDI_BASE_URL = (Deno.env.get("EXPANDI_BASE_URL")?.trim() || "https://api.liaufa.com").replace(/\/+$/, "");
const EXPANDI_BASE_PATH = (Deno.env.get("EXPANDI_BASE_PATH")?.trim() || "/api/v1/open-api/v2").replace(/\/+$/, "");
const EXPANDI_API_KEY = Deno.env.get("EXPANDI_API_KEY")?.trim() || "";
const EXPANDI_API_SECRET = Deno.env.get("EXPANDI_API_SECRET")?.trim() || "";
const EXPANDI_LOGIN = Deno.env.get("EXPANDI_LOGIN")?.trim() || "";
const EXPANDI_PASSWORD = Deno.env.get("EXPANDI_PASSWORD")?.trim() || "";
const EXPANDI_SYNC_SECRET = Deno.env.get("EXPANDI_SYNC_SECRET")?.trim() || "";
const EXPANDI_TOKEN_PATHS = (Deno.env.get("EXPANDI_TOKEN_PATHS")?.trim() || "/token/,/api/token/,/auth/token/,/jwt/token/").split(",").map((x) => x.trim()).filter(Boolean);
const EXPANDI_MAX_PAGES = Math.max(1, asInt(Deno.env.get("EXPANDI_MAX_PAGES")?.trim() || "200") || 200);
const EXPANDI_MIN_DAY = (Deno.env.get("EXPANDI_MIN_DAY")?.trim() || "2025-01-01");
let EXPANDI_JWT_CACHE: string | null | undefined = undefined;
let EXPANDI_CAMPAIGN_IDS_CACHE: Set<number> | null = null;

function nowIso(): string {
  return new Date().toISOString();
}

function asObj(v: unknown): Json {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Json) : {};
}

function asList(v: unknown): Json[] {
  if (Array.isArray(v)) return v.filter((x): x is Json => !!x && typeof x === "object");
  const obj = asObj(v);
  if (Array.isArray(obj.results)) return obj.results.filter((x): x is Json => !!x && typeof x === "object");
  if (Array.isArray(obj.data)) return obj.data.filter((x): x is Json => !!x && typeof x === "object");
  return [];
}

function asText(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s || null;
}

function asInt(v: unknown): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function asBool(v: unknown): boolean | null {
  if (typeof v === "boolean") return v;
  if (typeof v === "string") {
    const t = v.trim().toLowerCase();
    if (t === "true") return true;
    if (t === "false") return false;
  }
  return null;
}

function asIso(v: unknown): string | null {
  const s = asText(v);
  return s || null;
}

function asDate(v: unknown): string | null {
  const iso = asIso(v);
  if (!iso) return null;
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function getPath(input: unknown, path: string): unknown {
  if (!path) return undefined;
  const parts = path.split(".");
  let current: unknown = input;
  for (const part of parts) {
    if (!current || typeof current !== "object" || Array.isArray(current)) return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

function pickInt(input: unknown, paths: string[]): number | null {
  for (const path of paths) {
    const v = getPath(input, path);
    const n = asInt(v);
    if (n !== null) return n;
  }
  return null;
}

function pickText(input: unknown, paths: string[]): string | null {
  for (const path of paths) {
    const v = getPath(input, path);
    const s = asText(v);
    if (s !== null) return s;
  }
  return null;
}

function pickPrimaryCampaignContact(contact: Json): Json | null {
  const rows = asList(contact.campaigninstancecontacts_set);
  if (!rows.length) return null;
  const scored = rows
    .map((row) => {
      const active = asBool(row.campaign_instance_active) === true ? 1 : 0;
      const updated = asIso(row.updated) || "";
      const created = asIso(row.created) || "";
      const id = asInt(row.id) || 0;
      return { row, score: [active, updated, created, id] as const };
    })
    .sort((a, b) => {
      if (a.score[0] !== b.score[0]) return b.score[0] - a.score[0];
      if (a.score[1] !== b.score[1]) return b.score[1].localeCompare(a.score[1]);
      if (a.score[2] !== b.score[2]) return b.score[2].localeCompare(a.score[2]);
      return b.score[3] - a.score[3];
    });
  return scored[0]?.row ?? null;
}

function extractUrlsFromText(text: string | null): string[] {
  if (!text) return [];
  const re = /https?:\/\/[^\s<>"')\]]+/gi;
  const out = new Set<string>();
  for (const m of text.matchAll(re)) {
    const raw = (m[0] || "").trim();
    if (!raw) continue;
    const cleaned = raw.replace(/[),.;!?]+$/g, "");
    if (cleaned) out.add(cleaned);
  }
  return [...out];
}

function extractDomainsFromUrls(urls: string[]): string[] {
  const out = new Set<string>();
  for (const u of urls) {
    try {
      const host = new URL(u).hostname.trim().toLowerCase();
      if (host) out.add(host);
    } catch {
      // ignore malformed URL
    }
  }
  return [...out];
}

function extractPathId(path: string, marker: string): number | null {
  const re = new RegExp(`/${marker}/([^/]+)/`, "i");
  const m = path.match(re);
  return m?.[1] ? asInt(m[1]) : null;
}

function sbHeaders(extra?: Record<string, string>): HeadersInit {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    "Content-Type": "application/json",
    ...(extra || {}),
  };
}

async function sbInsert(table: string, rows: Json[], upsert = false, onConflict?: string): Promise<void> {
  if (!rows.length) return;
  const params = new URLSearchParams();
  if (onConflict) params.set("on_conflict", onConflict);
  const url = `${SUPABASE_URL}/rest/v1/${table}${params.toString() ? `?${params.toString()}` : ""}`;
  const prefer = upsert ? "resolution=merge-duplicates,return=minimal" : "return=minimal";
  const res = await fetch(url, {
    method: "POST",
    headers: sbHeaders({ Prefer: prefer }),
    body: JSON.stringify(rows),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase INSERT ${table} failed (${res.status}): ${text.slice(0, 500)}`);
  }
}

async function sbInsertSafe(table: string, rows: Json[], upsert = false, onConflict?: string): Promise<void> {
  if (!rows.length) return;
  try {
    await sbInsert(table, rows, upsert, onConflict);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`Supabase INSERT safe skip for ${table}: ${msg}`);
  }
}

async function logIngestRun(row: Json): Promise<void> {
  await sbInsertSafe("expandi_ingest_runs", [row], false);
}

async function writeQuarantineRows(entity: string, endpointPath: string, reason: string, rows: Json[]): Promise<number> {
  if (!rows.length) return 0;
  const createdAt = nowIso();
  const payload = rows.map((row) => {
    const raw = asObj(row.raw_payload ?? row);
    return {
      entity,
      endpoint_path: endpointPath,
      reason,
      record_id: asInt(row.id),
      li_account_id: asInt(row.li_account_id ?? row.li_account),
      campaign_instance_id: asInt(row.campaign_instance_id),
      messenger_id: asInt(row.messenger_id ?? row.messenger),
      event_datetime: asIso(
        row.event_datetime ?? row.received_datetime ?? row.send_datetime ?? row.created_at_source ?? row.created,
      ),
      raw_payload: raw,
      created_at: createdAt,
    } as Json;
  });
  await sbInsertSafe("expandi_ingest_quarantine", payload, false);
  return payload.length;
}

async function fetchCampaignIdsFromTable(table: string): Promise<Set<number>> {
  const out = new Set<number>();
  let offset = 0;
  const limit = 1000;
  while (true) {
    const url = `${SUPABASE_URL}/rest/v1/${table}?select=id&order=id.asc&limit=${limit}&offset=${offset}`;
    const res = await fetch(url, { headers: sbHeaders() });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`Supabase read ${table} failed (${res.status}): ${text.slice(0, 300)}`);
    }
    const rows = await res.json().catch(() => []) as Array<{ id?: unknown }>;
    if (!Array.isArray(rows) || rows.length === 0) break;
    for (const row of rows) {
      const id = asInt(row?.id);
      if (id !== null) out.add(id);
    }
    if (rows.length < limit) break;
    offset += limit;
  }
  return out;
}

async function fetchCatalogCampaignIds(forceRefresh = false): Promise<Set<number>> {
  if (!forceRefresh && EXPANDI_CAMPAIGN_IDS_CACHE) return EXPANDI_CAMPAIGN_IDS_CACHE;
  let out: Set<number>;
  try {
    out = await fetchCampaignIdsFromTable("expandi_campaign_catalog");
    if (!out.size) {
      out = await fetchCampaignIdsFromTable("expandi_campaign_instances");
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`Historical campaign catalog unavailable, falling back to live campaign instances: ${msg}`);
    out = await fetchCampaignIdsFromTable("expandi_campaign_instances");
  }
  EXPANDI_CAMPAIGN_IDS_CACHE = out;
  return out;
}

function isOnOrAfterMinDay(ts: unknown): boolean {
  const iso = asIso(ts);
  if (!iso) return false;
  const day = iso.slice(0, 10);
  return day >= EXPANDI_MIN_DAY;
}

async function setSyncStateInt(key: string, value: number): Promise<void> {
  await sbInsert("expandi_sync_state", [{ key, value: String(value), value_int: value, updated_at: nowIso() }], true, "key");
}

async function getSyncStateInt(key: string): Promise<number | null> {
  const url = `${SUPABASE_URL}/rest/v1/expandi_sync_state?select=value_int&key=eq.${encodeURIComponent(key)}&limit=1`;
  const res = await fetch(url, { headers: sbHeaders() });
  if (!res.ok) return null;
  const rows = await res.json().catch(() => []) as Array<{ value_int?: unknown }>;
  if (!Array.isArray(rows) || !rows.length) return null;
  return asInt(rows[0]?.value_int);
}

function toQueryString(input: unknown): string {
  if (!input || typeof input !== "object") return "";
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(input as Record<string, unknown>)) {
    if (v === null || v === undefined) continue;
    p.set(k, String(v));
  }
  const qs = p.toString();
  return qs ? `?${qs}` : "";
}

function parseNextUrl(next: string, fallbackPath: string): { path: string; query: Record<string, string> | undefined } | null {
  const raw = next.trim();
  if (!raw) return null;

  const normalizePath = (pathname: string): string => {
    const clean = pathname.trim();
    if (!clean) return "/";
    if (clean.startsWith(EXPANDI_BASE_PATH)) return clean;
    return clean.startsWith("/") ? clean : `/${clean}`;
  };

  if (raw.startsWith("http://") || raw.startsWith("https://")) {
    try {
      const url = new URL(raw);
      const query = Object.fromEntries(url.searchParams.entries());
      return { path: normalizePath(url.pathname), query: Object.keys(query).length ? query : undefined };
    } catch {
      return null;
    }
  }

  if (raw.startsWith("?")) {
    const params = new URLSearchParams(raw.slice(1));
    const query = Object.fromEntries(params.entries());
    return { path: fallbackPath, query: Object.keys(query).length ? query : undefined };
  }

  if (raw.startsWith("/")) {
    const [pathname, queryString] = raw.split("?", 2);
    const params = new URLSearchParams(queryString || "");
    const query = Object.fromEntries(params.entries());
    return { path: normalizePath(pathname), query: Object.keys(query).length ? query : undefined };
  }

  const [pathname, queryString] = raw.split("?", 2);
  const params = new URLSearchParams(queryString || "");
  const query = Object.fromEntries(params.entries());
  return { path: normalizePath(pathname), query: Object.keys(query).length ? query : undefined };
}

function pickToken(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") return null;
  const obj = payload as Record<string, unknown>;
  for (const k of ["access", "token", "jwt", "access_token"]) {
    const v = obj[k];
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  if (obj.data && typeof obj.data === "object") {
    const data = obj.data as Record<string, unknown>;
    for (const k of ["access", "token", "jwt", "access_token"]) {
      const v = data[k];
      if (typeof v === "string" && v.trim()) return v.trim();
    }
  }
  return null;
}

async function tryFetchExpandiToken(): Promise<string | null> {
  if (EXPANDI_JWT_CACHE !== undefined) return EXPANDI_JWT_CACHE;

  const payloads: Json[] = [];
  if (EXPANDI_LOGIN && EXPANDI_PASSWORD) {
    payloads.push({ username: EXPANDI_LOGIN, password: EXPANDI_PASSWORD });
    payloads.push({ login: EXPANDI_LOGIN, password: EXPANDI_PASSWORD });
    payloads.push({ email: EXPANDI_LOGIN, password: EXPANDI_PASSWORD });
  }
  if (EXPANDI_API_KEY && EXPANDI_API_SECRET) {
    payloads.push({ username: EXPANDI_API_KEY, password: EXPANDI_API_SECRET });
    payloads.push({ api_key: EXPANDI_API_KEY, api_secret: EXPANDI_API_SECRET });
    payloads.push({ key: EXPANDI_API_KEY, secret: EXPANDI_API_SECRET });
  }
  if (!payloads.length) {
    EXPANDI_JWT_CACHE = null;
    return null;
  }

  for (const path of EXPANDI_TOKEN_PATHS) {
    for (const p of payloads) {
      const cleanPath = path.startsWith("/") ? path : `/${path}`;
      const url = `${EXPANDI_BASE_URL}${cleanPath}`;
      const res = await fetch(url, {
        method: "POST",
        headers: { accept: "application/json", "content-type": "application/json" },
        body: JSON.stringify(p),
      });
      if (!res.ok) continue;
      const text = await res.text();
      let parsed: unknown = null;
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = text;
      }
      const token = pickToken(parsed);
      if (token) {
        EXPANDI_JWT_CACHE = token;
        return token;
      }
    }
  }
  EXPANDI_JWT_CACHE = null;
  return null;
}

async function expandiRequest(path: string, method = "GET", body?: unknown, query?: unknown): Promise<Response> {
  const rawPath = path.startsWith("/") ? path : `/${path}`;
  // Keep docs endpoints on root, prefix Open API endpoints with basePath.
  const cleanPath =
    rawPath.startsWith("/open-swagger/")
      ? rawPath
      : rawPath.startsWith(`${EXPANDI_BASE_PATH}/`) || rawPath === EXPANDI_BASE_PATH
        ? rawPath
        : `${EXPANDI_BASE_PATH}${rawPath}`;
  const url = `${EXPANDI_BASE_URL}${cleanPath}${toQueryString(query)}`;

  // First try real JWT flow using API key + secret.
  const jwt = await tryFetchExpandiToken();

  // Fallback headers (for APIs that accept key/secret directly).
  const headers: Record<string, string> = {
    accept: "application/json",
    "content-type": "application/json",
  };
  // Swagger-declared auth headers. Keep them always when configured.
  if (EXPANDI_API_KEY) {
    headers.key = EXPANDI_API_KEY;
    headers["X-API-Key"] = EXPANDI_API_KEY;
  }
  if (EXPANDI_API_SECRET) {
    headers.secret = EXPANDI_API_SECRET;
    headers["X-API-Secret"] = EXPANDI_API_SECRET;
  }
  if (jwt) {
    headers.Authorization = `Bearer ${jwt}`;
  }

  return await fetch(url, {
    method: method.toUpperCase(),
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

function mapAccounts(rows: Json[]): Json[] {
  return rows
    .map((r) => {
      const id = asInt(r.id);
      if (id === null) return null;
      return {
        id,
        workspace_id: asInt(r.workspace_id),
        name: asText(r.name),
        login: asText(r.login),
        headline: asText(r.headline),
        job_title: asText(r.job_title),
        image_base64: asText(r.image_base64),
        li_account_user_id: asInt(r.li_account_user_id),
        li_account_user_role_id: asInt(r.li_account_user_role_id),
        li_account_user_role_name: asText(r.li_account_user_role_name),
        raw_payload: r,
        synced_at: nowIso(),
        updated_at: nowIso(),
      } as Json;
    })
    .filter((x): x is Json => !!x);
}

function mapCampaignInstances(path: string, rows: Json[]): Json[] {
  const pathLiAccountId = extractPathId(path, "li_accounts");
  return rows
    .map((r) => {
      const id = asInt(r.id);
      if (id === null) return null;
      return {
        id,
        li_account_id: asInt(r.li_account) ?? pathLiAccountId,
        campaign_id: asInt(r.campaign_id) ?? asInt(r.campaign),
        name: asText(r.name),
        campaign_type: asInt(r.campaign_type),
        active: asBool(r.active),
        archived: asBool(r.archived),
        step_count: asInt(r.step_count),
        first_action_action_type: asInt(r.first_action_action_type),
        nr_contacts_total: asInt(r.nr_contacts_total),
        campaign_status: asText(r.campaign_status),
        limit_requests_daily: asInt(r.limit_requests_daily),
        limit_follow_up_messages_daily: asInt(r.limit_follow_up_messages_daily),
        stats_datetime: asIso(r.stats_datetime),
        activated: asIso(r.activated),
        deactivated: asIso(r.deactivated),
        stats: r.stats ?? null,
        raw_payload: r,
        synced_at: nowIso(),
        updated_at: nowIso(),
      } as Json;
    })
    .filter((x): x is Json => !!x && asInt(x.li_account_id) !== null);
}

function mapCampaignStatsSnapshots(rows: Json[]): Json[] {
  const out: Json[] = [];
  const syncedAt = nowIso();
  for (const r of rows) {
    const campaignInstanceId = asInt(r.id);
    const liAccountId = asInt(r.li_account);
    if (campaignInstanceId === null || liAccountId === null) continue;
    const stats = asObj(r.stats);
    const snapshotDate = asDate(r.stats_datetime) || syncedAt.slice(0, 10);
    out.push({
      snapshot_date: snapshotDate,
      li_account_id: liAccountId,
      campaign_instance_id: campaignInstanceId,
      campaign_name: asText(r.name),
      connected: asInt(stats.connected) ?? 0,
      contacted_people: asInt(stats.contacted_people) ?? 0,
      replied_first_action: asInt(stats.replied_first_action) ?? 0,
      replied_other_actions: asInt(stats.replied_other_actions) ?? 0,
      people_in_campaign: asInt(stats.people_in_campaign) ?? 0,
      step_count: asInt(stats.step_count) ?? asInt(r.step_count) ?? 0,
      raw_stats: stats,
      synced_at: syncedAt,
      updated_at: syncedAt,
    });
  }
  return out;
}

function mapMessengers(path: string, rows: Json[]): Json[] {
  const pathLiAccountId = extractPathId(path, "li_accounts");
  return rows
    .map((r) => {
      const id = asInt(r.id);
      if (id === null) return null;
      const contact = asObj(r.contact);
      const campaignContact = pickPrimaryCampaignContact(contact);
      const campaignContactInfo = asObj(campaignContact?.contact_information);
      const campaignInstanceId = pickInt(r, [
        "campaign_instance_id",
        "campaign_instance",
        "campaign_instance.id",
        "campaign.id",
        "contact.campaign_instance_id",
      ]) ?? asInt(campaignContact?.campaign_instance);
      const campaignId = pickInt(r, [
        "campaign_id",
        "campaign",
        "campaign.id",
        "campaign_instance.campaign_id",
      ]);
      return {
        id,
        li_account_id: asInt(r.li_account) ?? pathLiAccountId,
        contact_id: asInt(contact.id) ?? asInt(r.contact),
        contact_profile_link: asText(contact.profile_link) ?? asText(campaignContactInfo.profile_link),
        contact_profile_link_sn: asText(contact.profile_link_sn) ?? asText(campaignContactInfo.profile_link_sn),
        contact_public_identifier: asText(contact.public_identifier) ?? asText(campaignContactInfo.public_identifier),
        contact_entity_urn: asText(contact.entity_urn) ?? asText(campaignContactInfo.entity_urn),
        contact_email: asText(contact.email) ?? asText(campaignContactInfo.email),
        contact_phone: asText(contact.phone) ?? asText(campaignContactInfo.phone),
        contact_address: asText(contact.address) ?? asText(campaignContactInfo.address),
        contact_name: asText(contact.name) ?? asText(campaignContactInfo.name),
        contact_job_title: asText(contact.job_title) ?? asText(campaignContactInfo.job_title),
        contact_company_name: asText(contact.company_name) ?? asText(campaignContactInfo.company_name),
        contact_status: asInt(r.contact_status),
        conversation_status: asInt(r.conversation_status),
        last_message_id: asInt(r.last_message),
        has_new_messages: asBool(r.has_new_messages),
        last_datetime: asIso(r.last_datetime),
        connected_at: asIso(r.connected_at),
        invited_at: asIso(r.invited_at),
        is_blacklisted: asBool(r.is_blacklisted),
        reason_failed: asInt(r.reason_failed),
        campaign_instance_id: campaignInstanceId,
        campaign_id: campaignId,
        campaign_name: pickText(r, [
          "campaign_name",
          "campaign_instance.name",
          "campaign.name",
        ]) ?? asText(campaignContact?.campaign_instance_name),
        campaign_contact_status: asInt(campaignContact?.status),
        campaign_running_status: asInt(campaignContact?.campaign_running_status),
        last_action_id: asInt(campaignContact?.last_action),
        nr_steps_before_responding: asInt(campaignContact?.nr_steps_before_responding),
        first_outbound_at: asIso(r.first_outbound_at),
        first_inbound_at: asIso(r.first_inbound_at),
        replied_at: asIso(r.replied_at),
        is_replied: asBool(r.is_replied),
        raw_payload: r,
        synced_at: nowIso(),
        updated_at: nowIso(),
      } as Json;
    })
    .filter((x): x is Json => !!x && asInt(x.li_account_id) !== null);
}

function mapMessages(path: string, rows: Json[]): Json[] {
  const pathMessengerId = extractPathId(path, "messengers");
  return rows
    .map((r) => {
      const id = asInt(r.id);
      if (id === null) return null;
      const messengerObj = asObj(r.messenger);
      const messengerContact = asObj(messengerObj.contact);
      const campaignContact = pickPrimaryCampaignContact(messengerContact);
      const sendAt = asIso(r.send_datetime);
      const receivedAt = asIso(r.received_datetime);
      const sendBy = asText(r.send_by);
      const body = asText(r.body);
      const extractedUrls = extractUrlsFromText(body);
      const extractedDomains = extractDomainsFromUrls(extractedUrls);
      const outbound = sendAt !== null;
      const inbound = receivedAt !== null;
      let direction: string | null = null;
      if (outbound && !inbound) direction = "outbound";
      if (inbound && !outbound) direction = "inbound";
      if (inbound && outbound) {
        const normalizedSendBy = (sendBy || "").toLowerCase();
        if (/lead|contact|prospect|recipient/.test(normalizedSendBy)) direction = "inbound";
        else if (/me|user|account|owner|sender|admin/.test(normalizedSendBy)) direction = "outbound";
      }
      return {
        id,
        messenger_id: asInt(r.messenger) ?? asInt(messengerObj.id) ?? pathMessengerId,
        li_account_id: asInt(r.li_account) ?? asInt(messengerObj.li_account),
        created_at_source: asIso(r.created),
        updated_at_source: asIso(r.updated),
        send_datetime: sendAt,
        received_datetime: receivedAt,
        body,
        status: asInt(r.status),
        send_by: sendBy,
        send_by_id: asInt(r.send_by_id),
        flag_direct: asBool(r.flag_direct),
        flag_mobile: asBool(r.flag_mobile),
        flag_open_inmail: asBool(r.flag_open_inmail),
        inmail: asBool(r.inmail),
        inmail_type: asInt(r.inmail_type),
        inmail_accepted: asBool(r.inmail_accepted),
        reason_failed: asInt(r.reason_failed),
        attachment: asText(r.attachment),
        attachment_size: asInt(r.attachment_size),
        has_attachment: asText(r.attachment) !== null || asInt(r.attachment_size) !== null,
        extracted_urls: extractedUrls,
        extracted_domains: extractedDomains,
        campaign_instance_id: pickInt(r, [
          "campaign_instance_id",
          "campaign_instance",
          "campaign_instance.id",
          "campaign.id",
          "messenger.campaign_instance_id",
        ]) ?? asInt(campaignContact?.campaign_instance),
        campaign_id: pickInt(r, [
          "campaign_id",
          "campaign",
          "campaign.id",
          "campaign_instance.campaign_id",
        ]),
        campaign_step_id: pickInt(r, [
          "campaign_step_id",
          "step_id",
          "campaign_step.id",
          "step.id",
          "action_id",
        ]),
        direction,
        is_outbound: outbound,
        is_inbound: inbound,
        event_datetime: receivedAt ?? sendAt ?? asIso(r.created),
        raw_payload: r,
        synced_at: nowIso(),
      } as Json;
    })
    .filter((x): x is Json => !!x && asInt(x.messenger_id) !== null);
}

function normalizeEndpointPath(path: string): string {
  const raw = path.startsWith("/") ? path : `/${path}`;
  if (raw.startsWith(`${EXPANDI_BASE_PATH}/`)) return raw.slice(EXPANDI_BASE_PATH.length);
  if (raw === EXPANDI_BASE_PATH) return "/";
  return raw;
}

async function normalizeAndUpsert(path: string, parsed: unknown): Promise<Record<string, number>> {
  const out: Record<string, number> = {};
  const rows = asList(parsed);
  if (!rows.length) return out;
  const normalizedPath = normalizeEndpointPath(path);

  if (/^\/?li_accounts\/?$/i.test(normalizedPath)) {
    const mapped = mapAccounts(rows);
    await sbInsert("expandi_accounts", mapped, true, "id");
    out.expandi_accounts = mapped.length;
    return out;
  }

  if (/^\/?li_accounts\/[^/]+\/campaign_instances\/?$/i.test(normalizedPath)) {
    const mapped = mapCampaignInstances(normalizedPath, rows);
    await sbInsert("expandi_campaign_instances", mapped, true, "id");
    out.expandi_campaign_instances = mapped.length;
    const catalogRows = mapped.map((row) => ({
      ...row,
      first_seen_at: nowIso(),
      last_seen_at: nowIso(),
      is_live: true,
      catalog_source: "campaign_instances",
    }));
    await sbInsert("expandi_campaign_catalog", catalogRows, true, "id");
    out.expandi_campaign_catalog = catalogRows.length;
    EXPANDI_CAMPAIGN_IDS_CACHE = null;
    const snapshots = mapCampaignStatsSnapshots(rows);
    await sbInsert(
      "expandi_campaign_stats_snapshots",
      snapshots,
      true,
      "snapshot_date,li_account_id,campaign_instance_id",
    );
    out.expandi_campaign_stats_snapshots = snapshots.length;
    return out;
  }

  if (/^\/?li_accounts\/[^/]+\/messengers\/?$/i.test(normalizedPath)) {
    const catalogCampaignIds = await fetchCatalogCampaignIds();
    const mappedAll = mapMessengers(normalizedPath, rows);
    const accepted: Json[] = [];
    const rejected: Json[] = [];
    for (const row of mappedAll) {
      const cid = asInt(row.campaign_instance_id);
      if (cid !== null && catalogCampaignIds.has(cid)) accepted.push(row);
      else rejected.push(row);
    }
    await sbInsert("expandi_messengers", accepted, true, "id");
    if (rejected.length) {
      await writeQuarantineRows("expandi_messengers", normalizedPath, "campaign_instance_id_not_in_catalog", rejected);
    }
    out.expandi_messengers = accepted.length;
    out.expandi_messengers_rejected = rejected.length;
    return out;
  }

  if (/^\/?li_accounts\/messengers\/[^/]+\/messages\/?$/i.test(normalizedPath)) {
    const catalogCampaignIds = await fetchCatalogCampaignIds();
    const mappedAll = mapMessages(normalizedPath, rows);
    const accepted: Json[] = [];
    const rejectedCatalog: Json[] = [];
    const rejectedCutoff: Json[] = [];
    for (const row of mappedAll) {
      const cid = asInt(row.campaign_instance_id);
      if (cid === null || !catalogCampaignIds.has(cid)) {
        rejectedCatalog.push(row);
        continue;
      }
      if (!isOnOrAfterMinDay(row.event_datetime ?? row.received_datetime ?? row.send_datetime ?? row.created_at_source)) {
        rejectedCutoff.push(row);
        continue;
      }
      accepted.push(row);
    }
    await sbInsert("expandi_messages", accepted, true, "id");
    if (rejectedCatalog.length) {
      await writeQuarantineRows("expandi_messages", normalizedPath, "campaign_instance_id_not_in_catalog", rejectedCatalog);
    }
    if (rejectedCutoff.length) {
      await writeQuarantineRows("expandi_messages", normalizedPath, "event_before_min_day", rejectedCutoff);
    }
    out.expandi_messages = accepted.length;
    out.expandi_messages_rejected_catalog = rejectedCatalog.length;
    out.expandi_messages_rejected_cutoff = rejectedCutoff.length;
    return out;
  }

  return out;
}

function mergeCounts(total: Record<string, number>, next: Record<string, number>): void {
  for (const [k, v] of Object.entries(next)) {
    total[k] = (total[k] || 0) + v;
  }
}

function extractIds(parsed: unknown, key = "id"): number[] {
  const ids = asList(parsed).map((r) => asInt(r[key])).filter((x): x is number => x !== null);
  return [...new Set(ids)];
}

async function requestStoreAndNormalize(args: {
  path: string;
  method: string;
  payload?: unknown;
  query?: unknown;
  storeTable: string;
  storeRaw?: boolean;
  normalize: boolean;
  followPagination?: boolean;
  maxPages?: number;
}): Promise<{
  ok: boolean;
  status_code: number;
  endpoint_path: string;
  method: string;
  normalized: Record<string, number>;
  parsed: unknown;
  total_count: number | null;
  pages_fetched: number;
}> {
  let normalized: Record<string, number> = {};
  let firstParsed: unknown = null;
  let firstStatus = 500;
  let firstOk = false;
  let anyFailed = false;
  let failureStatus: number | null = null;
  let pagesFetched = 0;
  let firstCount: number | null = null;
  const followPagination = args.followPagination === true && args.method.toUpperCase() === "GET";
  const maxPages = Math.max(1, Math.min(EXPANDI_MAX_PAGES, asInt(args.maxPages) || EXPANDI_MAX_PAGES));
  const allRows: Json[] = [];

  let currentPath = args.path;
  let currentQuery = args.query;
  const seen = new Set<string>();

  while (true) {
    const requestKey = `${currentPath}${toQueryString(currentQuery)}`;
    if (seen.has(requestKey)) break;
    seen.add(requestKey);

    const res = await expandiRequest(currentPath, args.method, args.payload, currentQuery);
    const text = await res.text();
    let parsed: unknown = null;
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = text;
    }

    if (pagesFetched === 0) {
      firstParsed = parsed;
      firstStatus = res.status;
      firstOk = res.ok;
      firstCount = pickInt(parsed, ["count"]);
    }
    pagesFetched += 1;
    if (!res.ok) {
      anyFailed = true;
      if (failureStatus === null) failureStatus = res.status;
    }
    if (followPagination) {
      const pageRows = asList(parsed);
      if (pageRows.length) allRows.push(...pageRows);
    }

    if (args.storeRaw !== false) {
      await sbInsert(args.storeTable, [{
        source: "expandi_probe",
        endpoint_path: requestKey,
        method: args.method,
        status_code: res.status,
        ok: res.ok,
        response_headers: Object.fromEntries(res.headers.entries()),
        response_body: parsed,
        synced_at: nowIso(),
      }]);
    }

    if (res.ok && args.normalize) {
      const pageNormalized = await normalizeAndUpsert(currentPath, parsed);
      mergeCounts(normalized, pageNormalized);
    }

    if (!followPagination || !res.ok || pagesFetched >= maxPages) break;
    const nextUrl = asText(asObj(parsed).next);
    if (!nextUrl) break;
    const next = parseNextUrl(nextUrl, currentPath);
    if (!next) break;
    currentPath = next.path;
    currentQuery = next.query;
  }

  if (followPagination) {
    if (allRows.length) {
      firstParsed = { results: allRows, count: firstCount };
    }
  }

  return {
    ok: firstOk && !anyFailed,
    status_code: failureStatus ?? firstStatus,
    endpoint_path: args.path,
    method: args.method,
    normalized,
    parsed: firstParsed,
    total_count: firstCount,
    pages_fetched: pagesFetched,
  };
}

Deno.serve(async (req: Request) => {
  const startedAt = nowIso();
  try {
    if (EXPANDI_SYNC_SECRET) {
      const headerSecret = (req.headers.get("x-sync-secret") || "").trim();
      const auth = (req.headers.get("authorization") || "").trim();
      const bearer = auth.toLowerCase().startsWith("bearer ") ? auth.slice(7).trim() : "";
      const ok = headerSecret === EXPANDI_SYNC_SECRET || bearer === EXPANDI_SYNC_SECRET;
      if (!ok) return new Response(JSON.stringify({ ok: false, error: "Unauthorized" }), { status: 401 });
    }

    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
      return new Response(JSON.stringify({ ok: false, error: "SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not configured" }), { status: 500 });
    }
    if (!EXPANDI_API_KEY && !(EXPANDI_LOGIN && EXPANDI_PASSWORD)) {
      return new Response(JSON.stringify({ ok: false, error: "Set EXPANDI_API_KEY (or EXPANDI_LOGIN+EXPANDI_PASSWORD)" }), { status: 500 });
    }

    const input = req.method === "POST" ? await req.json().catch(() => ({})) as Json : {};
    const path = String(input.path || "/open-swagger/").trim();
    const method = String(input.method || "GET").trim().toUpperCase();
    const query = input.query;
    const payload = input.payload;
    const storeTable = String(input.store_table || "expandi_raw").trim();
    const storeRaw = input.store_raw !== false;
    const normalize = input.normalize !== false;
    const mode = String(input.mode || "").trim().toLowerCase();
    const backfillTick = mode === "backfill_tick";
    const fullSync = input.full_sync === true || mode === "full" || backfillTick;
    const campaignsTick = mode === "campaigns_tick" || mode === "campaigns_only_tick" || mode === "campaigns";

    if (campaignsTick) {
      const totals: Record<string, number> = {};
      const errors: Json[] = [];
      let requestsTotal = 0;
      let requestsFailed = 0;
      let pagesFetchedTotal = 0;

      const maxPagesAccounts = Math.max(1, asInt(input.max_pages_accounts) || 2);
      const maxPagesCampaigns = Math.max(1, asInt(input.max_pages_campaigns) || 10);

      const campaignsStoreRaw = input.store_raw === true;
      const accountRes = await requestStoreAndNormalize({
        path: "/li_accounts/",
        method: "GET",
        storeTable,
        storeRaw: campaignsStoreRaw,
        normalize,
        followPagination: true,
        maxPages: maxPagesAccounts,
      });
      requestsTotal += 1;
      pagesFetchedTotal += accountRes.pages_fetched;
      mergeCounts(totals, accountRes.normalized);
      if (!accountRes.ok) {
        requestsFailed += 1;
        errors.push({
          endpoint_path: accountRes.endpoint_path,
          status_code: accountRes.status_code,
        });
      }

      const accountIds = extractIds(accountRes.parsed);
      const accountsPerRun = Math.max(1, asInt(input.accounts_per_run) || 1);
      const savedStart = await getSyncStateInt("expandi_campaigns_next_account_idx");
      const requestedStart = asInt(input.start_account_idx);
      const startIdxRaw = requestedStart ?? savedStart ?? 0;
      const startIdx = accountIds.length ? ((startIdxRaw % accountIds.length) + accountIds.length) % accountIds.length : 0;

      const selectedAccountIds: number[] = [];
      if (accountIds.length > 0) {
        const n = Math.min(accountsPerRun, accountIds.length);
        for (let i = 0; i < n; i++) {
          selectedAccountIds.push(accountIds[(startIdx + i) % accountIds.length]);
        }
      }

      for (const liAccountId of selectedAccountIds) {
        const campaignRes = await requestStoreAndNormalize({
          path: `/li_accounts/${liAccountId}/campaign_instances/`,
          method: "GET",
          storeTable,
          storeRaw: campaignsStoreRaw,
          normalize,
          followPagination: true,
          maxPages: maxPagesCampaigns,
        });
        requestsTotal += 1;
        pagesFetchedTotal += campaignRes.pages_fetched;
        mergeCounts(totals, campaignRes.normalized);
        if (!campaignRes.ok) {
          requestsFailed += 1;
          errors.push({
            endpoint_path: campaignRes.endpoint_path,
            status_code: campaignRes.status_code,
            li_account_id: liAccountId,
          });
        }
      }

      if (accountIds.length > 0) {
        const nextIdx = (startIdx + selectedAccountIds.length) % accountIds.length;
        await setSyncStateInt("expandi_campaigns_next_account_idx", nextIdx);
      }
      await setSyncStateInt("expandi_last_sync_ts", Math.floor(Date.now() / 1000));

      const responseBody = {
        ok: requestsFailed === 0,
        mode: "campaigns_tick",
        requests_total: requestsTotal,
        pages_fetched_total: pagesFetchedTotal,
        requests_failed: requestsFailed,
        accounts_total: accountIds.length,
        accounts_processed: selectedAccountIds.length,
        accounts_per_run: accountsPerRun,
        max_pages_accounts: maxPagesAccounts,
        max_pages_campaigns: maxPagesCampaigns,
        start_account_idx: startIdx,
        next_account_idx: accountIds.length ? (startIdx + selectedAccountIds.length) % accountIds.length : 0,
        normalized: totals,
        errors,
      };
      const responseStatus = requestsFailed === 0 ? 200 : 207;
      await logIngestRun({
        mode: "campaigns_tick",
        ok: responseBody.ok,
        requests_total: requestsTotal,
        pages_fetched_total: pagesFetchedTotal,
        requests_failed: requestsFailed,
        normalized: totals,
        errors,
        started_at: startedAt,
        finished_at: nowIso(),
      });
      return Response.json(responseBody, { status: responseStatus });
    }

    if (fullSync) {
      const totals: Record<string, number> = {};
      const errors: Json[] = [];
      let requestsTotal = 0;
      let requestsFailed = 0;
      let pagesFetchedTotal = 0;

      const maxPagesAccounts = Math.max(1, asInt(input.max_pages_accounts) || (backfillTick ? 1 : 10));
      const maxPagesCampaigns = Math.max(1, asInt(input.max_pages_campaigns) || (backfillTick ? 1 : 10));
      const maxPagesMessengers = Math.max(1, asInt(input.max_pages_messengers) || (backfillTick ? 1 : 10));
      const maxPagesMessages = Math.max(1, asInt(input.max_pages_messages) || (backfillTick ? 1 : 5));

      const accountRes = await requestStoreAndNormalize({
        path: "/li_accounts/",
        method: "GET",
        storeTable,
        storeRaw,
        normalize,
        followPagination: true,
        maxPages: maxPagesAccounts,
      });
      requestsTotal += 1;
      pagesFetchedTotal += accountRes.pages_fetched;
      mergeCounts(totals, accountRes.normalized);
      if (!accountRes.ok) {
        requestsFailed += 1;
        errors.push({
          endpoint_path: accountRes.endpoint_path,
          status_code: accountRes.status_code,
        });
      }

      const accountIds = extractIds(accountRes.parsed);
      const accountsPerRun = Math.max(1, asInt(input.accounts_per_run) || (backfillTick ? 1 : 1));
      const maxMessengersPerRun = Math.max(1, asInt(input.max_messengers_per_run) || (backfillTick ? 1 : 1));
      const savedStart = await getSyncStateInt("expandi_full_sync_next_account_idx");
      const requestedStart = asInt(input.start_account_idx);
      const startIdxRaw = requestedStart ?? savedStart ?? 0;
      const startIdx = accountIds.length ? ((startIdxRaw % accountIds.length) + accountIds.length) % accountIds.length : 0;

      const selectedAccountIds: number[] = [];
      if (accountIds.length > 0) {
        const n = Math.min(accountsPerRun, accountIds.length);
        for (let i = 0; i < n; i++) {
          selectedAccountIds.push(accountIds[(startIdx + i) % accountIds.length]);
        }
      }

      for (const liAccountId of selectedAccountIds) {
        const campaignPath = `/li_accounts/${liAccountId}/campaign_instances/`;
        const campaignRes = await requestStoreAndNormalize({
          path: campaignPath,
          method: "GET",
          storeTable,
          storeRaw,
          normalize,
          followPagination: true,
          maxPages: maxPagesCampaigns,
        });
        requestsTotal += 1;
        pagesFetchedTotal += campaignRes.pages_fetched;
        mergeCounts(totals, campaignRes.normalized);
        if (!campaignRes.ok) {
          requestsFailed += 1;
          errors.push({
            endpoint_path: campaignRes.endpoint_path,
            status_code: campaignRes.status_code,
            li_account_id: liAccountId,
          });
        }

        const messengersPath = `/li_accounts/${liAccountId}/messengers/`;
        const messengersPageKey = `expandi_full_sync_next_messengers_page_${liAccountId}`;
        const savedMessengersPage = await getSyncStateInt(messengersPageKey);
        const requestedMessengersPage = asInt(input.start_messengers_page);
        const startMessengersPage = Math.max(1, requestedMessengersPage ?? savedMessengersPage ?? 1);
        const messengersRes = await requestStoreAndNormalize({
          path: messengersPath,
          method: "GET",
          query: { page: startMessengersPage },
          storeTable,
          storeRaw,
          normalize,
          followPagination: true,
          maxPages: maxPagesMessengers,
        });
        requestsTotal += 1;
        pagesFetchedTotal += messengersRes.pages_fetched;
        mergeCounts(totals, messengersRes.normalized);
        if (!messengersRes.ok) {
          requestsFailed += 1;
          errors.push({
            endpoint_path: messengersRes.endpoint_path,
            status_code: messengersRes.status_code,
            li_account_id: liAccountId,
          });
        }

        const messengerIds = extractIds(messengersRes.parsed);
        const messengerStartKey = `expandi_full_sync_next_messenger_idx_${liAccountId}`;
        const savedMessengerStart = await getSyncStateInt(messengerStartKey);
        const messengerStartRaw = savedMessengerStart ?? 0;
        const messengerStart = messengerIds.length ? ((messengerStartRaw % messengerIds.length) + messengerIds.length) % messengerIds.length : 0;
        const selectedMessengerIds: number[] = [];
        if (messengerIds.length > 0) {
          const n = Math.min(maxMessengersPerRun, messengerIds.length);
          for (let i = 0; i < n; i++) {
            selectedMessengerIds.push(messengerIds[(messengerStart + i) % messengerIds.length]);
          }
        }

        for (const messengerId of selectedMessengerIds) {
          const messagesPath = `/li_accounts/messengers/${messengerId}/messages/`;
          const messagesRes = await requestStoreAndNormalize({
            path: messagesPath,
            method: "GET",
            storeTable,
            storeRaw,
            normalize,
            followPagination: true,
            maxPages: maxPagesMessages,
          });
          requestsTotal += 1;
          pagesFetchedTotal += messagesRes.pages_fetched;
          mergeCounts(totals, messagesRes.normalized);
          if (!messagesRes.ok) {
            requestsFailed += 1;
            errors.push({
              endpoint_path: messagesRes.endpoint_path,
              status_code: messagesRes.status_code,
              li_account_id: liAccountId,
              messenger_id: messengerId,
            });
          }
        }

        if (messengerIds.length > 0) {
          const nextMessengerIdx = (messengerStart + selectedMessengerIds.length) % messengerIds.length;
          await setSyncStateInt(messengerStartKey, nextMessengerIdx);
        }

        // Advance messengers page window so next run continues from later pages instead of re-reading page 1.
        const totalMessengers = messengersRes.total_count ?? messengerIds.length;
        const totalMessengerPages = Math.max(1, Math.ceil(totalMessengers / 10)); // Expandi list endpoints return 10 items/page.
        let nextMessengersPage = startMessengersPage + Math.max(1, messengersRes.pages_fetched);
        if (nextMessengersPage > totalMessengerPages) {
          nextMessengersPage = ((nextMessengersPage - 1) % totalMessengerPages) + 1;
        }
        await setSyncStateInt(messengersPageKey, nextMessengersPage);
      }

      if (accountIds.length > 0) {
        const nextIdx = (startIdx + selectedAccountIds.length) % accountIds.length;
        await setSyncStateInt("expandi_full_sync_next_account_idx", nextIdx);
      }

      await setSyncStateInt("expandi_last_sync_ts", Math.floor(Date.now() / 1000));

      const responseBody = {
        ok: requestsFailed === 0,
        mode: backfillTick ? "backfill_tick" : "full_sync",
        requests_total: requestsTotal,
        pages_fetched_total: pagesFetchedTotal,
        requests_failed: requestsFailed,
        accounts_total: accountIds.length,
        accounts_per_run: accountsPerRun,
        max_messengers_per_run: maxMessengersPerRun,
        max_pages_accounts: maxPagesAccounts,
        max_pages_campaigns: maxPagesCampaigns,
        max_pages_messengers: maxPagesMessengers,
        max_pages_messages: maxPagesMessages,
        accounts_processed: selectedAccountIds.length,
        start_account_idx: startIdx,
        next_account_idx: accountIds.length ? (startIdx + selectedAccountIds.length) % accountIds.length : 0,
        normalized: totals,
        errors,
      };
      const responseStatus = requestsFailed === 0 ? 200 : 207;
      await logIngestRun({
        mode: backfillTick ? "backfill_tick" : "full_sync",
        ok: responseBody.ok,
        requests_total: requestsTotal,
        pages_fetched_total: pagesFetchedTotal,
        requests_failed: requestsFailed,
        normalized: totals,
        errors,
        started_at: startedAt,
        finished_at: nowIso(),
      });
      return Response.json(responseBody, { status: responseStatus });
    }

    const single = await requestStoreAndNormalize({
      path,
      method,
      payload,
      query,
      storeTable,
      storeRaw,
      normalize,
      followPagination: input.follow_pagination !== false,
    });

    await setSyncStateInt("expandi_last_sync_ts", Math.floor(Date.now() / 1000));

    const responseBody = {
      ok: single.ok,
      status_code: single.status_code,
      endpoint_path: single.endpoint_path,
      method: single.method,
      normalized: single.normalized,
      pages_fetched: single.pages_fetched,
      preview: typeof single.parsed === "string" ? single.parsed.slice(0, 300) : single.parsed,
    };
    const responseStatus = single.ok ? 200 : 502;
    await logIngestRun({
      mode: "single",
      ok: single.ok,
      requests_total: 1,
      pages_fetched_total: single.pages_fetched,
      requests_failed: single.ok ? 0 : 1,
      normalized: single.normalized,
      errors: single.ok ? [] : [{ endpoint_path: single.endpoint_path, status_code: single.status_code }],
      started_at: startedAt,
      finished_at: nowIso(),
    });
    return Response.json(responseBody, { status: responseStatus });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error("expandi-sync failed", message);
    await logIngestRun({
      mode: "error",
      ok: false,
      requests_total: 0,
      pages_fetched_total: 0,
      requests_failed: 1,
      normalized: {},
      errors: [{ message }],
      started_at: startedAt,
      finished_at: nowIso(),
    });
    return new Response(JSON.stringify({ ok: false, error: message }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
});
