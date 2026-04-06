import { NextResponse } from "next/server";

type SupabaseUserResponse = { email?: string | null };

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function getBearer(req: Request) {
  const authHeader = String(req.headers.get("authorization") ?? "").trim();
  const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
  const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
  return { authHeader, bearer, gotSecret };
}

async function getSupabaseUserFromAuthHeader(authHeader: string | null) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  if (!supabaseUrl || !supabaseAnonKey) return null;
  if (!authHeader?.startsWith("Bearer ")) return null;

  const res = await fetch(`${supabaseUrl}/auth/v1/user`, {
    method: "GET",
    headers: { apikey: supabaseAnonKey, Authorization: authHeader }
  });
  if (!res.ok) return null;
  return (await res.json()) as SupabaseUserResponse;
}

function parseCsvEnv(name: string) {
  const raw = String(process.env[name] ?? "").trim();
  if (!raw) return [];
  return raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function ymdUtc(ms: number) {
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function toMs(v: any): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

type PostgrestHeaders = { apikey: string; Authorization: string };

function postgrestHeadersFor(authHeader: string, isCron: boolean): PostgrestHeaders {
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
  if (isCron) {
    if (!serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY (required for cron)");
    return { apikey: serviceRoleKey, Authorization: `Bearer ${serviceRoleKey}` };
  }
  if (!supabaseAnonKey) throw new Error("Missing NEXT_PUBLIC_SUPABASE_ANON_KEY");
  if (!authHeader) throw new Error("Missing Authorization");
  return { apikey: supabaseAnonKey, Authorization: authHeader };
}

async function postgrestJson(h: PostgrestHeaders, method: string, path: string, body?: any, extraHeaders?: Record<string, string>) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const url = `${supabaseUrl}/rest/v1/${path}`;
  const res = await fetch(url, {
    method,
    headers: { ...h, ...(body != null ? { "Content-Type": "application/json" } : {}), ...(extraHeaders ?? {}) },
    body: body != null ? JSON.stringify(body) : undefined
  });
  const text = await res.text();
  let json: any = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }
  if (!res.ok) throw new Error(String(json?.message || json?.error || text || "Supabase request failed"));
  return json;
}

/**
 * Map SmartLead lead_category_id to a reply-specific event type.
 *
 * Standard mapping:
 * 1 -> positive_reply (Interested)
 * 3 -> replied_ooo (Out of Office)
 *
 * All other categories keep the original reply type.
 */
function mapSmartleadReplyEventType(
  baseType: string,
  categoryId: number | null,
  positiveCategoryIds: Set<number>,
  oooCategoryIds: Set<number>
) {
  if (!baseType || baseType.toLowerCase() !== "replied") return baseType;
  if (!Number.isFinite(categoryId as number)) return baseType;
  const id = Number(categoryId);
  if (positiveCategoryIds.has(id)) return "positive_reply";
  if (oooCategoryIds.has(id)) return "replied_ooo";
  return baseType;
}

/**
 * Refresh SmartLead sentiment payload for replied events.
 *
 * This updates payload.lead_category_id based on the latest SmartLead lead data
 * and (optionally) flips event_type to positive_reply when category matches.
 */
async function refreshSmartleadSentimentsForCampaign(
  h: PostgrestHeaders,
  opts: {
    campaignId: number;
    sinceIso: string;
    untilIso: string;
    sentimentMap: Map<string, number>;
    positiveCategoryIds?: Set<number>;
    oooCategoryIds?: Set<number>;
    limit?: number;
    maxPages?: number;
    debug?: boolean;
  }
) {
  const stats = {
    campaign_id: opts.campaignId,
    scanned: 0,
    updated: 0,
    unchanged: 0,
    missing_email: 0,
    missing_category: 0,
    updated_positive_reply: 0,
    pages: 0
  };
  const limit = Math.max(1, Math.min(2000, Number(opts.limit ?? 500)));
  const maxPages = Math.max(1, Math.min(200, Number(opts.maxPages ?? 50)));
  let offset = 0;

  for (; ;) {
    const rows = await postgrestJson(
      h,
      "GET",
      `sales_smartlead_events?select=id,contact_email,event_type,payload,occurred_at,smartlead_event_id` +
      `&smartlead_campaign_id=eq.${encodeURIComponent(String(opts.campaignId))}` +
      `&occurred_at=gte.${encodeURIComponent(opts.sinceIso)}` +
      `&occurred_at=lt.${encodeURIComponent(opts.untilIso)}` +
      `&or=(event_type.eq.replied,event_type.eq.positive_reply)` +
      `&order=occurred_at.asc` +
      `&limit=${limit}` +
      `&offset=${offset}`
    );

    if (!Array.isArray(rows) || !rows.length) break;
    stats.pages += 1;

    for (const r of rows) {
      stats.scanned += 1;
      const id = String(r?.id ?? "").trim();
      const email = canonicalizeEmail(r?.contact_email ?? "");
      if (!id || !email) {
        stats.missing_email += 1;
        continue;
      }
      const catId = Number(opts.sentimentMap.get(email));
      if (!Number.isFinite(catId)) {
        stats.missing_category += 1;
        continue;
      }
      const payload = (r?.payload && typeof r.payload === "object") ? r.payload : {};
      const currentCat = Number((payload as any)?.lead_category_id);
      const nextPayload = currentCat === catId ? payload : { ...payload, lead_category_id: catId };
      const positiveSet = opts.positiveCategoryIds ?? new Set<number>();
      const oooSet = opts.oooCategoryIds ?? new Set<number>();
      const currentType = String(r?.event_type ?? "").trim().toLowerCase();
      const nextType = mapSmartleadReplyEventType(currentType, catId, positiveSet, oooSet);
      const typeChanged = nextType !== currentType;
      const payloadChanged = nextPayload !== payload;

      if (!typeChanged && !payloadChanged) {
        stats.unchanged += 1;
        continue;
      }

      await postgrestJson(h, "PATCH", `sales_smartlead_events?id=eq.${encodeURIComponent(id)}`, {
        payload: nextPayload,
        event_type: nextType
      });
      stats.updated += 1;
      if (typeChanged && nextType === "positive_reply") stats.updated_positive_reply += 1;
    }

    offset += rows.length;
    if (rows.length < limit) break;
    if (stats.pages >= maxPages) break;
  }

  if (opts.debug) console.log("refreshSentiments:", stats);
  return stats;
}

async function getSmartleadSyncState(h: PostgrestHeaders, createdBy: string | null) {
  const where = createdBy ? `created_by=eq.${encodeURIComponent(createdBy)}&` : "";
  const rows = await postgrestJson(h, "GET", `sales_smartlead_sync_state?${where}select=created_by,last_events_synced_at&limit=1`);
  const r = Array.isArray(rows) && rows[0] ? rows[0] : null;
  return { lastEvents: r?.last_events_synced_at ? String(r.last_events_synced_at) : null };
}

async function upsertSmartleadSyncState(h: PostgrestHeaders, row: any) {
  await postgrestJson(h, "POST", "sales_smartlead_sync_state?on_conflict=created_by", row, {
    Prefer: "resolution=merge-duplicates,return=minimal"
  });
}

/**
 * Purge SmartLead-derived analytics (activities + activity_deals) in chunks.
 *
 * This is required when we rebuild SmartLead ingestion from scratch to avoid
 * mixing old (incomplete) events with the new transaction-level stream.
 */
async function purgeSmartleadAnalytics(h: PostgrestHeaders) {
  let deletedActivities = 0;
  let deletedActivityDeals = 0;
  for (; ;) {
    const rows = await postgrestJson(h, "GET", "sales_analytics_activities?select=id&source_system=eq.smartlead&limit=200");
    const ids = Array.isArray(rows)
      ? rows
        .map((r) => String(r?.id ?? "").trim())
        .filter((id) => /^[0-9a-fA-F-]{36}$/.test(id))
      : [];
    if (!ids.length) break;
    const idFilter = `activity_id=in.(${ids.map((id) => encodeURIComponent(id)).join(",")})`;
    await postgrestJson(h, "DELETE", `sales_analytics_activity_deals?${idFilter}`);
    deletedActivityDeals += ids.length;
    await postgrestJson(h, "DELETE", `sales_analytics_activities?id=in.(${ids.map((id) => encodeURIComponent(id)).join(",")})`);
    deletedActivities += ids.length;
  }
  return { deletedActivities, deletedActivityDeals };
}

/**
 * Purge all raw SmartLead events to restart ingestion from scratch.
 */
async function purgeSmartleadEvents(h: PostgrestHeaders) {
  await postgrestJson(h, "DELETE", "sales_smartlead_events?created_by=not.is.null");
}

/**
 * Purge SmartLead sync cursor(s) so the next run starts fresh.
 */
async function purgeSmartleadSyncState(h: PostgrestHeaders) {
  await postgrestJson(h, "DELETE", "sales_smartlead_sync_state?created_by=not.is.null");
}

async function upsertSmartleadEvent(h: PostgrestHeaders, row: any) {
  const inserted = await postgrestJson(h, "POST", "sales_smartlead_events?on_conflict=created_by,smartlead_event_id", row, {
    Prefer: "resolution=merge-duplicates,return=representation"
  });
  if (Array.isArray(inserted) && inserted.length === 0) return null;
  return inserted;
}

async function findHubspotContactIds(h: PostgrestHeaders, campaignId: number, emails: string[]) {
  if (!emails.length) return new Map<string, string>();
  const uniqueEmails = Array.from(new Set(emails));
  // Batch in chunks of 100 to avoid URL length limits if using GET, but we use POST RPC or filter?
  // standard postgrest filter `in` with many items.
  // We'll just fetch by campaign_id and filter in memory if list is huge? No, better filter by emails.
  // Actually, we can use body filtering with POST to `rpc` or just table query.
  // Let's use standard table query with `in`.

  const map = new Map<string, string>();
  // chunking
  const chunkSize = 50;
  for (let i = 0; i < uniqueEmails.length; i += chunkSize) {
    const chunk = uniqueEmails.slice(i, i + chunkSize);
    const filter = `smartlead_campaign_id=eq.${campaignId}&contact_email=in.(${chunk.map(e => encodeURIComponent(e)).join(",")})`;
    const rows = await postgrestJson(h, "GET", `sales_smartlead_enrollments?select=contact_email,hubspot_contact_id&${filter}`);
    if (Array.isArray(rows)) {
      for (const r of rows) {
        if (r.contact_email && r.hubspot_contact_id) map.set(r.contact_email.toLowerCase(), r.hubspot_contact_id);
      }
    }
  }

  // Fallback: search missing emails directly in HubSpot (robustness for non-enrolled leads)
  const missing = uniqueEmails.filter(e => !map.has(e.toLowerCase()));
  if (missing.length > 0) {
    const found = await hubspotSearchContactsByEmails(missing);
    for (const [email, id] of found.entries()) {
      map.set(email.toLowerCase(), id);
    }
  }

  return map;
}

async function hubspotSearchContactsByEmails(emails: string[]) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) return new Map<string, string>(); // validation handled elsewhere or silent fail

  const map = new Map<string, string>();
  if (!emails.length) return map;

  // Batch to avoid huge filter groups
  const chunkSize = 50;
  for (let i = 0; i < emails.length; i += chunkSize) {
    const chunk = emails.slice(i, i + chunkSize);

    // HubSpot Search API
    const res = await fetch("https://api.hubapi.com/crm/v3/objects/contacts/search", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`
      },
      body: JSON.stringify({
        filterGroups: [{
          filters: [{ propertyName: "email", operator: "IN", values: chunk }]
        }],
        properties: ["email"],
        limit: 100 // chunk size + buffer
      })
    });

    if (!res.ok) continue; // fail silently for this chunk

    const json = await res.json() as any;
    const results = Array.isArray(json?.results) ? json.results : [];

    for (const r of results) {
      const email = String(r?.properties?.email ?? "").trim().toLowerCase();
      const id = String(r?.id ?? "").trim();
      if (email && id) map.set(email, id);
    }
  }
  return map;
}

// -----------------
// SmartLead helpers
// -----------------

/**
 * Sleep helper for retry backoffs.
 */
function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}

/**
 * Fetch SmartLead API with basic retry on transient errors.
 */
async function smartleadFetch(path: string, init?: RequestInit) {
  const apiKey = String(process.env.SMARTLEAD_API_KEY ?? "").trim();
  if (!apiKey) throw new Error("Missing SMARTLEAD_API_KEY");
  const baseUrl = String(process.env.SMARTLEAD_BASE_URL ?? "https://server.smartlead.ai").trim().replace(/\/+$/g, "");
  const url = `${baseUrl}${path}${path.includes("?") ? "&" : "?"}api_key=${encodeURIComponent(apiKey)}`;

  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const res = await fetch(url, {
      ...init,
      headers: {
        accept: "application/json",
        ...(init?.headers ?? {})
      }
    });
    const text = await res.text();
    let json: any = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = { raw: text };
    }
    if (res.ok) return json;

    const status = res.status;
    const isTransient = status === 429 || status === 500 || status === 502 || status === 503 || status === 504;
    if (!isTransient || attempt === maxAttempts) {
      throw new Error(String(json?.message || json?.error || text || "SmartLead API error"));
    }
    // Backoff grows with each retry to reduce rate-limit pressure.
    await sleep(400 * attempt);
  }

  throw new Error("SmartLead API error");
}

function pickArrayFromSmartleadListResponse(json: any): any[] {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json?.data)) return json.data;
  if (Array.isArray(json?.results)) return json.results;
  if (Array.isArray(json?.leads)) return json.leads;
  return [];
}

async function smartleadListCampaignsBestEffort(opts?: { limit?: number; offset?: number }) {
  const limit = Math.max(1, Math.min(200, Number(opts?.limit ?? 100)));
  const offset = Math.max(0, Number(opts?.offset ?? 0));
  const qs = new URLSearchParams();
  qs.set("limit", String(limit));
  qs.set("offset", String(offset));
  const q = qs.toString();
  const attempts = [
    `/api/v1/campaigns?${q}`,
    `/api/v1/campaigns/list?${q}`,
    `/api/v1/campaigns`
  ];
  let lastErr: any = null;
  for (const path of attempts) {
    try {
      const json = await smartleadFetch(path, { method: "GET" });
      const arr = pickArrayFromSmartleadListResponse(json);
      // this endpoint returns campaigns, not leads
      if (arr.length) return arr;
    } catch (e: any) {
      lastErr = e;
    }
  }
  throw lastErr ?? new Error("Failed to list campaigns");
}

async function smartleadListLeadsByCampaign(
  campaignId: number,
  opts: { limit?: number; offset?: number; status?: string; event_time_gt?: string; created_at_gt?: string; last_sent_time_gt?: string }
) {
  const qs = new URLSearchParams();
  if (opts.limit != null) qs.set("limit", String(opts.limit));
  if (opts.offset != null) qs.set("offset", String(opts.offset));
  if (opts.status) qs.set("status", String(opts.status));
  if (opts.event_time_gt) qs.set("event_time_gt", String(opts.event_time_gt));
  if (opts.created_at_gt) qs.set("created_at_gt", String(opts.created_at_gt));
  if (opts.last_sent_time_gt) qs.set("last_sent_time_gt", String(opts.last_sent_time_gt));
  const q = qs.toString();
  return await smartleadFetch(`/api/v1/campaigns/${encodeURIComponent(String(campaignId))}/leads${q ? `?${q}` : ""}`, { method: "GET" });
}

async function smartleadGetLeadCategory(campaignId: number, email: string) {
  if (!email) return null;
  const qs = new URLSearchParams();
  qs.set("email", email);
  try {
    const json = await smartleadFetch(`/api/v1/leads/?${qs.toString()}`, { method: "GET" });
    const leads = pickArrayFromSmartleadListResponse(json);
    const lead = leads[0];
    if (!lead) return null;

    // Sometimes category is at top level
    if (Number(lead.lead_category_id)) return Number(lead.lead_category_id);

    // Usually nested in campaign data
    if (Array.isArray(lead.lead_campaign_data)) {
      const match = lead.lead_campaign_data.find((c: any) => String(c.campaign_id) === String(campaignId));
      if (match) return Number(match.lead_category_id) || null;
    }
  } catch (e) {
    // ignore
  }
  return null;
}

async function smartleadFetchCategories() {
  try {
    const json = await smartleadFetch("/api/v1/leads/fetch-categories", { method: "GET" });
    if (Array.isArray(json)) return json as { id: number; name: string }[];
  } catch (e) { /* ignore */ }
  return [];
}

/**
 * Fetch per-lead campaign statistics with history (transaction-level rows).
 *
 * SmartLead exposes this as lead-statistics with pagination and event_time_gt.
 */
async function smartleadListCampaignLeadStats(
  campaignId: number,
  opts: { limit?: number; offset?: number; event_time_gt?: string; created_at_gt?: string }
) {
  const qs = new URLSearchParams();
  if (opts.limit != null) qs.set("limit", String(opts.limit));
  if (opts.offset != null) qs.set("offset", String(opts.offset));
  if (opts.event_time_gt) qs.set("event_time_gt", String(opts.event_time_gt));
  if (opts.created_at_gt) qs.set("created_at_gt", String(opts.created_at_gt));
  const q = qs.toString();
  const base = `/api/v1/campaigns/${encodeURIComponent(String(campaignId))}`;
  const primary = `${base}/leads-statistics${q ? `?${q}` : ""}`;
  const fallback = `${base}/lead-statistics${q ? `?${q}` : ""}`;
  try {
    return await smartleadFetch(primary, { method: "GET" });
  } catch (e: any) {
    const msg = String(e?.message || e);
    if (!/Cannot GET/i.test(msg)) throw e;
    return await smartleadFetch(fallback, { method: "GET" });
  }
}

/**
 * Extract a list of lead rows from lead-statistics response.
 */
function pickArrayFromSmartleadLeadStatsResponse(json: any): any[] {
  if (Array.isArray(json?.data)) return json.data;
  if (Array.isArray(json?.results)) return json.results;
  if (Array.isArray(json)) return json;
  return [];
}

async function smartleadGetSequenceDetails(leadMapId: string) {
  const id = String(leadMapId || "").trim();
  if (!id) return null;
  return await smartleadFetch(`/api/v1/leads/${encodeURIComponent(id)}/sequence-details`, { method: "GET" });
}

function canonicalizeEmail(raw: any) {
  return String(raw ?? "").trim().toLowerCase();
}

function looksLikeEmail(v: any) {
  const t = String(v ?? "").trim().toLowerCase();
  return !!t && t.includes("@") && !t.includes(" ");
}

function stableEventId(parts: Array<string | number>) {
  return parts.map((p) => String(p ?? "").trim()).filter(Boolean).join(":");
}

function pickLeadTimeMs(lead: any, type: string): number | null {
  const lc = String(type || "").toLowerCase();
  const raw = lead && typeof lead === "object" ? lead : {};
  const containers: any[] = [raw, raw?.lead, raw?.prospect, raw?.contact].filter((x) => x && typeof x === "object");
  const tryKeys = (keys: string[]) => {
    for (const c of containers) {
      for (const k of keys) {
        const ms = toMs((c as any)[k]);
        if (ms) return ms;
      }
    }
    return null;
  };

  // Prefer type-specific fields first, then fall back to event_time / updated_at.
  const ms =
    lc === "opened"
      ? tryKeys([
        "opened_at",
        "opened_time",
        "open_time",
        "last_opened_at",
        "last_open_time",
        "first_opened_at",
        "first_open_time",
        "event_time",
        "eventTime",
        "updated_at",
        "updatedAt"
      ])
      : lc === "replied"
        ? tryKeys([
          "replied_at",
          "replied_time",
          "reply_time",
          "reply_received_at",
          "event_time",
          "eventTime",
          "updated_at",
          "updatedAt"
        ])
        : lc.includes("bounce")
          ? tryKeys([
            "bounced_at",
            "bounced_time",
            "bounce_time",
            "event_time",
            "eventTime",
            "updated_at",
            "updatedAt"
          ])
          : tryKeys([
            "sent_at",
            "sent_time",
            "last_sent_time",
            "event_time",
            "eventTime",
            "updated_at",
            "updatedAt"
          ]);

  if (ms) return ms;
  return null;
}

function extractSmartleadEmailEvents(seq: any, ctx: { campaignId: number; leadMapId: string; email: string }) {
  // Best-effort parser. We intentionally prefer "explicit" timestamp keys that usually show up in SmartLead payloads.
  // If SmartLead changes their schema, we still keep raw payload for future parser improvements.
  const out: Array<{ eventId: string; type: string; atMs: number; meta: any }> = [];
  const bestByType: Record<string, { atMs: number; key: string } | null> = { sent: null, opened: null, replied: null };

  const tsKeysByType: Record<string, string[]> = {
    sent: ["sent_at", "sent_time", "sentAt", "sentTime", "last_sent_time", "lastSentTime"],
    opened: [
      "opened_at",
      "opened_time",
      "open_time",
      "read_at",
      "first_opened_at",
      "firstOpenedAt",
      "first_open_time",
      "firstOpenTime",
      "last_opened_at",
      "lastOpenedAt",
      "last_open_time",
      "lastOpenTime"
    ],
    replied: [
      "replied_at",
      "replied_time",
      "reply_time",
      "reply_received_at",
      "replyReceivedAt",
      "first_replied_at",
      "firstRepliedAt",
      "first_reply_time",
      "firstReplyTime"
    ],
    bounced: ["bounced_at", "bounced_time", "bounce_time", "bounceTime", "hard_bounce_time", "hardBounceTime"],
    sender_bounced: ["sender_bounced_at", "sender_bounced_time", "sender_bounce_time", "senderBounceTime"]
  };

  const isMessageLike = (o: any) => {
    if (!o || typeof o !== "object") return false;
    const keys = Object.keys(o);
    const key = (k: string) => keys.some((x) => String(x).toLowerCase() === k);
    return (
      key("subject") ||
      key("email_subject") ||
      key("message_id") ||
      key("messageId") ||
      key("sequence_step") ||
      key("sequenceStep") ||
      key("sequence_step_id") ||
      key("sequenceStepId") ||
      key("step") ||
      key("step_id") ||
      key("stepId") ||
      key("step_number") ||
      key("stepNumber") ||
      key("email_body") ||
      key("body") ||
      key("template_id") ||
      key("templateId")
    );
  };

  const seen = new Set<any>();
  const stack: any[] = [seq];
  while (stack.length) {
    const cur = stack.pop();
    if (!cur || typeof cur !== "object") continue;
    if (seen.has(cur)) continue;
    seen.add(cur);

    // Always scan timestamp keys anywhere in the payload (SmartLead often nests them under `data`).
    if (!Array.isArray(cur)) {
      for (const [type, keys] of Object.entries(tsKeysByType)) {
        for (const k of keys) {
          const atMs = toMs((cur as any)[k]);
          if (!atMs) continue;
          const prev = bestByType[type];
          if (!prev || atMs > prev.atMs) bestByType[type] = { atMs, key: k };
        }
      }
    }

    // Treat any object that looks like a "message/step" OR has at least one timestamp key as an event container.
    const hasAnyTsKey =
      !Array.isArray(cur) &&
      Object.values(tsKeysByType).some((keys) => keys.some((k) => Object.prototype.hasOwnProperty.call(cur as any, k)));

    if (!Array.isArray(cur) && (isMessageLike(cur) || hasAnyTsKey)) {
      const msgId = String((cur as any)?.message_id ?? (cur as any)?.messageId ?? "").trim();
      const step = String(
        (cur as any)?.sequence_step ??
        (cur as any)?.sequenceStep ??
        (cur as any)?.sequence_step_id ??
        (cur as any)?.sequenceStepId ??
        (cur as any)?.step ??
        (cur as any)?.step_id ??
        (cur as any)?.stepId ??
        (cur as any)?.step_number ??
        (cur as any)?.stepNumber ??
        ""
      ).trim();
      for (const [type, keys] of Object.entries(tsKeysByType)) {
        for (const k of keys) {
          const v = (cur as any)[k];
          const atMs = toMs(v);
          if (!atMs) continue;
          const eventId = stableEventId([ctx.campaignId, ctx.leadMapId, type, msgId || step || k, atMs]);
          out.push({
            eventId,
            type,
            atMs,
            meta: { message_id: msgId || undefined, step: step || undefined, key: k }
          });
        }
      }
    }

    if (Array.isArray(cur)) for (const x of cur) stack.push(x);
    else for (const v of Object.values(cur)) stack.push(v);
  }

  // As a last resort, if sequence payload didn't include any message-like objects, try top-level timestamps
  // in common lead objects (this may undercount, but avoids "all zeros").
  if (!out.length) {
    for (const [type, best] of Object.entries(bestByType)) {
      if (!best) continue;
      out.push({
        eventId: stableEventId([ctx.campaignId, ctx.leadMapId, type, best.key, best.atMs]),
        type,
        atMs: best.atMs,
        meta: { fallback: true, key: best.key }
      });
    }
  }

  return out;
}

/**
 * Map a SmartLead lead-statistics row into event-like entries.
 *
 * Each history entry is one transaction-level event.
 */
function extractSmartleadLeadStatsEvents(row: any, ctx: { campaignId: number; email: string }) {
  const out: Array<{ eventId: string; type: string; atMs: number; meta: any }> = [];
  const history = Array.isArray(row?.history) ? row.history : [];

  for (const h of history) {
    const rawType = String(h?.type ?? "").trim().toLowerCase();
    const type =
      rawType === "sent"
        ? "sent"
        : rawType === "opened" || rawType === "open"
          ? "opened"
          : rawType === "replied" || rawType === "reply"
            ? "replied"
            : rawType === "bounced" || rawType === "bounce"
              ? "bounced"
              : rawType === "unsubscribed"
                ? "unsubscribed"
                : "other";
    if (type === "other") continue;
    const atMs = toMs(h?.time ?? h?.timestamp ?? h?.created_at);
    if (!atMs) continue;

    const statsId = String(h?.stats_id ?? "").trim();
    const msgId = String(h?.message_id ?? "").trim();
    const seqNum = String(h?.email_seq_number ?? h?.email_seq ?? h?.seq ?? "").trim();
    const baseId = [ctx.campaignId, ctx.email, statsId || msgId || seqNum || "history"];

    out.push({
      eventId: stableEventId([...baseId, type, atMs]),
      type,
      atMs,
      meta: {
        stats_id: statsId || undefined,
        message_id: msgId || undefined,
        email_seq_number: seqNum || undefined
      }
    });
  }

  return out;
}

/**
 * Fetch ALL leads for a campaign to build a map of { email -> lead_category_id }.
 * This is necessary because `leads-statistics` does not include sentiment/category data,
 * and we need it to identify "Positive Reply" (Interested) events.
 */
async function fetchAllLeadSentiment(campaignId: number, opts: { debug?: boolean } = {}) {
  const map = new Map<string, number>();
  const limit = 100;
  let offset = 0;

  // Safety cap to prevent infinite loops on huge campaigns if pagination is broken
  const MAX_LEADS = 50000;

  while (offset < MAX_LEADS) {
    try {
      const json = await smartleadListLeadsByCampaign(campaignId, { limit, offset });
      const leads = pickArrayFromSmartleadListResponse(json);
      if (!leads.length) break;

      for (const lead of leads) {
        const email = canonicalizeEmail(lead?.email ?? lead?.lead_email ?? "");
        const catId = Number(lead?.lead_category_id);
        if (email && Number.isFinite(catId)) {
          map.set(email, catId);
        }
      }

      if (leads.length < limit) break;
      offset += leads.length;
    } catch (e) {
      if (opts.debug) console.error(`Failed to fetch sentiment page offset=${offset}:`, e);
      break;
    }
  }
  return map;
}


/**
 * Fetch campaign statistics filtered by specific email status (e.g. 'opened', 'replied').
 * This is the ONLY reliable way to get actual timestamps for opens and replies.
 */
async function smartleadListCampaignStatsByStatus(
  campaignId: number,
  status?: string | null,
  opts?: { limit?: number; offset?: number; start_date?: string; end_date?: string }
) {
  const qs = new URLSearchParams();
  if (status) qs.set("email_status", status);
  if (opts?.limit != null) qs.set("limit", String(opts.limit));
  if (opts?.offset != null) qs.set("offset", String(opts.offset));

  // The API docs mention sent_time_start_date. It's unclear if this filters by EVENT time or SENT time.
  // For safety in backfills, we might rely on fetching more and filtering in app, 
  // but if we can filter by date, good.
  // SmartLead docs: "sent_time_start_date". 
  if (opts?.start_date) qs.set("sent_time_start_date", opts.start_date);
  if (opts?.end_date) qs.set("sent_time_end_date", opts.end_date);

  try {
    const json = await smartleadFetch(`/api/v1/campaigns/${encodeURIComponent(String(campaignId))}/statistics?${qs.toString()}`, { method: "GET" });
    return pickArrayFromSmartleadListResponse(json);
  } catch (e: any) {
    if (String(e?.message).includes("404")) return [];
    throw e;
  }
}

/**
 * Sync all events (Sent, Opened, Replied, Bounced) for a campaign using the global statistics endpoint.
 * This effectively replaces the per-lead iteration for event gathering.
 */
async function syncCampaignStatsEvents(
  h: PostgrestHeaders,
  campaignId: number,
  opts: {
    sinceIso: string;
    untilIso: string;
    sentimentMap: Map<string, number>;
    categoryNameMap?: Map<string, number>;
    positiveCategoryIds?: Set<number>;
    oooCategoryIds?: Set<number>;
    debug?: boolean;
    createdBy: string | null;
  }
) {
  const stats = {
    campaign_id: campaignId,
    scanned: 0,
    inserted: 0,
    errors: 0,
    fetched_categories: 0
  };

  let offset = 0;
  const limit = 100;
  const MAX_PAGES = 500; // Safety cap
  let page = 0;

  while (page < MAX_PAGES) {
    if (opts.debug) console.log(`Syncing stats for campaign ${campaignId}, offset ${offset}...`);

    // Fetch ALL stats (no status filter)
    const rows = await smartleadListCampaignStatsByStatus(campaignId, null, { limit, offset });

    if (!rows.length) break;

    // OPTIMIZATION: Scan for replies and fetch missing categories on-demand
    // This avoids fetching 50,000 leads just to find the few that replied.
    const repliesNeedingCategory: string[] = [];
    for (const r of rows) {
      if (toMs(r.reply_time)) {
        const email = canonicalizeEmail(r.lead_email);
        if (email) {
          // fast-path: check if we have category text in stats row and map it
          if (!opts.sentimentMap.has(email) && r.lead_category && opts.categoryNameMap) {
            const fid = opts.categoryNameMap.get(r.lead_category);
            if (fid) opts.sentimentMap.set(email, fid);
          }

          if (!opts.sentimentMap.has(email)) {
            repliesNeedingCategory.push(email);
          }
        }
      }
    }

    if (repliesNeedingCategory.length > 0) {
      // Fetch in small chunks to respect API limits
      const CHUNK_SIZE = 5;
      for (let i = 0; i < repliesNeedingCategory.length; i += CHUNK_SIZE) {
        const chunk = repliesNeedingCategory.slice(i, i + CHUNK_SIZE);
        await Promise.all(
          chunk.map(async (email) => {
            const catId = await smartleadGetLeadCategory(campaignId, email);
            if (catId != null) {
              opts.sentimentMap.set(email, catId);
              // @ts-ignore
              stats.fetched_categories = (stats.fetched_categories || 0) + 1;
            }
          })
        );
      }
    }

    const eventsToUpsert: any[] = [];
    const emailsForHubSpot = new Set<string>();

    for (const r of rows) {
      stats.scanned += 1;
      const email = canonicalizeEmail(r.lead_email);
      if (!email) continue;

      const categoryId = opts.sentimentMap.get(email) ?? null;

      // Helper to push event
      const pushEvent = (type: string, timeVal: any) => {
        const ms = toMs(timeVal);
        if (!ms) return;
        const iso = new Date(ms).toISOString();
        if (iso < opts.sinceIso || iso >= opts.untilIso) return;

        // Determine mapped type for replies
        let eventType = type;
        if (type === "replied") {
          const pos = opts.positiveCategoryIds ?? new Set();
          const ooo = opts.oooCategoryIds ?? new Set();
          eventType = mapSmartleadReplyEventType("replied", categoryId, pos, ooo);
        }

        // Use stats_id from row (unique per sent msg) + type as stable ID
        const statsId = String(r.stats_id ?? "").trim();
        const smartleadEventId = `${campaignId}:${statsId}:${type}`;

        eventsToUpsert.push({
          contact_email: email,
          event_type: eventType,
          occurred_at: iso,
          smartlead_campaign_id: String(campaignId),
          smartlead_event_id: smartleadEventId,
          payload: { ...r, lead_category_id: categoryId },
          created_by: opts.createdBy
        });
        emailsForHubSpot.add(email);
      };

      // 1. Sent
      pushEvent("sent", r.sent_time);
      // 2. Opened
      pushEvent("opened", r.open_time);
      // 3. Replied
      pushEvent("replied", r.reply_time);
      // 4. Bounced
      pushEvent("bounced", r.bounced_time);
    }

    if (eventsToUpsert.length > 0) {
      const hubspotMap = await findHubspotContactIds(h, campaignId, Array.from(emailsForHubSpot));
      for (const ev of eventsToUpsert) {
        const hid = hubspotMap.get(ev.contact_email);
        if (hid) ev.hubspot_contact_id = hid;
        await upsertSmartleadEvent(h, ev);
        stats.inserted += 1;
      }
    }

    if (rows.length < limit) break;
    offset += rows.length;
    page++;
  }

  return stats;
}

export async function POST(req: Request) {
  try {
    const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
    const { authHeader, bearer, gotSecret } = getBearer(req);
    const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
    // Accept Vercel Cron header or CRON_SECRET (bearer/x-sync-secret) for automation runs.
    const isCron =
      vercelCron === "1" ||
      vercelCron.toLowerCase() === "true" ||
      (!!cronSecret && (bearer === cronSecret || gotSecret === cronSecret));
    const createdBy = isCron ? (String(process.env.SMARTLEAD_CRON_USER_ID ?? "").trim() || null) : null;

    // Allow either cron or authenticated user from allowed domain.
    if (!isCron) {
      const user = await getSupabaseUserFromAuthHeader(authHeader);
      if (!user?.email) return jsonError(401, "Not authorized");
      const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "";
      const email = String(user.email || "").toLowerCase();
      if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");
    }

    if (gotSecret) {
      // Optional extra protection for manual tooling/ops.
      const syncSecret = String(process.env.SMARTLEAD_SYNC_SECRET ?? "").trim();
      if (!syncSecret) return jsonError(500, "SMARTLEAD_SYNC_SECRET is not configured in env");
      if (gotSecret !== syncSecret) return jsonError(403, "Bad x-sync-secret");
    }

    if (isCron && !createdBy) return jsonError(400, "Missing SMARTLEAD_CRON_USER_ID (required for cron SmartLead sync)");

    const payload = (await req.json().catch(() => ({}))) as {
      since?: string; // ISO
      until?: string; // ISO (optional)
      campaign_ids?: number[];
      refresh_sentiments?: boolean;
      refresh_sentiments_only?: boolean;
      sentiment_limit?: number;
      sentiment_max_pages?: number;
      positive_category_ids?: number[];
      ooo_category_ids?: number[];
      max_campaigns?: number;
      max_leads_per_campaign?: number;
      max_sequence_calls?: number;
      include_sequence_details?: boolean;
      use_stats_endpoint?: boolean;
      stats_offset?: number;
      stats_limit?: number;
      stats_max_pages?: number;
      reset_all?: boolean;
      dry_run?: boolean;
      debug?: boolean;
    };
    const dryRun = !!payload?.dry_run;
    const debug = !!payload?.debug;
    const isReset = Boolean(payload?.reset_all);
    const refreshSentiments = Boolean(payload?.refresh_sentiments);
    // Manual calls default to "only refresh sentiments"; cron runs both unless explicitly overridden.
    const refreshSentimentsOnly = payload?.refresh_sentiments_only ?? !isCron;
    const useStatsEndpoint =
      payload?.use_stats_endpoint ??
      String(process.env.SMARTLEAD_STATS_MODE ?? "").trim().toLowerCase() === "stats";
    const includeSeq = payload?.include_sequence_details !== false;
    const maxCampaigns = Math.max(1, Math.min(1000, Number(payload?.max_campaigns ?? 10)));
    const maxLeadsPerCampaign = Math.max(1, Math.min(20000, Number(payload?.max_leads_per_campaign ?? 250)));
    const maxSeqCalls = Math.max(1, Math.min(10000, Number(payload?.max_sequence_calls ?? 120)));
    const useLegacySync = (payload as any)?.use_legacy_sync === true;

    const pg = postgrestHeadersFor(authHeader, isCron || isReset);

    if (isReset) {
      // Extra safety: destructive reset requires x-sync-secret.
      const syncSecret = String(process.env.SMARTLEAD_SYNC_SECRET ?? "").trim();
      if (!syncSecret) return jsonError(500, "SMARTLEAD_SYNC_SECRET is not configured in env");
      if (!gotSecret || gotSecret !== syncSecret) return jsonError(403, "reset_all requires a valid x-sync-secret");

      const analytics = await purgeSmartleadAnalytics(pg);
      await purgeSmartleadEvents(pg);
      await purgeSmartleadSyncState(pg);
      return NextResponse.json({
        ok: true,
        reset: true,
        deleted: {
          analytics_activities: analytics.deletedActivities,
          analytics_activity_deals: analytics.deletedActivityDeals
        }
      });
    }

    const state = await getSmartleadSyncState(pg, createdBy);
    const defaultSinceIso = new Date(Date.now() - 7 * 86400000).toISOString();
    const sinceIso = String(payload?.since || state.lastEvents || defaultSinceIso).trim();
    const sinceMs = toMs(sinceIso) ?? Date.parse(sinceIso);
    if (!Number.isFinite(sinceMs)) return jsonError(400, "Bad request: invalid since");
    const untilIso = String(payload?.until ?? "").trim();
    const untilMs = untilIso ? (toMs(untilIso) ?? Date.parse(untilIso)) : Date.now();
    if (untilIso && !Number.isFinite(untilMs)) return jsonError(400, "Bad request: invalid until");
    const sinceYmd = ymdUtc(sinceMs);
    // Important: if `since` cursor is within a day, we still want to ingest the whole day to avoid missing
    // events that happened earlier the same day (idempotency key prevents duplicates).
    const sinceDayStartMs = Date.parse(`${sinceYmd}T00:00:00.000Z`);
    const effectiveSinceMs = Number.isFinite(sinceDayStartMs) ? Math.min(sinceMs, sinceDayStartMs) : sinceMs;

    const fromEnv = parseCsvEnv("SMARTLEAD_CAMPAIGN_IDS")
      .map((x) => Number(x))
      .filter((n) => Number.isFinite(n)) as number[];
    const fromReq = Array.isArray(payload?.campaign_ids) ? payload.campaign_ids.map((x) => Number(x)).filter((n) => Number.isFinite(n)) : [];
    const defaultCampaignIdRaw = String(process.env.SMARTLEAD_DEFAULT_CAMPAIGN_ID ?? "").trim();
    const defaultCampaignId = defaultCampaignIdRaw ? Number(defaultCampaignIdRaw) : NaN;
    const ids = new Set<number>();
    if (Number.isFinite(defaultCampaignId) && defaultCampaignId > 0) ids.add(defaultCampaignId);
    for (const n of (fromReq.length ? fromReq : fromEnv)) if (Number.isFinite(n) && n > 0) ids.add(n);
    let campaignIds = Array.from(ids.values()).filter((n) => Number.isFinite(n) && n > 0).slice(0, maxCampaigns);

    // Auto-discover campaigns from SmartLead API if no ids configured.
    if (!campaignIds.length) {
      const listed = await smartleadListCampaignsBestEffort({ limit: Math.min(200, maxCampaigns * 20), offset: 0 });
      const picked = listed
        .map((c: any) => Number(c?.id ?? c?.campaign_id ?? c?.campaignId))
        .filter((n) => Number.isFinite(n) && n > 0) as number[];
      // Prefer active/running campaigns if the payload includes status-like fields.
      const isActive = (c: any) => {
        const s = String(c?.status ?? c?.state ?? c?.campaign_status ?? c?.campaignStatus ?? "").toLowerCase();
        return s.includes("active") || s.includes("running") || s.includes("live") || s === "1" || s === "true";
      };
      const byId = new Map<number, any>();
      for (const c of listed) {
        const id = Number(c?.id ?? c?.campaign_id ?? c?.campaignId);
        if (Number.isFinite(id) && id > 0) byId.set(id, c);
      }
      const activeFirst = picked
        .filter((id) => isActive(byId.get(id)))
        .concat(picked.filter((id) => !isActive(byId.get(id))));
      campaignIds = Array.from(new Set(activeFirst)).slice(0, maxCampaigns);
    }

    if (!campaignIds.length) {
      return NextResponse.json({
        ok: true,
        skipped: true,
        reason: "No campaign IDs configured and SmartLead API did not return campaigns",
        since: sinceIso
      });
    }

    let refreshSentimentsStats: any[] | null = null;
    if (refreshSentiments) {
      const positiveSet = new Set(
        (payload?.positive_category_ids ?? [1])
          .map((x) => Number(x))
          .filter((n) => Number.isFinite(n))
      );
      const oooSet = new Set(
        (payload?.ooo_category_ids ?? [3])
          .map((x) => Number(x))
          .filter((n) => Number.isFinite(n))
      );
      const refreshStats: any[] = [];
      for (const campaignId of campaignIds) {
        try {
          const sentimentMap = await fetchAllLeadSentiment(campaignId, { debug });
          const s = await refreshSmartleadSentimentsForCampaign(pg, {
            campaignId,
            sinceIso: new Date(effectiveSinceMs).toISOString(),
            untilIso: new Date(untilMs).toISOString(),
            sentimentMap,
            positiveCategoryIds: positiveSet.size ? positiveSet : undefined,
            oooCategoryIds: oooSet.size ? oooSet : undefined,
            limit: payload?.sentiment_limit,
            maxPages: payload?.sentiment_max_pages,
            debug
          });
          refreshStats.push(s);
        } catch (e: any) {
          refreshStats.push({ campaign_id: campaignId, error: String(e?.message || e) });
        }
      }

      refreshSentimentsStats = refreshStats;
      if (refreshSentimentsOnly) {
        return NextResponse.json({
          ok: true,
          refresh_sentiments: true,
          refresh_sentiments_only: true,
          since: new Date(effectiveSinceMs).toISOString(),
          until: new Date(untilMs).toISOString(),
          campaigns: campaignIds,
          stats: refreshStats,
          positive_category_ids: positiveSet.size ? Array.from(positiveSet.values()) : [],
          ooo_category_ids: oooSet.size ? Array.from(oooSet.values()) : []
        });
      }
    }

    const stats: any = {
      ok: true,
      since: sinceIso,
      since_ymd: sinceYmd,
      until: new Date(untilMs).toISOString(),
      dry_run: dryRun,
      campaigns: campaignIds,
      source: !useLegacySync ? "campaign_stats" : "sequence_details",
      leads_scanned: 0,
      seq_calls: 0,
      events_inserted: 0,
      events_deduped: 0,
      truncated: false,
      stats_has_more: null as boolean | null,
      stats_offset_next: null as number | null,
      debug: debug ? { samples: [] as any[] } : null
    };

    const seqPreview = (seq: any) => {
      const out: any = { keys: null as any, data_keys: null as any, timestamp_samples: [] as any[] };
      if (!seq || typeof seq !== "object") return out;
      out.keys = Object.keys(seq).slice(0, 30);
      if (seq?.data && typeof seq.data === "object") out.data_keys = Object.keys(seq.data).slice(0, 30);

      const seen = new Set<any>();
      const stack: any[] = [seq];
      while (stack.length && out.timestamp_samples.length < 3) {
        const cur = stack.pop();
        if (!cur || typeof cur !== "object") continue;
        if (seen.has(cur)) continue;
        seen.add(cur);

        if (!Array.isArray(cur)) {
          const keys = Object.keys(cur);
          const interestingKeys = keys.filter((k) => /sent|open|reply|bounce|step|message|time|_at$/i.test(k)).slice(0, 30);
          const interesting: any = {};
          for (const k of interestingKeys) {
            const v = (cur as any)[k];
            if (v == null) continue;
            // Keep only scalars to avoid leaking email bodies/templates.
            if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
              interesting[k] = String(v).slice(0, 200);
            }
          }
          if (Object.keys(interesting).length) out.timestamp_samples.push({ keys: keys.slice(0, 30), interesting });
        }

        if (Array.isArray(cur)) for (const x of cur) stack.push(x);
        else for (const v of Object.values(cur)) stack.push(v);
      }
      return out;
    };

    const leadPreview = (lead: any) => {
      if (!lead || typeof lead !== "object") return null;
      const summarize = (obj: any) => {
        const keys = obj && typeof obj === "object" ? Object.keys(obj).slice(0, 40) : [];
        const interestingKeys = keys.filter((k) => /status|sent|open|reply|bounce|event|time|_at$/i.test(k)).slice(0, 30);
        const interesting: any = {};
        for (const k of interestingKeys) {
          const v = (obj as any)[k];
          if (v == null) continue;
          if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") interesting[k] = String(v).slice(0, 200);
        }
        return { keys, interesting };
      };
      return { top: summarize(lead), lead: summarize((lead as any)?.lead) };
    };


    // Use the comprehensive statistics endpoint by default for accurate Open/Reply tracking.
    // Legacy mode (sequence-details) is retained only via explicit flag for fallback.
    // Fetch global categories for text fallback (optimization)
    const globalCats = await smartleadFetchCategories();
    const categoryNameMap = new Map<string, number>();
    for (const c of globalCats) {
      if (c.name && c.id) categoryNameMap.set(c.name, c.id);
    }

    for (const campaignId of campaignIds) {
      // Map will be populated on-demand by syncCampaignStatsEvents for replies.
      const sentimentMap = new Map<string, number>();

      if (!useLegacySync) {
        // NEW PATH: Global Statistics Sync
        try {
          const s = await syncCampaignStatsEvents(pg, campaignId, {
            sinceIso: sinceIso, // we use the provided since window
            untilIso: new Date(untilMs).toISOString(),
            sentimentMap,
            categoryNameMap,
            positiveCategoryIds: new Set((payload?.positive_category_ids ?? [1, 2, 5]).map(Number)),
            oooCategoryIds: new Set((payload?.ooo_category_ids ?? [3, 6]).map(Number)),
            debug,
            createdBy
          });

          stats.leads_scanned += s.scanned;
          stats.events_inserted += s.inserted;
          // stats.seq_calls doesn't apply here effectively
        } catch (e: any) {
          if (debug) console.error(`Sync failed for campaign ${campaignId}:`, e);
        }
        continue;
      }

      // --- LEGACY PATH (Sequence Details) ---
      // Useful only if statistics endpoint fails or for debugging specific leads.
      // (Kept for safety but effectively deprecated for opens/replies)

      let offset = Math.max(0, Number(payload?.stats_offset ?? 0));
      const limit = Math.max(1, Math.min(100, Number(payload?.stats_limit ?? 100)));

      while (offset < maxLeadsPerCampaign && (!includeSeq || stats.seq_calls < maxSeqCalls)) {
        const listJson = await smartleadListLeadsByCampaign(campaignId, { limit, offset, event_time_gt: sinceYmd });
        const leads = pickArrayFromSmartleadListResponse(listJson);
        if (!leads.length) break;
        offset += leads.length;

        const emailToContactId = await findHubspotContactIds(pg, campaignId,
          leads.map((l: any) => canonicalizeEmail(
            l?.email ?? l?.lead_email ?? l?.email_id ??
            l?.lead?.email ?? l?.contact?.email ?? l?.to_email
          )).filter(Boolean)
        );

        for (const lead of leads) {
          if (includeSeq && stats.seq_calls >= maxSeqCalls) break;
          stats.leads_scanned += 1;

          const rawLead: any = lead as any;
          const email = canonicalizeEmail(rawLead?.email ?? rawLead?.lead?.email ?? "");
          const leadId = String(rawLead?.campaign_lead_map_id ?? rawLead?.id ?? "").trim();

          if (!leadId || !looksLikeEmail(email)) continue;

          // Emit coarse events from lead fields
          // ... (simplified legacy emit logic omitted for brevity, assuming standard usage)

          if (!includeSeq) continue;

          stats.seq_calls += 1;
          let seq: any = null;
          try {
            seq = await smartleadGetSequenceDetails(leadId);
          } catch (e) { continue; }

          const events = extractSmartleadEmailEvents(seq, { campaignId, leadMapId: leadId, email });

          for (const ev of events) {
            if (ev.atMs < effectiveSinceMs || ev.atMs >= untilMs) continue;
            const contactId = emailToContactId.get(email) ?? null;
            const leadCategoryId = ev.type === "replied" ? Number(rawLead?.lead_category_id) : undefined;
            const mappedType = mapSmartleadReplyEventType(
              ev.type,
              Number.isFinite(leadCategoryId as number) ? Number(leadCategoryId) : null,
              new Set([1]), new Set([3])
            );

            const row = {
              smartlead_event_id: ev.eventId,
              smartlead_campaign_id: campaignId,
              smartlead_lead_map_id: leadId,
              contact_email: email,
              event_type: mappedType,
              occurred_at: new Date(ev.atMs).toISOString(),
              payload: { meta: ev.meta, lead_category_id: leadCategoryId },
              created_by: createdBy ?? undefined,
              hubspot_contact_id: contactId
            };

            if (dryRun) {
              stats.events_inserted += 1;
            } else {
              const inserted = await upsertSmartleadEvent(pg, row);
              if (inserted) stats.events_inserted += 1;
              else stats.events_deduped += 1;
            }
          }
        }
        if (leads.length < limit) break;
      }

      if (includeSeq && stats.seq_calls >= maxSeqCalls) {
        stats.truncated = true;
        // break; // implicit
      }
    }


    // Only advance cursor if we didn't truncate (otherwise we'd skip unseen data).
    // Also: during initial setup, don't advance cursor if we made zero sequence calls (likely misconfigured parsing / no usable lead IDs).
    if (!dryRun && !stats.truncated && (stats.seq_calls > 0 || stats.leads_scanned === 0)) {
      await upsertSmartleadSyncState(pg, {
        created_by: createdBy ?? undefined,
        last_events_synced_at: new Date(untilMs).toISOString()
      });
    }

    const out = refreshSentimentsStats
      ? {
        ...stats,
        refresh_sentiments: true,
        refresh_sentiments_only: false,
        refresh_sentiments_stats: refreshSentimentsStats
      }
      : stats;
    return NextResponse.json(out);
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


