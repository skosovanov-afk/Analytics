/* eslint-disable no-console */

type Json = Record<string, unknown>;

type SmartLeadCampaign = {
  id?: number | string;
  name?: string;
  status?: string;
};

type SmartLeadLeadRow = {
  status?: string;
  lead_category_id?: number | null;
  created_at?: string | null;
  lead?: {
    id?: number | string;
    lead_id?: number | string;
    email?: string;
    work_email?: string;
    personal_email?: string;
    contact_email?: string;
    first_name?: string;
    name?: string;
    last_name?: string;
    company?: string;
    linkedin?: string;
    linkedin_url?: string;
    status?: string;
    is_unsubscribed?: boolean;
    emails_sent_count?: number;
    last_email_date?: string | null;
    created_at?: string | null;
  };
  email?: string;
  lead_email?: string;
  contact_email?: string;
  first_name?: string;
  last_name?: string;
  company?: string;
  linkedin?: string;
  id?: number | string;
  lead_id?: number | string;
};

type SmartLeadStatsRow = {
  lead_email?: string;
  sequence_number?: number | string | null;
  stats_id?: string | number | null;
  email_subject?: string | null;
  email_message?: string | null;
  open_count?: number | string | null;
  click_count?: number | string | null;
  is_unsubscribed?: boolean | null;
  is_bounced?: boolean | null;
  sent_time?: string | null;
  reply_time?: string | null;
};

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")?.trim() || "";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")?.trim() || "";
const SMARTLEAD_API_KEY = Deno.env.get("SMARTLEAD_API_KEY")?.trim() || "";
const SMARTLEAD_BASE_URL = (Deno.env.get("SMARTLEAD_BASE_URL")?.trim() || "https://server.smartlead.ai").replace(/\/+$/, "");

const DEFAULT_CAMPAIGNS_PER_RUN = Number(Deno.env.get("SMARTLEAD_CAMPAIGNS_PER_RUN") || "3");
const DEFAULT_PAGE_SIZE = Number(Deno.env.get("SMARTLEAD_PAGE_SIZE") || "100");
const DEFAULT_BATCH_SIZE = Number(Deno.env.get("SMARTLEAD_BATCH_SIZE") || "500");
const SYNC_SECRET = Deno.env.get("SMARTLEAD_SYNC_SECRET")?.trim() || "";

function nowIso(): string {
  return new Date().toISOString();
}

function asInt(v: unknown): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function asIso(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s || null;
}

function toList(payload: unknown): Json[] {
  if (Array.isArray(payload)) return payload.filter((x): x is Json => !!x && typeof x === "object");
  if (payload && typeof payload === "object") {
    const obj = payload as Record<string, unknown>;
    for (const key of ["data", "results", "leads", "campaigns"]) {
      const val = obj[key];
      if (Array.isArray(val)) return val.filter((x): x is Json => !!x && typeof x === "object");
    }
  }
  return [];
}

async function slGet(path: string): Promise<unknown> {
  const sep = path.includes("?") ? "&" : "?";
  const url = `${SMARTLEAD_BASE_URL}${path}${sep}api_key=${encodeURIComponent(SMARTLEAD_API_KEY)}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30_000);
  let res: Response;
  try {
    res = await fetch(url, { headers: { accept: "application/json" }, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`SmartLead GET failed (${res.status}) ${path}: ${text.slice(0, 500)}`);
  }
  return await res.json();
}

function sbHeaders(extra?: Record<string, string>): HeadersInit {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    "Content-Type": "application/json",
    ...(extra || {}),
  };
}

async function sbGet(table: string, query: URLSearchParams, preferCount = false): Promise<{ data: Json[]; contentRange?: string }> {
  const url = `${SUPABASE_URL}/rest/v1/${table}?${query.toString()}`;
  const headers = sbHeaders(preferCount ? { Prefer: "count=exact" } : undefined);
  const res = await fetch(url, { method: "GET", headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase GET ${table} failed (${res.status}): ${text.slice(0, 500)}`);
  }
  const data = (await res.json()) as Json[];
  return { data, contentRange: res.headers.get("content-range") || undefined };
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

function buildSmartleadEventIdentity(row: Json): string | null {
  const campaignId = asInt(row.campaign_id);
  const eventType = String(row.event_type || "").trim().toLowerCase();
  if (campaignId === null || !eventType) return null;

  const statsId = String(row.stats_id || "").trim();
  if (statsId) return `${campaignId}|${eventType}|${statsId}`;

  const email = String(row.email || "").trim().toLowerCase();
  const occurredAt = asIso(row.occurred_at);
  const sequenceNumber = asInt(row.sequence_number) || 0;
  if (!email || !occurredAt) return null;
  return `${campaignId}|${eventType}|${email}|${sequenceNumber}|${occurredAt}`;
}

function dedupeSmartleadEvents(rows: Json[]): Json[] {
  const byIdentity = new Map<string, Json>();
  const passthrough: Json[] = [];

  for (const row of rows) {
    const identity = buildSmartleadEventIdentity(row);
    if (!identity) {
      passthrough.push(row);
      continue;
    }
    byIdentity.set(identity, { ...row, event_identity_key: identity });
  }

  return [...byIdentity.values(), ...passthrough];
}

async function sbRpc(fnName: string): Promise<Json> {
  const url = `${SUPABASE_URL}/rest/v1/rpc/${fnName}`;
  const res = await fetch(url, {
    method: "POST",
    headers: sbHeaders({ Prefer: "return=representation" }),
    body: "{}",
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase RPC ${fnName} failed (${res.status}): ${text.slice(0, 500)}`);
  }
  return (await res.json()) as Json;
}

async function sbPatch(table: string, filters: Record<string, string>, payload: Json): Promise<void> {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(filters)) q.set(k, v);
  const url = `${SUPABASE_URL}/rest/v1/${table}?${q.toString()}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: sbHeaders({ Prefer: "return=minimal" }),
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase PATCH ${table} failed (${res.status}): ${text.slice(0, 500)}`);
  }
}

async function sbDelete(table: string, filters: Record<string, string>): Promise<void> {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(filters)) q.set(k, v);
  const url = `${SUPABASE_URL}/rest/v1/${table}?${q.toString()}`;
  const res = await fetch(url, {
    method: "DELETE",
    headers: sbHeaders({ Prefer: "return=minimal" }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase DELETE ${table} failed (${res.status}): ${text.slice(0, 500)}`);
  }
}

async function getSyncStateInt(key: string, defaultValue: number): Promise<number> {
  const q = new URLSearchParams({ select: "key,value", key: `eq.${key}`, limit: "1" });
  const { data } = await sbGet("sync_state", q);
  const raw = data[0]?.value;
  const parsed = asInt(raw);
  return parsed ?? defaultValue;
}

async function setSyncStateInt(key: string, value: number): Promise<void> {
  await sbInsert("sync_state", [{ key, value }], true, "key");
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function pickLeadId(row: SmartLeadLeadRow): number | null {
  const lead = row.lead || {};
  for (const v of [lead.id, lead.lead_id, row.lead_id, row.id]) {
    const parsed = asInt(v);
    if (parsed !== null) return parsed;
  }
  return null;
}

function pickLeadEmail(row: SmartLeadLeadRow): string | null {
  const lead = row.lead || {};
  for (const v of [lead.email, lead.work_email, lead.personal_email, lead.contact_email, row.email, row.lead_email, row.contact_email]) {
    if (!v) continue;
    const e = String(v).trim().toLowerCase();
    if (e) return e;
  }
  return null;
}

async function syncCampaign(
  campaign: SmartLeadCampaign,
  pageSize: number,
  batchSize: number,
): Promise<{ campaignId: number; campaignName: string | null; leadsUpserted: number; eventsInserted: number; sent: number; reply: number }> {
  const campaignId = asInt(campaign.id);
  if (campaignId === null) throw new Error("Campaign id is missing");
  const campaignName = campaign.name ? String(campaign.name) : null;

  let offset = 0;
  const leadsPayload: Json[] = [];
  const emailToLeadId = new Map<string, number>();

  while (true) {
    const raw = await slGet(`/api/v1/campaigns/${campaignId}/leads?limit=${pageSize}&offset=${offset}`);
    const rows = toList(raw) as SmartLeadLeadRow[];
    if (!rows.length) break;

    for (const row of rows) {
      const lead = row.lead || {};
      const leadId = pickLeadId(row);
      if (leadId === null) continue;
      const email = pickLeadEmail(row);
      if (email) emailToLeadId.set(email, leadId);

      leadsPayload.push({
        campaign_id: campaignId,
        campaign_name: campaignName,
        lead_id: leadId,
        email,
        first_name: lead.first_name || lead.name || row.first_name || null,
        last_name: lead.last_name || row.last_name || null,
        company: lead.company || row.company || null,
        linkedin: lead.linkedin || lead.linkedin_url || row.linkedin || null,
        lead_status: row.status || lead.status || null,
        lead_category_id: asInt(row.lead_category_id),
        is_unsubscribed: lead.is_unsubscribed ?? null,
        emails_sent_count: asInt(lead.emails_sent_count),
        last_email_date: asIso(lead.last_email_date),
        created_at_source: asIso(row.created_at || lead.created_at),
        updated_at_source: nowIso(),
        raw_payload: row,
        synced_at: nowIso(),
      });
    }

    if (rows.length < pageSize) break;
    offset += rows.length;
  }

  // Keep historical lead rows and only mark presence in the latest snapshot.
  // 1) mark all campaign leads as not present now
  await sbPatch("smartlead_leads", { campaign_id: `eq.${campaignId}` }, {
    is_present_now: false,
    updated_at_source: nowIso(),
    synced_at: nowIso(),
  });
  // 2) upsert current snapshot rows back as present
  for (const part of chunk(leadsPayload, batchSize)) {
    const withPresence = part.map((row) => ({
      ...row,
      is_present_now: true,
      last_seen_at: nowIso(),
    }));
    await sbInsert("smartlead_leads", withPresence, true, "campaign_id,lead_id");
  }

  offset = 0;
  const eventsPayload: Json[] = [];
  let sent = 0;
  let reply = 0;

  while (true) {
    const raw = await slGet(`/api/v1/campaigns/${campaignId}/statistics?limit=${pageSize}&offset=${offset}`);
    const stats = toList(raw) as SmartLeadStatsRow[];
    if (!stats.length) break;

    for (const s of stats) {
      const email = String(s.lead_email || "").trim().toLowerCase() || null;
      const leadId = email ? emailToLeadId.get(email) ?? null : null;
      const seq = asInt(s.sequence_number);
      const statsId = s.stats_id === null || s.stats_id === undefined || String(s.stats_id).trim() === "" ? null : String(s.stats_id);
      const openCount = asInt(s.open_count) || 0;
      const clickCount = asInt(s.click_count) || 0;
      const sentTime = asIso(s.sent_time);
      const replyTime = asIso(s.reply_time);

      if (sentTime) {
        eventsPayload.push({
          campaign_id: campaignId,
          campaign_name: campaignName,
          lead_id: leadId,
          email,
          event_type: "sent",
          sequence_number: seq,
          occurred_at: sentTime,
          stats_id: statsId,
          message_id: null,
          subject: s.email_subject || null,
          message_body: s.email_message || null,
          from_email: null,
          to_email: email,
          open_count: openCount,
          click_count: clickCount,
          is_unsubscribed: s.is_unsubscribed ?? null,
          is_bounced: s.is_bounced ?? null,
          raw_payload: s,
          synced_at: nowIso(),
        });
        sent += 1;
      }

      if (replyTime) {
        eventsPayload.push({
          campaign_id: campaignId,
          campaign_name: campaignName,
          lead_id: leadId,
          email,
          event_type: "reply",
          sequence_number: seq,
          occurred_at: replyTime,
          stats_id: statsId,
          message_id: null,
          subject: s.email_subject || null,
          message_body: s.email_message || null,
          from_email: email,
          to_email: null,
          open_count: 0,
          click_count: 0,
          is_unsubscribed: s.is_unsubscribed ?? null,
          is_bounced: s.is_bounced ?? null,
          raw_payload: s,
          synced_at: nowIso(),
        });
        reply += 1;
      }
    }

    if (stats.length < pageSize) break;
    offset += stats.length;
  }

  const dedupedEventsPayload = dedupeSmartleadEvents(eventsPayload);
  for (const part of chunk(dedupedEventsPayload, batchSize)) {
    await sbInsert("smartlead_events", part, true, "event_identity_key");
  }

  // Fetch reply bodies from SmartLead message-history API (best-effort, max 10 per campaign)
  let replyBodiesFetched = 0;
  const MAX_REPLY_BODY_FETCHES = 10;
  try {
    // Find reply events that were just upserted and need body text
    const replyEmails = new Set<string>();
    for (const ev of dedupedEventsPayload) {
      if (ev.event_type === "reply" && ev.email && ev.lead_id) {
        replyEmails.add(String(ev.email));
      }
    }

    // Check which ones already have reply_body in DB
    const needBody: Array<{ email: string; leadId: number }> = [];
    for (const email of replyEmails) {
      if (needBody.length >= MAX_REPLY_BODY_FETCHES) break;
      const leadId = emailToLeadId.get(email);
      if (!leadId) continue;
      const q = new URLSearchParams({
        select: "id,reply_body",
        campaign_id: `eq.${campaignId}`,
        email: `eq.${email}`,
        event_type: "eq.reply",
        reply_body: "is.null",
        limit: "1",
      });
      const { data } = await sbGet("smartlead_events", q);
      if (data.length > 0) {
        needBody.push({ email, leadId });
      }
    }

    for (const { email, leadId } of needBody) {
      try {
        // Resolve global lead ID (SmartLead global, not campaign-specific)
        const leadLookup = await slGet(`/api/v1/leads/?email=${encodeURIComponent(email)}`);
        const globalId = leadLookup?.id;
        if (!globalId) continue;

        const historyRaw = await slGet(
          `/api/v1/campaigns/${campaignId}/leads/${globalId}/message-history?show_plain_text_response=true`
        );
        const history = historyRaw?.history ?? (Array.isArray(historyRaw) ? historyRaw : []);
        const replyMsgs = history
          .filter((m: any) => String(m.type ?? "").toUpperCase() === "REPLY")
          .map((m: any) => ({
            body: String(m.email_body ?? m.body ?? "").trim(),
            time: m.time ?? m.sent_at ?? m.received_at ?? "",
          }))
          .filter((r: any) => r.body);

        if (replyMsgs.length > 0) {
          // Update all reply events for this email in this campaign
          const replyBody = replyMsgs[replyMsgs.length - 1].body; // latest reply
          await sbPatch("smartlead_events", {
            campaign_id: `eq.${campaignId}`,
            email: `eq.${email}`,
            event_type: "eq.reply",
            reply_body: "is.null",
          }, { reply_body: replyBody });
          replyBodiesFetched++;
        }
      } catch (e) {
        // Best-effort: don't fail the whole sync for reply body fetch
        console.warn(`Reply body fetch failed for ${email} in campaign ${campaignId}: ${e}`);
      }
    }
  } catch (e) {
    console.warn(`Reply body enrichment failed for campaign ${campaignId}: ${e}`);
  }

  return {
    campaignId,
    campaignName,
    leadsUpserted: leadsPayload.length,
    eventsInserted: dedupedEventsPayload.length,
    replyBodiesFetched,
    sent,
    reply,
  };
}

async function rebuildDailyStatsForCampaigns(campaignIds: number[], batchSize: number): Promise<number> {
  if (!campaignIds.length) return 0;

  for (const cid of campaignIds) {
    await sbDelete("smartlead_stats_daily", { campaign_id: `eq.${cid}` });
  }

  const byKey = new Map<string, {
    date: string;
    campaign_id: number;
    campaign_name: string | null;
    touch_number: number;
    sent_count: number;
    reply_count: number;
    open_count: number;
    click_count: number;
    emails: Set<string>;
  }>();

  for (const cid of campaignIds) {
    let from = 0;
    const page = 1000;

    while (true) {
      const q = new URLSearchParams({
        select: "campaign_id,campaign_name,sequence_number,event_type,occurred_at,open_count,click_count,email",
        campaign_id: `eq.${cid}`,
        order: "id.asc",
        limit: String(page),
        offset: String(from),
      });
      const { data } = await sbGet("smartlead_events", q);
      if (!data.length) break;

      for (const r of data) {
        const occurredAt = asIso(r.occurred_at);
        if (!occurredAt) continue;
        const date = occurredAt.slice(0, 10);
        const campaignId = asInt(r.campaign_id);
        if (campaignId === null) continue;
        const touch = asInt(r.sequence_number) || 0;

        const key = `${date}|${campaignId}|${touch}`;
        if (!byKey.has(key)) {
          byKey.set(key, {
            date,
            campaign_id: campaignId,
            campaign_name: r.campaign_name ? String(r.campaign_name) : null,
            touch_number: touch,
            sent_count: 0,
            reply_count: 0,
            open_count: 0,
            click_count: 0,
            emails: new Set<string>(),
          });
        }

        const slot = byKey.get(key)!;
        const eventType = String(r.event_type || "").toLowerCase();
        if (eventType === "sent") slot.sent_count += 1;
        if (eventType === "reply") slot.reply_count += 1;
        slot.open_count += asInt(r.open_count) || 0;
        slot.click_count += asInt(r.click_count) || 0;

        const email = String(r.email || "").trim().toLowerCase();
        if (email) slot.emails.add(email);
      }

      if (data.length < page) break;
      from += data.length;
    }
  }

  const rows: Json[] = [];
  for (const v of byKey.values()) {
    rows.push({
      date: v.date,
      campaign_id: v.campaign_id,
      campaign_name: v.campaign_name,
      touch_number: v.touch_number,
      sent_count: v.sent_count,
      reply_count: v.reply_count,
      open_count: v.open_count,
      click_count: v.click_count,
      unique_leads_count: v.emails.size,
      updated_at: nowIso(),
    });
  }

  for (const part of chunk(rows, batchSize)) {
    await sbInsert("smartlead_stats_daily", part, false);
  }

  return rows.length;
}

async function listOrphanedCampaignIds(): Promise<number[]> {
  const page = 1000;
  const existing = new Set<number>();
  let offset = 0;

  while (true) {
    const q = new URLSearchParams({
      select: "campaign_id",
      order: "campaign_id.asc",
      limit: String(page),
      offset: String(offset),
    });
    const { data } = await sbGet("smartlead_stats_daily", q);
    if (!data.length) break;
    for (const row of data) {
      const cid = asInt(row.campaign_id);
      if (cid !== null) existing.add(cid);
    }
    if (data.length < page) break;
    offset += data.length;
  }

  const orphaned = new Set<number>();
  offset = 0;
  while (true) {
    const q = new URLSearchParams({
      select: "campaign_id",
      order: "campaign_id.asc",
      limit: String(page),
      offset: String(offset),
    });
    const { data } = await sbGet("smartlead_events", q);
    if (!data.length) break;
    for (const row of data) {
      const cid = asInt(row.campaign_id);
      if (cid !== null && !existing.has(cid)) orphaned.add(cid);
    }
    if (data.length < page) break;
    offset += data.length;
  }

  return Array.from(orphaned.values()).sort((a, b) => a - b);
}

async function rebuildOrphanedDailyStats(batchSize: number): Promise<Json> {
  const orphanedIds = await listOrphanedCampaignIds();
  if (!orphanedIds.length) {
    return { ok: true, orphaned_campaigns: 0, campaign_ids: [], rows_inserted: 0, source: "inline_fallback" };
  }

  const rowsInserted = await rebuildDailyStatsForCampaigns(orphanedIds, batchSize);
  return {
    ok: true,
    orphaned_campaigns: orphanedIds.length,
    campaign_ids: orphanedIds,
    rows_inserted: rowsInserted,
    source: "inline_fallback",
  };
}

Deno.serve(async (req: Request) => {
  try {
    if (!SYNC_SECRET) {
      return new Response(JSON.stringify({ ok: false, error: "SYNC_SECRET not configured" }), { status: 500 });
    }
    {
      const headerSecret = (req.headers.get("x-sync-secret") || "").trim();
      const auth = (req.headers.get("authorization") || "").trim();
      const bearer = auth.toLowerCase().startsWith("bearer ") ? auth.slice(7).trim() : "";
      const ok = headerSecret === SYNC_SECRET || bearer === SYNC_SECRET;
      if (!ok) {
        return new Response(JSON.stringify({ ok: false, error: "Unauthorized" }), { status: 401 });
      }
    }

    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
      return new Response(JSON.stringify({ ok: false, error: "SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not configured" }), { status: 500 });
    }
    if (!SMARTLEAD_API_KEY) {
      return new Response(JSON.stringify({ ok: false, error: "SMARTLEAD_API_KEY not configured" }), { status: 500 });
    }

    const body = req.method === "POST" ? await req.json().catch(() => ({})) : {};
    const campaignsPerRun = Math.max(1, Math.min(20, asInt((body as Json).campaigns_per_run) || DEFAULT_CAMPAIGNS_PER_RUN || 3));
    const pageSize = Math.max(1, Math.min(500, asInt((body as Json).page_size) || DEFAULT_PAGE_SIZE || 100));
    const batchSize = Math.max(1, Math.min(2000, asInt((body as Json).batch_size) || DEFAULT_BATCH_SIZE || 500));

    const campaignsRaw = await slGet("/api/v1/campaigns");
    const campaigns = toList(campaignsRaw) as SmartLeadCampaign[];
    if (!campaigns.length) {
      return Response.json({ ok: true, message: "No campaigns returned by SmartLead", campaigns_total: 0 });
    }

    const total = campaigns.length;
    const cursor = await getSyncStateInt("smartlead_campaign_cursor", 0);
    const start = ((cursor % total) + total) % total;

    const selected: SmartLeadCampaign[] = [];
    for (let i = 0; i < Math.min(campaignsPerRun, total); i++) {
      selected.push(campaigns[(start + i) % total]);
    }

    const processed: Json[] = [];
    const processedIds: number[] = [];
    let sentTotal = 0;
    let replyTotal = 0;
    let leadsTotal = 0;
    let eventsTotal = 0;

    for (const campaign of selected) {
      const stats = await syncCampaign(campaign, pageSize, batchSize);
      processed.push(stats as unknown as Json);
      processedIds.push(stats.campaignId);
      sentTotal += stats.sent;
      replyTotal += stats.reply;
      leadsTotal += stats.leadsUpserted;
      eventsTotal += stats.eventsInserted;
    }

    const dailyRows = await rebuildDailyStatsForCampaigns(processedIds, batchSize);

    // Rebuild stats for orphaned campaigns (deleted/archived in SmartLead
    // but still have events in smartlead_events with no stats_daily rows)
    let orphanedResult: Json = {};
    try {
      orphanedResult = await sbRpc("rebuild_orphaned_smartlead_stats");
    } catch (err) {
      console.error("rebuild_orphaned_smartlead_stats failed:", err instanceof Error ? err.message : String(err));
      orphanedResult = await rebuildOrphanedDailyStats(batchSize);
    }

    const nextCursor = (start + selected.length) % total;
    await setSyncStateInt("smartlead_campaign_cursor", nextCursor);
    await setSyncStateInt("smartlead_last_sync_ts", Math.floor(Date.now() / 1000));

    return Response.json({
      ok: true,
      campaigns_total: total,
      campaigns_processed: selected.length,
      campaigns_per_run: campaignsPerRun,
      cursor_start: start,
      cursor_next: nextCursor,
      leads_upserted: leadsTotal,
      events_inserted: eventsTotal,
      sent_total: sentTotal,
      reply_total: replyTotal,
      daily_rows_rebuilt: dailyRows,
      orphaned_rebuild: orphanedResult,
      processed,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error("smartlead-sync failed", message);
    return new Response(JSON.stringify({ ok: false, error: message }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
});
