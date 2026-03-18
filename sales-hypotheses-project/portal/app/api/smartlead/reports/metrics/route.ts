import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { smartleadFetch } from "@/app/lib/smartlead";
import { getSupabaseUserFromAuthHeader } from "@/app/lib/supabase-server";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function ymdAddDaysLocal(ymd: string, days: number) {
  const [y, m, d] = String(ymd || "").split("-").map((x) => Number(x));
  if (!y || !m || !d) return "";
  const dt = new Date(y, m - 1, d);
  dt.setDate(dt.getDate() + days);
  const yy = dt.getFullYear();
  const mm = String(dt.getMonth() + 1).padStart(2, "0");
  const dd = String(dt.getDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

function looksLikeMissingRelation(msg: string) {
  const t = String(msg || "").toLowerCase();
  return t.includes("could not find the table") || t.includes("schema cache") || (t.includes("relation") && t.includes("does not exist"));
}

async function postgrestGetPaged(authHeader: string, pathBase: string, maxRows: number) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const out: any[] = [];
  const limit = Math.max(1, Math.min(20000, Number(maxRows || 20000)));
  for (let offset = 0; offset < limit; offset += 1000) {
    const url = `${supabaseUrl}/rest/v1/${pathBase}&limit=1000&offset=${offset}`;
    const res = await fetch(url, { headers: { apikey: supabaseAnonKey, Authorization: authHeader } });
    const json = (await res.json().catch(() => null)) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "Supabase query failed"));
    const rows = Array.isArray(json) ? json : [];
    out.push(...rows);
    if (rows.length < 1000) break;
  }
  return out.slice(0, limit);
}

/**
 * Fetch SmartLead events from Supabase using the service role.
 *
 * This bypasses RLS so audit can compare against full ingestion.
 */
async function fetchSmartleadEventsForAudit(opts: {
  sinceIso: string;
  untilIso: string;
  campaignIds: number[];
  maxRows: number;
}) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
  if (!supabaseUrl || !serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");

  const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });
  const limit = Math.max(1, Math.min(20000, Number(opts.maxRows || 20000)));
  const out: any[] = [];
  for (let offset = 0; offset < limit; offset += 1000) {
    let query = supabaseAdmin
      .from("sales_smartlead_events")
      .select("event_type,occurred_at,smartlead_campaign_id")
      .gte("occurred_at", opts.sinceIso)
      .lt("occurred_at", opts.untilIso)
      .range(offset, offset + 1000 - 1);
    if (opts.campaignIds.length) query = query.in("smartlead_campaign_id", opts.campaignIds);
    const res = await query;
    if (res.error) throw res.error;
    const rows = Array.isArray(res.data) ? res.data : [];
    out.push(...rows);
    if (rows.length < 1000) break;
  }
  return out.slice(0, limit);
}

/**
 * Fetch SmartLead day-wise analytics for a date range.
 *
 * This hits SmartLead API directly and returns per-day sent/open/reply/bounce.
 */
async function smartleadFetchDayWiseStats(opts: {
  startDate: string;
  endDate: string;
  campaignIds: number[];
}) {
  const qs = new URLSearchParams();
  qs.set("start_date", opts.startDate);
  qs.set("end_date", opts.endDate);
  if (opts.campaignIds.length) {
    qs.set("campaign_ids", opts.campaignIds.map((n) => String(n)).join(","));
  }
  return smartleadFetch(`/api/v1/analytics/day-wise-overall-stats?${qs.toString()}`, { method: "GET" });
}

/**
 * Fetch campaign analytics by date range for a single campaign.
 *
 * SmartLead returns totals for that campaign within the date window.
 */
async function smartleadFetchCampaignByDate(opts: { campaignId: number; startDate: string; endDate: string }) {
  const qs = new URLSearchParams();
  qs.set("start_date", opts.startDate);
  qs.set("end_date", opts.endDate);
  return smartleadFetch(`/api/v1/campaigns/${encodeURIComponent(String(opts.campaignId))}/analytics-by-date?${qs.toString()}`, {
    method: "GET"
  });
}

/**
 * Normalize SmartLead analytics payloads into consistent numeric counters.
 *
 * SmartLead responses differ between endpoints and versions, so we try multiple keys.
 */
function pickSmartleadAnalyticsTotals(a: any) {
  const leads_contacted = Number(a?.unique_sent_count ?? a?.uniqueSentCount ?? a?.unique_sent ?? a?.uniqueSent ?? 0) || 0;
  const emails_sent = Number(a?.sent_count ?? a?.sentCount ?? a?.total_sent ?? a?.totalSent ?? a?.emails_sent ?? 0) || 0;
  const emails_opened = Number(a?.unique_open_count ?? a?.uniqueOpenCount ?? a?.open_count ?? a?.openCount ?? 0) || 0;
  const emails_replied = Number(a?.reply_count ?? 0) || 0;
  const bounceTotal = Number(a?.bounce_count ?? 0) || 0;
  const bounced = Math.max(0, bounceTotal);
  const positive_reply =
    Number(
      a?.positive_reply_count ??
        a?.positive_replies_count ??
        a?.positive_replied_count ??
        a?.positiveReplyCount ??
        a?.positiveRepliesCount ??
        0
    ) || 0;
  const replied_ooo = Number(a?.ooo_count ?? a?.replied_ooo_count ?? a?.replied_with_ooo_count ?? a?.reply_ooo_count ?? 0) || 0;
  return { leads_contacted, emails_sent, emails_opened, emails_replied, bounced, positive_reply, replied_ooo };
}

function pickArrayFromSmartleadListResponse(json: any): any[] {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json?.data)) return json.data;
  if (Array.isArray(json?.results)) return json.results;
  if (Array.isArray(json?.campaigns)) return json.campaigns;
  return [];
}

async function smartleadListCampaignsBestEffort(opts?: { limit?: number; offset?: number }) {
  const limit = Math.max(1, Math.min(200, Number(opts?.limit ?? 100)));
  const offset = Math.max(0, Number(opts?.offset ?? 0));
  const qs = new URLSearchParams();
  qs.set("limit", String(limit));
  qs.set("offset", String(offset));
  const q = qs.toString();
  const attempts = [`/api/v1/campaigns?${q}`, `/api/v1/campaigns/list?${q}`, `/api/v1/campaigns`];
  let lastErr: any = null;
  for (const path of attempts) {
    try {
      const json = await smartleadFetch(path, { method: "GET" });
      const arr = pickArrayFromSmartleadListResponse(json);
      if (arr.length) return arr;
    } catch (e: any) {
      lastErr = e;
    }
  }
  throw lastErr ?? new Error("Failed to list campaigns");
}

type SmartleadDbCounts = {
  totals: { emails_sent: number; emails_opened: number; emails_replied: number };
  per_campaign: Record<
    string,
    { emails_sent: number; emails_opened: number; emails_replied: number }
  >;
};

/**
 * Aggregate SmartLead events stored in Supabase into per-campaign counters.
 *
 * This helps identify gaps between SmartLead analytics and our DB ingestion.
 */
function aggregateSmartleadDbCounts(rows: any[]): SmartleadDbCounts {
  const totals = { emails_sent: 0, emails_opened: 0, emails_replied: 0 };
  const per_campaign: SmartleadDbCounts["per_campaign"] = {};

  for (const r of Array.isArray(rows) ? rows : []) {
    const campaignId = String(r?.smartlead_campaign_id ?? "").trim();
    const eventType = String(r?.event_type ?? "").trim().toLowerCase();
    if (!campaignId) continue;
    if (!per_campaign[campaignId]) {
      per_campaign[campaignId] = { emails_sent: 0, emails_opened: 0, emails_replied: 0 };
    }

    if (eventType === "sent") {
      per_campaign[campaignId].emails_sent += 1;
      totals.emails_sent += 1;
    } else if (eventType === "opened") {
      per_campaign[campaignId].emails_opened += 1;
      totals.emails_opened += 1;
    } else if (eventType === "replied") {
      per_campaign[campaignId].emails_replied += 1;
      totals.emails_replied += 1;
    }
  }

  return { totals, per_campaign };
}

type SmartleadDbUiTotals = {
  totals: {
    leads_contacted: number;
    emails_sent: number;
    emails_opened: number;
    emails_replied: number;
    bounced: number;
    positive_reply: number;
    replied_ooo: number;
  };
  per_campaign: Record<
    string,
    {
      leads_contacted: number;
      emails_sent: number;
      emails_opened: number;
      emails_replied: number;
      bounced: number;
      positive_reply: number;
      replied_ooo: number;
    }
  >;
};

/**
 * Aggregate SmartLead events into UI totals using only DB data.
 *
 * SmartLead UI uses "unique per lead per day" for sent/opened/replied.
 * We follow the same to align metrics with the UI.
 */
function aggregateSmartleadDbUiTotals(rows: any[]): SmartleadDbUiTotals {
  const totals = {
    leads_contacted: 0,
    emails_sent: 0,
    emails_opened: 0,
    emails_replied: 0,
    bounced: 0,
    positive_reply: 0,
    replied_ooo: 0
  };
  const per_campaign: SmartleadDbUiTotals["per_campaign"] = {};
  const uniqueSentEmails = new Set<string>();
  const sentByDay = new Set<string>();
  const openedByDay = new Set<string>();
  const repliedByDay = new Set<string>();
  const positiveByDay = new Set<string>();
  const oooByDay = new Set<string>();
  const bouncedByDay = new Set<string>();
  const perCampaignSets: Record<
    string,
    {
      sentByDay: Set<string>;
      openedByDay: Set<string>;
      repliedByDay: Set<string>;
      positiveByDay: Set<string>;
      oooByDay: Set<string>;
      bouncedByDay: Set<string>;
      leads: Set<string>;
    }
  > = {};

  for (const r of Array.isArray(rows) ? rows : []) {
    const campaignId = String(r?.smartlead_campaign_id ?? "").trim();
    const eventType = String(r?.event_type ?? "").trim().toLowerCase();
    const email = String(r?.contact_email ?? "").trim().toLowerCase();
    const at = String(r?.occurred_at ?? "").trim();
    const day = at ? at.slice(0, 10) : "";
    const dayKey = email && day ? `${email}#${day}` : "";
    if (!campaignId) continue;
    if (!per_campaign[campaignId]) {
      per_campaign[campaignId] = {
        leads_contacted: 0,
        emails_sent: 0,
        emails_opened: 0,
        emails_replied: 0,
        bounced: 0,
        positive_reply: 0,
        replied_ooo: 0
      };
    }
    if (!perCampaignSets[campaignId]) {
      perCampaignSets[campaignId] = {
        sentByDay: new Set<string>(),
        openedByDay: new Set<string>(),
        repliedByDay: new Set<string>(),
        positiveByDay: new Set<string>(),
        oooByDay: new Set<string>(),
        bouncedByDay: new Set<string>(),
        leads: new Set<string>()
      };
    }

    if (eventType === "sent") {
      if (email) uniqueSentEmails.add(email);
      if (email) perCampaignSets[campaignId].leads.add(email);
      if (dayKey) sentByDay.add(dayKey);
      if (dayKey) perCampaignSets[campaignId].sentByDay.add(dayKey);
    } else if (eventType === "opened") {
      if (dayKey) openedByDay.add(dayKey);
      if (dayKey) perCampaignSets[campaignId].openedByDay.add(dayKey);
    } else if (eventType === "replied") {
      if (dayKey) repliedByDay.add(dayKey);
      if (dayKey) perCampaignSets[campaignId].repliedByDay.add(dayKey);
    } else if (eventType === "positive_reply") {
      if (dayKey) positiveByDay.add(dayKey);
      if (dayKey) perCampaignSets[campaignId].positiveByDay.add(dayKey);
    } else if (eventType === "replied_ooo") {
      if (dayKey) oooByDay.add(dayKey);
      if (dayKey) perCampaignSets[campaignId].oooByDay.add(dayKey);
    } else if (eventType === "bounced") {
      if (dayKey) bouncedByDay.add(dayKey);
      if (dayKey) perCampaignSets[campaignId].bouncedByDay.add(dayKey);
    }
  }

  totals.leads_contacted = uniqueSentEmails.size;
  totals.emails_sent = sentByDay.size;
  totals.emails_opened = openedByDay.size;
  totals.emails_replied = repliedByDay.size;
  totals.positive_reply = positiveByDay.size;
  totals.replied_ooo = oooByDay.size;
  totals.bounced = bouncedByDay.size;

  for (const id of Object.keys(per_campaign)) {
    const sets = perCampaignSets[id];
    per_campaign[id].leads_contacted = sets?.leads?.size ?? 0;
    per_campaign[id].emails_sent = sets?.sentByDay?.size ?? 0;
    per_campaign[id].emails_opened = sets?.openedByDay?.size ?? 0;
    per_campaign[id].emails_replied = sets?.repliedByDay?.size ?? 0;
    per_campaign[id].positive_reply = sets?.positiveByDay?.size ?? 0;
    per_campaign[id].replied_ooo = sets?.oooByDay?.size ?? 0;
    per_campaign[id].bounced = sets?.bouncedByDay?.size ?? 0;
  }

  return { totals, per_campaign };
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");
    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json()) as {
      since: string;
      until: string;
      campaign_ids?: Array<string | number>;
      audit?: boolean;
      use_smartlead_daywise?: boolean;
      use_smartlead_campaign_by_date?: boolean;
      debug_smartlead?: boolean;
      prefer_db?: boolean;
    };
    const since = String(payload?.since ?? "").trim();
    const until = String(payload?.until ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(since) || !/^\d{4}-\d{2}-\d{2}$/.test(until)) {
      return jsonError(400, "since/until must be YYYY-MM-DD");
    }
    const audit = Boolean(payload?.audit);

    const campaignIds = Array.isArray(payload?.campaign_ids)
      ? payload.campaign_ids.map((x) => Number(x)).filter((n) => Number.isFinite(n))
      : [];
    const campaignFilter = campaignIds.length ? `&smartlead_campaign_id=in.(${campaignIds.map((n) => encodeURIComponent(String(n))).join(",")})` : "";

    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
    if (!supabaseUrl || !supabaseAnonKey) return jsonError(500, "Missing Supabase env");

    const sinceIso = `${since}T00:00:00.000Z`;
    const untilExcl = ymdAddDaysLocal(until, 1);
    const untilIso = `${untilExcl}T00:00:00.000Z`;
    const where =
      // Include campaign id + contact email so we can count unique leads.
      `sales_smartlead_events?select=event_type,occurred_at,smartlead_campaign_id,contact_email` +
      `&occurred_at=gte.${encodeURIComponent(sinceIso)}` +
      `&occurred_at=lt.${encodeURIComponent(untilIso)}` +
      campaignFilter;

    if (payload?.use_smartlead_daywise) {
      const json = await smartleadFetchDayWiseStats({ startDate: since, endDate: until, campaignIds });
      return NextResponse.json({
        ok: true,
        since,
        until,
        campaign_ids: campaignIds,
        source: "smartlead_day_wise",
        daily_stats: json?.data?.daily_stats ?? [],
        raw: payload?.debug_smartlead ? json : undefined
      });
    }

    const preferDb = payload?.prefer_db !== false;
    if (preferDb) {
      const rows = await postgrestGetPaged(authHeader ?? "", where, 20000);
      const dbTotals = aggregateSmartleadDbUiTotals(rows);
      return NextResponse.json({
        ok: true,
        since,
        until,
        campaign_ids: campaignIds,
        source: "db_events",
        total: dbTotals.totals,
        per_campaign: Object.keys(dbTotals.per_campaign).map((id) => ({
          campaign_id: Number(id),
          ...dbTotals.per_campaign[id]
        }))
      });
    }

    if (payload?.use_smartlead_campaign_by_date) {
      let leads_contacted = 0;
      let emails_sent = 0;
      let emails_opened = 0;
      let emails_replied = 0;
      let bounced = 0;
      let positive_reply = 0;
      let replied_ooo = 0;
      const per_campaign: any[] = [];
      const ids =
        campaignIds.length > 0
          ? campaignIds
          : (await smartleadListCampaignsBestEffort({ limit: 200, offset: 0 }))
            .map((c: any) => Number(c?.id ?? c?.campaign_id ?? c?.campaignId))
            .filter((n) => Number.isFinite(n) && n > 0)
            .slice(0, 50);

      for (const id of ids) {
        if (!Number.isFinite(id) || id <= 0) continue;
        try {
          const json = await smartleadFetchCampaignByDate({ campaignId: id, startDate: since, endDate: until });
          const payloadData = (json as any)?.data ?? json;
          const stats = pickSmartleadAnalyticsTotals(payloadData);
          leads_contacted += stats.leads_contacted;
          emails_sent += stats.emails_sent;
          emails_opened += stats.emails_opened;
          emails_replied += stats.emails_replied;
          bounced += stats.bounced;
          positive_reply += stats.positive_reply;
          replied_ooo += stats.replied_ooo;
          per_campaign.push({ campaign_id: id, ...stats });
        } catch (e: any) {
          per_campaign.push({ campaign_id: id, error: String(e?.message || e) });
        }
      }
      return NextResponse.json({
        ok: true,
        since,
        until,
        campaign_ids: campaignIds,
        source: "smartlead_campaign_by_date",
        total: { leads_contacted, emails_sent, emails_opened, emails_replied, bounced, positive_reply, replied_ooo },
        per_campaign
      });
    }

    // Prefer SmartLead's own campaign analytics when filtering by campaign(s): it matches SmartLead UI counters.
    // IMPORTANT: SmartLead distinguishes:
    // - unique_sent_count => "leads contacted" (unique prospects touched)
    // - sent_count => "messages sent" (all emails sent, can be > leads contacted)
    if (campaignIds.length) {
      let leads_contacted = 0;
      let emails_sent = 0;
      let emails_opened = 0;
      let emails_replied = 0;
      let bounced = 0;
      let positive_reply = 0;
      let replied_ooo = 0;
      const per_campaign: any[] = [];
      for (const id of campaignIds) {
        if (!Number.isFinite(id) || id <= 0) continue;
        try {
          const a = await smartleadFetch(`/api/v1/campaigns/${encodeURIComponent(String(id))}/analytics`, { method: "GET" });
          const stats = pickSmartleadAnalyticsTotals(a);
          leads_contacted += stats.leads_contacted;
          emails_sent += stats.emails_sent;
          emails_opened += stats.emails_opened;
          emails_replied += stats.emails_replied;
          bounced += stats.bounced;
          positive_reply += stats.positive_reply;
          replied_ooo += stats.replied_ooo;
          per_campaign.push({ campaign_id: id, ...stats });
        } catch (e: any) {
          per_campaign.push({ campaign_id: id, error: String(e?.message || e) });
        }
      }
      let dbCounts: SmartleadDbCounts | null = null;
      let diff: Record<string, any> | null = null;
      if (audit) {
        const rows = await fetchSmartleadEventsForAudit({
          sinceIso,
          untilIso,
          campaignIds,
          maxRows: 20000
        });
        dbCounts = aggregateSmartleadDbCounts(rows);
        diff = {};
        for (const c of per_campaign) {
          const id = String(c.campaign_id);
          const db = dbCounts.per_campaign[id] ?? { emails_sent: 0, emails_opened: 0, emails_replied: 0 };
          diff[id] = {
            campaign_id: c.campaign_id,
            smartlead: {
              emails_sent: c.emails_sent,
              emails_opened: c.opened,
              emails_replied: c.replied
            },
            db,
            missing: {
              emails_sent: Math.max(0, (c.emails_sent ?? 0) - (db.emails_sent ?? 0)),
              emails_opened: Math.max(0, (c.opened ?? 0) - (db.emails_opened ?? 0)),
              emails_replied: Math.max(0, (c.replied ?? 0) - (db.emails_replied ?? 0))
            }
          };
        }
      }

      return NextResponse.json({
        ok: true,
        since,
        until,
        campaign_ids: campaignIds,
        source: "smartlead_campaign_analytics",
        // Note: these are campaign totals, not necessarily limited to since/until.
        total: { leads_contacted, emails_sent, emails_opened, emails_replied, bounced, positive_reply, replied_ooo },
        per_campaign,
        audit: audit
          ? {
            db_range: { since, until },
            db_totals: dbCounts?.totals ?? null,
            db_per_campaign: dbCounts?.per_campaign ?? null,
            diff_per_campaign: diff
          }
          : undefined
      });
    }

    // If no campaign filter is provided, still prefer SmartLead analytics by summing across campaigns
    // (this avoids confusing results from partial/legacy DB ingests).
    try {
      const arr = await smartleadListCampaignsBestEffort({ limit: 200, offset: 0 });
      const ids = arr.map((c: any) => Number(c?.id ?? c?.campaign_id ?? c?.campaignId)).filter((n) => Number.isFinite(n) && n > 0) as number[];
      if (ids.length) {
        let leads_contacted = 0;
        let emails_sent = 0;
        let emails_opened = 0;
        let emails_replied = 0;
        let bounced = 0;
        let positive_reply = 0;
        let replied_ooo = 0;
        const per_campaign: any[] = [];
        for (const id of ids.slice(0, 50)) {
          try {
            const a = await smartleadFetch(`/api/v1/campaigns/${encodeURIComponent(String(id))}/analytics`, { method: "GET" });
            const stats = pickSmartleadAnalyticsTotals(a);
            leads_contacted += stats.leads_contacted;
            emails_sent += stats.emails_sent;
            emails_opened += stats.emails_opened;
            emails_replied += stats.emails_replied;
            bounced += stats.bounced;
            positive_reply += stats.positive_reply;
            replied_ooo += stats.replied_ooo;
            per_campaign.push({ campaign_id: id, ...stats });
          } catch (e: any) {
            per_campaign.push({ campaign_id: id, error: String(e?.message || e) });
          }
        }
        return NextResponse.json({
          ok: true,
          since,
          until,
          campaign_ids: [],
          source: "smartlead_campaign_analytics_all",
          // Note: these are campaign totals, not necessarily limited to since/until.
          total: { leads_contacted, emails_sent, emails_opened, emails_replied, bounced, positive_reply, replied_ooo },
          per_campaign_count: per_campaign.length
        });
      }
    } catch {
      // Fall back to DB events below.
    }

    const list = await postgrestGetPaged(authHeader ?? "", where, 20000);
    const total = { emails_sent: 0, emails_opened: 0, emails_replied: 0 };
    for (const r of list) {
      const t = String(r?.event_type ?? "").trim().toLowerCase();
      if (t === "sent") total.emails_sent += 1;
      else if (t === "opened") total.emails_opened += 1;
      else if (t === "replied") total.emails_replied += 1;
    }

    return NextResponse.json({ ok: true, since, until, campaign_ids: campaignIds, source: "db_events", total, limited: list.length >= 20000 });
  } catch (e: any) {
    const msg = String(e?.message || e);
    if (looksLikeMissingRelation(msg)) {
      return NextResponse.json({ ok: true, disabled: true, reason: msg, since: "", until: "", total: { emails_sent: 0, emails_opened: 0, emails_replied: 0 } });
    }
    return jsonError(500, msg);
  }
}


