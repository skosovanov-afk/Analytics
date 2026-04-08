import { NextResponse } from "next/server";
import {
  getSupabaseUserFromAuthHeader,
  postgrestHeadersFor,
} from "@/app/lib/supabase-server";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

/** Fetch all rows from PostgREST with pagination (Supabase default max = 1000 rows per request) */
async function postgrestAllRows(h: any, path: string, pageSize = 1000): Promise<any[]> {
  const sbUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const all: any[] = [];
  let offset = 0;
  while (true) {
    const sep = path.includes("?") ? "&" : "?";
    const url = `${sbUrl}/rest/v1/${path}${sep}limit=${pageSize}&offset=${offset}`;
    const res = await fetch(url, {
      headers: { ...h, "Content-Type": "application/json" },
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`PostgREST error: ${text}`);
    }
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) break;
    all.push(...rows);
    if (rows.length < pageSize) break;
    offset += pageSize;
  }
  return all;
}

function buildDateFilter(column: string, since: string | null, until: string | null): string {
  let filter = "";
  if (since) filter += `&${column}=gte.${since}`;
  if (until) filter += `&${column}=lte.${until}`;
  return filter;
}

/**
 * POST /api/dashboard/stats
 * Body: { since?: string, until?: string }
 *
 * When no dates: returns tal_analytics_v directly (matches TAL page exactly).
 * When dates provided: aggregates from daily tables with date filtering.
 */
export async function POST(req: Request) {
  try {
    const auth = req.headers.get("authorization") ?? "";
    const user = await getSupabaseUserFromAuthHeader(auth);
    if (!user?.email) return jsonError(401, "Not authorized");

    const body = await req.json().catch(() => ({}));
    const since: string | null = body.since || null;
    const until: string | null = body.until || null;
    const hasDateFilter = Boolean(since || until);

    const h = postgrestHeadersFor(auth, false);

    // ── All-time: use tal_analytics_v (matches TAL page exactly) ──
    if (!hasDateFilter) {
      const tals = await postgrestAllRows(h,
        "tal_analytics_v?select=id,name,description,criteria,created_at,email_sent,email_replies,email_reply_rate,email_meetings,email_held_meetings,email_qualified_leads,li_invited,li_accepted,li_replies,li_accept_rate,li_meetings,li_held_meetings,li_qualified_leads,app_invitations,app_touches,app_replies,app_reply_rate,app_meetings,app_held_meetings,app_qualified_leads,tg_touches,tg_replies,tg_reply_rate,tg_meetings,tg_held_meetings,tg_qualified_leads,total_meetings,total_held_meetings,total_qualified_leads&order=created_at.desc"
      );
      return NextResponse.json({ ok: true, since: null, until: null, tals });
    }

    // ── Date-filtered: aggregate from daily tables ──

    // 1. Load tal_campaigns mapping
    const talCampaigns = await postgrestAllRows(h,
      "tal_campaigns?select=tal_id,channel,campaign_name,source_campaign_key"
    );

    // 2. Load TAL names
    const tals = await postgrestAllRows(h,
      "tals?select=id,name,description,criteria,created_at&order=created_at.desc"
    );

    // Build lookup maps
    const bySourceKey = new Map<string, string>();
    const byName = new Map<string, string>();
    for (const m of talCampaigns) {
      if (m.source_campaign_key) {
        bySourceKey.set(`${m.channel}:${m.source_campaign_key}`, m.tal_id);
      }
      const nameKey = `${m.channel}:${String(m.campaign_name ?? "").trim().toLowerCase()}`;
      if (!byName.has(nameKey)) byName.set(nameKey, m.tal_id);
    }

    function findTalId(channel: string, sourceKey: string | null, campaignName: string): string | null {
      if (sourceKey) {
        const hit = bySourceKey.get(`${channel}:${sourceKey}`);
        if (hit) return hit;
      }
      return byName.get(`${channel}:${campaignName.trim().toLowerCase()}`) ?? null;
    }

    // 3. Fetch daily data with date filters
    const dateFilterSl = buildDateFilter("date", since, until);
    const dateFilterLi = buildDateFilter("day", since, until);
    const dateFilterManual = buildDateFilter("record_date", since, until);

    const [slRows, liRows, manualRows] = await Promise.all([
      // Email: smartlead_stats_daily
      postgrestAllRows(h,
        `smartlead_stats_daily?select=campaign_id,campaign_name,sent_count,reply_count${dateFilterSl}`
      ),
      // LinkedIn: linkedin_kpi_daily_v2 (same view used by Expandi page for date ranges)
      postgrestAllRows(h,
        `linkedin_kpi_daily_v2?select=campaign_name,connection_req,accepted,sent_messages,replies,booked_meetings,held_meetings${dateFilterLi}`
      ).catch((err) => { console.error("linkedin_kpi_daily_v2 fetch failed:", err); return []; }),
      // App, Telegram, Email meetings, LinkedIn meetings: manual_stats
      postgrestAllRows(h,
        `manual_stats?select=channel,campaign_name,account_name,metric_name,value${dateFilterManual}&metric_name=in.(invitations,total_touches,replies,booked_meetings,held_meetings,qualified_leads)`
      ),
    ]);

    // 4. Aggregate per TAL
    type TalMetrics = {
      email_sent: number; email_replies: number; email_meetings: number; email_held_meetings: number; email_qualified_leads: number;
      li_invited: number; li_accepted: number; li_replies: number; li_meetings: number; li_held_meetings: number; li_qualified_leads: number;
      app_invitations: number; app_touches: number; app_replies: number; app_meetings: number; app_held_meetings: number; app_qualified_leads: number;
      tg_touches: number; tg_replies: number; tg_meetings: number; tg_held_meetings: number; tg_qualified_leads: number;
    };

    const emptyMetrics = (): TalMetrics => ({
      email_sent: 0, email_replies: 0, email_meetings: 0, email_held_meetings: 0, email_qualified_leads: 0,
      li_invited: 0, li_accepted: 0, li_replies: 0, li_meetings: 0, li_held_meetings: 0, li_qualified_leads: 0,
      app_invitations: 0, app_touches: 0, app_replies: 0, app_meetings: 0, app_held_meetings: 0, app_qualified_leads: 0,
      tg_touches: 0, tg_replies: 0, tg_meetings: 0, tg_held_meetings: 0, tg_qualified_leads: 0,
    });

    const byTal = new Map<string, TalMetrics>();
    function getOrCreate(talId: string) {
      if (!byTal.has(talId)) byTal.set(talId, emptyMetrics());
      return byTal.get(talId)!;
    }

    // Smartlead (email sent/replies)
    for (const row of slRows as any[]) {
      const sourceKey = row.campaign_id != null ? `smartlead:id:${row.campaign_id}` : null;
      const name = String(row.campaign_name ?? "");
      const talId = findTalId("smartlead", sourceKey, name);
      if (!talId) continue;
      const m = getOrCreate(talId);
      m.email_sent += Number(row.sent_count) || 0;
      m.email_replies += Number(row.reply_count) || 0;
    }

    // LinkedIn (from linkedin_kpi_daily_v2 - same as Expandi page uses for date ranges)
    for (const row of liRows as any[]) {
      const name = String(row.campaign_name ?? "").trim();
      if (!name) continue;
      const sourceKey = `expandi:canonical:${name.toLowerCase()}`;
      const talId = findTalId("expandi", sourceKey, name);
      if (!talId) continue;
      const m = getOrCreate(talId);
      m.li_invited += Number(row.connection_req) || 0;
      m.li_accepted += Number(row.accepted) || 0;
      m.li_replies += Number(row.replies) || 0;
      m.li_meetings += Number(row.booked_meetings) || 0;
      m.li_held_meetings += Number(row.held_meetings) || 0;
    }

    // Manual stats (app, telegram, email meetings, linkedin meetings)
    for (const row of manualRows as any[]) {
      const ch = String(row.channel ?? "");
      const campaignName = String(row.campaign_name ?? row.account_name ?? "").trim();
      if (!campaignName) continue;
      const metric = String(row.metric_name ?? "");
      const value = Number(row.value) || 0;

      if (ch === "app") {
        const sourceKey = `app:name:${campaignName.toLowerCase()}`;
        const talId = findTalId("app", sourceKey, campaignName);
        if (!talId) continue;
        const m = getOrCreate(talId);
        if (metric === "invitations") m.app_invitations += value;
        if (metric === "total_touches") m.app_touches += value;
        if (metric === "replies") m.app_replies += value;
        if (metric === "booked_meetings") m.app_meetings += value;
        if (metric === "held_meetings") m.app_held_meetings += value;
        if (metric === "qualified_leads") m.app_qualified_leads += value;
      } else if (ch === "telegram") {
        const sourceKey = `telegram:name:${campaignName.toLowerCase()}`;
        const talId = findTalId("telegram", sourceKey, campaignName);
        if (!talId) continue;
        const m = getOrCreate(talId);
        if (metric === "total_touches") m.tg_touches += value;
        if (metric === "replies") m.tg_replies += value;
        if (metric === "booked_meetings") m.tg_meetings += value;
        if (metric === "held_meetings") m.tg_held_meetings += value;
        if (metric === "qualified_leads") m.tg_qualified_leads += value;
      } else if (ch === "email" || ch === "smartlead") {
        // Email meetings from manual_stats
        let talId: string | null = null;
        for (const mc of talCampaigns) {
          if (mc.channel !== "smartlead") continue;
          if (String(mc.campaign_name ?? "").toLowerCase().trim() === campaignName.toLowerCase()) {
            talId = mc.tal_id; break;
          }
        }
        if (!talId) continue;
        const m = getOrCreate(talId);
        if (metric === "booked_meetings") m.email_meetings += value;
        if (metric === "held_meetings") m.email_held_meetings += value;
        if (metric === "qualified_leads") m.email_qualified_leads += value;
      }
      // LinkedIn meetings: taken from linkedin_kpi_daily_v2 (booked_meetings/held_meetings columns)
    }

    // 5. Build response
    const result = tals.map((t: any) => {
      const m = byTal.get(t.id) ?? emptyMetrics();
      return {
        id: t.id,
        name: t.name,
        description: t.description,
        criteria: t.criteria,
        created_at: t.created_at,
        ...m,
        email_reply_rate: m.email_sent ? Math.round((m.email_replies / m.email_sent) * 10000) / 100 : null,
        li_accept_rate: m.li_invited ? Math.round((m.li_accepted / m.li_invited) * 10000) / 100 : null,
        app_reply_rate: m.app_touches ? Math.round((m.app_replies / m.app_touches) * 10000) / 100 : null,
        tg_reply_rate: m.tg_touches ? Math.round((m.tg_replies / m.tg_touches) * 10000) / 100 : null,
        total_meetings: m.email_meetings + m.li_meetings + m.app_meetings + m.tg_meetings,
        total_held_meetings: m.email_held_meetings + m.li_held_meetings + m.app_held_meetings + m.tg_held_meetings,
        total_qualified_leads: m.email_qualified_leads + m.li_qualified_leads + m.app_qualified_leads + m.tg_qualified_leads,
      };
    });

    return NextResponse.json({ ok: true, since, until, tals: result });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}
