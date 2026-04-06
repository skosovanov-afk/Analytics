import { NextResponse } from "next/server";
import {
  getSupabaseUserFromAuthHeader,
  postgrestHeadersFor,
  postgrestJson,
  type PostgrestHeaders
} from "@/app/lib/supabase-server";

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

async function postgrestGetPaged(h: PostgrestHeaders, pathBase: string, maxRows: number) {
  const out: any[] = [];
  const limit = Math.max(1, Math.min(20000, Number(maxRows || 20000)));
  for (let offset = 0; offset < limit; offset += 1000) {
    const sep = pathBase.includes("?") ? "&" : "?";
    const rows = await postgrestJson(h, "GET", `${pathBase}${sep}limit=1000&offset=${offset}`);
    const batch = Array.isArray(rows) ? rows : [];
    out.push(...batch);
    if (batch.length < 1000) break;
  }
  return out.slice(0, limit);
}

type DailyRow = {
  date: string;
  campaign_id: number | null;
  campaign_name: string | null;
  sent_count: number | null;
  open_count: number | null;
  reply_count: number | null;
  unique_leads_count: number | null;
};

type EventRow = {
  campaign_id: number | null;
  campaign_name: string | null;
  email: string | null;
  is_bounced: boolean | null;
  occurred_at: string | null;
};

type CampaignTotals = {
  campaign_id: number;
  campaign_name: string;
  leads_contacted: number;
  emails_sent: number;
  emails_opened: number;
  emails_replied: number;
  bounced: number;
  positive_reply: number;
  replied_ooo: number;
};

function num(v: unknown) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function ensureCampaign(map: Map<number, CampaignTotals>, campaignId: number, campaignName: string) {
  if (!map.has(campaignId)) {
    map.set(campaignId, {
      campaign_id: campaignId,
      campaign_name: campaignName,
      leads_contacted: 0,
      emails_sent: 0,
      emails_opened: 0,
      emails_replied: 0,
      bounced: 0,
      positive_reply: 0,
      replied_ooo: 0
    });
  }
  return map.get(campaignId)!;
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");
    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json()) as {
      since: string;
      until: string;
      campaign_ids?: Array<string | number>;
    };
    const since = String(payload?.since ?? "").trim();
    const until = String(payload?.until ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(since) || !/^\d{4}-\d{2}-\d{2}$/.test(until)) {
      return jsonError(400, "since/until must be YYYY-MM-DD");
    }

    const campaignIds = Array.isArray(payload?.campaign_ids)
      ? payload.campaign_ids.map((x) => Number(x)).filter((n) => Number.isFinite(n) && n > 0)
      : [];

    const h = postgrestHeadersFor(String(authHeader ?? ""), false);
    const sinceIso = `${since}T00:00:00.000Z`;
    const untilExcl = ymdAddDaysLocal(until, 1);
    const untilIso = `${untilExcl}T00:00:00.000Z`;
    const campaignFilter = campaignIds.length
      ? `&campaign_id=in.(${campaignIds.map((n) => encodeURIComponent(String(n))).join(",")})`
      : "";

    const [dailyRows, eventRows] = await Promise.all([
      postgrestGetPaged(
        h,
        "smartlead_stats_daily" +
          "?select=date,campaign_id,campaign_name,sent_count,open_count,reply_count,unique_leads_count" +
          `&date=gte.${encodeURIComponent(since)}` +
          `&date=lte.${encodeURIComponent(until)}` +
          campaignFilter +
          "&order=date.asc",
        20000
      ) as Promise<DailyRow[]>,
      postgrestGetPaged(
        h,
        "smartlead_events" +
          "?select=campaign_id,campaign_name,email,is_bounced,occurred_at" +
          `&occurred_at=gte.${encodeURIComponent(sinceIso)}` +
          `&occurred_at=lt.${encodeURIComponent(untilIso)}` +
          campaignFilter +
          "&order=occurred_at.asc",
        20000
      ) as Promise<EventRow[]>
    ]);

    const total = {
      leads_contacted: 0,
      emails_sent: 0,
      emails_opened: 0,
      emails_replied: 0,
      bounced: 0,
      positive_reply: 0,
      replied_ooo: 0
    };
    const perCampaign = new Map<number, CampaignTotals>();
    const bouncedKeys = new Set<string>();

    for (const row of dailyRows) {
      const campaignId = Number(row?.campaign_id);
      const campaignName = String(row?.campaign_name ?? "").trim();
      const sent = num(row?.sent_count);
      const opened = num(row?.open_count);
      const replied = num(row?.reply_count);
      const leads = num(row?.unique_leads_count);

      total.leads_contacted += leads;
      total.emails_sent += sent;
      total.emails_opened += opened;
      total.emails_replied += replied;

      if (Number.isFinite(campaignId) && campaignId > 0) {
        const item = ensureCampaign(perCampaign, campaignId, campaignName);
        item.leads_contacted += leads;
        item.emails_sent += sent;
        item.emails_opened += opened;
        item.emails_replied += replied;
      }
    }

    for (const row of eventRows) {
      if (!row?.is_bounced) continue;
      const campaignId = Number(row?.campaign_id);
      const emailValue = String(row?.email ?? "").trim().toLowerCase();
      const occurredAt = String(row?.occurred_at ?? "").trim();
      const day = occurredAt ? occurredAt.slice(0, 10) : "";
      const bounceKey = `${campaignId}:${emailValue}:${day}`;
      if (!emailValue || !day || bouncedKeys.has(bounceKey)) continue;
      bouncedKeys.add(bounceKey);
      total.bounced += 1;

      if (Number.isFinite(campaignId) && campaignId > 0) {
        const campaignName = String(row?.campaign_name ?? "").trim();
        const item = ensureCampaign(perCampaign, campaignId, campaignName);
        item.bounced += 1;
      }
    }

    return NextResponse.json({
      ok: true,
      since,
      until,
      campaign_ids: campaignIds,
      source: "legacy_smartlead",
      total,
      per_campaign: Array.from(perCampaign.values()).sort((a, b) => b.emails_sent - a.emails_sent)
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}
