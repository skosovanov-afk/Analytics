import { NextResponse } from "next/server";

type SupabaseUserResponse = { email?: string | null };

type ExpandiDailyRow = {
  day?: string | null;
  campaign_instance_id?: number | string | null;
  campaign_name?: string | null;
  sent_messages?: number | string | null;
  received_messages?: number | string | null;
  sent_invitations?: number | string | null;
  new_connections?: number | string | null;
  new_replies?: number | string | null;
};

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
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
  return (
    t.includes("could not find the table") ||
    t.includes("schema cache") ||
    (t.includes("relation") && t.includes("does not exist")) ||
    (t.includes("column") && t.includes("does not exist"))
  );
}

async function postgrestGetPaged(authHeader: string, pathBase: string, maxRows: number) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const out: ExpandiDailyRow[] = [];
  const limit = Math.max(1, Math.min(50000, Number(maxRows || 50000)));
  for (let offset = 0; offset < limit; offset += 1000) {
    const url = `${supabaseUrl}/rest/v1/${pathBase}&limit=1000&offset=${offset}`;
    const res = await fetch(url, { headers: { apikey: supabaseAnonKey, Authorization: authHeader } });
    const json = (await res.json().catch(() => null)) as unknown;
    if (!res.ok) {
      const err = json as { message?: string; error?: string } | null;
      throw new Error(String(err?.message || err?.error || "Supabase query failed"));
    }
    const rows = Array.isArray(json) ? (json as ExpandiDailyRow[]) : [];
    out.push(...rows);
    if (rows.length < 1000) break;
  }
  return out.slice(0, limit);
}

function toNum(v: unknown) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function pct(num: number, den: number) {
  if (!Number.isFinite(num) || !Number.isFinite(den) || den <= 0) return 0;
  return Number(((num / den) * 100).toFixed(2));
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
      group_by_name?: boolean;
    };

    const since = String(payload?.since ?? "").trim();
    const until = String(payload?.until ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(since) || !/^\d{4}-\d{2}-\d{2}$/.test(until)) {
      return jsonError(400, "since/until must be YYYY-MM-DD");
    }

    const campaignIds = Array.isArray(payload?.campaign_ids)
      ? payload.campaign_ids.map((x) => Number(x)).filter((n) => Number.isFinite(n))
      : [];
    const groupByName = payload?.group_by_name !== false;

    const untilExcl = ymdAddDaysLocal(until, 1);
    const campaignFilter = campaignIds.length
      ? `&campaign_instance_id=in.(${campaignIds.map((n) => encodeURIComponent(String(n))).join(",")})`
      : "";

    const where =
      `expandi_campaign_daily_v?select=day,campaign_instance_id,campaign_name,sent_messages,received_messages,sent_invitations,new_connections,new_replies` +
      `&day=gte.${encodeURIComponent(since)}` +
      `&day=lt.${encodeURIComponent(untilExcl)}` +
      campaignFilter;

    const rows = await postgrestGetPaged(authHeader ?? "", where, 50000);

    const total = {
      sent_messages: 0,
      received_messages: 0,
      sent_invitations: 0,
      new_connections: 0,
      new_replies: 0
    };

    const perCampaign = new Map<number, {
      campaign_id: number;
      campaign_name: string | null;
      sent_messages: number;
      received_messages: number;
      sent_invitations: number;
      new_connections: number;
      new_replies: number;
      connect_rate_pct: number;
      reply_rate_pct: number;
    }>();
    const perCampaignName = new Map<string, {
      campaign_name: string;
      campaign_ids: number[];
      sent_messages: number;
      received_messages: number;
      sent_invitations: number;
      new_connections: number;
      new_replies: number;
      connect_rate_pct: number;
      reply_rate_pct: number;
    }>();

    for (const r of rows) {
      const campaignId = Number(r.campaign_instance_id);
      if (!Number.isFinite(campaignId)) continue;
      const sent = toNum(r.sent_messages);
      const received = toNum(r.received_messages);
      const invitations = toNum(r.sent_invitations);
      const connected = toNum(r.new_connections);
      const replied = toNum(r.new_replies);

      total.sent_messages += sent;
      total.received_messages += received;
      total.sent_invitations += invitations;
      total.new_connections += connected;
      total.new_replies += replied;

      if (!perCampaign.has(campaignId)) {
        perCampaign.set(campaignId, {
          campaign_id: campaignId,
          campaign_name: r.campaign_name ? String(r.campaign_name) : null,
          sent_messages: 0,
          received_messages: 0,
          sent_invitations: 0,
          new_connections: 0,
          new_replies: 0,
          connect_rate_pct: 0,
          reply_rate_pct: 0
        });
      }
      const slot = perCampaign.get(campaignId)!;
      slot.sent_messages += sent;
      slot.received_messages += received;
      slot.sent_invitations += invitations;
      slot.new_connections += connected;
      slot.new_replies += replied;

      const rawName = String(r.campaign_name ?? "").trim();
      const campaignName = rawName || `Campaign #${campaignId}`;
      const nameKey = campaignName.toLowerCase();
      if (!perCampaignName.has(nameKey)) {
        perCampaignName.set(nameKey, {
          campaign_name: campaignName,
          campaign_ids: [],
          sent_messages: 0,
          received_messages: 0,
          sent_invitations: 0,
          new_connections: 0,
          new_replies: 0,
          connect_rate_pct: 0,
          reply_rate_pct: 0
        });
      }
      const byName = perCampaignName.get(nameKey)!;
      if (!byName.campaign_ids.includes(campaignId)) byName.campaign_ids.push(campaignId);
      byName.sent_messages += sent;
      byName.received_messages += received;
      byName.sent_invitations += invitations;
      byName.new_connections += connected;
      byName.new_replies += replied;
    }

    const perCampaignInstanceList = Array.from(perCampaign.values())
      .map((x) => ({
        ...x,
        connect_rate_pct: pct(x.new_connections, x.sent_invitations),
        reply_rate_pct: pct(x.new_replies, x.sent_messages)
      }))
      .sort((a, b) => b.sent_messages - a.sent_messages);
    const perCampaignNameList = Array.from(perCampaignName.values())
      .map((x) => ({
        ...x,
        campaign_ids: x.campaign_ids.sort((a, b) => a - b),
        connect_rate_pct: pct(x.new_connections, x.sent_invitations),
        reply_rate_pct: pct(x.new_replies, x.sent_messages)
      }))
      .sort((a, b) => b.sent_messages - a.sent_messages);

    return NextResponse.json({
      ok: true,
      since,
      until,
      campaign_ids: campaignIds,
      source: "expandi_campaign_daily_v",
      grouping: { by_name: groupByName },
      total: {
        ...total,
        connect_rate_pct: pct(total.new_connections, total.sent_invitations),
        reply_rate_pct: pct(total.new_replies, total.sent_messages)
      },
      per_campaign: groupByName ? perCampaignNameList : perCampaignInstanceList,
      per_campaign_name: perCampaignNameList,
      per_campaign_instance: perCampaignInstanceList
    });
  } catch (e: unknown) {
    const msg = String((e as { message?: string } | null)?.message || e);
    if (looksLikeMissingRelation(msg)) {
      return NextResponse.json({
        ok: true,
        disabled: true,
        reason: msg,
        since: "",
        until: "",
        total: { sent_messages: 0, received_messages: 0, sent_invitations: 0, new_connections: 0, new_replies: 0, connect_rate_pct: 0, reply_rate_pct: 0 },
        per_campaign: []
      });
    }
    return jsonError(500, msg);
  }
}
