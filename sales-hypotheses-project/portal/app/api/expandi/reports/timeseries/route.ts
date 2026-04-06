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

function daysInclusive(since: string, until: string) {
  const s = String(since || "").trim();
  const u = String(until || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s) || !/^\d{4}-\d{2}-\d{2}$/.test(u)) return [];
  const out: string[] = [];
  const cur = new Date(`${s}T00:00:00.000Z`);
  const end = new Date(`${u}T00:00:00.000Z`);
  if (!Number.isFinite(cur.getTime()) || !Number.isFinite(end.getTime())) return [];
  while (cur.getTime() <= end.getTime() && out.length < 4000) {
    out.push(cur.toISOString().slice(0, 10));
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

function pickBucketSizeDays(days: number) {
  if (days <= 45) return 1;
  if (days <= 180) return 7;
  if (days <= 365) return 14;
  return 28;
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
      split_by_campaign_name?: boolean;
    };
    const since = String(payload?.since ?? "").trim();
    const until = String(payload?.until ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(since) || !/^\d{4}-\d{2}-\d{2}$/.test(until)) {
      return jsonError(400, "since/until must be YYYY-MM-DD");
    }

    const fullDays = daysInclusive(since, until);
    const bucketSizeDays = pickBucketSizeDays(fullDays.length);
    const bucketStarts = fullDays.filter((_, i) => i % bucketSizeDays === 0);
    const dayToFullIdx = new Map(fullDays.map((d, i) => [d, i]));
    const bucketIndexByDay = new Map(bucketStarts.map((d, i) => [d, i]));

    const init = () => bucketStarts.map(() => 0);
    const series: Record<string, number[]> = {
      sent_messages: init(),
      received_messages: init(),
      sent_invitations: init(),
      new_connections: init(),
      new_replies: init(),
      connect_rate_pct: init(),
      reply_rate_pct: init()
    };
    const splitByCampaignName = payload?.split_by_campaign_name !== false;
    const seriesByCampaignName = new Map<string, {
      campaign_name: string;
      campaign_ids: number[];
      series: Record<string, number[]>;
    }>();

    const campaignIds = Array.isArray(payload?.campaign_ids)
      ? payload.campaign_ids.map((x) => Number(x)).filter((n) => Number.isFinite(n))
      : [];

    const untilExcl = ymdAddDaysLocal(until, 1);
    const campaignFilter = campaignIds.length
      ? `&campaign_instance_id=in.(${campaignIds.map((n) => encodeURIComponent(String(n))).join(",")})`
      : "";

    const where =
      `expandi_campaign_daily_v?select=day,campaign_instance_id,campaign_name,sent_messages,received_messages,sent_invitations,new_connections,new_replies` +
      `&day=gte.${encodeURIComponent(since)}` +
      `&day=lt.${encodeURIComponent(untilExcl)}` +
      campaignFilter +
      `&order=day.asc`;

    const rows = await postgrestGetPaged(authHeader ?? "", where, 50000);

    for (const r of rows) {
      const day = String(r.day || "").slice(0, 10);
      if (!day) continue;
      const fullIdx = dayToFullIdx.get(day);
      if (fullIdx == null) continue;
      const bucketStart = fullDays[Math.floor(fullIdx / bucketSizeDays) * bucketSizeDays];
      const bucketIdx = bucketIndexByDay.get(bucketStart);
      if (bucketIdx == null) continue;

      series.sent_messages[bucketIdx] += toNum(r.sent_messages);
      series.received_messages[bucketIdx] += toNum(r.received_messages);
      series.sent_invitations[bucketIdx] += toNum(r.sent_invitations);
      series.new_connections[bucketIdx] += toNum(r.new_connections);
      series.new_replies[bucketIdx] += toNum(r.new_replies);

      if (splitByCampaignName) {
        const campaignId = Number(r.campaign_instance_id);
        const rawName = String(r.campaign_name ?? "").trim();
        const campaignName = rawName || (Number.isFinite(campaignId) ? `Campaign #${campaignId}` : "Unknown campaign");
        const key = campaignName.toLowerCase();
        if (!seriesByCampaignName.has(key)) {
          seriesByCampaignName.set(key, {
            campaign_name: campaignName,
            campaign_ids: [],
            series: {
              sent_messages: init(),
              received_messages: init(),
              sent_invitations: init(),
              new_connections: init(),
              new_replies: init(),
              connect_rate_pct: init(),
              reply_rate_pct: init()
            }
          });
        }
        const slot = seriesByCampaignName.get(key)!;
        if (Number.isFinite(campaignId) && !slot.campaign_ids.includes(campaignId)) {
          slot.campaign_ids.push(campaignId);
        }
        slot.series.sent_messages[bucketIdx] += toNum(r.sent_messages);
        slot.series.received_messages[bucketIdx] += toNum(r.received_messages);
        slot.series.sent_invitations[bucketIdx] += toNum(r.sent_invitations);
        slot.series.new_connections[bucketIdx] += toNum(r.new_connections);
        slot.series.new_replies[bucketIdx] += toNum(r.new_replies);
      }
    }

    for (let i = 0; i < bucketStarts.length; i++) {
      series.connect_rate_pct[i] = pct(series.new_connections[i], series.sent_invitations[i]);
      series.reply_rate_pct[i] = pct(series.new_replies[i], series.sent_messages[i]);
    }
    for (const slot of seriesByCampaignName.values()) {
      for (let i = 0; i < bucketStarts.length; i++) {
        slot.series.connect_rate_pct[i] = pct(slot.series.new_connections[i], slot.series.sent_invitations[i]);
        slot.series.reply_rate_pct[i] = pct(slot.series.new_replies[i], slot.series.sent_messages[i]);
      }
    }

    const seriesByCampaignNameList = Array.from(seriesByCampaignName.values())
      .map((x) => ({ ...x, campaign_ids: x.campaign_ids.sort((a, b) => a - b) }))
      .sort((a, b) => {
        const aSent = a.series.sent_messages.reduce((acc, n) => acc + n, 0);
        const bSent = b.series.sent_messages.reduce((acc, n) => acc + n, 0);
        return bSent - aSent;
      });

    return NextResponse.json({
      ok: true,
      since,
      until,
      campaign_ids: campaignIds,
      source: "expandi_campaign_daily_v",
      days: bucketStarts,
      bucket_size_days: bucketSizeDays,
      series,
      series_by_campaign_name: splitByCampaignName ? seriesByCampaignNameList : []
    });
  } catch (e: unknown) {
    const msg = String((e as { message?: string } | null)?.message || e);
    if (looksLikeMissingRelation(msg)) {
      return NextResponse.json({
        ok: true,
        disabled: true,
        reason: msg,
        days: [],
        bucket_size_days: 1,
        series: { sent_messages: [], received_messages: [], sent_invitations: [], new_connections: [], new_replies: [], connect_rate_pct: [], reply_rate_pct: [] }
      });
    }
    return jsonError(500, msg);
  }
}
