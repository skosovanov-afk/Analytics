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
  sent_count: number | null;
  open_count: number | null;
  reply_count: number | null;
};

function num(v: unknown) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");
    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json()) as { since: string; until: string; campaign_ids?: Array<string | number> };
    const since = String(payload?.since ?? "").trim();
    const until = String(payload?.until ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(since) || !/^\d{4}-\d{2}-\d{2}$/.test(until)) {
      return jsonError(400, "since/until must be YYYY-MM-DD");
    }

    const campaignIds = Array.isArray(payload?.campaign_ids)
      ? payload.campaign_ids.map((x) => Number(x)).filter((n) => Number.isFinite(n) && n > 0)
      : [];

    const fullDays = daysInclusive(since, until);
    const bucketSizeDays = pickBucketSizeDays(fullDays.length);
    const bucketStarts = fullDays.filter((_, i) => i % bucketSizeDays === 0);
    const idx = new Map(bucketStarts.map((d, i) => [d, i]));
    const dayToFullIdx = new Map(fullDays.map((d, i) => [d, i]));
    const init = () => bucketStarts.map(() => 0);
    const series: Record<string, number[]> = {
      emails_sent: init(),
      emails_opened: init(),
      emails_replied: init()
    };

    const h = postgrestHeadersFor(String(authHeader ?? ""), false);
    const campaignFilter = campaignIds.length
      ? `&campaign_id=in.(${campaignIds.map((n) => encodeURIComponent(String(n))).join(",")})`
      : "";

    const rows = (await postgrestGetPaged(
      h,
      "smartlead_stats_daily" +
        "?select=date,campaign_id,sent_count,open_count,reply_count" +
        `&date=gte.${encodeURIComponent(since)}` +
        `&date=lte.${encodeURIComponent(until)}` +
        campaignFilter +
        "&order=date.asc",
      20000
    )) as DailyRow[];

    for (const row of rows) {
      const day = String(row?.date ?? "").slice(0, 10);
      if (!day) continue;
      const fullIdx = dayToFullIdx.get(day);
      if (fullIdx == null) continue;
      const bucketStart = fullDays[Math.floor(fullIdx / bucketSizeDays) * bucketSizeDays];
      const i = idx.get(bucketStart);
      if (i == null) continue;
      series.emails_sent[i] += num(row?.sent_count);
      series.emails_opened[i] += num(row?.open_count);
      series.emails_replied[i] += num(row?.reply_count);
    }

    return NextResponse.json({ ok: true, since, until, campaign_ids: campaignIds, days: bucketStarts, bucket_size_days: bucketSizeDays, series });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}
