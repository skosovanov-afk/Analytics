import { NextResponse } from "next/server";

type SupabaseUserResponse = { email?: string | null };

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

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");
    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json()) as { since: string; until: string; campaign_ids?: Array<string | number> };
    const since = String(payload?.since ?? "").trim();
    const until = String(payload?.until ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(since) || !/^\d{4}-\d{2}-\d{2}$/.test(until)) {
      return jsonError(400, "since/until must be YYYY-MM-DD");
    }

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
    /**
     * Track unique contacts per day bucket to align with SmartLead UI counts.
     *
     * SmartLead "Emails Sent" is unique leads contacted, not message count.
     */
    const uniquePerBucket = {
      sent: bucketStarts.map(() => new Set<string>()),
      opened: bucketStarts.map(() => new Set<string>()),
      replied: bucketStarts.map(() => new Set<string>())
    };

    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
    if (!supabaseUrl || !supabaseAnonKey) return jsonError(500, "Missing Supabase env");

    const sinceIso = `${since}T00:00:00.000Z`;
    const untilExcl = ymdAddDaysLocal(until, 1);
    const untilIso = `${untilExcl}T00:00:00.000Z`;
    const campaignIds = Array.isArray(payload?.campaign_ids)
      ? payload.campaign_ids.map((x) => Number(x)).filter((n) => Number.isFinite(n))
      : [];
    const campaignFilter = campaignIds.length ? `&smartlead_campaign_id=in.(${campaignIds.map((n) => encodeURIComponent(String(n))).join(",")})` : "";

    const where =
      `sales_smartlead_events?select=event_type,occurred_at,smartlead_campaign_id,smartlead_event_id,contact_email` +
      `&occurred_at=gte.${encodeURIComponent(sinceIso)}` +
      `&occurred_at=lt.${encodeURIComponent(untilIso)}` +
      campaignFilter +
      `&order=occurred_at.asc`;
    const rows = await postgrestGetPaged(authHeader!, where, 20000);

    for (const r of Array.isArray(rows) ? rows : []) {
      const at = String(r?.occurred_at ?? "").trim();
      const day = at ? at.slice(0, 10) : "";
      if (!day) continue;
      // map to bucket start day
      const fullIdx = dayToFullIdx.get(day);
      if (fullIdx == null) continue;
      const bucketStart = fullDays[Math.floor(fullIdx / bucketSizeDays) * bucketSizeDays];
      const i = idx.get(bucketStart);
      if (i == null) continue;
      const t = String(r?.event_type ?? "").trim().toLowerCase();
      // Ignore legacy synthetic `sent` events that were mistakenly generated from lead fields.
      // They can inflate Sent and distort the chart for historical data.
      const eid = String(r?.smartlead_event_id ?? "");
      if (t === "sent" && (eid.includes("lead_fields") || eid.includes("lead_status"))) continue;
      const email = String(r?.contact_email ?? "").trim().toLowerCase();
      if (!email) continue;
      if (t === "sent") uniquePerBucket.sent[i].add(email);
      else if (t === "opened") uniquePerBucket.opened[i].add(email);
      else if (t === "replied") uniquePerBucket.replied[i].add(email);
    }

    // Convert unique sets to counts for the response payload.
    for (let i = 0; i < bucketStarts.length; i++) {
      series.emails_sent[i] = uniquePerBucket.sent[i].size;
      series.emails_opened[i] = uniquePerBucket.opened[i].size;
      series.emails_replied[i] = uniquePerBucket.replied[i].size;
    }

    return NextResponse.json({ ok: true, since, until, campaign_ids: campaignIds, days: bucketStarts, bucket_size_days: bucketSizeDays, series });
  } catch (e: any) {
    const msg = String(e?.message || e);
    if (looksLikeMissingRelation(msg)) {
      return NextResponse.json({
        ok: true,
        disabled: true,
        reason: msg,
        days: [],
        bucket_size_days: 1,
        series: { emails_sent: [], emails_opened: [], emails_replied: [] }
      });
    }
    return jsonError(500, msg);
  }
}


