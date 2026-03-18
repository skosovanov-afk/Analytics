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

async function postgrestGetPaged(authHeader: string, pathBase: string, maxRows: number) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const out: any[] = [];
  const limit = Math.max(1, Math.min(5000, Number(maxRows || 5000)));
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

function looksLikeMissingRelation(msg: string) {
  const t = String(msg || "").toLowerCase();
  return t.includes("could not find the table") || t.includes("schema cache") || (t.includes("relation") && t.includes("does not exist"));
}

function looksLikeMissingColumn(msg: string) {
  const t = String(msg || "").toLowerCase();
  return t.includes("column") && t.includes("does not exist");
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");
    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json().catch(() => ({}))) as {
      since: string;
      until: string;
      campaign_ids?: Array<string | number>;
      only_pushed?: boolean;
      limit?: number;
    };
    const since = String(payload?.since ?? "").trim();
    const until = String(payload?.until ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(since) || !/^\d{4}-\d{2}-\d{2}$/.test(until)) {
      return jsonError(400, "since/until must be YYYY-MM-DD");
    }
    const limit = Math.max(1, Math.min(5000, Number(payload?.limit ?? 5000)));
    const campaignIds = Array.isArray(payload?.campaign_ids)
      ? payload.campaign_ids.map((x) => Number(x)).filter((n) => Number.isFinite(n))
      : [];
    const campaignFilter = campaignIds.length ? `&smartlead_campaign_id=in.(${campaignIds.map((n) => encodeURIComponent(String(n))).join(",")})` : "";
    const pushedFilter = payload?.only_pushed ? `&hubspot_engagement_id=not.is.null` : "";

    const sinceIso = `${since}T00:00:00.000Z`;
    const untilExcl = ymdAddDaysLocal(until, 1);
    const untilIso = `${untilExcl}T00:00:00.000Z`;
    const where =
      `sales_smartlead_events?select=id,smartlead_event_id,smartlead_campaign_id,smartlead_lead_map_id,contact_email,event_type,occurred_at,hubspot_contact_id,hubspot_engagement_id,updated_at,payload` +
      `&occurred_at=gte.${encodeURIComponent(sinceIso)}` +
      `&occurred_at=lt.${encodeURIComponent(untilIso)}` +
      campaignFilter +
      pushedFilter +
      `&order=occurred_at.asc`;

    try {
      const rows = await postgrestGetPaged(authHeader ?? "", where, limit);
      // Hide legacy synthetic `sent` events created from lead fields; they can inflate counts and confuse users.
      const filtered = (Array.isArray(rows) ? rows : []).filter((r) => {
        const t = String(r?.event_type ?? "").trim().toLowerCase();
        if (t !== "sent") return true;
        const eid = String(r?.smartlead_event_id ?? "");
        return !(eid.includes("lead_fields") || eid.includes("lead_status"));
      });
      return NextResponse.json({ ok: true, since, until, campaign_ids: campaignIds, only_pushed: !!payload?.only_pushed, rows: filtered });
    } catch (e: any) {
      const msg = String(e?.message || e);
      if (looksLikeMissingRelation(msg) || looksLikeMissingColumn(msg)) {
        return NextResponse.json({ ok: true, disabled: true, reason: msg, since, until, campaign_ids: campaignIds, rows: [] });
      }
      throw e;
    }
  } catch (e: any) {
    const msg = String(e?.message || e);
    // Extra safety: some PostgREST errors bubble up with different wrappers; don't spam 500 for schema mismatches.
    if (looksLikeMissingRelation(msg) || looksLikeMissingColumn(msg)) {
      return NextResponse.json({ ok: true, disabled: true, reason: msg, since: "", until: "", campaign_ids: [], rows: [] });
    }
    return jsonError(500, msg);
  }
}


