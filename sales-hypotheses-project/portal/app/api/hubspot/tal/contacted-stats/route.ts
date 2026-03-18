import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

/**
 * Chunk an array into fixed-size batches.
 *
 * We use this to avoid very large `in(...)` filters in PostgREST queries.
 */
function chunk<T>(xs: T[], size: number): T[][] {
  const out: T[][] = [];
  const n = Math.max(1, Math.floor(size));
  for (let i = 0; i < xs.length; i += n) out.push(xs.slice(i, i + n));
  return out;
}

function parseHubspotListIdFromUrl(url: string) {
  const t = String(url ?? "").trim();
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  return m?.[1] ? m[1] : null;
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
  return (await res.json()) as { id?: string | null; email?: string | null };
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    const email = String(user?.email ?? "").toLowerCase();
    if (!authHeader || !user?.id || !email) return jsonError(401, "Not authorized");

    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json().catch(() => ({}))) as { hubspot_tal_url?: string; days?: number };
    const talUrl = String(payload?.hubspot_tal_url ?? "").trim();
    const listId = parseHubspotListIdFromUrl(talUrl);
    if (!listId) return jsonError(400, "Missing/invalid hubspot_tal_url");

    const days = Math.max(1, Math.min(365, Number(payload?.days ?? 90)));
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
    if (!supabaseUrl || !serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });

    /**
     * Coverage stats (simple + deterministic):
     * - totals = exact TAL membership counts (companies/contacts)
     * - contacted = TAL members that have `last_touch_at >= since`
     *
     * Why:
     * - We already keep TAL membership synced in Supabase.
     * - We sync `last_touch_at` for each object via `/api/hubspot/tal/touch-sync`.
     * - This matches HubSpot's "Last activity date" semantics and avoids complex heuristics.
     */
    let totalCompanies = 0;
    let totalContacts = 0;
    let needsFallbackTotals = false;
    const exactCountsRes = await supabaseAdmin.rpc("sales_hubspot_tal_exact_counts", { p_tal_list_id: listId });
    if (exactCountsRes.error) {
      // Fallback when RPC isn't available or schema cache is stale (avoid 500s).
      needsFallbackTotals = true;
    } else {
      const exact = Array.isArray(exactCountsRes.data) ? exactCountsRes.data[0] : exactCountsRes.data;
      totalCompanies = Number(exact?.companies_count ?? 0) || 0;
      totalContacts = Number(exact?.contacts_count ?? 0) || 0;
    }

    // Load TAL company ids (used to scope touch queries without requiring DB foreign keys).
    const talCompanyIds: number[] = [];
    const pageSize = 1000;
    for (let offset = 0; ; offset += pageSize) {
      const res = await supabaseAdmin
        .from("sales_hubspot_tal_companies")
        .select("company_id")
        .eq("tal_list_id", listId)
        .order("company_id", { ascending: true })
        .range(offset, offset + pageSize - 1);
      if (res.error) throw res.error;
      const rows = Array.isArray(res.data) ? res.data : [];
      if (!rows.length) break;
      for (const r of rows) {
        const id = Number((r as any)?.company_id ?? 0);
        if (Number.isFinite(id) && id > 0) talCompanyIds.push(id);
      }
      if (rows.length < pageSize) break;
    }

    // Load TAL contact ids (prefer explicit contacts list if present).
    const talContactIds: number[] = [];
    const talContactsCountRes = await supabaseAdmin
      .from("sales_hubspot_tal_contacts")
      .select("contact_id", { count: "exact", head: true })
      .eq("tal_list_id", listId);
    if (talContactsCountRes.error) throw talContactsCountRes.error;
    const hasExplicitContactList = (Number(talContactsCountRes.count ?? 0) || 0) > 0;

    if (hasExplicitContactList) {
      for (let offset = 0; ; offset += pageSize) {
        const res = await supabaseAdmin
          .from("sales_hubspot_tal_contacts")
          .select("contact_id")
          .eq("tal_list_id", listId)
          .order("contact_id", { ascending: true })
          .range(offset, offset + pageSize - 1);
        if (res.error) throw res.error;
        const rows = Array.isArray(res.data) ? res.data : [];
        if (!rows.length) break;
        for (const r of rows) {
          const id = Number((r as any)?.contact_id ?? 0);
          if (Number.isFinite(id) && id > 0) talContactIds.push(id);
        }
        if (rows.length < pageSize) break;
      }
    } else {
      // Fallback: derive contacts from companies in the TAL (best-effort, matches `sales_hubspot_tal_exact_counts`).
      const uniq = new Set<number>();
      for (const companyChunk of chunk(talCompanyIds, 500)) {
        const res = await supabaseAdmin
          .from("sales_hubspot_company_contacts")
          .select("contact_id")
          .in("company_id", companyChunk);
        if (res.error) throw res.error;
        const rows = Array.isArray(res.data) ? res.data : [];
        for (const r of rows) {
          const id = Number((r as any)?.contact_id ?? 0);
          if (Number.isFinite(id) && id > 0) uniq.add(id);
        }
      }
      talContactIds.push(...Array.from(uniq));
    }
    if (needsFallbackTotals) {
      // Keep totals consistent even if the RPC is unavailable.
      totalCompanies = talCompanyIds.length;
      totalContacts = talContactIds.length;
    }

    /**
     * Count companies that have at least one TAL contact.
     *
     * Why: lets us answer "companies without contacts" for TAL coverage.
     */
    const talCompanyIdSet = new Set(talCompanyIds);
    const companiesWithContacts = new Set<number>();
    for (const contactChunk of chunk(talContactIds, 500)) {
      const res = await supabaseAdmin
        .from("sales_hubspot_company_contacts")
        .select("company_id")
        .in("contact_id", contactChunk);
      if (res.error) throw res.error;
      const rows = Array.isArray(res.data) ? res.data : [];
      for (const r of rows) {
        const id = Number((r as any)?.company_id ?? 0);
        if (Number.isFinite(id) && id > 0 && talCompanyIdSet.has(id)) companiesWithContacts.add(id);
      }
    }
    const companiesWithContactsCount = companiesWithContacts.size;
    const companiesWithoutContactsCount = Math.max(0, totalCompanies - companiesWithContactsCount);

    // Count touched companies by intersecting TAL ids with `company_touches(last_touch_at >= since)`
    // PLUS companies present in our unified fact table.
    let contactedCompanies = 0;
    for (const companyChunk of chunk(talCompanyIds, 500)) {
      // 1. Check native HubSpot touches
      const hsRes = await supabaseAdmin
        .from("sales_hubspot_company_touches")
        .select("company_id")
        .in("company_id", companyChunk)
        .gte("last_touch_at", since);
      if (hsRes.error) throw hsRes.error;
      const hsTouched = new Set((hsRes.data ?? []).map((r: any) => Number(r.company_id)));

      // 2. Check our unified analytics fact table
      const analyticsRes = await supabaseAdmin
        .from("sales_analytics_activities")
        .select("company_id")
        .in("company_id", companyChunk)
        .gte("occurred_at", since);
      if (analyticsRes.error) throw analyticsRes.error;
      const analyticsTouched = new Set((analyticsRes.data ?? []).map((r: any) => Number(r.company_id)));

      // Union of both sets
      const combined = new Set([...hsTouched, ...analyticsTouched]);
      contactedCompanies += combined.size;
    }

    // Count touched contacts by intersecting TAL ids with `contact_touches(last_touch_at >= since)`
    // PLUS contacts present in our unified fact table.
    let contactedContacts = 0;
    for (const contactChunk of chunk(talContactIds, 500)) {
      // 1. Check native HubSpot touches
      const hsRes = await supabaseAdmin
        .from("sales_hubspot_contact_touches")
        .select("contact_id")
        .in("contact_id", contactChunk)
        .gte("last_touch_at", since);
      if (hsRes.error) throw hsRes.error;
      const hsTouched = new Set((hsRes.data ?? []).map((r: any) => Number(r.contact_id)));

      // 2. Check our unified analytics fact table
      const analyticsRes = await supabaseAdmin
        .from("sales_analytics_activities")
        .select("contact_id")
        .in("contact_id", contactChunk)
        .gte("occurred_at", since);
      if (analyticsRes.error) throw analyticsRes.error;
      const analyticsTouched = new Set((analyticsRes.data ?? []).map((r: any) => Number(r.contact_id)));

      // Union of both sets
      const combined = new Set([...hsTouched, ...analyticsTouched]);
      contactedContacts += combined.size;
    }

    // Activity stats (emails/linkedin sent)
    const activityRes = await supabaseAdmin.rpc("sales_hypothesis_activity_stats", { p_tal_list_id: listId, p_since: since });
    let activity = { emails_sent: 0, linkedin_sent: 0, replies: 0 };
    if (!activityRes.error) {
      const r = Array.isArray(activityRes.data) ? activityRes.data[0] : activityRes.data;
      activity = {
        emails_sent: Number(r?.emails_sent_count ?? 0) || 0,
        linkedin_sent: Number(r?.linkedin_sent_count ?? 0) || 0,
        replies: Number(r?.replies_count ?? 0) || 0
      };
    }

    return NextResponse.json({
      ok: true,
      tal_list_id: listId,
      days,
      since,
      totals: { companies: totalCompanies, contacts: totalContacts },
      contacted: { companies: contactedCompanies, contacts: contactedContacts },
      coverage: {
        companies_with_contacts: companiesWithContactsCount,
        companies_without_contacts: companiesWithoutContactsCount
      },
      activity
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


