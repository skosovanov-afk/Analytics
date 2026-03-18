import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
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

    const payload = (await req.json().catch(() => ({}))) as { talUrl?: string; days?: number; limit?: number };
    const talUrl = String(payload?.talUrl ?? "").trim();
    const listId = parseHubspotListIdFromUrl(talUrl);
    if (!listId) return jsonError(400, "Missing TAL list id. Provide talUrl containing /lists/<id> or /objectLists/<id>.");

    const days = Math.max(1, Math.min(365, Number(payload?.days ?? 90)));
    const limit = Math.max(1, Math.min(50, Number(payload?.limit ?? 10)));
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
    if (!supabaseUrl || !serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });

    const contactsRes = await supabaseAdmin.rpc("sales_hubspot_tal_recent_contact_touches", {
      p_tal_list_id: listId,
      p_since: since,
      p_limit: limit
    });
    if (contactsRes.error) throw contactsRes.error;

    const companiesRes = await supabaseAdmin.rpc("sales_hubspot_tal_recent_company_touches", {
      p_tal_list_id: listId,
      p_since: since,
      p_limit: limit
    });
    if (companiesRes.error) throw companiesRes.error;

    return NextResponse.json({
      ok: true,
      tal_list_id: listId,
      days,
      since,
      contacts: Array.isArray(contactsRes.data) ? contactsRes.data : [],
      companies: Array.isArray(companiesRes.data) ? companiesRes.data : []
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


