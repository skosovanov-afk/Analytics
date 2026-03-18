import { NextResponse } from "next/server";

type SupabaseUserResponse = {
  email?: string | null;
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
    headers: {
      apikey: supabaseAnonKey,
      Authorization: authHeader
    }
  });
  if (!res.ok) return null;
  const data = (await res.json()) as SupabaseUserResponse;
  return data;
}

async function hubspotSearchDealsByHypothesisId(hypothesisId: string, limit: number) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");
  void token;
  void hypothesisId;
  void limit;
  // Backwards-compat route: previously filtered deals by a deal property "Hypothesis ID".
  // The new model is TAL-first (hypothesis is linked to a HubSpot TAL / list),
  // so deals are derived from TAL membership via company->deal associations.
  throw new Error("Deprecated endpoint. Use /api/hubspot/deals/by-tal (TAL-based integration).");
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");

    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) {
      return jsonError(403, "Forbidden");
    }

    const payload = (await req.json()) as { hypothesisId?: string; limit?: number };
    const hypothesisId = String(payload?.hypothesisId ?? "").trim();
    if (!hypothesisId) return jsonError(400, "Missing hypothesisId");

    const deals = await hubspotSearchDealsByHypothesisId(hypothesisId, Number(payload?.limit ?? 50));
    return NextResponse.json({ ok: true, deals });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


