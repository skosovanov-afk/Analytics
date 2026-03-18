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

let lastHubspotCallAt = 0;
async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function hubspotFetch(url: string, init?: RequestInit, opts?: { minIntervalMs?: number; maxRetries?: number }) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const minIntervalMs = Math.max(50, Math.min(500, Number(opts?.minIntervalMs ?? 140)));
  const maxRetries = Math.max(0, Math.min(8, Number(opts?.maxRetries ?? 5)));

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const now = Date.now();
    const wait = Math.max(0, lastHubspotCallAt + minIntervalMs - now);
    if (wait) await sleep(wait);
    lastHubspotCallAt = Date.now();

    const res = await fetch(url, {
      ...init,
      headers: {
        Authorization: `Bearer ${token}`,
        ...(init?.headers ?? {})
      }
    });

    if (res.status !== 429) return res;
    const retryAfter = Number(res.headers.get("retry-after") || "");
    const backoff = Number.isFinite(retryAfter) ? retryAfter * 1000 : 1200 + attempt * 800;
    await sleep(Math.min(10_000, backoff));
  }

  throw new Error("HubSpot rate limit: too many requests (429). Try again in ~10 seconds.");
}

export async function GET(req: Request) {
  try {
    const syncSecret = String(process.env.CONTACT_FORM_HUBSPOT_SYNC_SECRET ?? "").trim();
    const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
    const secretOk = !!syncSecret && gotSecret === syncSecret;

    if (!secretOk) {
      const authHeader = req.headers.get("authorization");
      const user = await getSupabaseUserFromAuthHeader(authHeader);
      if (!user?.email) return jsonError(401, "Not authorized");

      const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
      const email = String(user.email || "").toLowerCase();
      if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");
    }

    const res = await hubspotFetch("https://api.hubapi.com/crm/v3/pipelines/deals");
    const json = (await res.json()) as any;
    if (!res.ok) return jsonError(502, String(json?.message || json?.error || "HubSpot API error"));

    const pipelines = (Array.isArray(json?.results) ? json.results : []).map((p: any) => ({
      id: String(p?.id ?? ""),
      label: String(p?.label ?? ""),
      stages: (Array.isArray(p?.stages) ? p.stages : []).map((s: any) => ({
        id: String(s?.id ?? ""),
        label: String(s?.label ?? ""),
        displayOrder: Number(s?.displayOrder ?? 0),
        metadata: s?.metadata ?? null
      }))
    }));

    return NextResponse.json({ ok: true, pipelines });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


