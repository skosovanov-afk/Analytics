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

/**
 * Fetch GetSales API with basic rate limiting and retries.
 *
 * @param {string} path
 * @param {RequestInit} init
 * @returns {Promise<Response>}
 */
async function getsalesFetch(path: string, init?: RequestInit) {
  const token = process.env.GETSALES_API_TOKEN ?? process.env.GETSALES_BEARER_TOKEN ?? "";
  if (!token) throw new Error("Missing GETSALES_API_TOKEN");
  const base = (process.env.GETSALES_BASE_URL ?? "https://amazing.getsales.io").replace(/\/+$/, "");
  const url = `${base}${path}`;

  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), 30000);
  try {
    return await fetch(url, {
      ...init,
      signal: controller.signal,
      headers: {
        Authorization: `Bearer ${token}`,
        ...(init?.headers ?? {})
      }
    });
  } finally {
    clearTimeout(id);
  }
}

function pickArray(json: any) {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json?.data)) return json.data;
  return [];
}

/**
 * Normalize a GetSales flow (automation) payload into a concise UI shape.
 *
 * @param {any} raw
 */
function normalizeFlow(raw: any) {
  const uuid = String(raw?.uuid ?? "").trim();
  const name = String(raw?.name ?? raw?.title ?? raw?.flow_name ?? "").trim() || "Automation";
  const status = String(raw?.status ?? raw?.state ?? "").trim().toLowerCase() || "unknown";
  const updated_at = String(raw?.updated_at ?? raw?.created_at ?? "").trim();
  return { uuid, name, status, updated_at };
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");
    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json().catch(() => ({}))) as { limit?: number; offset?: number; all?: boolean };
    const limit = Math.max(1, Math.min(200, Number(payload?.limit ?? 200)));
    const offset = Math.max(0, Number(payload?.offset ?? 0));

    const qs = new URLSearchParams();
    qs.set("limit", String(limit));
    qs.set("offset", String(offset));
    qs.set("order_field", "updated_at");
    qs.set("order_type", "desc");

    const res = await getsalesFetch(`/flows/api/flows?${qs.toString()}`, { method: "GET" });
    const json = (await res.json().catch(() => null)) as any;
    if (!res.ok) return jsonError(res.status, String(json?.message || json?.error || "GetSales flows failed"));

    const rows = pickArray(json);
    const flows = rows.map(normalizeFlow).filter((f: any) => f.uuid);
    const total = Number(json?.total ?? flows.length) || flows.length;
    const has_more = Boolean(json?.has_more ?? (offset + flows.length < total));

    return NextResponse.json({
      ok: true,
      flows,
      limit,
      offset,
      total,
      has_more,
      source: "getsales_flows"
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}
