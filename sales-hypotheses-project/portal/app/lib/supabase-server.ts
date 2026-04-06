/**
 * Shared server-side Supabase utilities for API routes.
 * Import from here instead of duplicating in each route.
 */

export type SupabaseUserResponse = { email?: string | null };
export type PostgrestHeaders = { apikey: string; Authorization: string };

export async function getSupabaseUserFromAuthHeader(authHeader: string | null): Promise<SupabaseUserResponse | null> {
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

export function getBearer(req: Request) {
  const authHeader = String(req.headers.get("authorization") ?? "").trim();
  const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
  const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
  return { authHeader, bearer, gotSecret };
}

export function isCronAuthorized(req: Request): boolean {
  const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
  if (vercelCron === "1" || vercelCron.toLowerCase() === "true") return true;
  const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
  if (!cronSecret) return false;
  const { bearer, gotSecret } = getBearer(req);
  return bearer === cronSecret || gotSecret === cronSecret;
}

export function postgrestHeadersFor(authHeader: string, isCron: boolean): PostgrestHeaders {
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
  if (isCron) {
    if (!serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY (required for cron)");
    return { apikey: serviceRoleKey, Authorization: `Bearer ${serviceRoleKey}` };
  }
  if (!supabaseAnonKey) throw new Error("Missing NEXT_PUBLIC_SUPABASE_ANON_KEY");
  if (!authHeader) throw new Error("Missing Authorization");
  return { apikey: supabaseAnonKey, Authorization: authHeader };
}

export async function postgrestJson(
  h: PostgrestHeaders,
  method: string,
  path: string,
  body?: any,
  extraHeaders?: Record<string, string>
) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const url = `${supabaseUrl}/rest/v1/${path}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30_000);
  let res: Response;
  try {
    res = await fetch(url, {
      method,
      headers: {
        ...h,
        ...(body != null ? { "Content-Type": "application/json" } : {}),
        ...(extraHeaders ?? {})
      },
      body: body != null ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }
  const text = await res.text();
  let json: any = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }
  if (!res.ok) throw new Error(String(json?.message || json?.error || text || "Supabase request failed"));
  return json;
}
