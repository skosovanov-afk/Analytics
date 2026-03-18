import { NextResponse } from "next/server";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

type SupabaseUserResponse = { email?: string | null };

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

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

let lastGetSalesCallAt = 0;

function decodeJwtExpMs(token: string): number | null {
  const t = String(token || "").trim();
  const parts = t.split(".");
  if (parts.length < 2) return null;
  const b64url = parts[1];
  try {
    const b64 = b64url.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((b64url.length + 3) % 4);
    const jsonStr = Buffer.from(b64, "base64").toString("utf8");
    const payload = JSON.parse(jsonStr) as any;
    const exp = Number(payload?.exp ?? NaN);
    if (!Number.isFinite(exp) || exp <= 0) return null;
    return exp * 1000;
  } catch {
    return null;
  }
}

let cachedReportsToken: { token: string; expMs: number | null } | null = null;

async function getsalesLoginForJwt(base: string) {
  const email = String(process.env.GETSALES_LOGIN_EMAIL ?? "").trim();
  const password = String(process.env.GETSALES_LOGIN_PASSWORD ?? "").trim();
  if (!email || !password) return null;
  const url = `${base.replace(/\/+$/, "")}/id/api/users/get-jwt-token`;

  const now = Date.now();
  const wait = Math.max(0, lastGetSalesCallAt + 220 - now);
  if (wait) await sleep(wait);
  lastGetSalesCallAt = Date.now();

  const res = await fetch(url, {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    body: JSON.stringify({ email, password, remember: true })
  });
  const text = await res.text();
  let json: any = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }
  if (!res.ok) return null;
  const token = String(json?.token ?? json?.jwt ?? json?.access_token ?? json?.data?.token ?? "").trim();
  return token || null;
}

async function getReportsToken(base: string) {
  const envToken = String(process.env.GETSALES_REPORTS_TOKEN ?? "").trim();
  if (envToken) return envToken;
  const now = Date.now();
  const skew = 2 * 60 * 1000;
  if (cachedReportsToken?.token) {
    const exp = cachedReportsToken.expMs;
    if (!exp || now + skew < exp) return cachedReportsToken.token;
  }
  const token = await getsalesLoginForJwt(base);
  if (!token) return null;
  cachedReportsToken = { token, expMs: decodeJwtExpMs(token) };
  return token;
}

function isAllowedDiscoverBase(url: string) {
  try {
    const u = new URL(url);
    if (u.protocol !== "https:") return false;
    const h = u.hostname.toLowerCase();
    // Avoid SSRF: allow only known GetSales domains + the branded UI host used by the team.
    if (h === "amazing.getsales.io") return true;
    if (h === "api.getsales.io") return true;
    if (h.endsWith(".getsales.io")) return true;
    if (h === "app.voitechsales.com") return true;
    if (h.endsWith(".voitechsales.com")) return true;
    return false;
  } catch {
    return false;
  }
}

async function getsalesFetch(path: string, init?: RequestInit, opts?: { minIntervalMs?: number; base?: string; authMode?: "api" | "reports" }) {
  const baseRaw = (opts?.base || process.env.GETSALES_BASE_URL || "https://amazing.getsales.io").trim();
  if (!isAllowedDiscoverBase(baseRaw)) throw new Error(`Bad base for discover: ${baseRaw}`);
  const base = baseRaw.replace(/\/+$/, "");

  const authMode = String(opts?.authMode ?? "api").trim(); // api | reports
  let token = "";
  if (authMode === "reports") {
    token = (await getReportsToken(base)) || "";
    if (!token) throw new Error("Missing GetSales Reports auth (GETSALES_REPORTS_TOKEN or login env)");
  } else {
    token = process.env.GETSALES_API_TOKEN ?? process.env.GETSALES_BEARER_TOKEN ?? "";
    if (!token) throw new Error("Missing GETSALES_API_TOKEN");
  }

  const minIntervalMs = Math.max(80, Math.min(800, Number(opts?.minIntervalMs ?? 160)));
  const now = Date.now();
  const wait = Math.max(0, lastGetSalesCallAt + minIntervalMs - now);
  if (wait) await sleep(wait);
  lastGetSalesCallAt = Date.now();

  return fetch(`${base}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(init?.headers ?? {})
    }
  });
}

function safeJson(text: string) {
  try {
    return text ? JSON.parse(text) : null;
  } catch {
    return { raw: text };
  }
}

function summarize(json: any) {
  if (json == null) return { kind: "null", keys: [] as string[] };
  if (Array.isArray(json)) return { kind: "array", keys: [] as string[], array_len: json.length };
  if (typeof json === "object") return { kind: "object", keys: Object.keys(json).slice(0, 30) };
  return { kind: typeof json, keys: [] as string[] };
}

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const baseOverride = String(url.searchParams.get("base") ?? "").trim();
    const base = baseOverride || (process.env.GETSALES_BASE_URL ?? "https://amazing.getsales.io");
    const authMode = String(url.searchParams.get("auth") ?? "api").trim(); // api|reports

    // Require auth for discover (it can probe internal endpoints).
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");
    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const teamId = String(process.env.GETSALES_TEAM_ID ?? "").trim();

    const candidates: Array<{ path: string; method?: "GET" | "POST"; body?: any }> = [
      // Common guesses for “reports / analytics”
      { path: "/reports", method: "GET" },
      { path: "/reports/api", method: "GET" },
      { path: "/reports/api/linkedin", method: "GET" },
      { path: "/reports/api/reports", method: "GET" },
      { path: "/analytics", method: "GET" },
      { path: "/analytics/api", method: "GET" },
      { path: "/analytics/api/linkedin", method: "GET" },
      { path: "/analytics/api/reports", method: "GET" },
      { path: "/stats", method: "GET" },
      { path: "/stats/api", method: "GET" },
      // Internal app APIs (used by the /reports SPA, discovered via bundle inspection)
      { path: "/api/tasks?limit=1&offset=0", method: "GET" },
      { path: "/api/tasks/?limit=1&offset=0", method: "GET" },
      { path: "/api/tasks/count?limit=1", method: "GET" },
      // JS uses POST for metrics endpoints:
      { path: "/api/tasks/metrics", method: "POST", body: { sender_profiles_uuids: [], statuses: ["in_progress"], metrics: ["tasks_count"] } },
      { path: "/api/flows/list?limit=1&offset=0", method: "GET" },
      { path: "/api/flows/metrics", method: "POST", body: { uuids: [] } },
      { path: "/api/flows/all-sender-profiles", method: "POST", body: { uuids: [] } },
      // Likely “connections / invitations”
      { path: "/flows/api/linkedin-connections", method: "GET" },
      { path: "/flows/api/linkedin-connection-requests", method: "GET" },
      { path: "/flows/api/linkedin-invitations", method: "GET" },
      { path: "/flows/api/linkedin-invites", method: "GET" },
      { path: "/flows/api/linkedin-requests", method: "GET" },
      { path: "/flows/api/invitations", method: "GET" },
      { path: "/flows/api/connections", method: "GET" },
      // Workspaces / pipelines
      { path: "/flows/api/task-pipelines", method: "GET" },
      { path: "/flows/api/task-pipeline-stages", method: "GET" }
    ];

    const results: any[] = [];

    for (const c of candidates) {
      const method = c.method || "GET";
      const res = await getsalesFetch(
        c.path,
        {
        method,
        headers: {
          Accept: "application/json",
          ...(teamId ? { "team-id": teamId } : {}),
          "X-Requested-With": "XMLHttpRequest"
        },
        body: method === "POST" ? JSON.stringify(c.body ?? {}) : undefined
        },
        { base, authMode: authMode === "reports" ? "reports" : "api" }
      );
      const text = await res.text();
      const contentType = String(res.headers.get("content-type") ?? "").trim();
      const json = safeJson(text);
      const sample = String(text || "").slice(0, 240);
      const allow = String(res.headers.get("allow") ?? res.headers.get("Allow") ?? "").trim();

      // If method is wrong, OPTIONS often returns the allow-list.
      let optionsAllow = "";
      if (res.status === 405) {
        try {
          const opt = await getsalesFetch(
            c.path,
            { method: "OPTIONS", headers: { Accept: "application/json", "X-Requested-With": "XMLHttpRequest" } },
            { base }
          );
          optionsAllow = String(opt.headers.get("allow") ?? opt.headers.get("Allow") ?? "").trim();
          // read body to avoid keeping sockets open
          await opt.text();
        } catch {
          // ignore
        }
      }

      results.push({
        path: c.path,
        method,
        status: res.status,
        ok: res.ok,
        content_type: contentType,
        allow,
        options_allow: optionsAllow,
        sample,
        summary: summarize(json)
      });
      // avoid hammering
      await sleep(120);
    }

    return NextResponse.json({ ok: true, base, auth: authMode, team_id: teamId || null, results });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


