import { NextResponse } from "next/server";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function rid() {
  return crypto.randomUUID();
}

function trunc(s: any, n: number) {
  const t = String(s ?? "");
  if (t.length <= n) return t;
  return `${t.slice(0, n)}…`;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}

function isCronAuthorized(req: Request) {
  const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
  if (vercelCron === "1" || vercelCron.toLowerCase() === "true") return true;
  const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
  if (!cronSecret) return false;
  const authHeader = String(req.headers.get("authorization") ?? "").trim();
  const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
  const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
  return bearer === cronSecret || gotSecret === cronSecret;
}

async function callLocal(req: Request, path: string, init: RequestInit) {
  const url = new URL(path, req.url);
  const res = await fetch(url, init);
  const text = await res.text();
  let json: any = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }
  return { ok: res.ok && !!json?.ok, status: res.status, json };
}

async function callLocalWithTimeout(req: Request, path: string, init: RequestInit, timeoutMs: number) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort("timeout"), Math.max(1, timeoutMs));
  try {
    const out = await callLocal(req, path, { ...init, signal: controller.signal });
    clearTimeout(t);
    return out;
  } catch (e: any) {
    clearTimeout(t);
    const msg = String(e?.message || e);
    const isTimeout = msg.toLowerCase().includes("timeout") || e?.name === "AbortError";
    return { ok: false, status: 504, json: { ok: false, error: isTimeout ? "timeout" : msg } };
  }
}

async function callLocalWithRetry(
  req: Request,
  path: string,
  init: RequestInit,
  opts: { timeoutMs: number; maxAttempts: number; retryDelayMs: number }
) {
  const attempts = Math.max(1, Math.min(5, Number(opts.maxAttempts || 1)));
  const baseDelay = Math.max(200, Number(opts.retryDelayMs || 1000));
  let last: any = null;
  for (let i = 0; i < attempts; i++) {
    const out = await callLocalWithTimeout(req, path, init, opts.timeoutMs);
    last = out;
    if (out?.ok) return out;
    if (Number(out?.status) !== 429 && Number(out?.status) !== 504) return out;
    if (i < attempts - 1) await sleep(baseDelay * (i + 1));
  }
  return last ?? { ok: false, status: 500, json: { ok: false, error: "retry_failed" } };
}

async function runAll(req: Request, authHeader: string, isCron: boolean, requestId: string) {
  const bodySmartlead = JSON.stringify({ max_deals: 120, max_completed_leads: 200, retry_failed: false });

  const headersJson: Record<string, string> = { "Content-Type": "application/json" };
  if (authHeader) headersJson.Authorization = authHeader;
  const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
  if (vercelCron === "1" || vercelCron.toLowerCase() === "true") headersJson["x-vercel-cron"] = "1";

  const steps: any[] = [];
  const hasEnv = (name: string) => !!String(process.env[name] ?? "").trim();

  console.log(JSON.stringify({ at: new Date().toISOString(), requestId, scope: "sync_all", msg: "start", isCron }));

  async function safeStep(name: string, fn: () => Promise<any>) {
    const started = Date.now();
    try {
      const out = await fn();
      const dur_ms = Date.now() - started;
      const ok = !!out?.ok;
      if (!ok) {
        console.error(JSON.stringify({ at: new Date().toISOString(), requestId, scope: "sync_all", step: name, msg: "failed", dur_ms, status: out?.status, error: out?.json?.error ?? null }));
      } else {
        console.log(JSON.stringify({ at: new Date().toISOString(), requestId, scope: "sync_all", step: name, msg: "ok", dur_ms }));
      }
      return { name, ...out, dur_ms };
    } catch (e: any) {
      const dur_ms = Date.now() - started;
      const err = String(e?.message || e);
      console.error(JSON.stringify({ at: new Date().toISOString(), requestId, scope: "sync_all", step: name, msg: "exception", dur_ms, error: err }));
      return { name, ok: false, status: 500, json: { ok: false, error: err }, dur_ms };
    }
  }

  // Smartlead sync
  const smartleadEnabled = hasEnv("SMARTLEAD_API_KEY");
  if (!smartleadEnabled) {
    steps.push({ name: "smartlead_sync", ok: true, status: 200, json: { ok: true, skipped: true, reason: "SMARTLEAD_API_KEY not configured" } });
  } else {
    steps.push(
      await safeStep("smartlead_sync", () =>
        callLocalWithTimeout(req, "/api/smartlead/sync", { method: "POST", headers: headersJson, body: bodySmartlead }, 12_000)
      )
    );
  }

  // Analytics sync
  steps.push(
    await safeStep("analytics_sync", () =>
      callLocal(req, "/api/analytics/sync", { method: "POST", headers: headersJson, body: "{}" })
    )
  );

  const failed = steps.filter((s) => !s.ok);
  const ok = failed.length === 0;
  console.log(JSON.stringify({ at: new Date().toISOString(), requestId, scope: "sync_all", msg: "done", ok, failed: failed.map((s) => s.name) }));
  return { ok, request_id: requestId, steps, failed: failed.map((s) => ({ name: s.name, status: s.status, error: s.json?.error ?? "failed" })) };
}

export async function GET(req: Request) {
  if (!isCronAuthorized(req)) return jsonError(401, "Not authorized");
  const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
  const auth = cronSecret ? `Bearer ${cronSecret}` : "";
  const requestId = rid();
  const result = await runAll(req, auth, true, requestId);
  return NextResponse.json(result, { status: result.ok ? 200 : 500 });
}

export async function POST(req: Request) {
  const authHeader = String(req.headers.get("authorization") ?? "").trim();
  const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
  if (!bearer) return jsonError(401, "Not authorized");

  // Validate the bearer token against Supabase auth
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  if (!supabaseUrl || !supabaseAnonKey) return jsonError(500, "Supabase not configured");
  const userRes = await fetch(`${supabaseUrl}/auth/v1/user`, {
    method: "GET",
    headers: { apikey: supabaseAnonKey, Authorization: authHeader }
  });
  if (!userRes.ok) return jsonError(401, "Not authorized");
  const user = (await userRes.json()) as { email?: string | null };
  if (!user?.email) return jsonError(401, "Not authorized");
  const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "";
  const email = String(user.email || "").toLowerCase();
  if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

  const requestId = rid();
  const result = await runAll(req, authHeader, false, requestId);
  return NextResponse.json(result, { status: result.ok ? 200 : 500 });
}
