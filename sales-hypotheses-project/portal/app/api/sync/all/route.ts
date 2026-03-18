import { NextResponse } from "next/server";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function rid() {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const crypto = require("crypto");
    return typeof crypto?.randomUUID === "function" ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`;
  } catch {
    return `${Date.now()}-${Math.random()}`;
  }
}

function trunc(s: any, n: number) {
  const t = String(s ?? "");
  if (t.length <= n) return t;
  return `${t.slice(0, n)}…`;
}

/**
 * Sleep helper for retry backoffs.
 */
function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}

function getVercelProtectionBypassSecret() {
  // If the project has Vercel Deployment Protection enabled, server-to-server calls to own routes
  // may be blocked with "Authentication Required" HTML unless we use the automation bypass header.
  // Configure a secret in Vercel: Project Settings -> Security -> Deployment Protection -> Bypass for Automation.
  const v =
    String(process.env.VERCEL_PROTECTION_BYPASS_SECRET ?? "").trim() ||
    String(process.env.VERCEL_PROTECTION_BYPASS ?? "").trim() ||
    String(process.env.VERCEL_AUTOMATION_BYPASS_SECRET ?? "").trim();
  return v;
}

function getBearer(req: Request) {
  const authHeader = String(req.headers.get("authorization") ?? "").trim();
  const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
  const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
  return { authHeader, bearer, gotSecret };
}

function isCronAuthorized(req: Request) {
  // Vercel Cron adds `x-vercel-cron: 1` to scheduled requests.
  // We accept it so scheduled jobs can run without injecting secrets into requests.
  const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
  if (vercelCron === "1" || vercelCron.toLowerCase() === "true") return true;
  const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
  if (!cronSecret) return false;
  const { bearer, gotSecret } = getBearer(req);
  return bearer === cronSecret || gotSecret === cronSecret;
}

async function callLocal(req: Request, path: string, init: RequestInit) {
  const url = new URL(path, req.url);
  const bypass = getVercelProtectionBypassSecret();
  const headers: Record<string, string> = {
    ...((init.headers as any) ?? {})
  };
  if (bypass) headers["x-vercel-protection-bypass"] = bypass;
  const res = await fetch(url, { ...init, headers });
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

/**
 * Retry local calls on transient 429/504 errors.
 */
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
    const status = Number(out?.status ?? 0);
    if (out?.ok) return out;
    if (status !== 429 && status !== 504) return out;
    if (i < attempts - 1) {
      await sleep(baseDelay * (i + 1));
    }
  }
  return last ?? { ok: false, status: 500, json: { ok: false, error: "retry_failed" } };
}

async function runAll(req: Request, authHeaderForCalls: string, isCron: boolean, requestId: string) {
  // Keep the work short; each underlying endpoint already does "freshness" checks.
  // Default to ~1 month so the dashboard can show month dynamics after a fresh install.
  const bodyDaily = JSON.stringify({ backfill_days: 35, batch_days: 3, force: false });
  const bodyWeekly = JSON.stringify({ backfill_weeks: 0, batch_weeks: 1, force: false, debug: false });
  // For manual sync, pull enough history to fill month range. Writes are idempotent.
  const since30dIso = new Date(Date.now() - 30 * 86400000).toISOString();
  const bodyGetSales = JSON.stringify({ max: 200, since: since30dIso });
  const bodySmartlead = JSON.stringify({ max_deals: 120, max_completed_leads: 200, retry_failed: false });
  // Keep SmartLead activities sync bounded so /api/sync/all doesn't hang on Vercel.
  // It's safe to run repeatedly (idempotent inserts).
  const bodySmartleadActivities = JSON.stringify({
    max_campaigns: 1000,
    max_leads_per_campaign: 200,
    max_sequence_calls: 400,
    // Prefer true transactional rows from campaign statistics endpoint.
    use_stats_endpoint: true,
    include_sequence_details: false,
    debug: false
  });

  const headersJson: Record<string, string> = { "Content-Type": "application/json" };
  if (authHeaderForCalls) headersJson.Authorization = authHeaderForCalls;
  // If the top-level request was triggered by Vercel Cron, propagate it to sub-calls so they
  // can use SUPABASE_SERVICE_ROLE_KEY without needing an injected secret header.
  const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
  if (vercelCron === "1" || vercelCron.toLowerCase() === "true") headersJson["x-vercel-cron"] = "1";

  const steps: any[] = [];
  const hasEnv = (name: string) => !!String(process.env[name] ?? "").trim();

  console.log(
    JSON.stringify({
      at: new Date().toISOString(),
      requestId,
      scope: "sync_all",
      msg: "start",
      isCron,
      vercel_protection_bypass: !!getVercelProtectionBypassSecret()
    })
  );

  async function safeStep(name: string, fn: () => Promise<any>) {
    const started = Date.now();
    console.log(JSON.stringify({ at: new Date().toISOString(), requestId, scope: "sync_all", step: name, msg: "start" }));
    try {
      const out = await fn();
      const dur_ms = Date.now() - started;
      const ok = !!out?.ok;
      if (!ok) {
        console.error(
          JSON.stringify({
            at: new Date().toISOString(),
            requestId,
            scope: "sync_all",
            step: name,
            msg: "failed",
            dur_ms,
            status: out?.status,
            error: out?.json?.error ?? null,
            raw: out?.json?.raw ? trunc(out.json.raw, 500) : null
          })
        );
      } else {
        console.log(JSON.stringify({ at: new Date().toISOString(), requestId, scope: "sync_all", step: name, msg: "ok", dur_ms, status: out?.status }));
      }
      return { name, ...out, dur_ms };
    } catch (e: any) {
      const dur_ms = Date.now() - started;
      const err = String(e?.message || e);
      console.error(JSON.stringify({ at: new Date().toISOString(), requestId, scope: "sync_all", step: name, msg: "exception", dur_ms, error: err, stack: trunc(e?.stack, 800) }));
      return { name, ok: false, status: 500, json: { ok: false, error: err }, dur_ms };
    }
  }

  // 1) Daily snapshots (new deals + active delta)
  steps.push(
    await safeStep("hubspot_daily_sync", () =>
      callLocal(req, "/api/hubspot/global/daily/sync", { method: "POST", headers: headersJson, body: bodyDaily })
    )
  );

  // 2) Weekly snapshots (advanced dashboard / legacy charts)
  steps.push(
    await safeStep("hubspot_weekly_sync", () =>
      callLocal(req, "/api/hubspot/global/sync", { method: "POST", headers: headersJson, body: bodyWeekly })
    )
  );

  // 3) GetSales -> HubSpot activities (optional in cron)
  const getsalesEnabled = hasEnv("GETSALES_API_TOKEN") || hasEnv("GETSALES_BEARER_TOKEN");
  if (!getsalesEnabled) {
    steps.push({ name: "getsales_sync", ok: true, status: 200, json: { ok: true, skipped: true, reason: "GetSales is not configured (missing GETSALES_API_TOKEN)" } });
  } else if (!isCron || String(process.env.GETSALES_CRON_USER_ID ?? "").trim()) {
    steps.push(
      await safeStep("getsales_sync", () =>
        callLocal(req, "/api/getsales/sync", { method: "POST", headers: headersJson, body: bodyGetSales })
      )
    );
  } else {
    steps.push({ name: "getsales_sync", ok: true, status: 200, json: { ok: true, skipped: true, reason: "Missing GETSALES_CRON_USER_ID" } });
  }

  // 4) HubSpot SQL -> SmartLead enroll + SmartLead completed -> HubSpot notes (optional in cron)
  const smartleadEnabled = hasEnv("SMARTLEAD_API_KEY");
  if (!smartleadEnabled) {
    steps.push({ name: "smartlead_sync", ok: true, status: 200, json: { ok: true, skipped: true, reason: "SmartLead is not configured (missing SMARTLEAD_API_KEY)" } });
  } else if (!isCron || String(process.env.SMARTLEAD_CRON_USER_ID ?? "").trim()) {
    steps.push(
      await safeStep("smartlead_sync", () =>
        callLocalWithTimeout(req, "/api/smartlead/sync", { method: "POST", headers: headersJson, body: bodySmartlead }, 12_000)
      )
    );
    // 5) SmartLead activity events (sent/open/reply) for accurate Email reporting
    // Avoid multiple consecutive runs to reduce 429 rate limits from SmartLead.
    steps.push(
      await safeStep("smartlead_activities_sync", () =>
        callLocalWithRetry(
          req,
          "/api/smartlead/activities/sync",
          { method: "POST", headers: headersJson, body: bodySmartleadActivities },
          { timeoutMs: 20_000, maxAttempts: 3, retryDelayMs: 3000 }
        )
      )
    );
  } else {
    steps.push({ name: "smartlead_sync", ok: true, status: 200, json: { ok: true, skipped: true, reason: "Missing SMARTLEAD_CRON_USER_ID" } });
    steps.push({ name: "smartlead_activities_sync", ok: true, status: 200, json: { ok: true, skipped: true, reason: "Missing SMARTLEAD_CRON_USER_ID" } });
  }

  // 6) Analytics fact sync (roll up all sources into a single table).
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
  // Cron: Vercel adds `x-vercel-cron: 1` (or use Authorization: Bearer <CRON_SECRET>).
  if (!isCronAuthorized(req)) return jsonError(401, "Not authorized");
  const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
  // Keep backward-compat: if CRON_SECRET is configured, forward it; otherwise rely on x-vercel-cron propagation.
  const auth = cronSecret ? `Bearer ${cronSecret}` : "";
  const requestId = rid();
  const result = await runAll(req, auth, true, requestId);
  return NextResponse.json(result, { status: result.ok ? 200 : 500 });
}

export async function POST(req: Request) {
  // Manual: proxy the user's Bearer token to underlying routes.
  const { authHeader, bearer } = getBearer(req);
  if (!bearer) return jsonError(401, "Not authorized");
  const requestId = rid();
  const result = await runAll(req, authHeader, false, requestId);
  return NextResponse.json(result, { status: result.ok ? 200 : 500 });
}


