// Purge demo data from the Sales hypotheses system (global).
//
// What it deletes:
// - Hypotheses with title starting with "[DEMO]" (cascades to related hypothesis tables).
// - Channels with slug like "demo_%".
// - Metrics with slug like "demo_%".
//
// Safety:
// - Requires explicit `--yes`.
// - Uses SUPABASE_SERVICE_ROLE_KEY to bypass RLS (intentionally).
// - Does NOT touch ICP roles/company profiles (those are not demo-prefixed and may be reused).
//
// Usage:
//   node 99-applications/sales/tools/purge-demo.mjs --yes
//
// Env:
// - NEXT_PUBLIC_SUPABASE_URL (or SUPABASE_URL)
// - SUPABASE_SERVICE_ROLE_KEY
//
// Notes:
// - This is destructive for demo data and should be run rarely.
// Avoid printing secrets; do not log keys/tokens.

function parseArgs(argv) {
  const out = { yes: false, dryRun: false };
  for (let i = 2; i < argv.length; i++) {
    const a = String(argv[i] ?? "");
    if (a === "--yes") out.yes = true;
    if (a === "--dry-run") out.dryRun = true;
  }
  return out;
}

function mustEnv(name, fallbackNames = []) {
  const keys = [name, ...fallbackNames];
  for (const k of keys) {
    const v = String(process.env[k] ?? "").trim();
    if (v) return v;
  }
  throw new Error(`Missing env: ${name}`);
}

async function rest({ baseUrl, serviceKey }, { method, path, body, prefer }) {
  const url = `${baseUrl.replace(/\/+$/, "")}/rest/v1/${path.replace(/^\/+/, "")}`;
  const res = await fetch(url, {
    method,
    headers: {
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
      ...(body != null ? { "Content-Type": "application/json" } : {}),
      ...(prefer ? { Prefer: prefer } : {})
    },
    body: body != null ? JSON.stringify(body) : undefined
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${path} failed: ${res.status} ${text}`);
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function encodeInList(xs) {
  const safe = (Array.isArray(xs) ? xs : []).map((x) => String(x).replace(/"/g, ""));
  return `(${safe.map((x) => `"${x}"`).join(",")})`;
}

import { loadAuth, refreshIfNeeded, supabaseHeaders, getDefaultAuthFile } from "../../calls/tools/supabase-auth.mjs";

async function main() {
  const opts = parseArgs(process.argv);
  if (!opts.yes) {
    throw new Error("Refusing to run without --yes (this is destructive). Add --dry-run to preview.");
  }

  // Prefer env, otherwise reuse the existing Calls/Sales auth file (same Supabase project).
  const authFile = process.env.SALES_AUTH_FILE || process.env.CALLS_AUTH_FILE || getDefaultAuthFile();
  let auth = null;
  try {
    auth = loadAuth(authFile);
    auth = await refreshIfNeeded(authFile, auth);
  } catch {
    auth = null;
  }
  const baseUrl = String(process.env.NEXT_PUBLIC_SUPABASE_URL ?? process.env.SUPABASE_URL ?? auth?.supabase_url ?? "").trim();
  if (!baseUrl) throw new Error("Missing Supabase URL. Set NEXT_PUBLIC_SUPABASE_URL or provide a valid CALLS_AUTH_FILE/SALES_AUTH_FILE.");
  const serviceKey = mustEnv("SUPABASE_SERVICE_ROLE_KEY");

  const ctx = { baseUrl, serviceKey };

  // 1) Demo hypotheses (cascade deletes related rows).
  const demoHyps =
    (await rest(ctx, {
      method: "GET",
      path: `sales_hypotheses?select=id,title&title=ilike.${encodeURIComponent("[DEMO]%")}&limit=500`
    })) ?? [];
  const hypIds = (Array.isArray(demoHyps) ? demoHyps : []).map((h) => String(h?.id ?? "")).filter(Boolean);

  // 2) Demo metrics/channels ids.
  const demoMetrics =
    (await rest(ctx, {
      method: "GET",
      path: `sales_metrics?select=id,slug&slug=like.${encodeURIComponent("demo_%")}&limit=2000`
    })) ?? [];
  const metricIds = (Array.isArray(demoMetrics) ? demoMetrics : []).map((m) => String(m?.id ?? "")).filter(Boolean);

  const demoChannels =
    (await rest(ctx, {
      method: "GET",
      path: `sales_channels?select=id,slug&slug=like.${encodeURIComponent("demo_%")}&limit=2000`
    })) ?? [];
  const channelIds = (Array.isArray(demoChannels) ? demoChannels : []).map((c) => String(c?.id ?? "")).filter(Boolean);

  const summary = {
    dry_run: opts.dryRun,
    demo_hypotheses: hypIds.length,
    demo_metrics: metricIds.length,
    demo_channels: channelIds.length
  };

  if (opts.dryRun) {
    console.log(JSON.stringify({ ok: true, preview: summary, sample: { demoHyps: demoHyps.slice(0, 3), demoMetrics: demoMetrics.slice(0, 3), demoChannels: demoChannels.slice(0, 3) } }, null, 2));
    return;
  }

  // Delete hypothesis first (cascades)
  if (hypIds.length) {
    await rest(ctx, { method: "DELETE", path: `sales_hypotheses?id=in.${encodeInList(hypIds)}` });
  }

  // Remove references to demo metrics/channels before deleting library rows.
  if (metricIds.length) {
    await rest(ctx, { method: "DELETE", path: `sales_hypothesis_channel_metrics?metric_id=in.${encodeInList(metricIds)}` });
    await rest(ctx, { method: "DELETE", path: `sales_hypothesis_channel_metric_owners?metric_id=in.${encodeInList(metricIds)}` });
    await rest(ctx, { method: "DELETE", path: `sales_hypothesis_metrics?metric_id=in.${encodeInList(metricIds)}` });
    await rest(ctx, { method: "DELETE", path: `sales_metrics?id=in.${encodeInList(metricIds)}` });
  }

  if (channelIds.length) {
    await rest(ctx, { method: "DELETE", path: `sales_hypothesis_channel_metrics?channel_id=in.${encodeInList(channelIds)}` });
    await rest(ctx, { method: "DELETE", path: `sales_hypothesis_channel_metric_owners?channel_id=in.${encodeInList(channelIds)}` });
    await rest(ctx, { method: "DELETE", path: `sales_hypothesis_channel_owners?channel_id=in.${encodeInList(channelIds)}` });
    await rest(ctx, { method: "DELETE", path: `sales_channels?id=in.${encodeInList(channelIds)}` });
  }

  console.log(JSON.stringify({ ok: true, deleted: summary }, null, 2));
}

main().catch((e) => {
  console.error(String(e?.message || e));
  process.exit(1);
});

