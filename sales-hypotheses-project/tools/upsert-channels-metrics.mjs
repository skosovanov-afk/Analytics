// Upsert Sales channels + metrics (library) and attach them to a hypothesis.
//
// English-only names (repo rule).
//
// Usage:
//   node 99-applications/sales/tools/upsert-channels-metrics.mjs --hypothesis-id <uuid>
//
// Auth:
// - Uses Calls/Sales auth file (02-calls/_private_cache/auth.json) for Supabase URL + access token.
// - Uses SUPABASE_SERVICE_ROLE_KEY to upsert library objects globally (bypass RLS).
//
// Notes:
// - Channel selection for the hypothesis is stored in sales_hypotheses.cjm_json.channels (array of slugs).
// - Global metrics are linked via sales_hypothesis_metrics.
// - Per-channel metrics are linked via sales_hypothesis_channel_metrics.

import { loadAuth, refreshIfNeeded, getDefaultAuthFile, supabaseHeaders } from "../../calls/tools/supabase-auth.mjs";

function parseArgs(argv) {
  const out = { hypothesisId: null };
  for (let i = 2; i < argv.length; i++) {
    const a = String(argv[i] ?? "");
    if (a === "--hypothesis-id") out.hypothesisId = String(argv[++i] ?? "").trim() || null;
  }
  return out;
}

function must(v, msg) {
  if (!v) throw new Error(msg);
  return v;
}

function env(name, fallbacks = []) {
  for (const k of [name, ...fallbacks]) {
    const v = String(process.env[k] ?? "").trim();
    if (v) return v;
  }
  return "";
}

async function rest(auth, { method, table, qs = "", body, prefer, useServiceRole = false }) {
  const base = String(auth.supabase_url ?? "").replace(/\/+$/, "");
  const url = `${base}/rest/v1/${table}${qs ? `?${qs}` : ""}`;
  const serviceKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
  const headers = useServiceRole
    ? { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` }
    : supabaseHeaders(auth);
  const res = await fetch(url, {
    method,
    headers: {
      ...headers,
      ...(body != null ? { "Content-Type": "application/json" } : {}),
      ...(prefer ? { Prefer: prefer } : {})
    },
    body: body != null ? JSON.stringify(body) : undefined
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${table} failed: ${res.status} ${text}`);
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function upsertOne(auth, table, row, onConflict, select, useServiceRole) {
  const params = new URLSearchParams();
  params.set("on_conflict", onConflict);
  params.set("select", select);
  const data = await rest(auth, {
    method: "POST",
    table,
    qs: params.toString(),
    body: row,
    prefer: "resolution=merge-duplicates,return=representation",
    useServiceRole
  });
  if (!Array.isArray(data) || !data[0]) throw new Error(`Upsert ${table} returned empty`);
  return data[0];
}

async function main() {
  const opts = parseArgs(process.argv);
  const hypothesisId = must(opts.hypothesisId, "Missing --hypothesis-id");

  const authFile = process.env.SALES_AUTH_FILE || process.env.CALLS_AUTH_FILE || getDefaultAuthFile();
  let auth = loadAuth(authFile);
  auth = await refreshIfNeeded(authFile, auth);

  const serviceKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
  must(serviceKey, "Missing env: SUPABASE_SERVICE_ROLE_KEY");

  // 1) Library upserts (service role).
  const channels = [
    { slug: "email_outreach", name: "Email outreach", sort_order: 10 },
    { slug: "linkedin_outreach", name: "LinkedIn outreach", sort_order: 20 },
    { slug: "reddit", name: "Reddit", sort_order: 30 },
    { slug: "x", name: "X", sort_order: 40 },
    { slug: "company_blog", name: "Company blog", sort_order: 50 }
  ];

  const metrics = [
    // Global
    { slug: "activities_total", name: "Total activities", unit: "count", sort_order: 10 },
    { slug: "companies_touched", name: "Companies touched", unit: "count", sort_order: 20 },
    { slug: "contacts_touched", name: "Contacts touched", unit: "count", sort_order: 30 },
    { slug: "contacts_total", name: "Contacts total", unit: "count", sort_order: 40 },
    { slug: "leads_count", name: "Leads", unit: "count", sort_order: 50 },
    { slug: "opportunities_count", name: "Opportunities", unit: "count", sort_order: 60 },
    { slug: "deals_count", name: "Deals", unit: "count", sort_order: 70 },

    // Email outreach
    { slug: "emails_sent", name: "Emails sent", unit: "count", sort_order: 110 },
    { slug: "email_replies", name: "Email replies", unit: "count", sort_order: 120 },

    // LinkedIn outreach
    { slug: "linkedin_connects_sent", name: "LinkedIn invites sent", unit: "count", sort_order: 210 },
    { slug: "linkedin_invites_accepted", name: "LinkedIn invites accepted", unit: "count", sort_order: 220 },

    // Content/social posting (shared)
    { slug: "posts_published", name: "Posts published", unit: "count", sort_order: 310 },
    { slug: "post_views_total", name: "Post views (total)", unit: "views", sort_order: 320 }
  ];

  const upsertedChannels = [];
  for (const c of channels) {
    upsertedChannels.push(
      await upsertOne(
        auth,
        "sales_channels",
        { slug: c.slug, name: c.name, sort_order: c.sort_order, is_active: true },
        "slug",
        "id,slug,name",
        true
      )
    );
  }
  const channelBySlug = new Map(upsertedChannels.map((c) => [String(c.slug), c]));

  const upsertedMetrics = [];
  for (const m of metrics) {
    upsertedMetrics.push(
      await upsertOne(
        auth,
        "sales_metrics",
        { slug: m.slug, name: m.name, input_type: "number", unit: m.unit, sort_order: m.sort_order, is_active: true },
        "slug",
        "id,slug,name,unit,input_type",
        true
      )
    );
  }
  const metricBySlug = new Map(upsertedMetrics.map((m) => [String(m.slug), m]));

  // 2) Attach to hypothesis (user token).
  const hypRows = await rest(auth, { method: "GET", table: "sales_hypotheses", qs: `id=eq.${encodeURIComponent(hypothesisId)}&select=id,cjm_json` });
  const hyp = Array.isArray(hypRows) ? hypRows[0] : null;
  if (!hyp?.id) throw new Error("Hypothesis not found or not accessible");

  const currentChannels = Array.isArray(hyp?.cjm_json?.channels) ? hyp.cjm_json.channels.map((x) => String(x)) : [];
  const nextChannels = Array.from(new Set([...currentChannels, ...channels.map((c) => c.slug)])).filter(Boolean);
  const nextCjm = { ...(hyp?.cjm_json ?? {}), channels: nextChannels };
  await rest(auth, { method: "PATCH", table: "sales_hypotheses", qs: `id=eq.${encodeURIComponent(hypothesisId)}`, body: { cjm_json: nextCjm } });

  // Global metrics: all requested “общие метрики”
  const globalMetricSlugs = ["activities_total", "companies_touched", "contacts_touched", "contacts_total", "leads_count", "opportunities_count", "deals_count"];
  const globalRows = globalMetricSlugs
    .map((slug) => metricBySlug.get(slug))
    .filter(Boolean)
    .map((m) => ({ hypothesis_id: hypothesisId, metric_id: String(m.id) }));
  if (globalRows.length) {
    await rest(auth, { method: "POST", table: "sales_hypothesis_metrics", body: globalRows, prefer: "resolution=merge-duplicates,return=minimal" });
  }

  // Per-channel metrics mapping
  const perChannelMetricSlugs = {
    email_outreach: ["emails_sent", "email_replies", "leads_count", "opportunities_count", "deals_count"],
    linkedin_outreach: ["linkedin_connects_sent", "linkedin_invites_accepted", "leads_count", "opportunities_count", "deals_count"],
    reddit: ["posts_published", "post_views_total", "leads_count", "opportunities_count", "deals_count"],
    x: ["posts_published", "post_views_total", "leads_count", "opportunities_count", "deals_count"],
    company_blog: ["posts_published", "post_views_total", "leads_count", "opportunities_count", "deals_count"]
  };

  const chMetricRows = [];
  for (const [chSlug, slugs] of Object.entries(perChannelMetricSlugs)) {
    const ch = channelBySlug.get(chSlug);
    if (!ch) continue;
    for (const ms of slugs) {
      const m = metricBySlug.get(ms);
      if (!m) continue;
      chMetricRows.push({ hypothesis_id: hypothesisId, channel_id: String(ch.id), metric_id: String(m.id) });
    }
  }
  if (chMetricRows.length) {
    await rest(auth, { method: "POST", table: "sales_hypothesis_channel_metrics", body: chMetricRows, prefer: "resolution=merge-duplicates,return=minimal" });
  }

  console.log(
    JSON.stringify(
      {
        ok: true,
        hypothesis_id: hypothesisId,
        channels_added: channels.map((c) => c.slug),
        global_metrics_added: globalMetricSlugs,
        per_channel_metrics_added: Object.fromEntries(Object.entries(perChannelMetricSlugs).map(([k, v]) => [k, v]))
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(String(e?.message || e));
  process.exit(1);
});

