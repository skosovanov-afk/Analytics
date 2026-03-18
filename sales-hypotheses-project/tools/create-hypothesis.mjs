// Create a Sales hypothesis in Supabase from JSON stdin (for chat-driven creation).
//
// Usage:
//   node 99-applications/sales/tools/create-hypothesis.mjs < payload.json
//
// Input JSON (all fields optional unless noted):
// {
//   "title": "string (required)",
//   "status": "draft|active|paused|won|lost",
//   "priority": 0,
//   "timebox_days": 28,
//   "win_criteria": "string (required)",
//   "kill_criteria": "string (required)",
//   "vertical_name": "string",
//   "vertical_hubspot_url": "string url",
//   "hubspot_deals_view_url": "string url",
//   "hubspot_tal_url": "string url",
//   "opps_in_progress_count": 0,
//   "tal_companies_count_baseline": 0,
//   "contacts_count_baseline": 0,
//   "one_sentence_pitch": "string",
//   "product_description": "string",
//   "channels": ["OutboundEmail","Ads"],
//   "role_ids": ["uuid", "..."],
//   "company_profile_ids": ["uuid", "..."],
//   "metric_ids": ["uuid", "..."]
// }
//
// Auth:
// - Uses 02-calls/_private_cache/auth.json by default (same Supabase project) or SALES_AUTH_FILE/CALLS_AUTH_FILE.

import fs from "node:fs";
import { loadAuth, refreshIfNeeded, supabaseHeaders, getDefaultAuthFile } from "../../calls/tools/supabase-auth.mjs";

function readStdin() {
  const buf = fs.readFileSync(0);
  const text = String(buf ?? "").trim();
  if (!text) throw new Error("Empty stdin. Provide JSON payload.");
  return JSON.parse(text);
}

async function restInsert(auth, table, row, select = "id") {
  const res = await fetch(`${auth.supabase_url}/rest/v1/${table}?select=${encodeURIComponent(select)}`, {
    method: "POST",
    headers: {
      ...supabaseHeaders(auth),
      Prefer: "return=representation"
    },
    body: JSON.stringify(row)
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`INSERT ${table} failed: ${res.status} ${text}`);
  const data = text ? JSON.parse(text) : null;
  if (!Array.isArray(data) || !data[0]) throw new Error(`INSERT ${table} returned empty payload`);
  return data[0];
}

async function restBulkInsert(auth, table, rows) {
  if (!rows.length) return { ok: true, inserted: 0 };
  const res = await fetch(`${auth.supabase_url}/rest/v1/${table}`, {
    method: "POST",
    headers: {
      ...supabaseHeaders(auth),
      Prefer: "return=minimal"
    },
    body: JSON.stringify(rows)
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`INSERT ${table} failed: ${res.status} ${text}`);
  return { ok: true, inserted: rows.length };
}

function normalizeUrl(v) {
  const t = String(v ?? "").trim();
  if (!t) return null;
  if (t === "-") return null;
  return t;
}

function asArray(xs) {
  return (Array.isArray(xs) ? xs : []).map((x) => String(x ?? "").trim()).filter(Boolean);
}

async function main() {
  const input = readStdin();

  const title = String(input.title ?? "").trim();
  const win = String(input.win_criteria ?? "").trim();
  const kill = String(input.kill_criteria ?? "").trim();
  if (!title) throw new Error("title is required");
  if (!win) throw new Error("win_criteria is required");
  if (!kill) throw new Error("kill_criteria is required");

  const channels = asArray(input.channels);

  const authFile = process.env.SALES_AUTH_FILE || process.env.CALLS_AUTH_FILE || getDefaultAuthFile();
  let auth = loadAuth(authFile);
  auth = await refreshIfNeeded(authFile, auth);

  const hypothesisRow = {
    title,
    status: String(input.status ?? "draft"),
    priority: Number(input.priority ?? 0) || 0,
    timebox_days: Number(input.timebox_days ?? 28) || 28,
    win_criteria: win,
    kill_criteria: kill,
    vertical_name: String(input.vertical_name ?? "").trim() || null,
    vertical_hubspot_url: normalizeUrl(input.vertical_hubspot_url),
    hubspot_deals_view_url: normalizeUrl(input.hubspot_deals_view_url),
    hubspot_tal_url: normalizeUrl(input.hubspot_tal_url),
    opps_in_progress_count: Number(input.opps_in_progress_count ?? 0) || 0,
    tal_companies_count_baseline: input.tal_companies_count_baseline == null ? null : Number(input.tal_companies_count_baseline),
    contacts_count_baseline: input.contacts_count_baseline == null ? null : Number(input.contacts_count_baseline),
    one_sentence_pitch: String(input.one_sentence_pitch ?? "").trim() || null,
    product_description: String(input.product_description ?? "").trim() || null,
    cjm_json: { channels }
  };

  const created = await restInsert(auth, "sales_hypotheses", hypothesisRow, "id,title");
  const hypothesisId = String(created.id);

  const roleIds = asArray(input.role_ids);
  const companyIds = asArray(input.company_profile_ids);
  const metricIds = asArray(input.metric_ids);

  await restBulkInsert(auth, "sales_hypothesis_roles", roleIds.map((rid) => ({ hypothesis_id: hypothesisId, role_id: rid })));
  await restBulkInsert(
    auth,
    "sales_hypothesis_company_profiles",
    companyIds.map((cid) => ({ hypothesis_id: hypothesisId, company_profile_id: cid }))
  );
  await restBulkInsert(auth, "sales_hypothesis_metrics", metricIds.map((mid) => ({ hypothesis_id: hypothesisId, metric_id: mid })));

  console.log(
    JSON.stringify(
      {
        ok: true,
        hypothesis_id: hypothesisId,
        title: created.title ?? title
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e?.stack || String(e));
  process.exit(1);
});


