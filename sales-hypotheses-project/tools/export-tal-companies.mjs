// Export HubSpot TAL (Company list) companies and produce an English summary.
//
// Usage:
//   HUBSPOT_PRIVATE_APP_TOKEN=... node 99-applications/sales/tools/export-tal-companies.mjs \
//     --hypothesis-id <uuid> \
//     --out-dir 99-applications/sales/_private_cache
//
// Or:
//   HUBSPOT_PRIVATE_APP_TOKEN=... node 99-applications/sales/tools/export-tal-companies.mjs --tal-url <hubspot list url>
//
// Output:
// - Writes JSON to <out-dir>/tal-<listId>.json (gitignored)
// - Prints an English summary paragraph to stdout (safe to paste into hypothesis Client profile)
//
// Notes:
// - We do NOT print tokens.
// - We fetch a bounded sample of companies for summarization (names/domains/industry/country/employee bucket).

import fs from "node:fs";
import path from "node:path";
import { loadAuth, refreshIfNeeded, getDefaultAuthFile, supabaseHeaders } from "../../calls/tools/supabase-auth.mjs";

const HUBSPOT_TOKEN = String(process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "").trim();
if (!HUBSPOT_TOKEN) {
  console.error("Missing HUBSPOT_PRIVATE_APP_TOKEN (set env var).");
  process.exit(2);
}

function parseArgs(argv) {
  const out = { hypothesisId: null, talUrl: null, outDir: "99-applications/sales/_private_cache", maxCompanies: 500, sample: 80 };
  for (let i = 2; i < argv.length; i++) {
    const a = String(argv[i] ?? "");
    if (a === "--hypothesis-id") out.hypothesisId = String(argv[++i] ?? "").trim() || null;
    else if (a === "--tal-url") out.talUrl = String(argv[++i] ?? "").trim() || null;
    else if (a === "--out-dir") out.outDir = String(argv[++i] ?? "").trim() || out.outDir;
    else if (a === "--max") out.maxCompanies = Math.max(1, Math.min(5000, Number(argv[++i] ?? "500")));
    else if (a === "--sample") out.sample = Math.max(1, Math.min(300, Number(argv[++i] ?? "80")));
  }
  return out;
}

function must(v, msg) {
  if (!v) throw new Error(msg);
  return v;
}

async function hubspotFetch(url, init) {
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${HUBSPOT_TOKEN}`,
      ...(init?.headers || {})
    }
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`HubSpot API error ${res.status}: ${JSON.stringify(json).slice(0, 600)}`);
  }
  return json;
}

function parseHubspotListIdFromUrl(url) {
  const t = String(url ?? "").trim();
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  return m?.[1] ? String(m[1]) : null;
}

async function readHypothesisTalUrl(hypothesisId) {
  const authFile = process.env.SALES_AUTH_FILE || process.env.CALLS_AUTH_FILE || getDefaultAuthFile();
  let auth = loadAuth(authFile);
  auth = await refreshIfNeeded(authFile, auth);

  const base = String(auth.supabase_url ?? "").replace(/\/+$/, "");
  const url = `${base}/rest/v1/sales_hypotheses?id=eq.${encodeURIComponent(hypothesisId)}&select=id,hubspot_tal_url,vertical_name,tal_companies_count_baseline`;
  const res = await fetch(url, { method: "GET", headers: supabaseHeaders(auth) });
  const text = await res.text();
  if (!res.ok) throw new Error(`Supabase read hypothesis failed: ${res.status} ${text}`);
  const rows = text ? JSON.parse(text) : [];
  const r = Array.isArray(rows) ? rows[0] : null;
  const baseline = Number(r?.tal_companies_count_baseline ?? 0) || null;
  return {
    talUrl: String(r?.hubspot_tal_url ?? "").trim(),
    verticalName: String(r?.vertical_name ?? "").trim(),
    totalCompaniesBaseline: baseline && baseline > 0 ? baseline : null
  };
}

async function listCompanyIdsFromList(listId, maxCompanies) {
  const out = [];
  let after = null;
  while (out.length < maxCompanies) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(500, maxCompanies - out.length)));
    if (after) qs.set("after", String(after));
    const url = `https://api.hubapi.com/crm/v3/lists/${encodeURIComponent(listId)}/memberships?${qs.toString()}`;
    const json = await hubspotFetch(url, { method: "GET" });
    const results = Array.isArray(json?.results) ? json.results : [];
    for (const r of results) {
      const id = String(r?.recordId ?? r?.id ?? "").trim();
      if (id) out.push(id);
    }
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after || !results.length) break;
  }
  return out.slice(0, maxCompanies);
}

async function batchReadCompanies(companyIds, sampleN) {
  const ids = companyIds.slice(0, Math.max(1, sampleN));
  const properties = ["name", "domain", "industry", "country", "city", "state", "numberofemployees", "annualrevenue", "hs_object_id"];
  // HubSpot batch read limit: 100 inputs per request.
  const out = [];
  for (let i = 0; i < ids.length; i += 100) {
    const chunk = ids.slice(i, i + 100);
    const body = {
      properties,
      inputs: chunk.map((id) => ({ id }))
    };
    const json = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/companies/batch/read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    const results = Array.isArray(json?.results) ? json.results : [];
    out.push(...results);
  }
  return out;
}

function bucketEmployees(n) {
  const x = Number(n);
  if (!Number.isFinite(x) || x <= 0) return "Unknown";
  if (x < 50) return "1–49";
  if (x < 200) return "50–199";
  if (x < 1000) return "200–999";
  if (x < 5000) return "1k–4.9k";
  return "5k+";
}

function topK(map, k) {
  const xs = Array.from(map.entries());
  xs.sort((a, b) => (b[1] ?? 0) - (a[1] ?? 0));
  return xs.slice(0, k);
}

function makeEnglishSummary({ verticalName, listId, total, fetched, sampleCompanies }) {
  const countries = new Map();
  const industries = new Map();
  const employeeBuckets = new Map();

  for (const c of sampleCompanies) {
    const p = c?.properties ?? {};
    const country = String(p?.country ?? "").trim() || "Unknown";
    const industry = String(p?.industry ?? "").trim() || "Unknown";
    const eb = bucketEmployees(p?.numberofemployees);
    countries.set(country, (countries.get(country) ?? 0) + 1);
    industries.set(industry, (industries.get(industry) ?? 0) + 1);
    employeeBuckets.set(eb, (employeeBuckets.get(eb) ?? 0) + 1);
  }

  const topCountries = topK(countries, 3).map(([k, v]) => `${k} (${v})`).join(", ");
  const topIndustries = topK(industries, 3).map(([k, v]) => `${k} (${v})`).join(", ");
  const topSizes = topK(employeeBuckets, 3).map(([k, v]) => `${k} (${v})`).join(", ");

  const label = verticalName ? `“${verticalName}”` : "this segment";
  const suffix = fetched && fetched !== total ? ` (sampled ${fetched.toLocaleString()} IDs via API)` : "";
  return (
    `Target account list (HubSpot list #${listId}) for ${label} contains ${total.toLocaleString()} companies. ` +
    `Based on a sample of ${sampleCompanies.length} companies, the most common countries are ${topCountries || "Unknown"}, ` +
    `industries are ${topIndustries || "Unknown"}, and employee size buckets are ${topSizes || "Unknown"}. ` +
    `The list was built from AppFigures: Finance category apps with 1,000+ reviews on App Store / Google Play, then mapped to their companies for outreach.${suffix}`
  );
}

async function main() {
  const args = parseArgs(process.argv);
  let talUrl = String(args.talUrl ?? "").trim();
  let verticalName = "";
  let totalCompaniesBaseline = null;
  if (!talUrl) {
    const hypothesisId = must(args.hypothesisId, "Provide --tal-url or --hypothesis-id");
    const h = await readHypothesisTalUrl(hypothesisId);
    talUrl = h.talUrl;
    verticalName = h.verticalName;
    totalCompaniesBaseline = h.totalCompaniesBaseline ?? null;
  }
  if (!talUrl) throw new Error("Missing HubSpot TAL URL (hubspot_tal_url)");
  const listId = must(parseHubspotListIdFromUrl(talUrl), "Could not parse list id from TAL URL");

  const companyIds = await listCompanyIdsFromList(listId, args.maxCompanies);
  const sampleCompanies = await batchReadCompanies(companyIds, Math.min(args.sample, companyIds.length || args.sample));

  const outDirAbs = path.resolve(process.cwd(), args.outDir);
  fs.mkdirSync(outDirAbs, { recursive: true });
  const outPath = path.join(outDirAbs, `tal-${listId}.json`);

  const out = {
    tal_url: talUrl,
    list_id: listId,
    total_companies: totalCompaniesBaseline ?? companyIds.length,
    fetched_company_ids: companyIds.length,
    total_companies_baseline: totalCompaniesBaseline,
    sample_size: sampleCompanies.length,
    sample_companies: sampleCompanies.map((c) => ({
      id: String(c?.id ?? ""),
      name: String(c?.properties?.name ?? ""),
      domain: String(c?.properties?.domain ?? ""),
      industry: String(c?.properties?.industry ?? ""),
      country: String(c?.properties?.country ?? ""),
      city: String(c?.properties?.city ?? ""),
      numberofemployees: c?.properties?.numberofemployees ?? null,
      annualrevenue: c?.properties?.annualrevenue ?? null
    }))
  };

  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));

  const total = totalCompaniesBaseline ?? companyIds.length;
  const summary = makeEnglishSummary({ verticalName, listId, total, fetched: companyIds.length, sampleCompanies });
  console.log(summary);
  console.log(JSON.stringify({ ok: true, list_id: listId, total_companies: total, fetched_company_ids: companyIds.length, out_path: outPath }, null, 2));
}

main().catch((e) => {
  console.error(String(e?.message || e));
  process.exit(1);
});

