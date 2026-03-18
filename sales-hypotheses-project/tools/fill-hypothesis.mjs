// Fill a hypothesis with generated English content (messaging + VP + pains) via Supabase REST.
//
// Usage:
//   node 99-applications/sales/tools/fill-hypothesis.mjs --hypothesis-id <uuid>
//
// Auth:
// - Uses 02-calls/_private_cache/auth.json by default (or SALES_AUTH_FILE/CALLS_AUTH_FILE).
//
// Important:
// - Writes ONLY English content (repo rule).
// - Does not require service role; it uses the user's JWT and relies on RLS (owner/admin).

import fs from "node:fs";
import path from "node:path";
import { loadAuth, refreshIfNeeded, supabaseHeaders, getDefaultAuthFile } from "../../calls/tools/supabase-auth.mjs";

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

async function rest(auth, { method, table, qs = "", body, prefer }) {
  const url = `${String(auth.supabase_url).replace(/\/+$/, "")}/rest/v1/${table}${qs ? `?${qs}` : ""}`;
  const res = await fetch(url, {
    method,
    headers: {
      ...supabaseHeaders(auth),
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

function norm(s) {
  return String(s ?? "").trim();
}

function buildContent({ talSummary }) {
  const oneSentencePitch =
    "Oversecured helps regulated, mobile-first teams continuously find and fix real vulnerabilities in Android and iOS apps with deep SAST and Android DAST, low false positives, and actionable proof where possible.";

  const productDescription =
    [
      "Oversecured is an automated mobile application security testing platform combining SAST (Android + iOS) and DAST (Android).",
      "It is purpose-built for mobile: we use data-flow (taint) analysis to catch issues that pattern-based scanners miss and to reduce false positives.",
      "Teams upload build artifacts (Android APK/AAB — no source code required) or iOS Swift source code, run scans in the background, and get actionable findings with code context, remediation guidance, and (for Android, when applicable) exploitability proof such as PoCs, stack traces, and sometimes screencasts.",
      "Over time, you get version history and diffs (new/fixed/still present) and can integrate uploads from CI/CD via ready-to-use templates or API for continuous coverage without blocking releases by default."
    ].join("\n");

  const clientProfile =
    [
      "Segment: finance category mobile apps with meaningful scale and strong security, privacy, and trust requirements.",
      "",
      talSummary,
      "",
      "Sourcing method:",
      "- We built the longlist in AppFigures by taking all apps in the Finance category and filtering to apps with 1,000+ reviews on App Store / Google Play.",
      "- Then we mapped the apps to the owning companies (for outreach) and used the resulting company list as the HubSpot TAL.",
      "",
      "Typical characteristics (expected):",
      "- High regulatory and audit pressure (e.g., PCI-DSS, PSD2, SOC 2 / ISO 27001, and regional privacy requirements).",
      "- Mobile is a critical channel; releases are frequent and outages or security incidents are expensive.",
      "- Heavy dependency on third‑party SDKs (analytics, payments, fraud, messaging) and complex data flows.",
      "- Security and compliance teams need continuous, provable coverage without blocking delivery."
    ].filter(Boolean).join("\n");

  const painsByRole = {
    "AppSec Lead": {
      vp: "Continuously catch real, mobile-specific vulnerabilities across Android and iOS with low noise, and give engineers actionable proof and fix guidance fast enough for weekly releases.",
      pains: [
        "Manual mobile security testing does not scale to frequent releases and multiple apps/versions.",
        "Generic SAST/DAST tools miss mobile-specific issues and produce too many false positives.",
        "Triage and validation take too long; engineering loses trust in findings.",
        "Hard to track regressions and prove improvements over time."
      ].join("\n"),
      solution: [
        "Automated mobile SAST + Android DAST with data-flow (taint) analysis to reduce false positives and catch real issues.",
        "Actionable reports with code context and remediation guidance; Android validation can include PoCs/stack traces/screencasts when applicable.",
        "CI/CD-friendly workflow: upload builds via API/plugins; scans run asynchronously by default (no release blocking unless you choose to gate via API).",
        "Version history and diffs (new/fixed/still present) to manage vulnerability backlog and prevent regressions."
      ].join("\n")
    },
    "CISO / Head of Security": {
      vp: "Reduce mobile app risk and demonstrate continuous security posture improvement with audit-friendly evidence, without adding headcount or slowing product delivery.",
      pains: [
        "Mobile is a high-risk surface, but coverage is inconsistent and relies on periodic pen tests and manual checklists.",
        "Hard to quantify and report mobile risk reduction to leadership and stakeholders.",
        "Security tooling becomes operationally expensive (headcount + consultants) and still leaves gaps.",
        "Breaches, app store incidents, and customer due diligence create urgent, high-stakes pressure—especially in finance."
      ].join("\n"),
      solution: [
        "Continuous scanning across Android and iOS to close coverage gaps between releases.",
        "Low-noise findings with proof where possible (Android DAST validation) so teams can prioritize real risk.",
        "Trend visibility via scan history and diffs to show progress and support security governance.",
        "Integration into existing SDLC (upload from CI/CD) to reduce operational overhead."
      ].join("\n")
    },
    Compliance: {
      vp: "Generate reliable, repeatable evidence for mobile security controls and audits by turning mobile testing into a continuous, trackable process rather than a manual scramble.",
      pains: [
        "Audit preparation is time-consuming: evidence collection and documentation are manual and fragmented.",
        "Proving continuous compliance for mobile apps is difficult between release cycles.",
        "Security requirements change; it is hard to keep controls mapped and consistently validated.",
        "Limited visibility into third-party SDK risk and configuration issues in mobile apps."
      ].join("\n"),
      solution: [
        "Standardized, repeatable scanning process with consistent outputs that can be referenced during audits.",
        "Historical scan results and diffs to demonstrate continuous control verification over time.",
        "Coverage for mobile-specific issues (configuration, hardcoded secrets, insecure data handling) via purpose-built analysis.",
        "Reduced manual workload by automating detection and surfacing actionable remediation guidance."
      ].join("\n")
    }
  };

  return { oneSentencePitch, productDescription, clientProfile, painsByRole };
}

function parseHubspotListIdFromUrl(url) {
  const t = String(url ?? "").trim();
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  return m?.[1] ? String(m[1]) : null;
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

function makeEnglishTalSummaryFromCache({ verticalName, listId, talJson }) {
  const sample = Array.isArray(talJson?.sample_companies) ? talJson.sample_companies : [];
  // Prefer a known baseline total (from hypothesis) when available.
  // The cache file may contain only a bounded API sample of IDs.
  const total =
    Number(talJson?.total_companies_baseline ?? 0) ||
    Number(talJson?.total_companies ?? 0) ||
    sample.length;

  const countries = new Map();
  const industries = new Map();
  const employeeBuckets = new Map();

  for (const c of sample) {
    const country = String(c?.country ?? "").trim() || "Unknown";
    const industry = String(c?.industry ?? "").trim() || "Unknown";
    const eb = bucketEmployees(c?.numberofemployees);
    countries.set(country, (countries.get(country) ?? 0) + 1);
    industries.set(industry, (industries.get(industry) ?? 0) + 1);
    employeeBuckets.set(eb, (employeeBuckets.get(eb) ?? 0) + 1);
  }

  const topCountries = topK(countries, 3).map(([k, v]) => `${k} (${v})`).join(", ");
  const topIndustries = topK(industries, 3).map(([k, v]) => `${k} (${v})`).join(", ");
  const topSizes = topK(employeeBuckets, 3).map(([k, v]) => `${k} (${v})`).join(", ");

  const label = verticalName ? `“${verticalName}”` : "this segment";
  const sampleN = sample.length;
  return (
    `Target account list (HubSpot list #${listId}) for ${label} contains ${total.toLocaleString()} companies. ` +
    `Based on a sample of ${sampleN} companies, the most common countries are ${topCountries || "Unknown"}, ` +
    `industries are ${topIndustries || "Unknown"}, and employee size buckets are ${topSizes || "Unknown"}. ` +
    `The list was built from AppFigures: Finance category apps with 1,000+ reviews on App Store / Google Play, then mapped to their companies for outreach.`
  );
}

async function main() {
  const { hypothesisId } = parseArgs(process.argv);
  const hid = must(hypothesisId, "Missing --hypothesis-id");

  const authFile = process.env.SALES_AUTH_FILE || process.env.CALLS_AUTH_FILE || getDefaultAuthFile();
  let auth = loadAuth(authFile);
  auth = await refreshIfNeeded(authFile, auth);

  // Read hypothesis (we need vertical + TAL link etc; but we only write a few text fields here).
  const hypRows = await rest(auth, {
    method: "GET",
    table: "sales_hypotheses",
    qs: `id=eq.${encodeURIComponent(hid)}&select=id,vertical_name,hubspot_tal_url,tal_companies_count_baseline`
  });
  const hyp = Array.isArray(hypRows) ? hypRows[0] : null;
  if (!hyp?.id) throw new Error("Hypothesis not found or not accessible");

  // Read roles/companies selected for the hypothesis.
  const roles = await rest(auth, { method: "GET", table: "sales_hypothesis_roles", qs: `hypothesis_id=eq.${encodeURIComponent(hid)}&select=role_id,role:sales_icp_roles(name)` });
  const companies = await rest(auth, { method: "GET", table: "sales_hypothesis_company_profiles", qs: `hypothesis_id=eq.${encodeURIComponent(hid)}&select=company_profile_id,company:sales_icp_company_profiles(vertical_name,sub_vertical,region,size_bucket)` });

  const roleRows = Array.isArray(roles) ? roles : [];
  const companyRows = Array.isArray(companies) ? companies : [];
  if (!roleRows.length || !companyRows.length) throw new Error("Hypothesis has no roles or company profiles selected (VP/Pains grids would be empty).");

  // TAL summary is derived from the cached JSON produced by export-tal-companies.mjs.
  const talUrl = String(hyp?.hubspot_tal_url ?? "").trim();
  const listId = parseHubspotListIdFromUrl(talUrl);
  if (!listId) throw new Error("Hypothesis hubspot_tal_url is missing or cannot parse list id.");
  const talCachePath = path.resolve(process.cwd(), "99-applications/sales/_private_cache", `tal-${listId}.json`);
  if (!fs.existsSync(talCachePath)) {
    throw new Error(`Missing TAL cache file: ${talCachePath}. Run export-tal-companies.mjs first.`);
  }
  const talJson = JSON.parse(fs.readFileSync(talCachePath, "utf8"));
  // Ensure cache has a baseline total so summaries don't show the bounded fetch count.
  const baseline = Number(hyp?.tal_companies_count_baseline ?? 0) || null;
  if (baseline && baseline > 0) talJson.total_companies_baseline = baseline;
  const talSummary = makeEnglishTalSummaryFromCache({ verticalName: String(hyp?.vertical_name ?? "").trim(), listId, talJson });

  const content = buildContent({ talSummary });

  // 1) Update hypothesis text fields.
  await rest(auth, {
    method: "PATCH",
    table: "sales_hypotheses",
    qs: `id=eq.${encodeURIComponent(hid)}`,
    body: {
      one_sentence_pitch: content.oneSentencePitch,
      product_description: content.productDescription,
      company_profile_text: content.clientProfile
    }
  });

  // 2) Upsert VP + pains for every role x company intersection.
  const vpRows = [];
  const painRows = [];
  for (const r of roleRows) {
    const roleName = norm(r?.role?.name);
    const byRole = content.painsByRole[roleName] ?? null;
    if (!byRole) continue;
    for (const c of companyRows) {
      vpRows.push({
        hypothesis_id: hid,
        role_id: String(r.role_id),
        company_profile_id: String(c.company_profile_id),
        vp_json: { value_proposition: byRole.vp }
      });
      painRows.push({
        hypothesis_id: hid,
        role_id: String(r.role_id),
        company_profile_id: String(c.company_profile_id),
        pain_json: { pain_points: byRole.pains, product_solution: byRole.solution }
      });
    }
  }

  if (vpRows.length) {
    await rest(auth, { method: "POST", table: "sales_hypothesis_vps", body: vpRows, prefer: "resolution=merge-duplicates,return=minimal" });
  }
  if (painRows.length) {
    await rest(auth, { method: "POST", table: "sales_hypothesis_pains", body: painRows, prefer: "resolution=merge-duplicates,return=minimal" });
  }

  console.log(JSON.stringify({ ok: true, hypothesis_id: hid, vp_upserts: vpRows.length, pain_upserts: painRows.length }, null, 2));
}

main().catch((e) => {
  console.error(String(e?.message || e));
  process.exit(1);
});

