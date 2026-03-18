// Sync Sales hypotheses from Supabase into markdown files under 01-workspace/sales/.
//
// Source of truth: Supabase tables (public.sales_hypotheses + checkins + linked calls).
// Output: deterministic markdown snapshots + comparison table.
//
// Usage:
//   node 99-applications/sales/tools/sync-hypotheses.mjs
//   node 99-applications/sales/tools/sync-hypotheses.mjs --hypothesis-id <uuid>
//
// Auth:
// - Uses 02-calls/_private_cache/auth.json (same Supabase project) by default.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadAuth, refreshIfNeeded, supabaseHeaders, getDefaultAuthFile } from "../../calls/tools/supabase-auth.mjs";

const THIS_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_DIR = path.resolve(THIS_DIR, "../../../");
const OUT_DIR = path.resolve(REPO_DIR, "01-workspace/sales/hypotheses");

function parseArgs(argv) {
  const out = { hypothesisId: null };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--hypothesis-id") out.hypothesisId = String(argv[++i] ?? "").trim() || null;
  }
  return out;
}

function slugify(s) {
  return String(s ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 60) || "hypothesis";
}

function isoDate(x) {
  if (!x) return "";
  const d = new Date(x);
  if (Number.isNaN(d.getTime())) return "";
  return d.toISOString().slice(0, 10);
}

function safeJsonStringify(v) {
  try {
    return JSON.stringify(v ?? {}, null, 2);
  } catch {
    return "{}";
  }
}

async function restGet(auth, pathWithQuery) {
  const res = await fetch(`${auth.supabase_url}/rest/v1/${pathWithQuery}`, {
    method: "GET",
    headers: supabaseHeaders(auth)
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`GET ${pathWithQuery} failed: ${res.status} ${text}`);
  return text ? JSON.parse(text) : null;
}

function encodeInList(ids) {
  // PostgREST "in" syntax expects parentheses. Values must be quoted for UUIDs.
  const xs = (ids ?? []).map((x) => `"${String(x).replace(/"/g, "")}"`);
  return `(${xs.join(",")})`;
}

function mdEscape(s) {
  return String(s ?? "").replace(/\r\n/g, "\n").trim();
}

function asList(xs) {
  const out = [];
  for (const x of Array.isArray(xs) ? xs : []) {
    const t = String(x ?? "").trim();
    if (t) out.push(t);
  }
  return out;
}

function renderIcpReadable(icp) {
  const role = icp?.role ?? {};
  const company = icp?.company ?? {};
  const constraints = icp?.constraints ?? {};
  const lines = [];
  const persona = String(role.persona ?? "").trim();
  const decisionRole = String(role.decision_role ?? "").trim();
  const seniority = String(role.seniority ?? "").trim();
  const titles = asList(role.titles);
  const region = String(company.region ?? "").trim();
  const sizeBucket = String(company.size_bucket ?? "").trim();
  const tech = asList(company.tech_stack);

  if (persona) lines.push(`- Persona: ${persona}`);
  if (decisionRole) lines.push(`- Decision role: ${decisionRole}`);
  if (seniority) lines.push(`- Seniority: ${seniority}`);
  if (titles.length) lines.push(`- Titles: ${titles.join(", ")}`);
  if (region) lines.push(`- Region: ${region}`);
  if (sizeBucket) lines.push(`- Company size: ${sizeBucket}`);
  if (tech.length) lines.push(`- Tech stack: ${tech.join(", ")}`);

  const compliance = asList(constraints.compliance);
  if (compliance.length) lines.push(`- Compliance: ${compliance.join(", ")}`);
  const notes = String(constraints.notes ?? "").trim();
  if (notes) lines.push(`- Constraints notes: ${notes}`);

  return lines.length ? lines : ["(not set)"];
}

function renderCjmReadable(cjm) {
  const lines = [];
  const channels = asList(cjm?.channels);
  if (channels.length) lines.push(`- Channels: ${channels.join(", ")}`);
  const notes = String(cjm?.notes ?? "").trim();
  if (notes) lines.push(`- Notes: ${notes}`);
  return lines.length ? lines : ["(not set)"];
}

function renderVpReadable(vp) {
  const stmt = String(vp?.value_proposition ?? vp?.statement ?? "").trim();
  return stmt ? [stmt] : ["(not set)"];
}

function renderRolesReadable(roles) {
  const xs = Array.isArray(roles) ? roles : [];
  if (!xs.length) return ["(none)"];
  const lines = [];
  for (const x of xs) {
    const name = x?.role?.name ?? "—";
    lines.push(`- ${name} (role_id: ${x.role_id})`);
  }
  return lines;
}

function renderCompaniesReadable(companies) {
  const xs = Array.isArray(companies) ? companies : [];
  if (!xs.length) return ["(none)"];
  const lines = [];
  for (const x of xs) {
    const c = x?.company ?? {};
    const v = c.vertical_name ?? "—";
    const sv = c.sub_vertical ? ` / ${c.sub_vertical}` : "";
    const reg = c.region ? ` · ${c.region}` : "";
    const size = c.size_bucket ? ` · ${c.size_bucket}` : "";
    lines.push(`- ${v}${sv}${reg}${size} (company_profile_id: ${x.company_profile_id})`);
  }
  return lines;
}

function renderVpMatrixReadable({ roles, companies, vps }) {
  const roleNameById = new Map();
  for (const x of Array.isArray(roles) ? roles : []) roleNameById.set(String(x.role_id), String(x?.role?.name ?? "—"));

  const companyLabelById = new Map();
  for (const x of Array.isArray(companies) ? companies : []) {
    const c = x?.company ?? {};
    const v = c.vertical_name ?? "—";
    const sv = c.sub_vertical ? ` / ${c.sub_vertical}` : "";
    const reg = c.region ? ` · ${c.region}` : "";
    const size = c.size_bucket ? ` · ${c.size_bucket}` : "";
    companyLabelById.set(String(x.company_profile_id), `${v}${sv}${reg}${size}`);
  }

  const vpByKey = new Map();
  for (const x of Array.isArray(vps) ? vps : []) {
    const rid = String(x.role_id ?? "");
    const cid = String(x.company_profile_id ?? "");
    if (!rid || !cid) continue;
    const key = `${rid}::${cid}`;
    if (vpByKey.has(key)) continue; // keep latest if vps are ordered
    vpByKey.set(key, x.vp_json ?? {});
  }

  const lines = [];
  const roleIds = (Array.isArray(roles) ? roles : []).map((x) => String(x.role_id)).filter(Boolean);
  const companyIds = (Array.isArray(companies) ? companies : []).map((x) => String(x.company_profile_id)).filter(Boolean);
  if (!roleIds.length || !companyIds.length) return ["(no roles/companies selected)"];

  lines.push("- Intersections:");
  for (const cid of companyIds) {
    const companyLabel = companyLabelById.get(cid) ?? cid;
    lines.push(`  - ${companyLabel} (company_profile_id: ${cid}):`);
    for (const rid of roleIds) {
      const roleName = roleNameById.get(rid) ?? rid;
      const vp = vpByKey.get(`${rid}::${cid}`) ?? {};
      const stmt = String(vp?.value_proposition ?? vp?.statement ?? "").trim();
      if (!stmt) continue;
      lines.push(`    - ${roleName} (role_id: ${rid}):`);
      if (stmt) lines.push(`      - VP: ${mdEscape(stmt)}`);
    }
  }
  return lines;
}

function renderPainMatrixReadable({ roles, companies, pains }) {
  const roleNameById = new Map();
  for (const x of Array.isArray(roles) ? roles : []) roleNameById.set(String(x.role_id), String(x?.role?.name ?? "—"));

  const companyLabelById = new Map();
  for (const x of Array.isArray(companies) ? companies : []) {
    const c = x?.company ?? {};
    const v = c.vertical_name ?? "—";
    const sv = c.sub_vertical ? ` / ${c.sub_vertical}` : "";
    const reg = c.region ? ` · ${c.region}` : "";
    const size = c.size_bucket ? ` · ${c.size_bucket}` : "";
    companyLabelById.set(String(x.company_profile_id), `${v}${sv}${reg}${size}`);
  }

  const painByKey = new Map();
  for (const x of Array.isArray(pains) ? pains : []) {
    const rid = String(x.role_id ?? "");
    const cid = String(x.company_profile_id ?? "");
    if (!rid || !cid) continue;
    const key = `${rid}::${cid}`;
    if (painByKey.has(key)) continue; // keep latest if ordered
    painByKey.set(key, x.pain_json ?? {});
  }

  const lines = [];
  const roleIds = (Array.isArray(roles) ? roles : []).map((x) => String(x.role_id)).filter(Boolean);
  const companyIds = (Array.isArray(companies) ? companies : []).map((x) => String(x.company_profile_id)).filter(Boolean);
  if (!roleIds.length || !companyIds.length) return ["(no roles/companies selected)"];

  lines.push("- Intersections:");
  for (const cid of companyIds) {
    const companyLabel = companyLabelById.get(cid) ?? cid;
    lines.push(`  - ${companyLabel} (company_profile_id: ${cid}):`);
    for (const rid of roleIds) {
      const roleName = roleNameById.get(rid) ?? rid;
      const pj = painByKey.get(`${rid}::${cid}`) ?? {};
      const pain = String(pj?.pain_points ?? "").trim();
      const solve = String(pj?.product_solution ?? "").trim();
      if (!pain && !solve) continue;
      lines.push(`    - ${roleName} (role_id: ${rid}):`);
      // Pair pains and solutions by line index so #1 maps to #1.
      const painLines = mdEscape(pain)
        .split("\n")
        .map((x) => x.trim())
        .filter(Boolean);
      const solveLines = mdEscape(solve)
        .split("\n")
        .map((x) => x.trim())
        .filter(Boolean);
      const n = Math.max(painLines.length, solveLines.length);
      if (!n) continue;
      lines.push("      - Pairs (pain → solution):");
      for (let i = 0; i < n; i++) {
        const p = painLines[i] || "(missing)";
        const s = solveLines[i] || "(missing)";
        lines.push(`        - ${i + 1}) Pain: ${p}`);
        lines.push(`          - How we solve: ${s}`);
      }
    }
  }
  return lines;
}

function renderPerChannelActivity(channelActivityJson) {
  const out = [];
  const per = channelActivityJson?.per_channel ?? null;
  const channels = asList(channelActivityJson?.channels);
  if (per && typeof per === "object") {
    const keys = channels.length ? channels : Object.keys(per);
    if (keys.length) {
      out.push("- Per-channel:");
      for (const ch of keys) {
        const v = per?.[ch] ?? {};
        const activity = String(v?.activity ?? "").trim();
        const results = String(v?.results ?? "").trim();
        const callsHeld = v?.calls_held;
        const oppsCreated = v?.opps_created;
        const m = v?.metrics ?? null;
        out.push(`  - ${ch}:`);
        if (callsHeld != null) out.push(`    - Calls held: ${callsHeld}`);
        if (oppsCreated != null) out.push(`    - Opps created: ${oppsCreated}`);
        if (activity) out.push(`    - Activity: ${mdEscape(activity)}`);
        if (results) out.push(`    - Results: ${mdEscape(results)}`);
        if (m && typeof m === "object" && Object.keys(m).length) {
          out.push("    - Metrics:");
          for (const [k, vv] of Object.entries(m)) {
            out.push(`      - ${String(k)}: ${vv == null ? "—" : mdEscape(String(vv))}`);
          }
        }
        if (!activity && !results) out.push("    - (no data)");
      }
    }
    return out;
  }
  // legacy
  const ch = asList(channelActivityJson?.channels);
  if (ch.length) out.push(`- Channels used: ${ch.join(", ")}`);
  const chNotes = String(channelActivityJson?.notes ?? "").trim();
  if (chNotes) out.push(`- Channel notes: ${chNotes}`);
  return out;
}

function renderHypothesisMd(bundle) {
  const h = bundle?.hypothesis ?? {};
  const checkins = Array.isArray(bundle?.checkins) ? bundle.checkins : [];
  const calls = Array.isArray(bundle?.calls) ? bundle.calls : [];
  const roles = Array.isArray(bundle?.roles) ? bundle.roles : [];
  const companies = Array.isArray(bundle?.companies) ? bundle.companies : [];
  const vps = Array.isArray(bundle?.vps) ? bundle.vps : [];
  const pains = Array.isArray(bundle?.pains) ? bundle.pains : [];
  const channelOwners = Array.isArray(bundle?.channelOwners) ? bundle.channelOwners : [];
  const metrics = Array.isArray(bundle?.metrics) ? bundle.metrics : [];

  const lines = [];
  lines.push(`# ${mdEscape(h.title || "Hypothesis")}`);
  lines.push("");
  lines.push("## Metadata");
  lines.push("");
  lines.push(`- Hypothesis ID: ${h.id}`);
  if (h.parent_hypothesis_id) lines.push(`- Parent ID: ${h.parent_hypothesis_id}`);
  lines.push(`- Version: ${h.version ?? 1}`);
  lines.push(`- Status: ${h.status ?? "draft"}`);
  lines.push(`- Priority: ${h.priority ?? 0}`);
  if (h.owner_email) lines.push(`- Owner: ${h.owner_email}`);
  if (h.vertical_name) lines.push(`- Vertical: ${h.vertical_name}`);
  if (h.vertical_hubspot_url) lines.push(`- Vertical (HubSpot): ${h.vertical_hubspot_url}`);
  if (h.hubspot_tal_url) lines.push(`- TAL (HubSpot): ${h.hubspot_tal_url}`);
  if (h.hubspot_deals_view_url) lines.push(`- Deals view (HubSpot): ${h.hubspot_deals_view_url}`);
  lines.push("");

  lines.push("## North Star (Opportunity)");
  lines.push("");
  lines.push(`- Opps in progress (now): ${h.opps_in_progress_count ?? 0}`);
  lines.push("");

  lines.push("## Timebox + Exit Criteria");
  lines.push("");
  lines.push(`- Timebox (days): ${h.timebox_days ?? ""}`);
  if (h.win_criteria) lines.push(`- Win: ${mdEscape(h.win_criteria)}`);
  if (h.kill_criteria) lines.push(`- Kill: ${mdEscape(h.kill_criteria)}`);
  lines.push("");

  lines.push("## Baselines");
  lines.push("");
  if (h.tal_companies_count_baseline != null) lines.push(`- TAL companies baseline: ${h.tal_companies_count_baseline}`);
  if (h.contacts_count_baseline != null) lines.push(`- Contacts baseline: ${h.contacts_count_baseline}`);
  lines.push("");

  lines.push("## One sentence pitch");
  lines.push("");
  lines.push(h.one_sentence_pitch ? mdEscape(h.one_sentence_pitch) : "(not set)");
  lines.push("");

  lines.push("## Product description");
  lines.push("");
  lines.push(h.product_description ? mdEscape(h.product_description) : "(not set)");
  lines.push("");

  lines.push("## Client profile");
  lines.push("");
  lines.push(h.company_profile_text ? mdEscape(h.company_profile_text) : "(not set)");
  lines.push("");

  lines.push("## ICP");
  lines.push("");
  lines.push(...renderIcpReadable(h.icp_json));
  lines.push("");

  lines.push("## Channels (CJM)");
  lines.push("");
  lines.push(...renderCjmReadable(h.cjm_json));
  lines.push("");

  lines.push("## Channel owners");
  lines.push("");
  if (!channelOwners.length) {
    lines.push("(none)");
  } else {
    const bySlug = new Map();
    for (const x of channelOwners) {
      const slug = String(x?.channel?.slug ?? "").trim() || String(x?.channel_id ?? "");
      const email = String(x?.owner_email ?? "").trim();
      if (!slug || !email) continue;
      const arr = bySlug.get(slug) ?? [];
      if (!arr.includes(email)) arr.push(email);
      bySlug.set(slug, arr);
    }
    for (const [slug, emails] of Array.from(bySlug.entries()).sort((a, b) => String(a[0]).localeCompare(String(b[0])))) {
      lines.push(`- ${slug}: ${emails.join(", ")}`);
    }
  }
  lines.push("");

  lines.push("## VP scope (per hypothesis)");
  lines.push("");
  lines.push("Roles:");
  lines.push(...renderRolesReadable(roles).map((x) => `- ${x.replace(/^-\\s*/, "")}`));
  lines.push("");
  lines.push("Company profiles:");
  lines.push(...renderCompaniesReadable(companies).map((x) => `- ${x.replace(/^-\\s*/, "")}`));
  lines.push("");

  lines.push("## VP matrix (per hypothesis)");
  lines.push("");
  lines.push(...renderVpMatrixReadable({ roles, companies, vps }));
  lines.push("");

  lines.push("## Pains (problem → solution) matrix (per hypothesis)");
  lines.push("");
  lines.push(...renderPainMatrixReadable({ roles, companies, pains }));
  lines.push("");

  lines.push("## Metrics");
  lines.push("");
  if (!metrics.length) {
    lines.push("(none)");
  } else {
    for (const x of metrics) {
      const m = x?.metric ?? {};
      const name = String(m.name ?? m.slug ?? "metric");
      const slug = String(m.slug ?? "");
      const unit = String(m.unit ?? "").trim();
      const t = String(m.input_type ?? "number");
      lines.push(`- ${name}${slug ? ` (slug: ${slug})` : ""}${unit ? ` — unit: ${unit}` : ""} — type: ${t}`);
    }
  }
  lines.push("");

  lines.push("## Legacy fields (compat)");
  lines.push("");
  lines.push("- Legacy VP (vp_json):");
  lines.push(...renderVpReadable(h.vp_json).map((x) => `  ${x}`));
  lines.push("");

  lines.push("## Weekly check-ins");
  lines.push("");
  if (!checkins.length) {
    lines.push("(none)");
  } else {
    for (const c of checkins) {
      lines.push(`### Week of ${c.week_start}`);
      lines.push("");
      if (c.opps_in_progress_count != null) lines.push(`- Opps in progress: ${c.opps_in_progress_count}`);
      if (c.tal_companies_count != null) lines.push(`- TAL companies: ${c.tal_companies_count}`);
      if (c.contacts_count != null) lines.push(`- Contacts: ${c.contacts_count}`);
      if (c.notes) lines.push(`- Notes: ${mdEscape(c.notes)}`);
      if (c.blockers) lines.push(`- Blockers: ${mdEscape(c.blockers)}`);
      if (c.next_steps) lines.push(`- Next steps: ${mdEscape(c.next_steps)}`);
      lines.push(...renderPerChannelActivity(c.channel_activity_json));
      const callsHeld = c.metrics_snapshot_json?.calls_held;
      const oppsCreated = c.metrics_snapshot_json?.opps_created;
      if (callsHeld != null) lines.push(`- Calls held: ${callsHeld}`);
      if (oppsCreated != null) lines.push(`- Opps created: ${oppsCreated}`);
      const custom = c.metrics_snapshot_json?.metrics;
      if (custom && typeof custom === "object" && Object.keys(custom).length) {
        lines.push("- Metrics:");
        for (const [k, v] of Object.entries(custom)) {
          lines.push(`  - ${String(k)}: ${v == null ? "—" : mdEscape(String(v))}`);
        }
      }
      lines.push("");
    }
  }
  lines.push("");

  lines.push("## Linked calls");
  lines.push("");
  if (!calls.length) {
    lines.push("(none)");
  } else {
    for (const c of calls) {
      const title = c.title || "Untitled";
      const d = c.occurred_at ? isoDate(c.occurred_at) : "";
      const tag = c.tag ? ` (${c.tag})` : "";
      lines.push(`- ${title}${tag}${d ? ` — ${d}` : ""} — Call ID: ${c.call_id}`);
    }
  }
  lines.push("");

  return lines.join("\n");
}

function renderComparisonMd(hyps) {
  const rows = (hyps ?? []).slice().sort((a, b) => {
    const at = String(a.updated_at ?? "");
    const bt = String(b.updated_at ?? "");
    return bt.localeCompare(at);
  });

  const lines = [];
  lines.push("# Hypotheses comparison");
  lines.push("");
  lines.push("North Star: Opportunities (HubSpot deals). Source of truth: Supabase + HubSpot links.");
  lines.push("");
  lines.push("| Hypothesis | Status | Opps (now) | TAL baseline | Contacts baseline | Timebox | HubSpot deals view | Updated |");
  lines.push("|---|---:|---:|---:|---:|---:|---|---|");
  for (const h of rows) {
    const title = mdEscape(h.title || "Untitled").replace(/\|/g, "\\|");
    const status = mdEscape(h.status || "draft");
    const opps = Number(h.opps_in_progress_count ?? 0);
    const tal = h.tal_companies_count_baseline ?? "";
    const contacts = h.contacts_count_baseline ?? "";
    const tb = h.timebox_days ? `${h.timebox_days}d` : "";
    const deals = h.hubspot_deals_view_url ? `[link](${h.hubspot_deals_view_url})` : "";
    const upd = h.updated_at ? isoDate(h.updated_at) : "";
    lines.push(`| ${title} | ${status} | ${opps} | ${tal} | ${contacts} | ${tb} | ${deals} | ${upd} |`);
  }
  lines.push("");
  return lines.join("\n");
}

async function main() {
  const opts = parseArgs(process.argv);
  const authFile = process.env.SALES_AUTH_FILE || process.env.CALLS_AUTH_FILE || getDefaultAuthFile();
  let auth = loadAuth(authFile);
  auth = await refreshIfNeeded(authFile, auth);

  fs.mkdirSync(OUT_DIR, { recursive: true });

  const hyps = await restGet(
    auth,
    `sales_hypotheses?select=id,parent_hypothesis_id,version,title,status,priority,owner_user_id,owner_email,vertical_name,opps_in_progress_count,updated_at,created_at&order=updated_at.desc&limit=200`
  );
  const hypRows = Array.isArray(hyps) ? hyps : [];

  const target = opts.hypothesisId ? hypRows.filter((h) => String(h.id) === opts.hypothesisId) : hypRows;
  for (const row of target) {
    const hid = String(row.id);
    const h = await restGet(auth, `sales_hypotheses?id=eq.${hid}&select=*`).then((a) => (Array.isArray(a) ? a[0] : null));
    const checkins = await restGet(auth, `sales_hypothesis_checkins?hypothesis_id=eq.${hid}&select=*&order=week_start.desc`);
    const links = await restGet(auth, `sales_hypothesis_calls?hypothesis_id=eq.${hid}&select=call_id,tag,notes`);
    const roles = await restGet(auth, `sales_hypothesis_roles?hypothesis_id=eq.${hid}&select=role_id,role:sales_icp_roles(name)`);
    const companies = await restGet(
      auth,
      `sales_hypothesis_company_profiles?hypothesis_id=eq.${hid}&select=company_profile_id,company:sales_icp_company_profiles(vertical_name,sub_vertical,region,size_bucket)`
    );
    const vps = await restGet(
      auth,
      `sales_hypothesis_vps?hypothesis_id=eq.${hid}&select=role_id,company_profile_id,vp_json,updated_at&order=updated_at.desc&limit=2000`
    );
    const pains = await restGet(
      auth,
      `sales_hypothesis_pains?hypothesis_id=eq.${hid}&select=role_id,company_profile_id,pain_json,updated_at&order=updated_at.desc&limit=2000`
    );
    const channelOwners = await restGet(
      auth,
      `sales_hypothesis_channel_owners?hypothesis_id=eq.${hid}&select=channel_id,owner_email,channel:sales_channels(slug,name)&order=owner_email.asc&limit=5000`
    );
    const metrics = await restGet(
      auth,
      `sales_hypothesis_metrics?hypothesis_id=eq.${hid}&select=metric_id,metric:sales_metrics(slug,name,input_type,unit,sort_order,is_active)`
    );
    const linkRows = Array.isArray(links) ? links : [];
    const callIds = linkRows.map((x) => String(x.call_id)).filter(Boolean);
    let calls = [];
    if (callIds.length) {
      const callRows = await restGet(
        auth,
        `calls?id=in.${encodeInList(callIds)}&select=id,title,occurred_at,owner_email`
      );
      const byId = new Map((Array.isArray(callRows) ? callRows : []).map((c) => [String(c.id), c]));
      calls = linkRows.map((l) => {
        const c = byId.get(String(l.call_id));
        return {
          call_id: String(l.call_id),
          tag: l.tag ?? null,
          notes: l.notes ?? null,
          title: c?.title ?? null,
          occurred_at: c?.occurred_at ?? null,
          owner_email: c?.owner_email ?? null
        };
      });
    }
    const bundle = {
      hypothesis: h || row,
      checkins: Array.isArray(checkins) ? checkins : [],
      calls,
      roles: Array.isArray(roles) ? roles : [],
      companies: Array.isArray(companies) ? companies : [],
      vps: Array.isArray(vps) ? vps : [],
      pains: Array.isArray(pains) ? pains : [],
      channelOwners: Array.isArray(channelOwners) ? channelOwners : [],
      metrics: Array.isArray(metrics) ? metrics : []
    };
    const title = String(bundle?.hypothesis?.title ?? row.title ?? "Hypothesis");
    const slug = slugify(title);
    const outPath = path.join(OUT_DIR, `${slug}-${hid}.md`);
    fs.writeFileSync(outPath, renderHypothesisMd(bundle));
  }

  // Comparison file for quick trend view
  // Prefer reading actual baselines/timebox from bundles (slower, but more useful).
  const fullBundles = [];
  for (const row of hypRows) {
    const hid = String(row.id);
    const h = await restGet(auth, `sales_hypotheses?id=eq.${hid}&select=*`).then((a) => (Array.isArray(a) ? a[0] : null));
    fullBundles.push({
      id: (h?.id ?? hid),
      title: (h?.title ?? row.title),
      status: (h?.status ?? row.status),
      opps_in_progress_count: (h?.opps_in_progress_count ?? row.opps_in_progress_count),
      tal_companies_count_baseline: (h?.tal_companies_count_baseline ?? null),
      contacts_count_baseline: (h?.contacts_count_baseline ?? null),
      timebox_days: (h?.timebox_days ?? null),
      hubspot_deals_view_url: (h?.hubspot_deals_view_url ?? null),
      updated_at: (h?.updated_at ?? row.updated_at)
    });
  }
  const comparisonPath = path.join(OUT_DIR, `_comparison.md`);
  fs.writeFileSync(comparisonPath, renderComparisonMd(fullBundles));

  console.log(JSON.stringify({ ok: true, hypotheses_total: hypRows.length, written: target.length, out_dir: OUT_DIR }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});


