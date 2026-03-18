import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

type SupabaseUserResponse = { email?: string | null };

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

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

function parseHubspotListIdFromUrl(url: string) {
  const t = String(url ?? "").trim();
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  return m?.[1] ? m[1] : null;
}

function toMs(v: any): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

function canonicalizeEmail(raw: any) {
  return String(raw ?? "").trim().toLowerCase();
}

function pickDomainFromEmail(email: string) {
  const t = String(email || "").trim().toLowerCase();
  const at = t.lastIndexOf("@");
  if (at <= 0) return "";
  return t.slice(at + 1).trim();
}

function parseAdditionalEmails(raw: any) {
  const t = String(raw ?? "").trim();
  if (!t) return [];
  // HubSpot commonly stores "hs_additional_emails" as semicolon-separated
  return t
    .split(/[;,]/g)
    .map((x) => canonicalizeEmail(x))
    .filter(Boolean);
}

function hasTimeInRange(sortedAscMs: number[], startMs: number, endMs: number) {
  // Find any value in [startMs, endMs]
  if (!sortedAscMs.length) return null as number | null;
  let lo = 0;
  let hi = sortedAscMs.length - 1;
  // upper_bound(endMs) - 1
  let pos = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const v = sortedAscMs[mid];
    if (v <= endMs) {
      pos = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  if (pos < 0) return null;
  const v = sortedAscMs[pos];
  return v >= startMs ? v : null;
}

function levenshtein(a: string, b: string) {
  const s = String(a ?? "");
  const t = String(b ?? "");
  const n = s.length;
  const m = t.length;
  if (!n) return m;
  if (!m) return n;
  const dp = new Array<number>(m + 1);
  for (let j = 0; j <= m; j++) dp[j] = j;
  for (let i = 1; i <= n; i++) {
    let prev = dp[0];
    dp[0] = i;
    for (let j = 1; j <= m; j++) {
      const tmp = dp[j];
      const cost = s[i - 1] === t[j - 1] ? 0 : 1;
      dp[j] = Math.min(dp[j] + 1, dp[j - 1] + 1, prev + cost);
      prev = tmp;
    }
  }
  return dp[m];
}

function canonicalizeChannelLabel(raw: string) {
  const s0 = String(raw ?? "").trim();
  if (!s0 || s0 === "--") return "Unknown";
  const s = s0.normalize("NFKC").replace(/\s+/g, " ").trim();

  if (s === s.toUpperCase() && /[A-Z]/.test(s)) return s;

  if (!s.includes(" ") && (s.includes("/") || s.includes("."))) {
    return s.replace(/^https?:\/\//i, "").replace(/\/+$/g, "").toLowerCase();
  }

  const key = s.toLowerCase();
  const keyNoSep = key.replace(/[^a-z0-9]+/g, "");

  if (keyNoSep && levenshtein(keyNoSep, "linkedin") <= 1) return "LinkedIn";
  if (keyNoSep && levenshtein(keyNoSep, "google") <= 1) return "Google";

  const words = key
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean);
  if (!words.length) return s;
  const titled = words.map((w) => (w.length <= 2 ? w.toUpperCase() : w[0].toUpperCase() + w.slice(1))).join(" ");
  return titled || s;
}

function channelFromDealProps(props: any) {
  const referralKey = String(process.env.HUBSPOT_DEAL_REFERRAL_PROPERTY ?? "referralsource").trim();
  const referral = referralKey ? String((props as any)?.[referralKey] ?? "").trim() : "";
  if (referral && referral !== "--") return canonicalizeChannelLabel(referral);
  const a1 = String(props?.hs_analytics_source_data_1 ?? "").trim();
  const a = String(props?.hs_analytics_source ?? "").trim();
  const a2 = String(props?.hs_analytics_source_data_2 ?? "").trim();
  const best = a1 || a || a2;
  return canonicalizeChannelLabel(best || "Unknown");
}

let lastHubspotCallAt = 0;
async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function hubspotFetch(url: string, init?: RequestInit, opts?: { minIntervalMs?: number; maxRetries?: number }) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const minIntervalMs = Math.max(50, Math.min(500, Number(opts?.minIntervalMs ?? 160)));
  const maxRetries = Math.max(0, Math.min(8, Number(opts?.maxRetries ?? 5)));

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const now = Date.now();
    const wait = Math.max(0, lastHubspotCallAt + minIntervalMs - now);
    if (wait) await sleep(wait);
    lastHubspotCallAt = Date.now();

    const res = await fetch(url, {
      ...init,
      headers: {
        Authorization: `Bearer ${token}`,
        ...(init?.headers ?? {})
      }
    });

    if (res.status !== 429) return res;
    const retryAfter = Number(res.headers.get("retry-after") || "");
    const backoff = Number.isFinite(retryAfter) ? retryAfter * 1000 : 1200 + attempt * 800;
    await sleep(Math.min(10_000, backoff));
  }

  throw new Error("HubSpot rate limit: too many requests (429). Try again in ~10 seconds.");
}

async function hubspotFetchDealStageLabels() {
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/pipelines/deals");
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const stageLabelById = new Map<string, string>();
  const results = Array.isArray(json?.results) ? json.results : [];
  for (const p of results) {
    for (const s of Array.isArray(p?.stages) ? p.stages : []) {
      const id = String(s?.id ?? "").trim();
      const label = String(s?.label ?? "").trim();
      if (id) stageLabelById.set(id, label || id);
    }
  }
  return stageLabelById;
}

async function hubspotPickDefaultPipelineIds() {
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/pipelines/deals");
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const results = Array.isArray(json?.results) ? json.results : [];
  const scored = results
    .map((p: any) => {
      const id = String(p?.id ?? "").trim();
      const label = String(p?.label ?? "").trim().toLowerCase();
      let score = 0;
      if (label.includes("funnel")) score += 3;
      if (label.includes("sales")) score += 2;
      if (label.includes("pipeline")) score += 1;
      return { id, score };
    })
    .filter((x: any) => x.id);
  scored.sort((a: any, b: any) => b.score - a.score);
  return scored[0]?.id ? [scored[0].id] : [];
}

function stageBucketFromLabel(labelOrId: string) {
  const t = String(labelOrId ?? "").trim().toLowerCase();
  if (!t) return "unknown";
  if (t.includes("lead")) return "leads";
  if (t.includes("sql")) return "sql";
  if (t.includes("evaluate")) return "opportunity";
  if (t.includes("select")) return "opportunity";
  if (t.includes("negot")) return "opportunity";
  if (t.includes("purchase")) return "opportunity";
  if (t.includes("integrat")) return "clients";
  if (t.includes("active")) return "clients";
  if (t.includes("lost")) return "lost";
  if (t.includes("dormant")) return "lost";
  if (t.includes("churn")) return "lost";
  return "unknown";
}

async function hubspotFetchListCompanyIds(listId: string, limit: number) {
  // HubSpot CRM Lists API memberships endpoint does NOT include object type in the path:
  // GET /crm/v3/lists/{listId}/memberships
  // Ref: https://developers.hubspot.com/docs/api-reference/crm/lists-v3/lists/get-crm-v3-lists-listId-memberships
  const out: string[] = [];
  let after: string | null = null;
  while (out.length < limit) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(500, limit - out.length)));
    if (after) qs.set("after", after);
    const url = `https://api.hubapi.com/crm/v3/lists/${encodeURIComponent(listId)}/memberships?${qs.toString()}`;
    const res = await hubspotFetch(url);
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    for (const r of Array.isArray(json?.results) ? json.results : []) {
      const id = String(r?.recordId ?? r?.id ?? "").trim();
      if (id) out.push(id);
    }
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after) break;
  }
  return out;
}

async function hubspotFetchAssociatedContactIdsForDeal(dealId: string, limit: number) {
  const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}/associations/contacts?limit=${encodeURIComponent(
    String(Math.max(1, Math.min(50, limit)))
  )}`;
  const res = await hubspotFetch(url);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  return (Array.isArray(json?.results) ? json.results : []).map((r: any) => String(r?.id ?? "").trim()).filter(Boolean);
}

async function hubspotBatchReadContactsEmails(contactIds: string[]) {
  const ids = (contactIds || []).map(String).filter(Boolean);
  if (!ids.length) return new Map<string, { emails: string[] }>();
  const out = new Map<string, { emails: string[] }>();
  const props = ["email", "hs_additional_emails"];
  const batchSize = 100;
  for (let i = 0; i < ids.length; i += batchSize) {
    const chunk = ids.slice(i, i + batchSize);
    const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/contacts/batch/read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ properties: props, inputs: chunk.map((id) => ({ id })) })
    });
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    for (const r of Array.isArray(json?.results) ? json.results : []) {
      const id = String(r?.id ?? "").trim();
      if (!id) continue;
      const p = r?.properties ?? {};
      const emails = [
        canonicalizeEmail(p?.email),
        ...parseAdditionalEmails(p?.hs_additional_emails)
      ].filter(Boolean);
      out.set(id, { emails: Array.from(new Set(emails)) });
    }
  }
  return out;
}

async function hubspotBatchReadCompaniesDomains(companyIds: string[]) {
  const ids = (companyIds || []).map(String).filter(Boolean);
  if (!ids.length) return new Map<string, { domain: string; name: string }>();
  const out = new Map<string, { domain: string; name: string }>();
  const props = ["domain", "name"];
  const batchSize = 100;
  for (let i = 0; i < ids.length; i += batchSize) {
    const chunk = ids.slice(i, i + batchSize);
    const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/companies/batch/read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ properties: props, inputs: chunk.map((id) => ({ id })) })
    });
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    for (const r of Array.isArray(json?.results) ? json.results : []) {
      const id = String(r?.id ?? "").trim();
      if (!id) continue;
      const p = r?.properties ?? {};
      const domain = String(p?.domain ?? "").trim().toLowerCase();
      const name = String(p?.name ?? "").trim();
      out.set(id, { domain, name });
    }
  }
  return out;
}

async function hubspotFetchAssociatedCompanyIdsForDeal(dealId: string, limit: number) {
  const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}/associations/companies?limit=${encodeURIComponent(
    String(Math.max(1, Math.min(20, limit)))
  )}`;
  const res = await hubspotFetch(url);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  return (Array.isArray(json?.results) ? json.results : []).map((r: any) => String(r?.id ?? "").trim()).filter(Boolean);
}

async function hubspotBatchFetchAssociations(fromIds: string[], fromType: string, toType: string) {
  const ids = Array.from(new Set(fromIds.map((x) => String(x ?? "").trim()).filter(Boolean)));
  if (!ids.length) return new Map<string, string[]>();

  const out = new Map<string, string[]>();
  const batchSize = 100;
  for (let i = 0; i < ids.length; i += batchSize) {
    const chunk = ids.slice(i, i + batchSize);
    const res = await hubspotFetch(`https://api.hubapi.com/crm/v3/associations/${fromType}/${toType}/batch/read`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inputs: chunk.map((id) => ({ id })) })
    });
    const json = (await res.json()) as any;
    if (!res.ok) continue; // Skip failed batches to avoid crashing everything
    for (const r of Array.isArray(json?.results) ? json.results : []) {
      const fromId = String(r?.from?.id ?? "").trim();
      const toIds = (Array.isArray(r?.to) ? r.to : [])
        .map((x: any) => String(x?.id ?? "").trim())
        .filter(Boolean);
      if (fromId) out.set(fromId, toIds);
    }
  }
  return out;
}

async function hubspotSearchDealsCreatedBetweenPaged(opts: { sinceMs: number; untilMs: number; pipelineIds: string[]; stageIds?: string[]; maxDeals: number }) {
  const properties = [
    "dealname",
    "dealstage",
    "pipeline",
    "createdate",
    "hs_lastmodifieddate",
    "referralsource",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_analytics_source_data_2"
  ];

  const out: any[] = [];
  let after: string | null = null;
  while (out.length < opts.maxDeals) {
    const body: any = {
      filterGroups: [
        {
          filters: [
            { propertyName: "pipeline", operator: "IN", values: opts.pipelineIds.map(String) },
            ...(Array.isArray(opts.stageIds) && opts.stageIds.length
              ? [{ propertyName: "dealstage", operator: "IN", values: opts.stageIds.map(String) }]
              : []),
            { propertyName: "createdate", operator: "GTE", value: String(opts.sinceMs) },
            { propertyName: "createdate", operator: "LT", value: String(opts.untilMs) }
          ]
        }
      ],
      sorts: [{ propertyName: "createdate", direction: "ASCENDING" }],
      properties,
      limit: Math.min(200, opts.maxDeals - out.length)
    };
    if (after) body.after = after;

    const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    const results = Array.isArray(json?.results) ? json.results : [];
    out.push(...results);
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after || !results.length) break;
  }
  return out.slice(0, opts.maxDeals);
}

async function postgrestGet(authHeader: string, path: string) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const url = `${supabaseUrl}/rest/v1/${path}`;
  const res = await fetch(url, { headers: { apikey: supabaseAnonKey, Authorization: authHeader } });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || "Supabase query failed"));
  return json;
}

async function postgrestGetPaged(authHeader: string, pathBase: string, maxRows: number) {
  const out: any[] = [];
  const limit = Math.max(1, Math.min(5000, Number(maxRows || 5000)));
  for (let offset = 0; offset < limit; offset += 1000) {
    const page = await postgrestGet(authHeader, `${pathBase}&limit=1000&offset=${offset}`);
    const rows = Array.isArray(page) ? page : [];
    out.push(...rows);
    if (rows.length < 1000) break;
  }
  return out.slice(0, limit);
}

function looksLikeMissingRelation(msg: string) {
  const t = String(msg || "").toLowerCase();
  return t.includes("could not find the table") || t.includes("schema cache") || (t.includes("relation") && t.includes("does not exist"));
}

function looksLikeMissingColumn(msg: string) {
  const t = String(msg || "").toLowerCase();
  return t.includes("column") && t.includes("does not exist");
}

function stageCategoryFromLabel(labelOrId: string) {
  const t = String(labelOrId ?? "").trim().toLowerCase();
  if (!t) return "unknown";
  if (t.includes("lead")) return "lead";
  if (t.includes("sql")) return "sql";
  if (t.includes("evaluate")) return "evaluate";
  if (t.includes("purchase")) return "purchase";
  if (t.includes("integrat")) return "integration";
  if (t.includes("dormant")) return "dormant";
  if (t.includes("churn")) return "churn";
  return "unknown";
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");

    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json()) as {
      since: string; // ymd
      until: string; // ymd (exclusive)
      pipeline_ids: string[];
      stage_ids?: string[];
      maxDeals?: number;
      maxCompaniesPerTal?: number;
      include_getsales_influence?: boolean;
      getsales_lookback_days?: number;
      include_smartlead_influence?: boolean;
      smartlead_lookback_days?: number;
      smartlead_campaign_ids?: Array<string | number>;
    };

    const since = String(payload?.since ?? "").trim();
    const until = String(payload?.until ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(since) || !/^\d{4}-\d{2}-\d{2}$/.test(until)) {
      return jsonError(400, "since/until must be YYYY-MM-DD (until is exclusive)");
    }
    const defaultPipelineIds = String(process.env.HUBSPOT_FUNNEL_PIPELINE_IDS ?? "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const pipelineIds = Array.isArray(payload?.pipeline_ids) ? payload.pipeline_ids.map(String).filter(Boolean) : [];
    let effectivePipelineIds = pipelineIds.length ? pipelineIds : defaultPipelineIds;
    if (!effectivePipelineIds.length) effectivePipelineIds = await hubspotPickDefaultPipelineIds();
    if (!effectivePipelineIds.length) return jsonError(400, "pipeline_ids is required (or set HUBSPOT_FUNNEL_PIPELINE_IDS)");
    const stageIds = Array.isArray(payload?.stage_ids) ? payload.stage_ids.map(String).filter(Boolean) : [];

    const maxDeals = Math.max(1, Math.min(5000, Number(payload?.maxDeals ?? 2000)));
    const maxCompaniesPerTal = Math.max(1, Math.min(3000, Number(payload?.maxCompaniesPerTal ?? 2000)));
    const includeGs = Boolean(payload?.include_getsales_influence);
    const gsLookbackDays = Math.max(1, Math.min(180, Number(payload?.getsales_lookback_days ?? 60)));
    const includeSl = Boolean(payload?.include_smartlead_influence);
    const slLookbackDays = Math.max(1, Math.min(365, Number(payload?.smartlead_lookback_days ?? 60)));
    const slCampaignIds = Array.isArray(payload?.smartlead_campaign_ids)
      ? payload.smartlead_campaign_ids.map((x) => Number(x)).filter((n) => Number.isFinite(n)).slice(0, 50)
      : [];

    const sinceMs = Date.parse(`${since}T00:00:00.000Z`);
    const untilMs = Date.parse(`${until}T00:00:00.000Z`);
    if (!Number.isFinite(sinceMs) || !Number.isFinite(untilMs) || untilMs <= sinceMs) return jsonError(400, "Invalid since/until range");

    const portalId = process.env.HUBSPOT_PORTAL_ID ?? process.env.NEXT_PUBLIC_HUBSPOT_PORTAL_ID ?? "";
    const stageLabelById = await hubspotFetchDealStageLabels();

    // Map companies -> active hypothesis (for "unassigned")
    const hyps = await postgrestGet(
      authHeader!,
      "sales_hypotheses?select=id,title,status,hubspot_tal_url&status=eq.active&hubspot_tal_url=not.is.null&limit=500"
    );
    const companyToHyp = new Map<string, string>();
    const hypTitleById = new Map<string, string>();
    for (const h of Array.isArray(hyps) ? hyps : []) {
      const hid = String(h?.id ?? "");
      hypTitleById.set(hid, String(h?.title ?? hid));
      const listId = parseHubspotListIdFromUrl(String(h?.hubspot_tal_url ?? "")) || null;
      if (!listId) continue;
      const cids = await hubspotFetchListCompanyIds(listId, maxCompaniesPerTal);
      for (const cid of cids) if (!companyToHyp.has(cid)) companyToHyp.set(cid, hid);
    }

    const deals = await hubspotSearchDealsCreatedBetweenPaged({ sinceMs, untilMs, pipelineIds: effectivePipelineIds, stageIds, maxDeals });

    // Unified Influence Logic: determine if a deal was likely influenced by GetSales / SmartLead activity.
    let influenceTruncated = false;
    const influenceDebug: any = { note: "Unified analytics influence enabled" };
    const influenceByDealId = new Map<string, { source: string; atMs: number; email?: string }>();
    if ((includeGs || includeSl) && deals.length) {
      const maxDealsForInfluence = Math.min(deals.length, 600);
      if (deals.length > maxDealsForInfluence) influenceTruncated = true;
      const dealIds = deals.slice(0, maxDealsForInfluence).map((d) => String(d?.id ?? ""));

      const [batchContacts, batchCompanies] = await Promise.all([
        hubspotBatchFetchAssociations(dealIds, "deals", "contacts"),
        hubspotBatchFetchAssociations(dealIds, "deals", "companies")
      ]);

      const allContactIdsSet = new Set<string>();
      const allCompanyIdsSet = new Set<string>();
      for (const dId of dealIds) {
        (batchContacts.get(dId) || []).forEach(cid => allContactIdsSet.add(cid));
        (batchCompanies.get(dId) || []).forEach(coid => allCompanyIdsSet.add(coid));
      }

      const allContactIds = Array.from(allContactIdsSet).map(Number).filter(n => !isNaN(n));
      const allCompanyIds = Array.from(allCompanyIdsSet).map(Number).filter(n => !isNaN(n));

      if (allContactIds.length || allCompanyIds.length) {
        const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
        const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";
        const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey);

        const lookbackMs = Math.max(gsLookbackDays, slLookbackDays) * 86400000;
        const eventsSinceIso = new Date(sinceMs - lookbackMs).toISOString();

        let query = supabaseAdmin
          .from("sales_analytics_activities")
          .select("occurred_at, contact_id, company_id, source_system")
          .gte("occurred_at", eventsSinceIso)
          .lte("occurred_at", new Date(untilMs).toISOString());

        if (allContactIds.length && allCompanyIds.length) {
          query = query.or(`contact_id.in.(${allContactIds.join(",")}),company_id.in.(${allCompanyIds.join(",")})`);
        } else if (allContactIds.length) {
          query = query.in("contact_id", allContactIds);
        } else {
          query = query.in("company_id", allCompanyIds);
        }

        const { data: activities } = await query;

        if (activities && activities.length) {
          for (const d of deals.slice(0, maxDealsForInfluence)) {
            const dId = String(d?.id ?? "");
            const createdMs = toMs(d?.properties?.createdate);
            if (!createdMs) continue;

            const dContactIds = new Set((batchContacts.get(dId) || []).map(Number));
            const dCompanyIds = new Set((batchCompanies.get(dId) || []).map(Number));

            let best: { source: string; atMs: number } | null = null;
            for (const act of activities) {
              const matches = (act.contact_id && dContactIds.has(Number(act.contact_id))) ||
                (act.company_id && dCompanyIds.has(Number(act.company_id)));
              if (!matches) continue;

              const actMs = new Date(act.occurred_at).getTime();
              const lookback = act.source_system === "getsales" ? gsLookbackDays : slLookbackDays;
              if (actMs >= createdMs - (lookback * 86400000) && actMs < createdMs) {
                if (!best || actMs > best.atMs) {
                  best = { source: act.source_system, atMs: actMs };
                }
              }
            }
            if (best) influenceByDealId.set(dId, best);
          }
        }
      }
    }

    const allDealIds = deals.map((d) => String(d?.id ?? ""));
    const allDealToCompanyIds = await hubspotBatchFetchAssociations(allDealIds, "deals", "companies");

    const out: any[] = [];
    for (const d of deals) {
      const id = String(d?.id ?? "");
      const props = d?.properties ?? {};
      const stageId = String(props?.dealstage ?? "").trim();
      const stageLabel = stageLabelById.get(stageId) ?? stageId;
      const stageCategory = stageCategoryFromLabel(stageLabel || stageId);
      const stageBucket = stageBucketFromLabel(stageLabel || stageId);
      const channel = channelFromDealProps(props);
      const createdMs = toMs(props?.createdate);
      const lastmodMs = toMs(props?.hs_lastmodifieddate);

      const companyIds = id ? (allDealToCompanyIds.get(id) || []) : [];
      let hypId: string | null = null;
      for (const cid of companyIds) {
        const h = companyToHyp.get(cid) ?? null;
        if (h) {
          hypId = h;
          break;
        }
      }
      const hypKey = hypId || "__unassigned__";
      const hypTitle = hypId ? (hypTitleById.get(hypId) ?? hypId) : "Unassigned";

      const url = portalId && id ? `https://app.hubspot.com/contacts/${portalId}/record/0-3/${id}/` : null;
      const inf = id ? influenceByDealId.get(id) ?? null : null;
      out.push({
        id,
        url,
        dealname: String(props?.dealname ?? ""),
        pipeline: String(props?.pipeline ?? ""),
        createdate: createdMs ? new Date(createdMs).toISOString() : (props?.createdate ?? null),
        lastmodified: lastmodMs ? new Date(lastmodMs).toISOString() : (props?.hs_lastmodifieddate ?? null),
        dealstage_id: stageId || null,
        dealstage_label: stageLabel || null,
        stage_category: stageCategory,
        stage_bucket: stageBucket,
        channel,
        hypothesis_key: hypKey,
        hypothesis_title: hypTitle,
        influenced_getsales: Boolean(includeGs && inf?.source === "getsales"),
        influenced_smartlead: Boolean(includeSl && inf?.source === "smartlead"),
        influenced_email: inf?.email ?? null,
        influenced_last_activity_at: inf?.atMs ? new Date(inf.atMs).toISOString() : null
      });
    }

    return NextResponse.json({
      ok: true,
      since,
      until,
      deals_count: out.length,
      getsales_influence_truncated: influenceTruncated,
      getsales_lookback_days: includeGs ? gsLookbackDays : null,
      getsales_influence_debug: includeGs ? influenceDebug : null,
      smartlead_influence_truncated: includeSl ? influenceTruncated : null,
      smartlead_lookback_days: includeSl ? slLookbackDays : null,
      smartlead_campaign_ids: includeSl ? slCampaignIds : [],
      smartlead_influence_debug: includeSl ? influenceDebug : null,
      deals: out
    });
  } catch (e: any) {
    const msg = String(e?.message || e);
    // Extra safety: don't fail the whole endpoint if SmartLead schema is missing/mismatched.
    if (looksLikeMissingRelation(msg) || looksLikeMissingColumn(msg)) {
      return NextResponse.json({ ok: true, since: "", until: "", deals_count: 0, deals: [], smartlead_influence_truncated: null, smartlead_lookback_days: null, smartlead_influence_debug: { note: `SmartLead influence disabled: ${msg}` } });
    }
    return jsonError(500, msg);
  }
}


