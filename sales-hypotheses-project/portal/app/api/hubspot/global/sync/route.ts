import { NextResponse } from "next/server";

type SupabaseUserResponse = { id?: string | null; email?: string | null };

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

type PostgrestHeaders = { apikey: string; Authorization: string };

function getBearerToken(req: Request) {
  const authHeader = String(req.headers.get("authorization") ?? "").trim();
  const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
  const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
  return { authHeader, bearer, gotSecret };
}

function isCronAuthorized(req: Request) {
  // Vercel Cron adds `x-vercel-cron: 1` to scheduled requests.
  const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
  if (vercelCron === "1" || vercelCron.toLowerCase() === "true") return true;
  const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
  if (!cronSecret) return false;
  const { bearer, gotSecret } = getBearerToken(req);
  return bearer === cronSecret || gotSecret === cronSecret;
}

function postgrestHeadersForRequest(req: Request, authHeader: string | null): PostgrestHeaders {
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
  if (isCronAuthorized(req)) {
    if (!serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY (required for cron)");
    return { apikey: serviceRoleKey, Authorization: `Bearer ${serviceRoleKey}` };
  }
  if (!supabaseAnonKey) throw new Error("Missing NEXT_PUBLIC_SUPABASE_ANON_KEY");
  if (!authHeader) throw new Error("Missing Authorization");
  return { apikey: supabaseAnonKey, Authorization: authHeader };
}

function parseHubspotListIdFromUrl(url: string) {
  const t = String(url ?? "").trim();
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  return m?.[1] ? m[1] : null;
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

function isOppStage(cat: string) {
  const c = String(cat ?? "").toLowerCase();
  return c === "evaluate" || c === "purchase" || c === "integration" || c === "dormant" || c === "churn";
}
function isCustomerStage(cat: string) {
  const c = String(cat ?? "").toLowerCase();
  return c === "integration" || c === "dormant" || c === "churn";
}
function isChurnStage(cat: string) {
  const c = String(cat ?? "").toLowerCase();
  return c === "dormant" || c === "churn";
}
function isLeadStage(cat: string) {
  const c = String(cat ?? "").toLowerCase();
  return c === "lead" || c === "sql";
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

  // Keep ALLCAPS enums as-is (these are often curated values like INTEGRATION/IMPORT).
  if (s === s.toUpperCase() && /[A-Z]/.test(s)) return s;

  // URL-ish sources: normalize consistently (strip protocol, lowercase, trim trailing slash).
  if (!s.includes(" ") && (s.includes("/") || s.includes("."))) {
    return s.replace(/^https?:\/\//i, "").replace(/\/+$/g, "").toLowerCase();
  }

  const key = s.toLowerCase();
  const keyNoSep = key.replace(/[^a-z0-9]+/g, "");

  // Small fuzzy normalization for common channels
  if (keyNoSep && levenshtein(keyNoSep, "linkedin") <= 1) return "LinkedIn";
  if (keyNoSep && levenshtein(keyNoSep, "google") <= 1) return "Google";

  // Default: Title Case words (stable display), collapses minor punctuation/casing diffs.
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

function parseCsvEnv(name: string) {
  const raw = String(process.env[name] ?? "").trim();
  if (!raw) return [];
  return raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function toMs(v: any): number | null {
  if (v == null || v === "") return null;
  // HubSpot can return dates as ms-string ("173...") OR as ISO string in some contexts.
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

let lastHubspotCallAt = 0;
async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function hubspotFetch(url: string, init?: RequestInit, opts?: { minIntervalMs?: number; maxRetries?: number }) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const minIntervalMs = Math.max(50, Math.min(500, Number(opts?.minIntervalMs ?? 140)));
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

    // Rate limited: backoff and retry
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

async function hubspotSearchDealsModifiedBetween(sinceMs: number, untilMs: number, limit: number) {
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

  const body: any = {
    filterGroups: [
      {
        filters: [
          { propertyName: "hs_lastmodifieddate", operator: "GTE", value: String(sinceMs) },
          { propertyName: "hs_lastmodifieddate", operator: "LT", value: String(untilMs) }
        ]
      }
    ],
    sorts: [{ propertyName: "hs_lastmodifieddate", direction: "DESCENDING" }],
    properties,
    limit: Math.max(1, Math.min(200, limit))
  };

  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  return Array.isArray(json?.results) ? json.results : [];
}

async function hubspotFetchDealStageHistory(dealId: string) {
  const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}?properties=dealname,dealstage,createdate&propertiesWithHistory=dealstage`;
  const res = await hubspotFetch(url);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const props = json?.properties ?? {};
  const hist = json?.propertiesWithHistory?.dealstage;
  const history = Array.isArray(hist)
    ? hist
        .map((x: any) => ({ value: String(x?.value ?? ""), timestamp: toMs(x?.timestamp) }))
        .filter((x: any) => x.value && x.timestamp)
        .sort((a: any, b: any) => (a.timestamp ?? 0) - (b.timestamp ?? 0))
    : [];
  return { dealId, dealname: String(props?.dealname ?? ""), createdate: toMs(props?.createdate), history };
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

async function hubspotFetchAssociatedCompanyIdsForDeal(dealId: string, limit: number) {
  const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}/associations/companies?limit=${encodeURIComponent(
    String(Math.max(1, Math.min(20, limit)))
  )}`;
  const res = await hubspotFetch(url);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  return (Array.isArray(json?.results) ? json.results : []).map((r: any) => String(r?.id ?? "").trim()).filter(Boolean);
}

function inc(map: Record<string, number>, key: string, by = 1) {
  const k = key || "Unknown";
  map[k] = (map[k] ?? 0) + by;
}

function ymd(d: Date) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function startOfWeekUTC(d: Date) {
  const x = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = x.getUTCDay();
  const diff = (day + 6) % 7;
  x.setUTCDate(x.getUTCDate() - diff);
  return x;
}

async function postgrestGet(h: PostgrestHeaders, path: string) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const url = `${supabaseUrl}/rest/v1/${path}`;
  const res = await fetch(url, {
    headers: h
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || "Supabase query failed"));
  return json;
}

async function postgrestUpsert(h: PostgrestHeaders, table: string, row: any, onConflict: string) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const url = `${supabaseUrl}/rest/v1/${table}?on_conflict=${encodeURIComponent(onConflict)}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      ...h,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal"
    },
    body: JSON.stringify(row)
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Supabase upsert failed: ${txt}`);
  }
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const pg = postgrestHeadersForRequest(req, authHeader);
    const isCron = isCronAuthorized(req);
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user?.email || "").toLowerCase();
    if (!isCron) {
      if (!user?.id || !email) return jsonError(401, "Not authorized");
      if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");
    }
    const createdBy = isCron ? String(process.env.HUBSPOT_CRON_USER_ID ?? "").trim() : String(user?.id ?? "").trim();
    if (!createdBy) {
      return jsonError(400, isCron ? "Missing HUBSPOT_CRON_USER_ID (required for cron HubSpot sync)" : "Missing user id");
    }

    const payload = (await req.json()) as {
      periodStart?: string; // optional explicit week start
      force?: boolean;
      maxDeals?: number;
      maxCompaniesPerTal?: number;
      backfill_weeks?: number; // if set, consider last N weeks for backfill
      batch_weeks?: number; // process up to N weeks per call
      debug?: boolean; // include debug stats in response
      pipeline_ids?: string[]; // optional override: filter to pipeline IDs
      stage_ids?: string[]; // optional override: filter to current stage IDs
    };
    const force = !!payload?.force;
    const maxDeals = Math.max(1, Math.min(200, Number(payload?.maxDeals ?? 200)));
    const maxCompaniesPerTal = Math.max(1, Math.min(2000, Number(payload?.maxCompaniesPerTal ?? 2000)));
    const backfillWeeks = Math.max(0, Math.min(104, Number(payload?.backfill_weeks ?? 0)));
    const batchWeeks = Math.max(1, Math.min(8, Number(payload?.batch_weeks ?? 2)));

    const currentWeekStart = ymd(startOfWeekUTC(new Date()));

    // Preload existing week starts (and updated_at) so we can decide what to process.
    const minWeekStart =
      backfillWeeks > 0
        ? ymd(new Date(Date.parse(`${currentWeekStart}T00:00:00.000Z`) - backfillWeeks * 7 * 24 * 60 * 60 * 1000))
        : ymd(new Date(Date.parse(`${currentWeekStart}T00:00:00.000Z`) - 52 * 7 * 24 * 60 * 60 * 1000));

    const existingWeeks = await postgrestGet(
      pg,
      `sales_hubspot_global_snapshots?select=period_start,updated_at&period_start=gte.${minWeekStart}&period_start=lte.${currentWeekStart}&window_days=eq.7&order=period_start.asc&limit=500`
    );
    const have = new Map<string, string>();
    for (const r of Array.isArray(existingWeeks) ? existingWeeks : []) {
      have.set(String(r.period_start), String(r.updated_at ?? ""));
    }

    // Determine candidate week starts to process.
    // Priority:
    // - Explicit periodStart if provided
    // - Current week if missing or stale (>30m) or force
    // - Otherwise backfill missing weeks within backfill range
    const toProcess: string[] = [];
    let mode: "single" | "current" | "backfill" = "current";

    if (payload?.periodStart) {
      const ps = ymd(startOfWeekUTC(new Date(String(payload.periodStart))));
      toProcess.push(ps);
      mode = "single";
    } else {
      const filtersRequested =
        (Array.isArray(payload?.pipeline_ids) && payload.pipeline_ids.length > 0) ||
        (Array.isArray(payload?.stage_ids) && payload.stage_ids.length > 0);

      const curUpdated = have.get(currentWeekStart) || "";
      const curUpdatedMs = curUpdated ? Date.parse(curUpdated) : NaN;
      let curIsFresh = Number.isFinite(curUpdatedMs) && Date.now() - curUpdatedMs < 30 * 60 * 1000;

      // If snapshot is "fresh" but doesn't have the new fields, treat as stale to backfill once.
      if (curIsFresh && have.has(currentWeekStart)) {
        try {
          const existing = await postgrestGet(
            pg,
            `sales_hubspot_global_snapshots?select=data_json&period_start=eq.${currentWeekStart}&window_days=eq.7&limit=1`
          );
          const dj = Array.isArray(existing) && existing[0] ? (existing[0] as any)?.data_json : null;
          if (!dj || !dj.deals_by_metric) curIsFresh = false;
        } catch {
          // ignore; keep as is
        }
      }
      // If user provided filters, we must recompute current week even if it's "fresh",
      // otherwise UI will never reflect filter changes.
      const curIsStaleOrMissing = force || filtersRequested || !have.has(currentWeekStart) || !curIsFresh;
      if (curIsStaleOrMissing) {
        toProcess.push(currentWeekStart);
        mode = "current";
      }

      if (backfillWeeks > 0) {
        const missing: string[] = [];
        for (let i = backfillWeeks; i >= 1; i--) {
          const wk = ymd(new Date(Date.parse(`${currentWeekStart}T00:00:00.000Z`) - i * 7 * 24 * 60 * 60 * 1000));
          if (!have.has(wk)) missing.push(wk);
        }
        if (missing.length) {
          // oldest first
          for (const wk of missing) if (!toProcess.includes(wk)) toProcess.push(wk);
          mode = toProcess[0] === currentWeekStart ? "current" : "backfill";
        }
      } else if (!curIsStaleOrMissing) {
        // Default behavior: if current is fresh and no explicit backfill requested, do nothing.
        return NextResponse.json({ ok: true, skipped: true, reason: "fresh", period_start: currentWeekStart, mode: "current", remaining_missing: 0 });
      }
    }

    const batch = toProcess.slice(0, batchWeeks);

    // Build company->hypothesis map for ACTIVE hypotheses (best-effort) once per request.
    const hyps = await postgrestGet(
      pg,
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

    const stageLabelById = await hubspotFetchDealStageLabels();
    const portalId = process.env.HUBSPOT_PORTAL_ID ?? process.env.NEXT_PUBLIC_HUBSPOT_PORTAL_ID ?? "";

    const processed: Array<{ period_start: string; period_end: string; deals_scanned: number }> = [];

    for (const periodStart of batch) {
      const periodEnd = ymd(new Date(Date.parse(`${periodStart}T00:00:00.000Z`) + 7 * 24 * 60 * 60 * 1000));
      const sinceMs = Date.parse(`${periodStart}T00:00:00.000Z`);
      const untilMs = Date.parse(`${periodEnd}T00:00:00.000Z`);

      // Skip if updated recently (unless forced) – only for current week; for backfill we always compute missing.
      if (!force && payload?.periodStart) {
        const existing = await postgrestGet(pg, `sales_hubspot_global_snapshots?select=updated_at&period_start=eq.${periodStart}&window_days=eq.7&limit=1`);
        if (Array.isArray(existing) && existing[0]?.updated_at) {
          const last = Date.parse(String(existing[0].updated_at));
          if (Number.isFinite(last) && Date.now() - last < 30 * 60 * 1000) {
            // Only skip if it already has per-metric deals.
            const dj = (existing[0] as any)?.data_json ?? null;
            if (dj && dj.deals_by_metric) {
              processed.push({ period_start: periodStart, period_end: periodEnd, deals_scanned: 0 });
              continue;
            }
          }
        }
      }

      // Get deals modified in the window (MVP). This includes unassigned deals too.
      let deals = await hubspotSearchDealsModifiedBetween(sinceMs, untilMs, maxDeals);

      // Optional: only count deals from the "funnel pipeline(s)".
      const funnelPipelineIds = Array.isArray(payload?.pipeline_ids) && payload.pipeline_ids.length ? payload.pipeline_ids.map(String) : parseCsvEnv("HUBSPOT_FUNNEL_PIPELINE_IDS");
      if (funnelPipelineIds.length) {
        deals = deals.filter((d: any) => funnelPipelineIds.includes(String(d?.properties?.pipeline ?? "").trim()));
      }

      // Optional: further filter by current stage IDs (UI control).
      const stageIds = Array.isArray(payload?.stage_ids) ? payload.stage_ids.map(String).filter(Boolean) : [];
      if (stageIds.length) {
        deals = deals.filter((d: any) => stageIds.includes(String(d?.properties?.dealstage ?? "").trim()));
      }
      const createdInWindow = deals.filter((d: any) => {
        const ms = toMs(d?.properties?.createdate);
        return ms != null && ms >= sinceMs && ms < untilMs;
      }).length;

      const stageLabelCounts: Record<string, number> = {};
      const stageIdCounts: Record<string, number> = {};
      for (const d of deals) {
        const sid = String(d?.properties?.dealstage ?? "").trim() || "—";
        stageIdCounts[sid] = (stageIdCounts[sid] ?? 0) + 1;
        const lbl = stageLabelById.get(sid) ?? sid;
        stageLabelCounts[lbl] = (stageLabelCounts[lbl] ?? 0) + 1;
      }
      const topStages = Object.entries(stageLabelCounts)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10)
        .map(([label, count]) => ({ label, count }));

      const funnelByChannel: any = { new_leads: {}, new_opps: {}, new_customers: {}, new_churn: {} };
      const funnelByHyp: any = { new_leads: {}, new_opps: {}, new_customers: {}, new_churn: {} };

      let newLeads = 0;
      const newOppIds = new Set<string>();
      const newCustomerIds = new Set<string>();
      const newChurnIds = new Set<string>();

      const dealsByMetric: Record<string, any[]> = {
        new_leads: [],
        new_opps: [],
        new_customers: [],
        new_churn: []
      };

      const samples: any[] = [];
      for (const d of deals) {
        const id = String(d?.id ?? "");
        const props = d?.properties ?? {};
        const channel = channelFromDealProps(props);
        const companyIds = await hubspotFetchAssociatedCompanyIdsForDeal(id, 5);
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

        const hist = await hubspotFetchDealStageHistory(id);
        const history = hist.history || [];
        // initial stage (best-effort)
        const initStageId = history.length ? String(history[0].value) : String(props?.dealstage ?? "");
        const initLabel = stageLabelById.get(initStageId) ?? initStageId;
        const initCat = stageCategoryFromLabel(initLabel || initStageId);

      // current stage category (fallback to current props)
      const curStageId = String(props?.dealstage ?? "") || initStageId;
      const curLabel = stageLabelById.get(curStageId) ?? curStageId;
      const curCat = stageCategoryFromLabel(curLabel || curStageId);

        // new lead: created in window AND initial stage is lead/sql
        const createdMs = hist.createdate ?? toMs(props?.createdate);
        const createdInWindow = createdMs != null && createdMs >= sinceMs && createdMs < untilMs;

        const baseDealRow = {
          id,
          url: portalId && id ? `https://app.hubspot.com/contacts/${portalId}/record/0-3/${id}/` : null,
          dealname: String(props?.dealname ?? ""),
          pipeline: String(props?.pipeline ?? ""),
          createdate: createdMs ? new Date(createdMs).toISOString() : (props?.createdate ?? null),
          lastmodified: props?.hs_lastmodifieddate ?? null,
          dealstage_id: String(props?.dealstage ?? ""),
          dealstage_label: stageLabelById.get(String(props?.dealstage ?? "")) ?? null,
          stage_category: curCat,
          init_category: initCat,
          channel,
          hypothesis_key: hypKey,
          hypothesis_title: hypTitle
        };

        if (createdInWindow && isLeadStage(initCat)) {
          newLeads++;
          inc(funnelByChannel.new_leads, channel);
          inc(funnelByHyp.new_leads, hypKey);
          if (dealsByMetric.new_leads.length < 800) dealsByMetric.new_leads.push(baseDealRow);
        }

      // If deal was created inside the window already in later stages, count it as new opp/customer/churn too.
      // This matches how sales teams interpret "new opp/customer" even if the deal starts at evaluate/integration.
      if (createdInWindow) {
        if (isOppStage(curCat)) {
          newOppIds.add(id);
          inc(funnelByChannel.new_opps, channel);
          inc(funnelByHyp.new_opps, hypKey);
          if (dealsByMetric.new_opps.length < 800) dealsByMetric.new_opps.push(baseDealRow);
        }
        if (isCustomerStage(curCat)) {
          newCustomerIds.add(id);
          inc(funnelByChannel.new_customers, channel);
          inc(funnelByHyp.new_customers, hypKey);
          if (dealsByMetric.new_customers.length < 800) dealsByMetric.new_customers.push(baseDealRow);
        }
        if (isChurnStage(curCat)) {
          newChurnIds.add(id);
          inc(funnelByChannel.new_churn, channel);
          inc(funnelByHyp.new_churn, hypKey);
          if (dealsByMetric.new_churn.length < 800) dealsByMetric.new_churn.push(baseDealRow);
        }
      }

        // transitions in window
        for (let i = 1; i < history.length; i++) {
          const prev = history[i - 1];
          const cur = history[i];
          const ts = cur?.timestamp ?? null;
          if (!ts || ts < sinceMs || ts >= untilMs) continue;
          const prevLabel = stageLabelById.get(String(prev.value)) ?? String(prev.value);
          const curLabel = stageLabelById.get(String(cur.value)) ?? String(cur.value);
          const prevCat = stageCategoryFromLabel(prevLabel);
        const curCat2 = stageCategoryFromLabel(curLabel);

        if (isOppStage(curCat2) && !isOppStage(prevCat)) {
            newOppIds.add(id);
            inc(funnelByChannel.new_opps, channel);
            inc(funnelByHyp.new_opps, hypKey);
          if (dealsByMetric.new_opps.length < 800) dealsByMetric.new_opps.push(baseDealRow);
          }
        if (isCustomerStage(curCat2) && !isCustomerStage(prevCat)) {
            newCustomerIds.add(id);
            inc(funnelByChannel.new_customers, channel);
            inc(funnelByHyp.new_customers, hypKey);
          if (dealsByMetric.new_customers.length < 800) dealsByMetric.new_customers.push(baseDealRow);
          }
        if (isChurnStage(curCat2) && !isChurnStage(prevCat)) {
            newChurnIds.add(id);
            inc(funnelByChannel.new_churn, channel);
            inc(funnelByHyp.new_churn, hypKey);
          if (dealsByMetric.new_churn.length < 800) dealsByMetric.new_churn.push(baseDealRow);
          }
        }

        if (payload?.debug && samples.length < 5) {
          samples.push({
            id,
            dealname: String(props?.dealname ?? ""),
            createdate_raw: props?.createdate ?? null,
            createdate_ms: createdMs ?? null,
            lastmodified_raw: props?.hs_lastmodifieddate ?? null,
            dealstage_id: String(props?.dealstage ?? ""),
            dealstage_label: stageLabelById.get(String(props?.dealstage ?? "")) ?? null,
            init_category: initCat,
            current_category: curCat,
            channel,
            hypothesis_key: hypKey
          });
        }
      }

      const row = {
        period_start: periodStart,
        period_end: periodEnd,
        window_days: 7,
        new_leads_count: newLeads,
        new_opps_count: newOppIds.size,
        new_customers_count: newCustomerIds.size,
        new_churn_count: newChurnIds.size,
        funnel_by_channel_json: funnelByChannel,
        funnel_by_hypothesis_json: funnelByHyp,
        data_json: {
          deals_scanned: deals.length,
          hypotheses_active: Array.isArray(hyps) ? hyps.length : 0,
          created_in_window: deals.filter((d: any) => {
            const ms = toMs(d?.properties?.createdate);
            return ms != null && ms >= sinceMs && ms < untilMs;
          }).length
          ,
          deals_by_metric: dealsByMetric,
          funnel_pipeline_ids: funnelPipelineIds,
          filter_stage_ids: stageIds
        },
        created_by: createdBy
      };

      await postgrestUpsert(pg, "sales_hubspot_global_snapshots", row, "period_start,window_days");
      processed.push({
        period_start: periodStart,
        period_end: periodEnd,
        deals_scanned: deals.length,
        ...(payload?.debug
          ? {
              created_in_window: createdInWindow,
              top_stages: topStages,
              stage_ids_top: Object.entries(stageIdCounts)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 10)
                .map(([id, count]) => ({ id, label: stageLabelById.get(id) ?? id, count })),
              samples
            }
          : {})
      } as any);
    }

    // Remaining missing (best-effort within range)
    let remaining_missing = 0;
    if (backfillWeeks > 0) {
      for (let i = backfillWeeks; i >= 1; i--) {
        const wk = ymd(new Date(Date.parse(`${currentWeekStart}T00:00:00.000Z`) - i * 7 * 24 * 60 * 60 * 1000));
        if (!have.has(wk) && !processed.some((p) => p.period_start === wk)) remaining_missing++;
      }
    }

    return NextResponse.json({
      ok: true,
      mode,
      processed_weeks: processed,
      remaining_missing,
      current_week_start: currentWeekStart
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


