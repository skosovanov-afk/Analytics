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

function toMs(v: any): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

function ymd(d: Date) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function parseCsvEnv(name: string) {
  const raw = String(process.env[name] ?? "").trim();
  if (!raw) return [];
  return raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
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

function isInactiveStageLabel(labelOrId: string) {
  const t = String(labelOrId ?? "").trim().toLowerCase();
  if (!t) return false;
  return t.includes("lost") || t.includes("dormant");
}

function isActiveStageLabel(labelOrId: string) {
  return !isInactiveStageLabel(labelOrId);
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
  // Best-effort fallback when HUBSPOT_FUNNEL_PIPELINE_IDS is not configured.
  // We pick the first available pipeline (prefer ones with "sales"/"pipeline" in label).
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/pipelines/deals");
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const results = Array.isArray(json?.results) ? json.results : [];
  const scored = results
    .map((p: any) => {
      const id = String(p?.id ?? "").trim();
      const label = String(p?.label ?? "").trim().toLowerCase();
      let score = 0;
      if (label.includes("sales")) score += 2;
      if (label.includes("pipeline")) score += 1;
      if (label.includes("funnel")) score += 3;
      return { id, score };
    })
    .filter((x: any) => x.id);
  scored.sort((a: any, b: any) => b.score - a.score);
  const best = scored[0]?.id ? [scored[0].id] : [];
  return best;
}

async function hubspotSearchDealsCreatedBetweenPaged(opts: { sinceMs: number; untilMs: number; pipelineId: string; maxDeals: number }) {
  const properties = [
    "dealname",
    "dealstage",
    "pipeline",
    "createdate",
    "hs_lastmodifieddate",
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
            { propertyName: "pipeline", operator: "EQ", value: String(opts.pipelineId) },
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

async function hubspotSearchDealsModifiedBetweenPaged(opts: { sinceMs: number; untilMs: number; pipelineId: string; maxDeals: number }) {
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
            { propertyName: "pipeline", operator: "EQ", value: String(opts.pipelineId) },
            { propertyName: "hs_lastmodifieddate", operator: "GTE", value: String(opts.sinceMs) },
            { propertyName: "hs_lastmodifieddate", operator: "LT", value: String(opts.untilMs) }
          ]
        }
      ],
      sorts: [{ propertyName: "hs_lastmodifieddate", direction: "ASCENDING" }],
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
  return { dealId, createdate: toMs(props?.createdate), currentStageId: String(props?.dealstage ?? ""), history };
}

async function postgrestGet(h: PostgrestHeaders, path: string) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const url = `${supabaseUrl}/rest/v1/${path}`;
  const res = await fetch(url, { headers: h });
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
      pipeline_ids?: string[];
      backfill_days?: number;
      batch_days?: number;
      maxDealsPerDay?: number;
      maxDealsModifiedPerDay?: number;
      maxCompaniesPerTal?: number;
      force?: boolean;
    };

    let pipelineIds = Array.isArray(payload?.pipeline_ids) && payload.pipeline_ids.length ? payload.pipeline_ids.map(String) : parseCsvEnv("HUBSPOT_FUNNEL_PIPELINE_IDS");
    if (!pipelineIds.length) {
      // Fallback: auto-pick a pipeline from HubSpot to keep sync usable even if env is missing.
      pipelineIds = await hubspotPickDefaultPipelineIds();
    }
    if (!pipelineIds.length) return jsonError(400, "Missing pipeline_ids (select pipeline in UI) or HUBSPOT_FUNNEL_PIPELINE_IDS env.");

    const force = !!payload?.force;
    const backfillDays = Math.max(1, Math.min(370, Number(payload?.backfill_days ?? 365)));
    const batchDays = Math.max(1, Math.min(40, Number(payload?.batch_days ?? 14)));
    const maxDealsPerDay = Math.max(1, Math.min(5000, Number(payload?.maxDealsPerDay ?? 2000)));
    const maxDealsModifiedPerDay = Math.max(1, Math.min(2000, Number(payload?.maxDealsModifiedPerDay ?? 600)));
    const maxCompaniesPerTal = Math.max(1, Math.min(3000, Number(payload?.maxCompaniesPerTal ?? 2000)));

    const today = new Date();
    const endDay = ymd(new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate())));
    const startDay = ymd(new Date(Date.parse(`${endDay}T00:00:00.000Z`) - (backfillDays - 1) * 24 * 60 * 60 * 1000));

    // existing days for pipelines
    const existing = await postgrestGet(
      pg,
      `sales_hubspot_global_daily_snapshots?select=period_day,pipeline_id,updated_at&period_day=gte.${startDay}&period_day=lte.${endDay}&order=period_day.asc&limit=5000`
    );
    const have = new Map<string, string>(); // key = `${pipeline}|${day}` -> updated_at
    for (const r of Array.isArray(existing) ? existing : []) {
      const k = `${String(r?.pipeline_id ?? "")}|${String(r?.period_day ?? "")}`;
      have.set(k, String(r?.updated_at ?? ""));
    }

    // Build company->hypothesis map for ACTIVE hypotheses (best-effort)
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

    const toProcess: Array<{ pipeline_id: string; day: string }> = [];
    for (const pid of pipelineIds) {
      for (let i = 0; i < backfillDays; i++) {
        const day = ymd(new Date(Date.parse(`${startDay}T00:00:00.000Z`) + i * 24 * 60 * 60 * 1000));
        const k = `${pid}|${day}`;
        const upd = have.get(k) || "";
        const updMs = upd ? Date.parse(upd) : NaN;
        const isToday = day === endDay;
        const isFresh = Number.isFinite(updMs) && Date.now() - updMs < 30 * 60 * 1000;
        if (force || !have.has(k) || (isToday && !isFresh)) toProcess.push({ pipeline_id: pid, day });
      }
    }

    // IMPORTANT UX: process newest days first so the dashboard becomes актуальный immediately.
    // Otherwise a 365-day backfill starts from 1y ago and users see "history" instead of "today".
    toProcess.sort((a, b) => {
      const cd = String(b.day).localeCompare(String(a.day));
      if (cd) return cd;
      return String(a.pipeline_id).localeCompare(String(b.pipeline_id));
    });

    if (!toProcess.length) {
      return NextResponse.json({ ok: true, skipped: true, reason: "fresh", start_day: startDay, end_day: endDay, remaining_missing: 0 });
    }

    const batch = toProcess.slice(0, batchDays);
    const processed: Array<{ pipeline_id: string; period_day: string; deals_scanned: number }> = [];

    for (const item of batch) {
      const dayStart = item.day;
      const dayEnd = ymd(new Date(Date.parse(`${dayStart}T00:00:00.000Z`) + 24 * 60 * 60 * 1000));
      const sinceMs = Date.parse(`${dayStart}T00:00:00.000Z`);
      const untilMs = Date.parse(`${dayEnd}T00:00:00.000Z`);

      const deals = await hubspotSearchDealsCreatedBetweenPaged({
        sinceMs,
        untilMs,
        pipelineId: item.pipeline_id,
        maxDeals: maxDealsPerDay
      });

      // Compute net change of ACTIVE deals (A: excludes Lost/Dormant) for this day.
      // We look at deals modified in the day and compare active-status at day start vs day end using dealstage history.
      const modifiedDeals = await hubspotSearchDealsModifiedBetweenPaged({
        sinceMs,
        untilMs,
        pipelineId: item.pipeline_id,
        maxDeals: maxDealsModifiedPerDay
      });
      const modifiedIds = Array.from(new Set((modifiedDeals || []).map((d: any) => String(d?.id ?? "")).filter(Boolean)));
      let activeDelta = 0;
      const activeDeltaSamples: any[] = [];

      for (const id of modifiedIds) {
        const h = await hubspotFetchDealStageHistory(id);
        const hist = Array.isArray(h.history) ? h.history : [];

        // Stage at/before timestamp helper
        const stageAtOrBefore = (ts: number) => {
          let v: string | null = null;
          for (let i = 0; i < hist.length; i++) {
            const x = hist[i];
            if (!x?.timestamp) continue;
            if ((x.timestamp as number) <= ts) v = String(x.value);
            else break;
          }
          return v;
        };

        const beforeId = stageAtOrBefore(sinceMs - 1);
        const afterId = stageAtOrBefore(untilMs - 1);

        // If deal was created during the day and there is no "before", treat "before" as inactive (not in funnel yet).
        const createdMs = Number(h.createdate ?? NaN);
        const createdInWindow = Number.isFinite(createdMs) && createdMs >= sinceMs && createdMs < untilMs;
        const effBeforeId = beforeId ?? (createdInWindow ? "__none__" : null);
        const currentStageId = String(h.currentStageId ?? "").trim();
        const effAfterId =
          afterId ??
          (currentStageId || (effBeforeId && effBeforeId !== "__none__" ? String(effBeforeId) : "") || "__none__");

        const beforeLabel = effBeforeId === "__none__" ? "__none__" : (stageLabelById.get(String(effBeforeId)) ?? String(effBeforeId ?? ""));
        const afterLabel = effAfterId === "__none__" ? "__none__" : (stageLabelById.get(String(effAfterId)) ?? String(effAfterId ?? ""));

        const beforeActive = effBeforeId !== "__none__" && isActiveStageLabel(beforeLabel);
        const afterActive = effAfterId !== "__none__" && isActiveStageLabel(afterLabel);
        const delta = beforeActive === afterActive ? 0 : afterActive ? 1 : -1;
        activeDelta += delta;

        if (activeDeltaSamples.length < 20 && delta !== 0) {
          activeDeltaSamples.push({
            id,
            created_in_day: createdInWindow,
            before: beforeLabel,
            after: afterLabel,
            delta
          });
        }
      }

      const byChannel: Record<string, number> = {};
      const byHyp: Record<string, number> = {};
      const sampleDeals: any[] = [];

      for (const d of deals) {
        const id = String(d?.id ?? "");
        const props = d?.properties ?? {};
        const channel = channelFromDealProps(props);
        byChannel[channel] = (byChannel[channel] ?? 0) + 1;

        const companyIds = id ? await hubspotFetchAssociatedCompanyIdsForDeal(id, 5) : [];
        let hypId: string | null = null;
        for (const cid of companyIds) {
          const h = companyToHyp.get(cid) ?? null;
          if (h) {
            hypId = h;
            break;
          }
        }
        const hypKey = hypId || "__unassigned__";
        byHyp[hypKey] = (byHyp[hypKey] ?? 0) + 1;

        if (sampleDeals.length < 25) {
          const createdMs = toMs(props?.createdate);
          sampleDeals.push({
            id,
            dealname: String(props?.dealname ?? ""),
            createdate: createdMs ? new Date(createdMs).toISOString() : (props?.createdate ?? null),
            channel,
            hypothesis_key: hypKey,
            hypothesis_title: hypId ? (hypTitleById.get(hypId) ?? hypId) : "Unassigned"
          });
        }
      }

      const row = {
        period_day: dayStart,
        pipeline_id: item.pipeline_id,
        new_deals_count: deals.length,
        active_delta_count: activeDelta,
        new_deals_by_channel_json: byChannel,
        new_deals_by_hypothesis_json: byHyp,
        data_json: {
          deals_scanned: deals.length,
          funnel_pipeline_id: item.pipeline_id,
          sample_deals: sampleDeals,
          active_delta_count: activeDelta,
          active_delta_samples: activeDeltaSamples,
          deals_modified_scanned: modifiedIds.length
        },
        created_by: createdBy
      };

      await postgrestUpsert(pg, "sales_hubspot_global_daily_snapshots", row, "period_day,pipeline_id");
      processed.push({ pipeline_id: item.pipeline_id, period_day: dayStart, deals_scanned: deals.length });
    }

    const remaining_missing = Math.max(0, toProcess.length - batch.length);
    return NextResponse.json({
      ok: true,
      start_day: startDay,
      end_day: endDay,
      processed_days: processed,
      remaining_missing
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


