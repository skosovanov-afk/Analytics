import { NextResponse } from "next/server";

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

function toMs(v: any): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

function addDayExclusive(ymd: string) {
  const t = Date.parse(`${ymd}T00:00:00.000Z`);
  if (!Number.isFinite(t)) return "";
  const d = new Date(t + 86400000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
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

async function hubspotSearchDealsCreatedBetweenPaged(opts: { sinceMs: number; untilMs: number; pipelineIds: string[]; maxDeals: number }) {
  const properties = ["dealname", "dealstage", "pipeline", "createdate", "hs_lastmodifieddate"];
  const out: any[] = [];
  let after: string | null = null;
  while (out.length < opts.maxDeals) {
    const body: any = {
      filterGroups: [
        {
          filters: [
            { propertyName: "pipeline", operator: "IN", values: opts.pipelineIds.map(String) },
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

async function hubspotBatchReadDealsWithStageHistory(ids: string[]) {
  const inputs = ids.map((id) => ({ id: String(id) }));
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/batch/read", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      properties: ["dealname", "dealstage", "pipeline", "createdate", "hs_lastmodifieddate"],
      propertiesWithHistory: ["dealstage"],
      inputs
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  return Array.isArray(json?.results) ? json.results : [];
}

function stageCategoryFromLabel(labelOrId: string) {
  const t = String(labelOrId ?? "").trim().toLowerCase();
  if (!t) return "unknown";
  if (t.includes("lead")) return "lead";
  if (t.includes("sql")) return "sql";
  if (t.includes("evaluate")) return "evaluate";
  if (t.includes("select")) return "select";
  if (t.includes("negot")) return "negotiate";
  if (t.includes("purchase")) return "purchase";
  if (t.includes("integrat")) return "integration";
  if (t.includes("active")) return "active";
  if (t.includes("lost")) return "lost";
  if (t.includes("dormant")) return "dormant";
  if (t.includes("churn")) return "churn";
  return "unknown";
}

type FunnelBucket = "lead" | "sql" | "opportunity" | "clients";
function bucketFromStageCategory(cat: string): FunnelBucket | null {
  if (cat === "lead") return "lead";
  if (cat === "sql") return "sql";
  if (cat === "evaluate" || cat === "select" || cat === "negotiate" || cat === "purchase") return "opportunity";
  if (cat === "integration" || cat === "active") return "clients";
  return null;
}

const BUCKET_ORDER: FunnelBucket[] = ["lead", "sql", "opportunity", "clients"];

type LossBucket = "lost";
type AnyBucket = FunnelBucket | LossBucket;

function lossBucketFromStageCategory(cat: string): LossBucket | null {
  if (cat === "lost" || cat === "dormant" || cat === "churn") return "lost";
  return null;
}

function bucketFromStageCategoryAny(cat: string): AnyBucket | null {
  return (bucketFromStageCategory(cat) as AnyBucket | null) ?? lossBucketFromStageCategory(cat);
}

function bucketOrderIndex(b: AnyBucket) {
  if (b === "lost") return -1; // loss is terminal/outside linear funnel
  return BUCKET_ORDER.indexOf(b as FunnelBucket);
}

function isInWindowTs(ts: number, sinceMs: number, untilMs: number) {
  return Number.isFinite(ts) && ts >= sinceMs && ts < untilMs;
}

async function hubspotSearchDealsModifiedBetweenPaged(opts: { sinceMs: number; untilMs: number; pipelineIds: string[]; maxDeals: number }) {
  const properties = ["dealname", "dealstage", "pipeline", "createdate", "hs_lastmodifieddate"];
  const out: any[] = [];
  let after: string | null = null;
  while (out.length < opts.maxDeals) {
    const body: any = {
      filterGroups: [
        {
          filters: [
            { propertyName: "pipeline", operator: "IN", values: opts.pipelineIds.map(String) },
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

function quantile(xs: number[], q: number) {
  if (!xs.length) return null;
  const arr = xs.slice().sort((a, b) => a - b);
  const p = Math.max(0, Math.min(1, q));
  const idx = (arr.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return arr[lo];
  const w = idx - lo;
  return arr[lo] * (1 - w) + arr[hi] * w;
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
      since: string; // inclusive ymd
      until: string; // inclusive ymd
      pipeline_ids: string[];
      maxDeals?: number;
      mode?: "cohort_created" | "in_window";
    };

    const since = String(payload?.since ?? "").trim();
    const until = String(payload?.until ?? "").trim();
    const defaultPipelineIds = String(process.env.HUBSPOT_FUNNEL_PIPELINE_IDS ?? "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const pipelineIds = Array.isArray(payload?.pipeline_ids) ? payload.pipeline_ids.map(String).filter(Boolean) : [];
    let effectivePipelineIds = pipelineIds.length ? pipelineIds : defaultPipelineIds;
    if (!effectivePipelineIds.length) effectivePipelineIds = await hubspotPickDefaultPipelineIds();
    const maxDeals = Math.max(1, Math.min(2000, Number(payload?.maxDeals ?? 800)));
    const mode = (String(payload?.mode ?? "cohort_created") as any) as "cohort_created" | "in_window";

    if (!/^\d{4}-\d{2}-\d{2}$/.test(since)) return jsonError(400, "Bad request: since must be YYYY-MM-DD");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(until)) return jsonError(400, "Bad request: until must be YYYY-MM-DD");
    if (!effectivePipelineIds.length) return jsonError(400, "Bad request: pipeline_ids is required (or set HUBSPOT_FUNNEL_PIPELINE_IDS)");

    const sinceMs = Date.parse(`${since}T00:00:00.000Z`);
    const untilExcl = addDayExclusive(until);
    const untilMs = Date.parse(`${untilExcl}T00:00:00.000Z`);
    if (!Number.isFinite(sinceMs) || !Number.isFinite(untilMs) || untilMs <= sinceMs) return jsonError(400, "Bad request: invalid date range");

    const stageLabelById = await hubspotFetchDealStageLabels();
    const deals =
      mode === "in_window"
        ? await hubspotSearchDealsModifiedBetweenPaged({ sinceMs, untilMs, pipelineIds: effectivePipelineIds, maxDeals })
        : await hubspotSearchDealsCreatedBetweenPaged({ sinceMs, untilMs, pipelineIds: effectivePipelineIds, maxDeals });
    const dealIds = deals.map((d: any) => String(d?.id ?? "").trim()).filter(Boolean);

    // Fetch stage histories in batches to keep rate limits manageable.
    const results: any[] = [];
    // HubSpot limitation: batch read with property histories supports up to 50 inputs.
    for (let i = 0; i < dealIds.length; i += 50) {
      const batch = dealIds.slice(i, i + 50);
      const rows = await hubspotBatchReadDealsWithStageHistory(batch);
      results.push(...rows);
    }

    const reached: Record<AnyBucket, number> = { lead: 0, sql: 0, opportunity: 0, clients: 0, lost: 0 };
    const cohort = results.length;
    const daysToClients: number[] = [];
    const daysToLost: number[] = [];

    // For cohort mode, "X → Lost" should be logically bounded (<= 100%).
    // Count deals that reached the FROM bucket and then reached Lost later.
    // This prevents inflated rates when a deal becomes Lost without ever reaching Opportunity.
    const lostAfter: Record<FunnelBucket, number> = { lead: 0, sql: 0, opportunity: 0, clients: 0 };

    const reachedInWindow: Record<AnyBucket, number> = { lead: 0, sql: 0, opportunity: 0, clients: 0, lost: 0 };
    const transitionsInWindow: Record<string, number> = {};

    for (const r of results) {
      const currentStageId = String(r?.properties?.dealstage ?? "").trim();
      const history = Array.isArray(r?.propertiesWithHistory?.dealstage) ? r.propertiesWithHistory.dealstage : [];
      const createdMs = toMs(r?.properties?.createdate);

      const bucketSet = new Set<AnyBucket>();
      let clientsFirstMs: number | null = null;
      let lostFirstMs: number | null = null;
      let leadFirstMs: number | null = null;
      let sqlFirstMs: number | null = null;
      let oppFirstMs: number | null = null;

      // Build sorted events.
      const events: Array<{ at: number; bucket: AnyBucket }> = [];

      for (const h of history) {
        const stageId = String(h?.value ?? "").trim();
        const ts = toMs(h?.timestamp);
        const label = stageLabelById.get(stageId) ?? stageId;
        const cat = stageCategoryFromLabel(label);
        const b = bucketFromStageCategoryAny(cat);
        if (b && ts) {
          bucketSet.add(b);
          events.push({ at: ts, bucket: b });
          if (b === "lead" && (!leadFirstMs || ts < leadFirstMs)) leadFirstMs = ts;
          if (b === "sql" && (!sqlFirstMs || ts < sqlFirstMs)) sqlFirstMs = ts;
          if (b === "opportunity" && (!oppFirstMs || ts < oppFirstMs)) oppFirstMs = ts;
          if (b === "clients" && (!clientsFirstMs || ts < clientsFirstMs)) clientsFirstMs = ts;
          if (b === "lost" && (!lostFirstMs || ts < lostFirstMs)) lostFirstMs = ts;
        }
      }

      if (currentStageId) {
        const label = stageLabelById.get(currentStageId) ?? currentStageId;
        const cat = stageCategoryFromLabel(label);
        const b = bucketFromStageCategoryAny(cat);
        if (b) bucketSet.add(b);
      }

      // Cohort reach: if you reached a later bucket (clients), assume you passed earlier funnel buckets.
      // Lost is tracked separately (terminal); it does not imply passing earlier steps.
      let maxIdx = -1;
      for (const b of bucketSet) maxIdx = Math.max(maxIdx, bucketOrderIndex(b));
      if (maxIdx >= 0) {
        for (let i = 0; i <= maxIdx; i++) reached[BUCKET_ORDER[i]] += 1;
      }
      if (bucketSet.has("lost")) reached.lost += 1;

      // Cohort "X → Lost": reached X AND later reached Lost.
      // Use createdMs as a fallback for lead because some imported deals may lack early stage history.
      if (lostFirstMs) {
        const leadTs = leadFirstMs ?? createdMs ?? null;
        if (leadTs && lostFirstMs >= leadTs) lostAfter.lead += 1;
        if (sqlFirstMs && lostFirstMs >= sqlFirstMs) lostAfter.sql += 1;
        if (oppFirstMs && lostFirstMs >= oppFirstMs) lostAfter.opportunity += 1;
        if (clientsFirstMs && lostFirstMs >= clientsFirstMs) lostAfter.clients += 1;
      }

      if (createdMs && clientsFirstMs && clientsFirstMs >= createdMs) {
        const d = (clientsFirstMs - createdMs) / 86400000;
        if (Number.isFinite(d) && d >= 0) daysToClients.push(d);
      }
      if (createdMs && lostFirstMs && lostFirstMs >= createdMs) {
        const d = (lostFirstMs - createdMs) / 86400000;
        if (Number.isFinite(d) && d >= 0) daysToLost.push(d);
      }

      // In-window reach + transitions: only meaningful for mode=in_window
      if (mode === "in_window" && events.length) {
        events.sort((a, b) => a.at - b.at);
        const setIn = new Set<AnyBucket>();
        let prevBucket: AnyBucket | null = null;
        for (const ev of events) {
          if (!isInWindowTs(ev.at, sinceMs, untilMs)) {
            prevBucket = ev.bucket;
            continue;
          }
          setIn.add(ev.bucket);
          if (prevBucket && prevBucket !== ev.bucket) {
            const key = `${prevBucket}->${ev.bucket}`;
            transitionsInWindow[key] = (transitionsInWindow[key] ?? 0) + 1;
          }
          prevBucket = ev.bucket;
        }
        for (const b of setIn) reachedInWindow[b] += 1;
      }
    }

    const rate = (a: number, b: number) => (b > 0 ? a / b : 0);
    const conversions = {
      lead_to_sql: { from: reached.lead, to: reached.sql, rate: rate(reached.sql, reached.lead) },
      sql_to_opportunity: { from: reached.sql, to: reached.opportunity, rate: rate(reached.opportunity, reached.sql) },
      opportunity_to_clients: { from: reached.opportunity, to: reached.clients, rate: rate(reached.clients, reached.opportunity) },
      lead_to_clients: { from: reached.lead, to: reached.clients, rate: rate(reached.clients, reached.lead) },
      // IMPORTANT: for cohort mode, "X → Lost" is "reached X AND later reached Lost" (not just reached Lost).
      opportunity_to_lost: { from: reached.opportunity, to: lostAfter.opportunity, rate: rate(lostAfter.opportunity, reached.opportunity) },
      sql_to_lost: { from: reached.sql, to: lostAfter.sql, rate: rate(lostAfter.sql, reached.sql) },
      lead_to_lost: { from: reached.lead, to: lostAfter.lead, rate: rate(lostAfter.lead, reached.lead) }
    };

    const leadsPerClient = reached.clients > 0 ? reached.lead / reached.clients : null;
    const timeToClients = {
      n: daysToClients.length,
      median_days: quantile(daysToClients, 0.5),
      p75_days: quantile(daysToClients, 0.75)
    };
    const timeToLost = {
      n: daysToLost.length,
      median_days: quantile(daysToLost, 0.5),
      p75_days: quantile(daysToLost, 0.75)
    };

    const inWindowStats = mode === "in_window"
      ? {
          reached: reachedInWindow,
          transitions: transitionsInWindow,
          conversions: {
            lead_to_sql: {
              from: reachedInWindow.lead,
              to: transitionsInWindow["lead->sql"] ?? 0,
              rate: rate(transitionsInWindow["lead->sql"] ?? 0, reachedInWindow.lead)
            },
            sql_to_opportunity: {
              from: reachedInWindow.sql,
              to: transitionsInWindow["sql->opportunity"] ?? 0,
              rate: rate(transitionsInWindow["sql->opportunity"] ?? 0, reachedInWindow.sql)
            },
            opportunity_to_clients: {
              from: reachedInWindow.opportunity,
              to: transitionsInWindow["opportunity->clients"] ?? 0,
              rate: rate(transitionsInWindow["opportunity->clients"] ?? 0, reachedInWindow.opportunity)
            },
            opportunity_to_lost: {
              from: reachedInWindow.opportunity,
              to: transitionsInWindow["opportunity->lost"] ?? 0,
              rate: rate(transitionsInWindow["opportunity->lost"] ?? 0, reachedInWindow.opportunity)
            }
          }
        }
      : null;

    return NextResponse.json({
      ok: true,
      since,
      until,
      pipeline_ids: pipelineIds,
      mode,
      cohort,
      reached,
      conversions,
      leads_per_client: leadsPerClient,
      time_to_clients_days: timeToClients,
      time_to_lost_days: timeToLost,
      in_window: inWindowStats
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


