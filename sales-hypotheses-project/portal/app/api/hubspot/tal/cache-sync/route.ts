import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

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

function parseHubspotListIdFromUrl(url: string) {
  const t = String(url ?? "").trim();
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  return m?.[1] ? m[1] : null;
}

function parseHubspotDealsViewIdFromUrl(url: string) {
  const t = String(url ?? "").trim();
  const m = t.match(/\/views\/(\d+)(?:\b|\/|\?|#)/i);
  return m?.[1] ? m[1] : null;
}

function getBearerToken(req: Request) {
  const authHeader = String(req.headers.get("authorization") ?? "").trim();
  const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
  const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
  return { bearer, gotSecret };
}

function isCronAuthorized(req: Request) {
  const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
  if (vercelCron === "1" || vercelCron.toLowerCase() === "true") return true;
  const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
  if (!cronSecret) return false;
  const { bearer, gotSecret } = getBearerToken(req);
  return bearer === cronSecret || gotSecret === cronSecret;
}

let lastHubspotCallAt = 0;
async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

function normalizeEmail(v: any) {
  return String(v ?? "").trim().toLowerCase();
}

const cachedOwnerIdByEmail = new Map<string, string | null>();
async function hubspotOwnerIdByEmail(rawEmail: string): Promise<string | null> {
  const email = normalizeEmail(rawEmail);
  if (!email) return null;
  if (cachedOwnerIdByEmail.has(email)) return cachedOwnerIdByEmail.get(email) ?? null;

  const res = await hubspotFetch(
    `https://api.hubapi.com/crm/v3/owners/?email=${encodeURIComponent(email)}&includeInactive=true`
  );
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot owners lookup failed"));
  const results = Array.isArray(json?.results) ? json.results : [];
  const r = results.find((o: any) => normalizeEmail(o?.email) === email) ?? results[0] ?? null;
  const id = r?.id != null ? String(r.id) : "";
  const resolved = id || null;
  cachedOwnerIdByEmail.set(email, resolved);
  return resolved;
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

    const retryAfter = Number(res.headers.get("retry-after") || "");
    const backoff = Number.isFinite(retryAfter) ? retryAfter * 1000 : 1200 + attempt * 800;
    await sleep(Math.min(10_000, backoff));
  }

  throw new Error("HubSpot rate limit: too many requests (429). Try again in ~10 seconds.");
}

async function hubspotListMembershipCompanyIds(listId: string, limit: number, after: string | null) {
  // HubSpot CRM Lists API memberships endpoint:
  // GET /crm/v3/lists/{listId}/memberships
  // Ref: https://developers.hubspot.com/docs/api-reference/crm/lists-v3/lists/get-crm-v3-lists-listId-memberships
  const qs = new URLSearchParams();
  qs.set("limit", String(Math.max(1, Math.min(500, limit))));
  if (after) qs.set("after", after);
  const url = `https://api.hubapi.com/crm/v3/lists/${encodeURIComponent(listId)}/memberships?${qs.toString()}`;
  const res = await hubspotFetch(url);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const ids: number[] = (Array.isArray(json?.results) ? json.results : [])
    .map((r: any) => String(r?.recordId ?? r?.id ?? "").trim())
    .filter(Boolean)
    .map((s: string) => Number(s))
    .filter((n: number) => Number.isFinite(n) && n > 0);
  const nextAfter = json?.paging?.next?.after ? String(json.paging.next.after) : null;
  return { ids, nextAfter };
}

async function hubspotListMembershipContactIds(listId: string, limit: number, after: string | null) {
  // Same CRM Lists memberships endpoint works for contact lists.
  const qs = new URLSearchParams();
  qs.set("limit", String(Math.max(1, Math.min(500, limit))));
  if (after) qs.set("after", after);
  const url = `https://api.hubapi.com/crm/v3/lists/${encodeURIComponent(listId)}/memberships?${qs.toString()}`;
  const res = await hubspotFetch(url);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const ids: number[] = (Array.isArray(json?.results) ? json.results : [])
    .map((r: any) => String(r?.recordId ?? r?.id ?? "").trim())
    .filter(Boolean)
    .map((s: string) => Number(s))
    .filter((n: number) => Number.isFinite(n) && n > 0);
  const nextAfter = json?.paging?.next?.after ? String(json.paging.next.after) : null;
  return { ids, nextAfter };
}

function safeJsonParse(input: any) {
  if (typeof input !== "string") return input;
  try {
    return JSON.parse(input);
  } catch {
    return null;
  }
}

function extractHubspotViewFilterGroups(json: any) {
  if (Array.isArray(json?.filterGroups)) return json.filterGroups;
  if (Array.isArray(json?.filters)) return [{ filters: json.filters }];
  const state = safeJsonParse(json?.state);
  if (Array.isArray(state?.filterGroups)) return state.filterGroups;
  if (Array.isArray(state?.filters)) return [{ filters: state.filters }];
  if (Array.isArray(state?.query?.filterGroups)) return state.query.filterGroups;
  if (Array.isArray(state?.query?.filters)) return [{ filters: state.query.filters }];
  return null;
}

async function hubspotFetchDealsViewFilterGroups(viewId: string) {
  const endpoints = [
    `https://api.hubapi.com/crm/v3/views/deals/${encodeURIComponent(viewId)}`,
    `https://api.hubapi.com/crm/v3/views/0-3/${encodeURIComponent(viewId)}`,
    `https://api.hubapi.com/crm/v3/views/deal/${encodeURIComponent(viewId)}`
  ];
  for (const url of endpoints) {
    const res = await hubspotFetch(url);
    const json = (await res.json()) as any;
    if (!res.ok) continue;
    const groups = extractHubspotViewFilterGroups(json);
    if (groups && groups.length) return groups;
  }
  return null;
}

async function hubspotSearchDealsByFilterGroups(filterGroups: any[], ownerId: string | null) {
  const groups = Array.isArray(filterGroups) ? filterGroups : [];
  if (!groups.length) return [];

  const normalizedGroups = groups
    .map((g) => ({
      filters: Array.isArray(g?.filters) ? g.filters.filter((f: any) => f && f.propertyName && f.operator) : []
    }))
    .filter((g) => g.filters.length > 0);

  if (!normalizedGroups.length) return [];

  const ownerFilter = ownerId
    ? { propertyName: "hubspot_owner_id", operator: "EQ", value: String(ownerId) }
    : null;

  const effectiveGroups = ownerFilter
    ? normalizedGroups.map((g) => ({ filters: [...g.filters, ownerFilter] }))
    : normalizedGroups;

  const results: any[] = [];
  let after: string | undefined;
  for (let i = 0; i < 200; i++) {
    const body = {
      filterGroups: effectiveGroups,
      properties: ["dealstage", "hubspot_owner_id", "pipeline"],
      limit: 200,
      ...(after ? { after } : {})
    };
    const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot deals search failed"));
    const batch = Array.isArray(json?.results) ? json.results : [];
    results.push(...batch);
    after = json?.paging?.next?.after ? String(json.paging.next.after) : undefined;
    if (!after || !batch.length) break;
  }
  return results;
}

async function hubspotSearchDealsByTalCategory(opts: {
  talCategory: string;
  ownerId: string | null;
  pipelineAllow: Set<string>;
}) {
  const filters: any[] = [
    { propertyName: "tal_category", operator: "EQ", value: String(opts.talCategory) }
  ];
  if (opts.ownerId) {
    filters.push({ propertyName: "hubspot_owner_id", operator: "EQ", value: String(opts.ownerId) });
  }
  if (opts.pipelineAllow.size) {
    const pipelines = Array.from(opts.pipelineAllow);
    if (pipelines.length === 1) {
      filters.push({ propertyName: "pipeline", operator: "EQ", value: pipelines[0] });
    } else {
      filters.push({ propertyName: "pipeline", operator: "IN", values: pipelines });
    }
  }

  const results: any[] = [];
  let after: string | undefined;
  for (let i = 0; i < 200; i++) {
    const body = {
      filterGroups: [{ filters }],
      properties: ["dealstage", "hubspot_owner_id", "pipeline", "tal_category"],
      limit: 200,
      ...(after ? { after } : {})
    };
    const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot deals search failed"));
    const batch = Array.isArray(json?.results) ? json.results : [];
    results.push(...batch);
    after = json?.paging?.next?.after ? String(json.paging.next.after) : undefined;
    if (!after || !batch.length) break;
  }
  return results;
}

async function hubspotCompanyDealIds(companyId: number) {
  const out: number[] = [];
  let after: string | null = null;
  while (true) {
    const qs = new URLSearchParams();
    qs.set("limit", "500");
    if (after) qs.set("after", after);
    const url = `https://api.hubapi.com/crm/v3/objects/companies/${encodeURIComponent(String(companyId))}/associations/deals?${qs.toString()}`;
    const res = await hubspotFetch(url);
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    const ids: number[] = (Array.isArray(json?.results) ? json.results : [])
      .map((r: any) => String(r?.id ?? r?.toObjectId ?? "").trim())
      .filter(Boolean)
      .map((s: string) => Number(s))
      .filter((n: number) => Number.isFinite(n) && n > 0);
    out.push(...ids);
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after) break;
  }
  return out;
}

async function hubspotCompanyContactIds(companyId: number) {
  const out: number[] = [];
  let after: string | null = null;
  while (true) {
    const qs = new URLSearchParams();
    qs.set("limit", "500");
    if (after) qs.set("after", after);
    const url = `https://api.hubapi.com/crm/v3/objects/companies/${encodeURIComponent(String(companyId))}/associations/contacts?${qs.toString()}`;
    const res = await hubspotFetch(url);
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    const ids: number[] = (Array.isArray(json?.results) ? json.results : [])
      .map((r: any) => String(r?.id ?? r?.toObjectId ?? "").trim())
      .filter(Boolean)
      .map((s: string) => Number(s))
      .filter((n: number) => Number.isFinite(n) && n > 0);
    out.push(...ids);
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after) break;
  }
  return out;
}

async function hubspotDealContactIds(dealId: number) {
  const out: number[] = [];
  let after: string | null = null;
  while (true) {
    const qs = new URLSearchParams();
    qs.set("limit", "500");
    if (after) qs.set("after", after);
    const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(String(dealId))}/associations/contacts?${qs.toString()}`;
    const res = await hubspotFetch(url);
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    const ids: number[] = (Array.isArray(json?.results) ? json.results : [])
      .map((r: any) => String(r?.id ?? r?.toObjectId ?? "").trim())
      .filter(Boolean)
      .map((s: string) => Number(s))
      .filter((n: number) => Number.isFinite(n) && n > 0);
    out.push(...ids);
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after) break;
  }
  return out;
}

async function hubspotBatchAssociations(
  fromObject: "companies" | "deals" | "contacts",
  toObject: "deals" | "contacts" | "companies",
  fromIds: number[]
) {
  const ids = (fromIds ?? []).filter((n) => Number.isFinite(n) && n > 0);
  if (!ids.length) return new Map<number, number[]>();
  const chunkSize = 100;
  const out = new Map<number, number[]>();
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const res = await hubspotFetch(`https://api.hubapi.com/crm/v3/associations/${fromObject}/${toObject}/batch/read`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inputs: chunk.map((id) => ({ id: String(id) })) })
    });
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    const results = Array.isArray(json?.results) ? json.results : [];
    for (const r of results) {
      const fromId = Number(r?.from?.id ?? r?.from ?? 0);
      if (!Number.isFinite(fromId) || fromId <= 0) continue;
      const targets = Array.isArray(r?.to) ? r.to : [];
      const toIds = targets
        .map((t: any) => Number(t?.id ?? t?.toObjectId ?? 0))
        .filter((n: number) => Number.isFinite(n) && n > 0);
      if (!out.has(fromId)) out.set(fromId, []);
      if (toIds.length) out.get(fromId)?.push(...toIds);
    }
  }
  return out;
}

type DealStageMeta = { label: string; isClosed: boolean };

async function hubspotFetchDealStageMeta() {
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/pipelines/deals");
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const stageMetaById = new Map<string, DealStageMeta>();
  const results = Array.isArray(json?.results) ? json.results : [];
  for (const p of results) {
    for (const s of Array.isArray(p?.stages) ? p.stages : []) {
      const id = String(s?.id ?? "").trim();
      const label = String(s?.label ?? "").trim();
      const rawClosed = s?.metadata?.isClosed ?? s?.metadata?.closed;
      const isClosed = rawClosed === true || String(rawClosed).toLowerCase() === "true";
      if (id) stageMetaById.set(id, { label: label || id, isClosed });
    }
  }
  return stageMetaById;
}

function stageCategoryFromStage(labelOrId: string, meta?: DealStageMeta | null) {
  const t = String(labelOrId ?? "").trim().toLowerCase();
  if (!t) return "unknown";
  const hasWord = (w: string) => new RegExp(`\\b${w}\\b`, "i").test(t);
  // Strict mapping per user's pipeline:
  // Leads = "Lead", "SQL"
  // Opps = "Evaluate", "Select", "Negotiate", "Purchase"
  // Lost/Customer remain as before
  if ((hasWord("closed") && hasWord("lost")) || hasWord("dormant") || hasWord("churn") || hasWord("lost")) return "lost";
  if ((hasWord("closed") && hasWord("won")) || hasWord("customer") || hasWord("integrat") || hasWord("active")) return "customer";
  if (hasWord("lead") || hasWord("sql")) return "lead";
  if (hasWord("evaluate") || hasWord("select") || hasWord("negotiate") || hasWord("purchase")) return "opportunity";
  if (meta?.isClosed) return "lost";
  return "other";
}

async function hubspotBatchReadDeals(dealIds: number[]) {
  const ids = (dealIds ?? []).filter((n) => Number.isFinite(n) && n > 0);
  if (!ids.length) return [];
  const chunkSize = 100; // HubSpot batch read supports up to 100 inputs
  const out: any[] = [];
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/batch/read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        // Include fields used by the UI table (amount/channel/createdate).
        properties: ["dealstage", "pipeline", "tal_category", "hubspot_owner_id", "dealname", "amount", "channel", "createdate"],
        inputs: chunk.map((id) => ({ id: String(id) }))
      })
    });
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    const results = Array.isArray(json?.results) ? json.results : [];
    out.push(...results);
  }
  return out;
}

/**
 * Persist HubSpot deal metadata for DB-side metrics.
 *
 * Why: later analytics counts need pipeline + stage info without re-calling HubSpot.
 */
async function persistHubspotDeals(
  supabaseAdmin: any,
  deals: any[],
  stageMetaById: Map<string, DealStageMeta>
) {
  const rows = (deals ?? [])
    .map((d: any) => {
      const dealId = Number(d?.id ?? 0);
      if (!Number.isFinite(dealId) || dealId <= 0) return null;
      const pipelineId = String(d?.properties?.pipeline ?? "").trim();
      const stageId = String(d?.properties?.dealstage ?? "").trim();
      const label = stageMetaById.get(stageId)?.label || stageId;
      const stageCategory = stageCategoryFromStage(label, stageMetaById.get(stageId));
      return {
        deal_id: dealId,
        pipeline_id: pipelineId || null,
        dealstage_id: stageId || null,
        stage_label: label || null,
        stage_category: stageCategory || null,
        owner_id: String(d?.properties?.hubspot_owner_id ?? "").trim() || null,
        tal_category: String(d?.properties?.tal_category ?? "").trim() || null,
        dealname: String(d?.properties?.dealname ?? "").trim() || null,
        amount: String(d?.properties?.amount ?? "").trim() || null,
        channel: String(d?.properties?.channel ?? "").trim() || null,
        createdate: String(d?.properties?.createdate ?? "").trim() || null,
        updated_at: new Date().toISOString()
      };
    })
    .filter(Boolean);

  if (!rows.length) return;
  const up = await supabaseAdmin
    .from("sales_hubspot_deals")
    .upsert(rows, { onConflict: "deal_id" });
  if (up.error) throw up.error;
}

async function computeExactCounts(supabaseAdmin: any, listId: string) {
  const res = await supabaseAdmin.rpc("sales_hubspot_tal_exact_counts", { p_tal_list_id: listId });
  if (res.error) throw res.error;
  const row = Array.isArray(res.data) ? res.data[0] : res.data;
  return {
    companiesCount: Number(row?.companies_count ?? 0) || 0,
    dealsCount: Number(row?.deals_count ?? 0) || 0,
    contactsCount: Number(row?.contacts_count ?? 0) || 0
  };
}

async function processJobSlice(
  supabaseAdmin: any,
  job: any,
  opts: { batchMembershipPages: number; batchCompanies: number; batchDeals: number; batchContacts: number }
) {
  const jobId = String(job?.id ?? "");
  const listId = String(job?.tal_list_id ?? "").trim();
  if (!jobId || !listId) throw new Error("Invalid job (missing id/tal_list_id)");

  const phase = String(job?.phase ?? "memberships");

  if (phase === "memberships") {
    let after = job.memberships_after ? String(job.memberships_after) : null;
    let pagesDone = 0;
    while (pagesDone < opts.batchMembershipPages) {
      const { ids, nextAfter } = await hubspotListMembershipCompanyIds(listId, 500, after);
      after = nextAfter;
      pagesDone++;
      if (ids.length) {
        const rows = ids.map((cid: number) => ({ tal_list_id: listId, company_id: cid }));
        const up = await supabaseAdmin.from("sales_hubspot_tal_companies").upsert(rows, { onConflict: "tal_list_id,company_id" });
        if (up.error) throw up.error;
      }
      if (!after) break;
    }

    const countRes = await supabaseAdmin
      .from("sales_hubspot_tal_companies")
      .select("company_id", { count: "exact", head: true })
      .eq("tal_list_id", listId);
    if (countRes.error) throw countRes.error;
    const companiesCount = Number(countRes.count ?? 0) || 0;

    const contactsListId = String(job?.contacts_list_id ?? "").trim();
    const nextPhase = after ? "memberships" : contactsListId ? "contacts_list" : "company_contacts";
    const upd = await supabaseAdmin
      .from("sales_hubspot_tal_cache_jobs")
      .update({
        memberships_after: after,
        phase: nextPhase,
        companies_total: companiesCount,
        companies_processed: 0,
        last_company_id: 0,
        status: "running",
        updated_at: new Date().toISOString()
      })
      .eq("id", jobId)
      .select("*")
      .single();
    if (upd.error) throw upd.error;
    return { job: upd.data, done: false };
  }

  if (phase === "contacts_list") {
    const contactsListId = String(job?.contacts_list_id ?? "").trim();
    if (!contactsListId) {
      const upd = await supabaseAdmin
        .from("sales_hubspot_tal_cache_jobs")
        .update({ phase: "company_contacts", status: "running", last_company_id: 0 })
        .eq("id", jobId)
        .select("*")
        .single();
      if (upd.error) throw upd.error;
      return { job: upd.data, done: false };
    }

    let after = job.contacts_after ? String(job.contacts_after) : null;
    let pagesDone = 0;
    while (pagesDone < opts.batchMembershipPages) {
      const { ids, nextAfter } = await hubspotListMembershipContactIds(contactsListId, 500, after);
      after = nextAfter;
      pagesDone++;
      if (ids.length) {
        const rows = ids.map((cid: number) => ({ tal_list_id: listId, contact_id: cid }));
        const up = await supabaseAdmin.from("sales_hubspot_tal_contacts").upsert(rows, { onConflict: "tal_list_id,contact_id" });
        if (up.error) throw up.error;
      }
      if (!after) break;
    }

    const countRes = await supabaseAdmin
      .from("sales_hubspot_tal_contacts")
      .select("contact_id", { count: "exact", head: true })
      .eq("tal_list_id", listId);
    if (countRes.error) throw countRes.error;
    const contactsCount = Number(countRes.count ?? 0) || 0;

    const nextPhase = after ? "contacts_list" : "company_contacts";
    const upd = await supabaseAdmin
      .from("sales_hubspot_tal_cache_jobs")
      .update({
        contacts_after: after,
        contacts_list_processed: contactsCount,
        phase: nextPhase,
        last_company_id: 0,
        last_contact_id: 0,
        contacts_processed: 0,
        status: "running",
        updated_at: new Date().toISOString()
      })
      .eq("id", jobId)
      .select("*")
      .single();
    if (upd.error) throw upd.error;
    return { job: upd.data, done: false };
  }

  if (phase === "company_contacts") {
    const contactsListId = String(job?.contacts_list_id ?? "").trim();
    if (contactsListId) {
      const lastContactId = Number(job.last_contact_id ?? 0) || 0;
      const contactsRes = await supabaseAdmin
        .from("sales_hubspot_tal_contacts")
        .select("contact_id")
        .eq("tal_list_id", listId)
        .gt("contact_id", lastContactId)
        .order("contact_id", { ascending: true })
        .limit(opts.batchContacts);
      if (contactsRes.error) throw contactsRes.error;
      const contacts = (contactsRes.data ?? []) as Array<{ contact_id: number }>;
      if (!contacts.length) {
        const upd = await supabaseAdmin
          .from("sales_hubspot_tal_cache_jobs")
          .update({
            phase: "contact_deals",
            last_company_id: 0,
            last_contact_id: 0,
            status: "running",
            updated_at: new Date().toISOString()
          })
          .eq("id", jobId)
          .select("*")
          .single();
        if (upd.error) throw upd.error;
        return { job: upd.data, done: false };
      }

      let processed = 0;
      let maxId = lastContactId;
      const contactIds = contacts.map((c) => Number(c.contact_id)).filter((n) => Number.isFinite(n) && n > 0);
      const assoc = await hubspotBatchAssociations("contacts", "companies", contactIds);
      const rows: Array<{ company_id: number; contact_id: number }> = [];
      for (const cid of contactIds) {
        maxId = Math.max(maxId, cid);
        const companyIds = assoc.get(cid) ?? [];
        for (const companyId of companyIds) rows.push({ company_id: companyId, contact_id: cid });
        processed++;
      }
      if (rows.length) {
        const up = await supabaseAdmin.from("sales_hubspot_company_contacts").upsert(rows, { onConflict: "company_id,contact_id" });
        if (up.error) throw up.error;
      }
      const upd = await supabaseAdmin
        .from("sales_hubspot_tal_cache_jobs")
        .update({
          last_contact_id: maxId,
          contacts_processed: (Number(job.contacts_processed ?? 0) || 0) + processed,
          status: "running",
          updated_at: new Date().toISOString()
        })
        .eq("id", jobId)
        .select("*")
        .single();
      if (upd.error) throw upd.error;
      return { job: upd.data, done: false };
    }
    const lastCompanyId = Number(job.last_company_id ?? 0) || 0;
    const companiesRes = await supabaseAdmin
      .from("sales_hubspot_tal_companies")
      .select("company_id")
      .eq("tal_list_id", listId)
      .gt("company_id", lastCompanyId)
      .order("company_id", { ascending: true })
      .limit(opts.batchCompanies);
    if (companiesRes.error) throw companiesRes.error;
    const companies = (companiesRes.data ?? []) as Array<{ company_id: number }>;
    if (!companies.length) {
      const upd = await supabaseAdmin
        .from("sales_hubspot_tal_cache_jobs")
        .update({ phase: "company_deals", last_company_id: 0, status: "running", updated_at: new Date().toISOString() })
        .eq("id", jobId)
        .select("*")
        .single();
      if (upd.error) throw upd.error;
      return { job: upd.data, done: false };
    }

    let processed = 0;
    let maxId = lastCompanyId;
    for (const c of companies) {
      const cid = Number(c.company_id);
      maxId = Math.max(maxId, cid);
      const contactIds = await hubspotCompanyContactIds(cid);
      if (contactIds.length) {
        const rows = contactIds.map((contactId: number) => ({ company_id: cid, contact_id: contactId }));
        const up = await supabaseAdmin.from("sales_hubspot_company_contacts").upsert(rows, { onConflict: "company_id,contact_id" });
        if (up.error) throw up.error;
      }
      processed++;
    }

    const upd = await supabaseAdmin
      .from("sales_hubspot_tal_cache_jobs")
      .update({
        last_company_id: maxId,
        companies_processed: (Number(job.companies_processed ?? 0) || 0) + processed,
        status: "running",
        updated_at: new Date().toISOString()
      })
      .eq("id", jobId)
      .select("*")
      .single();
    if (upd.error) throw upd.error;
    return { job: upd.data, done: false };
  }

  if (phase === "contact_deals") {
    const lastContactId = Number(job.last_contact_id ?? 0) || 0;
    const contactsRes = await supabaseAdmin
      .from("sales_hubspot_tal_contacts")
      .select("contact_id")
      .eq("tal_list_id", listId)
      .gt("contact_id", lastContactId)
      .order("contact_id", { ascending: true })
      .limit(opts.batchContacts);
    if (contactsRes.error) throw contactsRes.error;
    const contacts = (contactsRes.data ?? []) as Array<{ contact_id: number }>;
    if (!contacts.length) {
      const upd = await supabaseAdmin
        .from("sales_hubspot_tal_cache_jobs")
        .update({ phase: "company_deals", last_company_id: 0, status: "running", updated_at: new Date().toISOString() })
        .eq("id", jobId)
        .select("*")
        .single();
      if (upd.error) throw upd.error;
      return { job: upd.data, done: false };
    }

    let processed = 0;
    let maxId = lastContactId;
    const contactIds = contacts.map((c) => Number(c.contact_id)).filter((n) => Number.isFinite(n) && n > 0);
    const assoc = await hubspotBatchAssociations("contacts", "deals", contactIds);
    const rows: Array<{ deal_id: number; contact_id: number }> = [];
    for (const cid of contactIds) {
      maxId = Math.max(maxId, cid);
      const dealIds = assoc.get(cid) ?? [];
      for (const did of dealIds) rows.push({ deal_id: did, contact_id: cid });
      processed++;
    }
    if (rows.length) {
      const up = await supabaseAdmin.from("sales_hubspot_deal_contacts").upsert(rows, { onConflict: "deal_id,contact_id" });
      if (up.error) throw up.error;
    }

    const upd = await supabaseAdmin
      .from("sales_hubspot_tal_cache_jobs")
      .update({
        last_contact_id: maxId,
        contacts_list_processed: (Number(job.contacts_list_processed ?? 0) || 0) + processed,
        status: "running",
        updated_at: new Date().toISOString()
      })
      .eq("id", jobId)
      .select("*")
      .single();
    if (upd.error) throw upd.error;
    return { job: upd.data, done: false };
  }

  if (phase === "company_deals") {
    const lastCompanyId = Number(job.last_company_id ?? 0) || 0;
    const companiesRes = await supabaseAdmin
      .from("sales_hubspot_tal_companies")
      .select("company_id")
      .eq("tal_list_id", listId)
      .gt("company_id", lastCompanyId)
      .order("company_id", { ascending: true })
      .limit(opts.batchCompanies);
    if (companiesRes.error) throw companiesRes.error;
    const companies = (companiesRes.data ?? []) as Array<{ company_id: number }>;
    if (!companies.length) {
      const upd = await supabaseAdmin
        .from("sales_hubspot_tal_cache_jobs")
        .update({ phase: "deal_contacts", last_deal_id: 0, status: "running", updated_at: new Date().toISOString() })
        .eq("id", jobId)
        .select("*")
        .single();
      if (upd.error) throw upd.error;
      return { job: upd.data, done: false };
    }
    let processed = 0;
    let maxId = lastCompanyId;
    const companyIds = companies.map((c) => Number(c.company_id)).filter((n) => Number.isFinite(n) && n > 0);
    const assoc = await hubspotBatchAssociations("companies", "deals", companyIds);
    const rows: Array<{ company_id: number; deal_id: number }> = [];
    for (const cid of companyIds) {
      maxId = Math.max(maxId, cid);
      const dealIds = assoc.get(cid) ?? [];
      for (const did of dealIds) rows.push({ company_id: cid, deal_id: did });
      processed++;
    }
    if (rows.length) {
      const up = await supabaseAdmin.from("sales_hubspot_company_deals").upsert(rows, { onConflict: "company_id,deal_id" });
      if (up.error) throw up.error;
    }
    const upd = await supabaseAdmin
      .from("sales_hubspot_tal_cache_jobs")
      .update({
        last_company_id: maxId,
        companies_processed: (Number(job.companies_processed ?? 0) || 0) + processed,
        status: "running",
        updated_at: new Date().toISOString()
      })
      .eq("id", jobId)
      .select("*")
      .single();
    if (upd.error) throw upd.error;
    return { job: upd.data, done: false };
  }

  if (phase === "deal_contacts") {
    const lastDealId = Number(job.last_deal_id ?? 0) || 0;
    const dealsRes = await supabaseAdmin.rpc("sales_hubspot_tal_next_deals", {
      p_tal_list_id: listId,
      p_last_deal_id: lastDealId,
      p_limit: opts.batchDeals
    });
    if (dealsRes.error) throw dealsRes.error;
    const dealIds = (Array.isArray(dealsRes.data) ? dealsRes.data : []).map((r: any) => Number(r?.deal_id)).filter((n: number) => Number.isFinite(n) && n > 0);
    if (!dealIds.length) {
      const upd = await supabaseAdmin
        .from("sales_hubspot_tal_cache_jobs")
        .update({ phase: "finalize", status: "running", updated_at: new Date().toISOString() })
        .eq("id", jobId)
        .select("*")
        .single();
      if (upd.error) throw upd.error;
      return { job: upd.data, done: false };
    }
    let processed = 0;
    let maxId = lastDealId;
    const assoc = await hubspotBatchAssociations("deals", "contacts", dealIds);
    const rows: Array<{ deal_id: number; contact_id: number }> = [];
    for (const did of dealIds) {
      maxId = Math.max(maxId, did);
      const contactIds = assoc.get(did) ?? [];
      for (const cid of contactIds) rows.push({ deal_id: did, contact_id: cid });
      processed++;
    }
    if (rows.length) {
      const up = await supabaseAdmin.from("sales_hubspot_deal_contacts").upsert(rows, { onConflict: "deal_id,contact_id" });
      if (up.error) throw up.error;
    }
    const upd = await supabaseAdmin
      .from("sales_hubspot_tal_cache_jobs")
      .update({
        last_deal_id: maxId,
        deals_processed: (Number(job.deals_processed ?? 0) || 0) + processed,
        status: "running",
        updated_at: new Date().toISOString()
      })
      .eq("id", jobId)
      .select("*")
      .single();
    if (upd.error) throw upd.error;
    return { job: upd.data, done: false };
  }

  if (phase === "finalize") {
    // 🔧 PATCH: Enhanced finalize phase logging
    console.log("🔧 PATCH: Starting finalize phase", JSON.stringify({
      scope: "finalize_phase_start",
      jobId: jobId,
      listId: listId,
      hypothesisId: job?.hypothesis_id,
      timestamp: new Date().toISOString()
    }));
    
    const counts = await computeExactCounts(supabaseAdmin, listId);

    // Best-effort lead/opp split (may be 0 if it fails)
    let leadsCount = 0;
    let oppsCount = 0;
    try {
      const loadTalDealIds = async () => {
        let lastDealId = 0;
        const allDealIds: number[] = [];
        for (let i = 0; i < 200; i++) {
          // Use next_deals (which includes company-deals AND contact-deals) to match exact_counts logic.
          const dealsRes = await supabaseAdmin.rpc("sales_hubspot_tal_next_deals", {
            p_tal_list_id: listId,
            p_last_deal_id: lastDealId,
            p_limit: 500
          });
          if (dealsRes.error) throw dealsRes.error;
          const batch = (Array.isArray(dealsRes.data) ? dealsRes.data : [])
            .map((r: any) => Number(r?.deal_id))
            .filter((n: number) => Number.isFinite(n) && n > 0) as number[];
          if (!batch.length) break;
          allDealIds.push(...batch);
          lastDealId = Math.max(lastDealId, ...batch);
        }
        return Array.from(new Set(allDealIds));
      };

      let ownerFilterEmail = "";
      let dealsViewUrl = "";
      let dealTalCategory = "";
      const hypothesisId = job?.hypothesis_id ? String(job.hypothesis_id) : "";
      if (hypothesisId) {
        const hRes = await supabaseAdmin
          .from("sales_hypotheses")
          .select("hubspot_deals_owner_email, hubspot_deals_view_url, hubspot_deal_tal_category")
          .eq("id", hypothesisId)
          .maybeSingle();
        ownerFilterEmail = String(hRes.data?.hubspot_deals_owner_email ?? "").trim();
        dealsViewUrl = String(hRes.data?.hubspot_deals_view_url ?? "").trim();
        dealTalCategory = String(hRes.data?.hubspot_deal_tal_category ?? "").trim();
      }
      const ownerFilterId = ownerFilterEmail ? await hubspotOwnerIdByEmail(ownerFilterEmail) : null;
      const pipelineEnv =
        String(process.env.HUBSPOT_WEBSITE_PIPELINE_ID ?? "").trim() ||
        String(process.env.HUBSPOT_FUNNEL_PIPELINE_IDS ?? "").trim();
      const pipelineAllow = new Set(
        pipelineEnv
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      );

      // 🔧 DEBUG: Log pipeline configuration
      console.log("🔧 PIPELINE DEBUG:", JSON.stringify({
        scope: "pipeline_config_debug",
        hypothesisId,
        pipelineEnv,
        pipelineAllowSet: Array.from(pipelineAllow),
        pipelineAllowSize: pipelineAllow.size,
        dealTalCategory,
        ownerFilterEmail,
        ownerFilterId
      }));

      const stageMetaById = await hubspotFetchDealStageMeta();
      const viewId = dealsViewUrl ? parseHubspotDealsViewIdFromUrl(dealsViewUrl) : null;
      let talDealIds: number[] | null = null;
      let deals: any[] = [];
      let dealsSource: "view" | "batch" | "tal_category" = "batch";

      // Always load TAL deals first for reliable counting
      talDealIds = await loadTalDealIds();
      deals = await hubspotBatchReadDeals(talDealIds);

      // Persist deal metadata so DB-side metrics can run without HubSpot calls.
      try {
        await persistHubspotDeals(supabaseAdmin, deals, stageMetaById);
      } catch (e: any) {
        console.log(JSON.stringify({
          scope: "hubspot_deals_upsert_error",
          hypothesisId,
          talListId: listId,
          error: String(e?.message || e)
        }));
      }

      // If dealTalCategory is set, filter deals by their tal_category property
      if (dealTalCategory) {
        const allDeals = deals;
        const categoryLower = dealTalCategory.toLowerCase();
        const filteredDeals = allDeals.filter((d: any) => {
          const dealCat = String(d?.properties?.tal_category ?? "").toLowerCase();
          if (!dealCat) return false; // Don't match empty categories if we are filtering

          return dealCat === categoryLower ||
            dealCat.includes(categoryLower) ||
            categoryLower.includes(dealCat);
        });

        if (filteredDeals.length > 0) {
          deals = filteredDeals;
          dealsSource = "tal_category";
        } else {
          // Fallback: keep all TAL deals if no matches (prevents zeroed dashboards).
          deals = allDeals;
          dealsSource = "batch";
        }
      }

      // Debug logging for troubleshooting
      console.log(JSON.stringify({
        scope: "hubspot_sync_debug",
        hypothesisId: job?.hypothesis_id,
        dealsSource,
        dealsCount: deals.length,
        dealTalCategory,
        dealsViewUrl: dealsViewUrl ? "set" : "empty",
        pipelineAllowSize: pipelineAllow.size,
        pipelineAllowValues: Array.from(pipelineAllow),
        ownerFilterId: ownerFilterId ?? "none"
      }));

      const oppsDebug: Array<{
        id: string;
        stageId: string;
        stageLabel: string;
        ownerId?: string;
        pipelineId?: string;
        source: "view" | "batch" | "tal_category";
      }> = [];
      const leadsDebug: Array<{
        id: string;
        stageId: string;
        stageLabel: string;
        ownerId?: string;
        pipelineId?: string;
        source: "view" | "batch" | "tal_category";
      }> = [];
      
      // 🔧 DEBUG: Log deals before filtering
      console.log("🔧 DEALS DEBUG:", JSON.stringify({
        scope: "deals_before_filtering",
        hypothesisId,
        totalDeals: deals.length,
        sampleDeals: deals.slice(0, 3).map(d => ({
          id: String(d?.id ?? ""),
          pipeline: String(d?.properties?.pipeline ?? ""),
          dealstage: String(d?.properties?.dealstage ?? ""),
          tal_category: String(d?.properties?.tal_category ?? ""),
          hubspot_owner_id: String(d?.properties?.hubspot_owner_id ?? "")
        }))
      }));

      for (const d of deals) {
        const dealId = String(d?.id ?? "");
        const ownerId = String(d?.properties?.hubspot_owner_id ?? "").trim();
        const pipelineId = String(d?.properties?.pipeline ?? "").trim();
        const stageId = String(d?.properties?.dealstage ?? "").trim();
        const talCat = String(d?.properties?.tal_category ?? "").trim();
        
        // Owner filter
        if (ownerFilterId) {
          if (!ownerId || ownerId !== ownerFilterId) {
            console.log(`🔧 FILTERED OUT (owner): Deal ${dealId}, owner ${ownerId} != ${ownerFilterId}`);
            continue;
          }
        }
        
        // Pipeline filter - STRICT: if pipeline filter is configured, deal MUST be in allowed pipeline
        if (pipelineAllow.size > 0) {
          if (!pipelineId || !pipelineAllow.has(pipelineId)) {
            console.log(`🔧 FILTERED OUT (pipeline): Deal ${dealId}, pipeline "${pipelineId}" not in allowed: [${Array.from(pipelineAllow).join(', ')}]`);
            continue;
          }
        }
        
        const meta = stageMetaById.get(stageId);
        const label = meta?.label || stageId;
        const cat = stageCategoryFromStage(label, meta);
        
        console.log(`🔧 DEAL PROCESSED: ${dealId}, pipeline: "${pipelineId}", stage: "${label}", category: "${cat}"`);
        
        if (cat === "lead") {
          leadsCount++;
          leadsDebug.push({
            id: dealId,
            stageId,
            stageLabel: label,
            ownerId: ownerId || undefined,
            pipelineId: pipelineId || undefined,
            source: dealsSource
          });
        }
        // Match HubSpot board expectation: opps = opportunity stages only (exclude customers).
        if (cat === "opportunity") {
          oppsCount++;
          oppsDebug.push({
            id: dealId,
            stageId,
            stageLabel: label,
            ownerId: ownerId || undefined,
            pipelineId: pipelineId || undefined,
            source: dealsSource
          });
        }
      }
      
      // 🔧 DEBUG: Final counts summary
      console.log("🔧 FINAL COUNTS:", JSON.stringify({
        scope: "final_counts_summary",
        hypothesisId,
        talListId: listId,
        totalDealsProcessed: deals.length,
        finalLeadsCount: leadsCount,
        finalOppsCount: oppsCount,
        pipelineFilterActive: pipelineAllow.size > 0,
        pipelineAllowed: Array.from(pipelineAllow),
        categoryFilterActive: !!dealTalCategory,
        categoryFilter: dealTalCategory
      }));
      
      if (leadsDebug.length) {
        console.log(
          JSON.stringify({
            scope: "hubspot_tal_leads_debug",
            hypothesisId,
            talListId: listId,
            dealsViewUrl,
            ownerFilterId,
            dealsSource,
            leadsCount,
            leads: leadsDebug
          })
        );
      }
      if (oppsDebug.length) {
        console.log(
          JSON.stringify({
            scope: "hubspot_tal_opps_debug",
            hypothesisId,
            talListId: listId,
            dealsViewUrl,
            ownerFilterId,
            dealsSource,
            oppsCount,
            opps: oppsDebug
          })
        );
      }
    } catch {
      // ignore
    }

    const hypothesisId = job?.hypothesis_id ? String(job.hypothesis_id) : "";
    let dealTalCategoryForCounts = "";
    if (hypothesisId) {
      const hRes = await supabaseAdmin
        .from("sales_hypotheses")
        .select("hubspot_deal_tal_category, vertical_name")
        .eq("id", hypothesisId)
        .maybeSingle();
      const categoryOverride = String(hRes.data?.hubspot_deal_tal_category ?? "").trim();
      const verticalName = String(hRes.data?.vertical_name ?? "").trim();
      // Match counts to the same TAL category rule as the deals table.
      dealTalCategoryForCounts = String(categoryOverride || verticalName).trim();
    }
    const pipelineEnvForAnalytics =
      String(process.env.HUBSPOT_WEBSITE_PIPELINE_ID ?? "").trim() ||
      String(process.env.HUBSPOT_FUNNEL_PIPELINE_IDS ?? "").trim();
    const pipelineAllowForAnalytics = pipelineEnvForAnalytics
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    let analyticsCounts: { deals_count: number; leads_count: number; opps_count: number } | null = null;
    try {
      // Prefer DB-side counts from TAL cache (open deals, no activity requirement).
      const analyticsRes = await supabaseAdmin.rpc("sales_tal_deal_counts_from_cache", {
        p_tal_list_id: listId,
        p_pipeline_ids: pipelineAllowForAnalytics.length ? pipelineAllowForAnalytics : null,
        // Keep counts aligned with TAL category used for the hypothesis.
        p_tal_category: dealTalCategoryForCounts ? String(dealTalCategoryForCounts).trim().toLowerCase() : null
      });
      if (!analyticsRes.error) {
        const r = Array.isArray(analyticsRes.data) ? analyticsRes.data[0] : analyticsRes.data;
        analyticsCounts = {
          deals_count: Number(r?.deals_count ?? 0) || 0,
          leads_count: Number(r?.leads_count ?? 0) || 0,
          opps_count: Number(r?.opps_count ?? 0) || 0
        };
      }
    } catch (e: any) {
      console.log(JSON.stringify({
        scope: "analytics_deal_counts_error",
        hypothesisId,
        talListId: listId,
        error: String(e?.message || e)
      }));
    }
    const finalLeadsCount = analyticsCounts ? analyticsCounts.leads_count : leadsCount;
    const finalOppsCount = analyticsCounts ? analyticsCounts.opps_count : oppsCount;
    
    // 🔧 PATCH: Log hypothesis check
    console.log("🔧 PATCH: Hypothesis check", JSON.stringify({
      scope: "hypothesis_check",
      hypothesisId: hypothesisId,
      hasHypothesis: !!hypothesisId,
      timestamp: new Date().toISOString()
    }));
    
    if (hypothesisId) {
      await supabaseAdmin
        .from("sales_hypotheses")
        .update({
          tal_companies_count_baseline: counts.companiesCount,
          opps_in_progress_count: counts.dealsCount,
          hubspot_tal_leads_count: finalLeadsCount,
          hubspot_tal_opps_count: finalOppsCount,
          contacts_count_baseline: counts.contactsCount
        })
        .eq("id", hypothesisId);

      // Create a snapshot for the progress chart (Performance Trend)
      // We use ISO week start for consistency
      const d = new Date();
      const day = d.getUTCDay();
      const diff = d.getUTCDate() - day + (day === 0 ? -6 : 1); // adjust when day is sunday
      const monday = new Date(d.setUTCDate(diff));
      monday.setUTCHours(0, 0, 0, 0);
      const periodStart = monday.toISOString().split("T")[0];
      const periodEnd = new Date(monday.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString().split("T")[0];

      // Fetch activity stats for the snapshot window (last 7 days)
      let activitiesJson: any = null;
      try {
        const activitySince = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
        const activityRes = await supabaseAdmin.rpc("sales_hypothesis_activity_stats", { p_tal_list_id: listId, p_since: activitySince });
        if (activityRes.error) {
          console.error(JSON.stringify({
            scope: "activity_stats_rpc_error",
            hypothesisId,
            talListId: listId,
            activitySince,
            error: activityRes.error
          }));
        } else {
          const r = Array.isArray(activityRes.data) ? activityRes.data[0] : activityRes.data;
          activitiesJson = {
            window_days: 7,
            emails_sent: Number(r?.emails_sent_count ?? 0) || 0,
            linkedin_sent: Number(r?.linkedin_sent_count ?? 0) || 0,
            replies: Number(r?.replies_count ?? 0) || 0
          };
          console.log(JSON.stringify({
            scope: "activity_stats_rpc_success",
            hypothesisId,
            talListId: listId,
            activitiesJson
          }));
        }
      } catch {
        // ignore
      }

      // Enhanced snapshot creation with comprehensive error handling
      const snapshotData = {
        hypothesis_id: hypothesisId,
        period_start: periodStart,
        period_end: periodEnd,
        window_days: 7,
        tal_list_id: listId,
        companies_in_tal_count: counts.companiesCount,
        deals_in_tal_count: finalLeadsCount + finalOppsCount,
        new_leads_count: finalLeadsCount,
        new_opps_count: finalOppsCount,
        activities_json: activitiesJson,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      };

      console.log(JSON.stringify({
        scope: "snapshot_creation_debug",
        hypothesisId,
        talListId: listId,
        periodStart,
        periodEnd,
        snapshotData,
        activitiesJsonPresent: !!activitiesJson
      }));

      // 🔧 PATCH: Pre-upsert logging
      console.log("🔧 PATCH: About to create snapshot", JSON.stringify({
        scope: "pre_snapshot_upsert",
        hypothesisId,
        talListId: listId,
        snapshotDataKeys: Object.keys(snapshotData),
        timestamp: new Date().toISOString()
      }));

      const snapshotResult = await supabaseAdmin
        .from("sales_hubspot_tal_snapshots")
        .upsert(snapshotData, { onConflict: "hypothesis_id,period_start,window_days" });

      // 🔧 PATCH: Post-upsert logging
      console.log("🔧 PATCH: Snapshot upsert completed", JSON.stringify({
        scope: "post_snapshot_upsert",
        hypothesisId,
        hasError: !!snapshotResult.error,
        errorCode: snapshotResult.error?.code,
        errorMessage: snapshotResult.error?.message,
        timestamp: new Date().toISOString()
      }));

      if (snapshotResult.error) {
        console.error(JSON.stringify({
          scope: "snapshot_creation_error",
          hypothesisId,
          talListId: listId,
          error: {
            code: snapshotResult.error.code,
            message: snapshotResult.error.message,
            details: snapshotResult.error.details,
            hint: snapshotResult.error.hint
          },
          snapshotData
        }));
        
        // Don't throw - let the job complete but log the error
        console.error("CRITICAL: Snapshot creation failed but job will continue");
      } else {
        console.log(JSON.stringify({
          scope: "snapshot_creation_success",
          hypothesisId,
          talListId: listId,
          periodStart,
          result: snapshotResult
        }));

        // Verify the snapshot was actually created
        const verifyResult = await supabaseAdmin
          .from("sales_hubspot_tal_snapshots")
          .select("id, created_at")
          .eq("hypothesis_id", hypothesisId)
          .eq("period_start", periodStart)
          .eq("window_days", 7)
          .maybeSingle();

        if (verifyResult.error) {
          console.error(JSON.stringify({
            scope: "snapshot_verification_error",
            hypothesisId,
            error: verifyResult.error
          }));
        } else if (!verifyResult.data) {
          console.error(JSON.stringify({
            scope: "snapshot_verification_missing",
            hypothesisId,
            message: "Snapshot upsert succeeded but record not found in database"
          }));
        } else {
          console.log(JSON.stringify({
            scope: "snapshot_verification_success",
            hypothesisId,
            snapshotId: verifyResult.data.id,
            createdAt: verifyResult.data.created_at
          }));
        }
      }
    }

    // 🔧 PATCH: Finalize phase completion logging
    console.log("🔧 PATCH: Finalize phase completed successfully", JSON.stringify({
      scope: "finalize_phase_complete",
      hypothesisId,
      jobId: jobId,
      timestamp: new Date().toISOString()
    }));

    const upd = await supabaseAdmin
      .from("sales_hubspot_tal_cache_jobs")
      .update({ status: "done", phase: "finalize", finished_at: new Date().toISOString(), updated_at: new Date().toISOString() })
      .eq("id", jobId)
      .select("*")
      .single();
    if (upd.error) throw upd.error;
    return { job: upd.data, done: true, counts: { ...counts, leadsCount, oppsCount } };
  }

  return { job, done: false };
}

export async function GET(req: Request) {
  try {
    if (!isCronAuthorized(req)) return jsonError(401, "Not authorized");

    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
    if (!supabaseUrl || !serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });

    const jobsRes = await supabaseAdmin
      .from("sales_hubspot_tal_cache_jobs")
      .select("*")
      .in("status", ["queued", "running"])
      .order("updated_at", { ascending: true })
      .limit(1);
    if (jobsRes.error) throw jobsRes.error;
    const jobs = Array.isArray(jobsRes.data) ? jobsRes.data : [];

    const processed: any[] = [];
    for (const j of jobs) {
      // Ensure "queued" becomes "running" before processing.
      if (String(j.status) === "queued") {
        await supabaseAdmin.from("sales_hubspot_tal_cache_jobs").update({ status: "running", started_at: new Date().toISOString() }).eq("id", j.id);
      }
      const started = Date.now();
      const budgetMs = 20_000; // Vercel-friendly; keep cron runs short and predictable
      let cur = j;
      let lastOut: any = null;
      while (Date.now() - started < budgetMs) {
        const phase = String(cur?.phase ?? "memberships");
        // company_contacts is the heaviest phase (many HubSpot calls). Keep slices small so progress updates are frequent.
        const batchCompanies = phase === "company_contacts" ? 10 : 100;
        lastOut = await processJobSlice(supabaseAdmin, cur, {
          batchMembershipPages: 8,
          batchCompanies,
          batchDeals: 50,
          batchContacts: 50
        });
        cur = lastOut.job;
        if (lastOut.done) break;
      }
      processed.push(cur);
    }

    return NextResponse.json({ ok: true, processed: processed.length, jobs: processed });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    const userId = String(user?.id ?? "").trim();
    if (!userId || !user?.email) return jsonError(401, "Not authorized");

    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json().catch(() => ({}))) as {
      talUrl?: string;
      talListId?: string;
      contactsListUrl?: string;
      contactsListId?: string;
      hypothesisId?: string;
      jobId?: string;
      batch_membership_pages?: number;
      batch_companies?: number;
      batch_deals?: number;
      batch_contacts?: number;
      update_hypothesis?: boolean;
    };

    const talUrl = String(payload?.talUrl ?? "").trim();
    const talListId = String(payload?.talListId ?? "").trim();
    const contactsListUrl = String(payload?.contactsListUrl ?? "").trim();
    const contactsListId = String(payload?.contactsListId ?? "").trim() || parseHubspotListIdFromUrl(contactsListUrl);
    const listId = talListId || parseHubspotListIdFromUrl(talUrl);
    if (!listId) return jsonError(400, "Missing TAL list id. Provide talListId or talUrl containing /lists/<id> or /objectLists/<id>.");

    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
    if (!supabaseUrl || !serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });

    const batchMembershipPages = Math.max(1, Math.min(8, Number(payload?.batch_membership_pages ?? 2)));
    const batchCompanies = Math.max(1, Math.min(50, Number(payload?.batch_companies ?? 10)));
    const batchDeals = Math.max(1, Math.min(80, Number(payload?.batch_deals ?? 20)));
    const batchContacts = Math.max(1, Math.min(80, Number(payload?.batch_contacts ?? 20)));
    const hypothesisId = String(payload?.hypothesisId ?? "").trim() || null;
    const updateHypothesis = payload?.update_hypothesis !== false && !!hypothesisId;

    // Load or create job
    let job: any = null;
    if (payload?.jobId) {
      const j = await supabaseAdmin.from("sales_hubspot_tal_cache_jobs").select("*").eq("id", payload.jobId).single();
      if (j.error) throw j.error;
      job = j.data;
    } else {
      // Reuse latest active job for this TAL if present; otherwise create
      const existing = await supabaseAdmin
        .from("sales_hubspot_tal_cache_jobs")
        .select("*")
        .eq("tal_list_id", listId)
        .in("status", ["queued", "running"])
        .order("updated_at", { ascending: false })
        .limit(1);
      job = (existing.data ?? [])[0] ?? null;
      if (!job) {
        const ins = await supabaseAdmin
          .from("sales_hubspot_tal_cache_jobs")
          .insert({
            tal_list_id: listId,
            contacts_list_id: contactsListId || null,
            hypothesis_id: hypothesisId,
            status: "running",
            phase: "memberships",
            started_at: new Date().toISOString(),
            created_by: userId
          })
          .select("*")
          .single();
        if (ins.error) throw ins.error;
        job = ins.data;

        // New job: clear existing TAL membership caches
        await supabaseAdmin.from("sales_hubspot_tal_companies").delete().eq("tal_list_id", listId);
        if (contactsListId) {
          await supabaseAdmin.from("sales_hubspot_tal_contacts").delete().eq("tal_list_id", listId);
        }
      } else if (job.status !== "running") {
        const up = await supabaseAdmin
          .from("sales_hubspot_tal_cache_jobs")
          .update({ status: "running", error: null, contacts_list_id: contactsListId || job.contacts_list_id || null })
          .eq("id", job.id)
          .select("*")
          .single();
        if (up.error) throw up.error;
        job = up.data;
      }
    }

    const started = Date.now();
    const budgetMs = 45_000; // allow manual sync to do real work without waiting for cron
    let cur = job;
    let lastOut: any = null;
    while (Date.now() - started < budgetMs) {
      const phase = String(cur?.phase ?? "memberships");
      const effectiveCompanies = phase === "company_contacts" ? Math.min(batchCompanies, 10) : batchCompanies;
      lastOut = await processJobSlice(supabaseAdmin, cur, {
        batchMembershipPages,
        batchCompanies: effectiveCompanies,
        batchDeals,
        batchContacts
      });
      cur = lastOut.job;
      if (lastOut.done) break;
    }
    return NextResponse.json({ ok: true, job: cur, counts: lastOut?.counts, done: !!lastOut?.done });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


