import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

type SupabaseUserResponse = { email?: string | null };
type ZeroBounceResponse = {
  address?: string | null;
  status?: string | null;
  sub_status?: string | null;
  did_you_mean?: string | null;
  domain?: string | null;
  processed_at?: string | null;
  mx_found?: boolean | string | null;
  mx_record?: string | null;
  error?: string | null;
};

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function env(name: string, fallbacks: string[] = []) {
  const keys = [name, ...fallbacks];
  for (const k of keys) {
    const v = process.env[k];
    if (v && String(v).trim()) return String(v);
  }
  throw new Error(`Missing env: ${name}`);
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

function normEmail(v: any) {
  return String(v ?? "")
    .trim()
    .toLowerCase();
}

function emailDomain(v: any) {
  const e = normEmail(v);
  if (!e) return "";
  const at = e.lastIndexOf("@");
  if (at < 0) return "";
  const host = e.slice(at + 1).trim().replace(/\.+$/g, "");
  return host;
}

function looksLikeEmail(v: any) {
  const t = String(v ?? "").trim();
  return !!t && t.includes("@") && !t.includes(" ");
}

function normalizeZeroBounceStatus(v: any) {
  return String(v ?? "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
}

/**
 * Validate email with ZeroBounce to enrich domain quality signals.
 *
 * Returns null when the API is not configured or fails, to avoid blocking deal sync.
 */
async function zeroBounceValidateEmail(email: string, ipAddress?: string | null): Promise<ZeroBounceResponse | null> {
  const apiKey = String(process.env.ZEROBOUNCE_API_KEY ?? "").trim();
  if (!apiKey || !looksLikeEmail(email)) return null;

  const baseUrl = String(process.env.ZEROBOUNCE_API_BASE_URL ?? "https://api.zerobounce.net").trim();
  const url = new URL("/v2/validate", baseUrl);
  url.searchParams.set("api_key", apiKey);
  url.searchParams.set("email", email);
  if (ipAddress) url.searchParams.set("ip_address", String(ipAddress).trim());

  try {
    const res = await fetch(url.toString(), { method: "GET" });
    const json = (await res.json().catch(() => null)) as ZeroBounceResponse | null;
    if (!res.ok) return null;
    if (json?.error) return null;
    return json ?? null;
  } catch {
    return null;
  }
}

/**
 * Map ZeroBounce response into HubSpot deal property values.
 *
 * We keep values compact to make it easy to filter and color deals in HubSpot.
 */
function buildDomainQualityProps(zb: ZeroBounceResponse | null) {
  if (!zb) return {};

  const statusRaw = normalizeZeroBounceStatus(zb.status);
  const subStatusRaw = normalizeZeroBounceStatus(zb.sub_status);
  const suggestion = String(zb.did_you_mean ?? "").trim();

  // Flag likely typos without auto-rejecting: reduce false negatives like cocacala.com.
  const isPossibleTypo = subStatusRaw === "possible_typo" || !!suggestion;

  let derivedStatus = statusRaw || "unknown";
  if (isPossibleTypo) derivedStatus = "suspect_typo";

  const statusProp = String(process.env.HUBSPOT_DEAL_DOMAIN_STATUS_PROPERTY ?? "lead_domain_status").trim();
  const subStatusProp = String(process.env.HUBSPOT_DEAL_DOMAIN_SUB_STATUS_PROPERTY ?? "").trim();
  const suggestionProp = String(process.env.HUBSPOT_DEAL_DOMAIN_SUGGESTION_PROPERTY ?? "").trim();
  const checkedAtProp = String(process.env.HUBSPOT_DEAL_DOMAIN_CHECKED_AT_PROPERTY ?? "").trim();

  const props: Record<string, any> = {};
  if (statusProp) props[statusProp] = derivedStatus;
  if (subStatusProp && subStatusRaw) props[subStatusProp] = subStatusRaw;
  if (suggestionProp && suggestion) props[suggestionProp] = suggestion;
  if (checkedAtProp) props[checkedAtProp] = new Date().toISOString();
  return props;
}

function buildContactDomainQualityProps(zb: ZeroBounceResponse | null) {
  if (!zb) return {};

  const statusRaw = normalizeZeroBounceStatus(zb.status);
  const subStatusRaw = normalizeZeroBounceStatus(zb.sub_status);
  const suggestion = String(zb.did_you_mean ?? "").trim();
  const isPossibleTypo = subStatusRaw === "possible_typo" || !!suggestion;
  let derivedStatus = statusRaw || "unknown";
  if (isPossibleTypo) derivedStatus = "suspect_typo";

  const statusProp = String(process.env.HUBSPOT_CONTACT_DOMAIN_STATUS_PROPERTY ?? "").trim();
  const subStatusProp = String(process.env.HUBSPOT_CONTACT_DOMAIN_SUB_STATUS_PROPERTY ?? "").trim();
  const suggestionProp = String(process.env.HUBSPOT_CONTACT_DOMAIN_SUGGESTION_PROPERTY ?? "").trim();
  const checkedAtProp = String(process.env.HUBSPOT_CONTACT_DOMAIN_CHECKED_AT_PROPERTY ?? "").trim();

  const props: Record<string, any> = {};
  if (statusProp) props[statusProp] = derivedStatus;
  if (subStatusProp && subStatusRaw) props[subStatusProp] = subStatusRaw;
  if (suggestionProp && suggestion) props[suggestionProp] = suggestion;
  if (checkedAtProp) props[checkedAtProp] = new Date().toISOString();
  return props;
}

let cachedHubspotOwnerId: string | null = null;
async function getHubspotOwnerId(): Promise<string | null> {
  if (cachedHubspotOwnerId) return cachedHubspotOwnerId;

  const explicit = String(process.env.HUBSPOT_WEBSITE_OWNER_ID ?? "").trim();
  if (explicit) {
    cachedHubspotOwnerId = explicit;
    return cachedHubspotOwnerId;
  }

  const email = normEmail(process.env.HUBSPOT_WEBSITE_OWNER_EMAIL ?? "emoskvin@oversecured.com");
  if (!email) return null;

  const res = await hubspotFetch(
    `https://api.hubapi.com/crm/v3/owners/?email=${encodeURIComponent(email)}&includeInactive=true`
  );
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot owners lookup failed"));
  const results = Array.isArray(json?.results) ? json.results : [];
  const r = results.find((o: any) => normEmail(o?.email) === email) ?? results[0] ?? null;
  const id = r?.id != null ? String(r.id) : "";
  cachedHubspotOwnerId = id || null;
  return cachedHubspotOwnerId;
}

function normalizeEnumValue(raw: string) {
  const t = String(raw ?? "").trim();
  if (!t) return "";
  // HubSpot enumeration values are typically machine-friendly (snake_case).
  // Keep it stable and predictable so we can safely "ensure option exists".
  const lower = t.toLowerCase();
  const replaced = lower.replace(/[^a-z0-9]+/g, "_");
  const collapsed = replaced.replace(/_+/g, "_").replace(/^_+|_+$/g, "");
  return collapsed.slice(0, 100);
}

function isInvalidOptionErrorMessage(msg: string) {
  const m = String(msg ?? "").toLowerCase();
  return m.includes("not one of the allowed options") || m.includes("is not a valid option") || m.includes("invalid_option");
}

async function findHubspotContactIdByEmail(email: string) {
  const e = normEmail(email);
  if (!e) return null;
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/contacts/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filterGroups: [{ filters: [{ propertyName: "email", operator: "EQ", value: e }] }],
      properties: ["email"],
      limit: 1
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot contacts search failed"));
  const r = Array.isArray(json?.results) ? json.results[0] : null;
  return r?.id ? String(r.id) : null;
}

async function hubspotCreateContact(props: Record<string, any>) {
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/contacts", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties: props })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot contact create failed"));
  return json?.id ? String(json.id) : null;
}

async function hubspotUpdateContact(contactId: string, props: Record<string, any>) {
  const id = String(contactId ?? "").trim();
  if (!id) return false;
  const res = await hubspotFetch(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties: props })
  });
  if (res.ok) return true;
  const json = await res.json().catch(() => null);
  const msg = String(json?.message || json?.error || "");
  throw new Error(msg || "HubSpot contact update failed");
}

async function findHubspotCompanyIdByName(name: string) {
  const n = String(name ?? "").trim();
  if (!n) return null;
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/companies/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filterGroups: [{ filters: [{ propertyName: "name", operator: "EQ", value: n }] }],
      properties: ["name"],
      limit: 1
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot companies search failed"));
  const r = Array.isArray(json?.results) ? json.results[0] : null;
  return r?.id ? String(r.id) : null;
}

async function hubspotAssociateContactToCompany(contactId: string, companyId: string) {
  const res = await hubspotFetch(
    `https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(contactId)}/associations/companies/${encodeURIComponent(
      companyId
    )}/contact_to_company`,
    { method: "PUT" }
  );
  if (res.ok) return true;
  const json = await res.json().catch(() => null);
  const msg = String(json?.message || json?.error || "");
  if (msg.toLowerCase().includes("association") && msg.toLowerCase().includes("already")) return true;
  throw new Error(msg || "HubSpot contact->company association failed");
}

async function findHubspotCompanyIdsForContact(contactId: string, limit = 5) {
  const cid = String(contactId ?? "").trim();
  if (!cid) return [];
  const res = await hubspotFetch(
    `https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(cid)}/associations/companies?limit=${encodeURIComponent(
      String(Math.max(1, Math.min(50, Number(limit) || 5)))
    )}`
  );
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot contact->companies list failed"));
  const results = Array.isArray(json?.results) ? json.results : [];
  return results.map((r: any) => String(r?.id ?? "")).filter(Boolean);
}

async function findHubspotDealIdByName(dealname: string) {
  const n = String(dealname ?? "").trim();
  if (!n) return null;
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filterGroups: [{ filters: [{ propertyName: "dealname", operator: "EQ", value: n }] }],
      properties: ["dealname"],
      limit: 1
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot deals search failed"));
  const r = Array.isArray(json?.results) ? json.results[0] : null;
  return r?.id ? String(r.id) : null;
}

async function hubspotCreateDeal(props: Record<string, any>) {
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties: props })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot deal create failed"));
  return json?.id ? String(json.id) : null;
}

async function hubspotUpdateDeal(dealId: string, props: Record<string, any>) {
  const id = String(dealId ?? "").trim();
  if (!id) return false;
  const res = await hubspotFetch(`https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties: props })
  });
  if (res.ok) return true;
  const json = await res.json().catch(() => null);
  const msg = String(json?.message || json?.error || "");
  throw new Error(msg || "HubSpot deal update failed");
}

async function hubspotGetDealProperty(propertyName: string) {
  const p = String(propertyName ?? "").trim();
  if (!p) return null;
  const res = await hubspotFetch(`https://api.hubapi.com/crm/v3/properties/deals/${encodeURIComponent(p)}`);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot deal property get failed"));
  return json;
}

async function hubspotEnsureDealPropertyHasOption(propertyName: string, value: string, label?: string) {
  const p = String(propertyName ?? "").trim();
  const v = String(value ?? "").trim();
  if (!p || !v) return false;

  const prop = await hubspotGetDealProperty(p);
  const options = Array.isArray(prop?.options) ? prop.options : [];
  if (options.some((o: any) => String(o?.value ?? "") === v)) return true;

  const next = [
    ...options.map((o: any) => ({
      label: String(o?.label ?? o?.value ?? ""),
      value: String(o?.value ?? ""),
      hidden: !!o?.hidden,
      displayOrder: Number(o?.displayOrder ?? 0)
    })),
    {
      label: String(label ?? v),
      value: v,
      hidden: false,
      displayOrder: options.length ? Math.max(...options.map((o: any) => Number(o?.displayOrder ?? 0))) + 1 : 0
    }
  ].filter((o: any) => o.value);

  const res = await hubspotFetch(`https://api.hubapi.com/crm/v3/properties/deals/${encodeURIComponent(p)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ options: next })
  });
  if (res.ok) return true;
  const json = await res.json().catch(() => null);
  const msg = String(json?.message || json?.error || "");
  throw new Error(msg || "HubSpot deal property update failed");
}

async function hubspotAssociateDealToContact(dealId: string, contactId: string) {
  const res = await hubspotFetch(
    `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}/associations/contacts/${encodeURIComponent(
      contactId
    )}/deal_to_contact`,
    { method: "PUT" }
  );
  if (res.ok) return true;
  const json = await res.json().catch(() => null);
  const msg = String(json?.message || json?.error || "");
  if (msg.toLowerCase().includes("association") && msg.toLowerCase().includes("already")) return true;
  throw new Error(msg || "HubSpot deal->contact association failed");
}

async function hubspotAssociateDealToCompany(dealId: string, companyId: string) {
  const res = await hubspotFetch(
    `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}/associations/companies/${encodeURIComponent(
      companyId
    )}/deal_to_company`,
    { method: "PUT" }
  );
  if (res.ok) return true;
  const json = await res.json().catch(() => null);
  const msg = String(json?.message || json?.error || "");
  if (msg.toLowerCase().includes("association") && msg.toLowerCase().includes("already")) return true;
  throw new Error(msg || "HubSpot deal->company association failed");
}

async function findExistingHubspotTaskIdForDeal(dealId: string, leadId: string) {
  const did = String(dealId ?? "").trim();
  if (!did) return null;
  const marker = `New website lead (#${String(leadId ?? "").trim()})`;
  if (!marker.trim()) return null;

  // Legacy engagements API: list engagements associated with a deal.
  // We only need a small window to dedupe (recent activities on the deal).
  const res = await hubspotFetch(
    `https://api.hubapi.com/engagements/v1/engagements/associated/deal/${encodeURIComponent(did)}/paged?limit=50`
  );
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot deal engagements list failed"));
  const results = Array.isArray(json?.results) ? json.results : [];
  for (const r of results) {
    const engagement = r?.engagement ?? null;
    const metadata = r?.metadata ?? null;
    const type = String(engagement?.type ?? "").toUpperCase();
    if (type !== "TASK") continue;
    const body = String(metadata?.body ?? "");
    const subject = String(metadata?.subject ?? "");
    if (subject === "Website inbound lead" && body.includes(marker)) {
      const id = engagement?.id ?? r?.id;
      return id != null ? Number(id) : null;
    }
  }
  return null;
}

async function hubspotCreateTaskForDeal(dealId: string, summary: string, dueAtMs: number) {
  const did = String(dealId ?? "").trim();
  if (!did) return null;
  const due = Number(dueAtMs);
  const safeDueAtMs = Number.isFinite(due) ? due : Date.now() + 60 * 60 * 1000;
  const ownerId = await getHubspotOwnerId();
  const res = await hubspotFetch("https://api.hubapi.com/engagements/v1/engagements", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      engagement: { active: true, type: "TASK", timestamp: safeDueAtMs, ownerId: ownerId ? Number(ownerId) : undefined },
      associations: { dealIds: [Number(did)] },
      metadata: {
        subject: "Website inbound lead",
        body: String(summary || "").trim(),
        status: "NOT_STARTED",
        dueDate: safeDueAtMs
      }
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot task create failed"));
  const id = json?.engagement?.id ?? json?.id;
  return id != null ? Number(id) : null;
}

async function safeLogUpsert(supabaseAdmin: any, row: any) {
  try {
    const res = await supabaseAdmin.from("contact_form_leads_hubspot").upsert(row, { onConflict: "lead_id" });
    if (res?.error) throw res.error;
    return true;
  } catch (e: any) {
    const msg = String(e?.message ?? e ?? "");
    // In this integration, the log table is required for idempotency.
    // If it's missing (or PostgREST schema cache is stale), we MUST fail the sync
    // to avoid creating duplicate deals on every cron run.
    if (msg.toLowerCase().includes("does not exist") || msg.toLowerCase().includes("not found")) {
      throw new Error(
        "Supabase table contact_form_leads_hubspot is not available. Apply 99-applications/sales/supabase/schema-hypotheses.sql and wait for schema cache refresh."
      );
    }
    throw e;
  }
}

export async function POST(req: Request) {
  try {
    const syncSecret = String(process.env.CONTACT_FORM_HUBSPOT_SYNC_SECRET ?? "").trim();
    const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
    const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();

    const authHeader = String(req.headers.get("authorization") ?? "").trim();
    const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
    const secretOk =
      (!!syncSecret && (gotSecret === syncSecret || bearer === syncSecret)) || (!!cronSecret && bearer === cronSecret);

    if (!secretOk) {
      const authHeader = req.headers.get("authorization");
      const user = await getSupabaseUserFromAuthHeader(authHeader);
      if (!user?.email) return jsonError(401, "Not authorized");

      const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
      const email = String(user.email || "").toLowerCase();
      if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");
    }

    const payload = (await req.json().catch(() => ({}))) as {
      limit?: number;
      lookback_days?: number;
      dry_run?: boolean;
      recheck_done?: boolean;
      lead_id?: number;
      email?: string;
      hubspot_deal_id?: string;
    };

    const limit = Math.max(1, Math.min(200, Number(payload?.limit ?? 50)));
    const lookbackDays = Math.max(1, Math.min(120, Number(payload?.lookback_days ?? 14)));
    const dryRun = !!payload?.dry_run;
    const recheckDone = !!payload?.recheck_done;
    const zeroBounceMode = String(process.env.ZEROBOUNCE_MODE ?? "mark_only").trim().toLowerCase();
    const requestedEmail = normEmail(payload?.email);
    const requestedLeadId = Number(payload?.lead_id ?? NaN);
    const requestedDealId = String(payload?.hubspot_deal_id ?? "").trim();

    const supabaseUrl = env("NEXT_PUBLIC_SUPABASE_URL");
    const serviceRoleKey = env("SUPABASE_SERVICE_ROLE_KEY", ["SUPABASE_SERVICE_ROLE_KEY"]);
    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });

    const pipelineId = env("HUBSPOT_WEBSITE_PIPELINE_ID");
    const dealstageId = env("HUBSPOT_WEBSITE_LEAD_STAGE_ID");
    const ownerId = await getHubspotOwnerId();

    const sinceIso = new Date(Date.now() - lookbackDays * 24 * 60 * 60 * 1000).toISOString();
    const hasLeadIdFilter = Number.isFinite(requestedLeadId) && requestedLeadId > 0;
    const hasEmailFilter = !!requestedEmail;
    const hasDealIdFilter = !!requestedDealId;

    // Optional targeting: allow processing a single lead/email/deal without lookback constraints.
    let targetedLeadIds: number[] = [];
    if (hasDealIdFilter) {
      const logRes = await supabaseAdmin
        .from("contact_form_leads_hubspot")
        .select("lead_id")
        .eq("hubspot_deal_id", requestedDealId)
        .limit(5);
      if (logRes.error) throw logRes.error;
      targetedLeadIds = (Array.isArray(logRes.data) ? logRes.data : [])
        .map((r: any) => Number(r?.lead_id))
        .filter((v) => Number.isFinite(v) && v > 0);
      if (targetedLeadIds.length === 0) return jsonError(404, "No lead found for hubspot_deal_id");
    }

    let leadsQuery = supabaseAdmin
      .from("contact_form_leads")
      .select("id,created_at,first_name,last_name,corporate_email,company,reason_for_contact,referral_source,source,remote_address");

    if (hasLeadIdFilter) leadsQuery = leadsQuery.eq("id", requestedLeadId);
    if (hasEmailFilter) leadsQuery = leadsQuery.eq("corporate_email", requestedEmail);
    if (targetedLeadIds.length > 0) leadsQuery = leadsQuery.in("id", targetedLeadIds);

    if (!hasLeadIdFilter && !hasEmailFilter && targetedLeadIds.length === 0) {
      leadsQuery = leadsQuery.gte("created_at", sinceIso).order("id", { ascending: true }).limit(Math.min(1000, limit * 10));
    } else {
      // Keep the scan bounded when targeting by email/deal (can still match multiple leads).
      leadsQuery = leadsQuery.order("id", { ascending: false }).limit(Math.min(1000, limit * 10));
    }

    const leadsRes = await leadsQuery;

    if (leadsRes.error) throw leadsRes.error;
    const leads = Array.isArray(leadsRes.data) ? leadsRes.data : [];

    // Log table is REQUIRED for idempotency (do not create duplicates on every cron run).
    let logByLeadId = new Map<string, any>();
    if (leads.length > 0) {
      const ids = leads.map((r: any) => r.id);
      const logRes = await supabaseAdmin
        .from("contact_form_leads_hubspot")
        .select("lead_id,status,attempts,hubspot_contact_id,hubspot_deal_id,hubspot_task_id,updated_at")
        .in("lead_id", ids);
      if (logRes.error) throw logRes.error;
      for (const r of Array.isArray(logRes.data) ? logRes.data : []) logByLeadId.set(String(r.lead_id), r);
    }

    const stats = {
      scanned: leads.length,
      skipped_done: 0,
      skipped_duplicate: 0,
      processed: 0,
      created_contacts: 0,
      created_deals: 0,
      existing_deals: 0,
      errors: 0
    };

    const errors: Array<{ lead_id: string; error: string }> = [];

    for (const lead of leads) {
      if (stats.processed >= limit) break;

      const leadId = String(lead?.id ?? "");
      const log = logByLeadId.get(leadId) ?? null;
      if (log?.status === "done" && !recheckDone) {
        stats.skipped_done++;
        continue;
      }

      const email = normEmail(lead?.corporate_email);
      if (!email) {
        stats.errors++;
        errors.push({ lead_id: leadId, error: "Missing corporate_email" });
        if (!dryRun) {
          await safeLogUpsert(supabaseAdmin, {
            lead_id: Number(lead.id),
            status: "failed",
            error: "Missing corporate_email"
          });
        }
        continue;
      }

      stats.processed++;

      const firstName = String(lead?.first_name ?? "").trim() || null;
      const lastName = String(lead?.last_name ?? "").trim() || null;
      const companyName = String(lead?.company ?? "").trim() || null;
      const reason = String(lead?.reason_for_contact ?? "").trim() || null;
      const referralSource = String(lead?.referral_source ?? "").trim() || null;
      const leadSource = String(lead?.source ?? "").trim() || null;
      const createdAt = String(lead?.created_at ?? "").trim() || null;
      const remoteAddress = String(lead?.remote_address ?? "").trim() || null;

      // Deal name: use company domain (derived from corporate_email). This keeps list views compact and consistent.
      // Fallback: company name, then email (last resort).
      const companyDomain = emailDomain(email);
      const dealname = (companyDomain || companyName || email).trim();
      const createdAtMs = createdAt ? Date.parse(createdAt) : NaN;
      const taskDueAtMs = Number.isFinite(createdAtMs) ? createdAtMs + 60 * 60 * 1000 : Date.now() + 60 * 60 * 1000;

      if (!dryRun) {
        await safeLogUpsert(supabaseAdmin, {
          lead_id: Number(lead.id),
          status: "processing",
          attempts: (Number(log?.attempts ?? 0) || 0) + 1,
          error: null
        });
      }

      if (dryRun) continue;

      try {
        const zb = zeroBounceMode === "off" ? null : await zeroBounceValidateEmail(email, remoteAddress);
        const domainProps = buildDomainQualityProps(zb);
        const contactDomainProps = buildContactDomainQualityProps(zb);

        if (log?.status === "done" && recheckDone) {
          const dealId = log?.hubspot_deal_id ? String(log.hubspot_deal_id) : "";
          if (!dealId) {
            throw new Error("Recheck requested but hubspot_deal_id is missing");
          }
          if (Object.keys(domainProps).length > 0) {
            await hubspotUpdateDeal(dealId, domainProps);
          }
          await safeLogUpsert(supabaseAdmin, {
            lead_id: Number(lead.id),
            status: "done",
            error: null,
            synced_at: new Date().toISOString()
          });
          continue;
        }

        let contactId = (log?.hubspot_contact_id ? String(log.hubspot_contact_id) : "") || null;
        if (!contactId) contactId = await findHubspotContactIdByEmail(email);
        if (!contactId) {
          contactId = await hubspotCreateContact({
            email,
            firstname: firstName,
            lastname: lastName,
            company: companyName,
            ...(Object.keys(contactDomainProps).length > 0 ? contactDomainProps : {}),
            ...(ownerId ? { hubspot_owner_id: ownerId } : {})
          });
          if (contactId) stats.created_contacts++;
        } else {
          // Best-effort: keep contact fields up to date (do not overwrite with null/empty)
          const patch: any = {};
          if (firstName) patch.firstname = firstName;
          if (lastName) patch.lastname = lastName;
          if (companyName) patch.company = companyName;
          if (ownerId) patch.hubspot_owner_id = ownerId;
          Object.assign(patch, contactDomainProps);
          if (Object.keys(patch).length > 0) await hubspotUpdateContact(contactId, patch);
        }
        if (!contactId) throw new Error("Failed to get/create HubSpot contact");

        // Best-effort: match existing HubSpot company and associate:
        // - Contact -> Company
        // - Deal -> Company (later, once deal is known)
        let companyId: string | null = null;
        if (companyName) {
          companyId = await findHubspotCompanyIdByName(companyName);
          if (companyId) await hubspotAssociateContactToCompany(contactId, companyId);
        }

        // DEDUPLICATION: Check if we already created a deal for this email in the last 3 days
        // to avoid duplicate deals from the same contact submitting multiple forms
        const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString();
        let recentDealForEmail: any = null;
        
        const recentLeadsRes = await supabaseAdmin
          .from("contact_form_leads")
          .select("id")
          .eq("corporate_email", email)
          .gte("created_at", threeDaysAgo)
          .neq("id", lead.id) // Exclude current lead
          .order("created_at", { ascending: false })
          .limit(10); // Check up to 10 recent leads from same email

        if (!recentLeadsRes.error && Array.isArray(recentLeadsRes.data) && recentLeadsRes.data.length > 0) {
          const recentLeadIds = recentLeadsRes.data.map((r: any) => Number(r?.id)).filter((v) => Number.isFinite(v) && v > 0);
          if (recentLeadIds.length > 0) {
            const recentLogsRes = await supabaseAdmin
              .from("contact_form_leads_hubspot")
              .select("lead_id,hubspot_deal_id,synced_at")
              .in("lead_id", recentLeadIds)
              .eq("status", "done")
              .not("hubspot_deal_id", "is", null)
              .order("synced_at", { ascending: false })
              .limit(1);
            
            if (!recentLogsRes.error && Array.isArray(recentLogsRes.data) && recentLogsRes.data.length > 0) {
              recentDealForEmail = recentLogsRes.data[0];
            }
          }
        }

        // Deal custom properties mapping (standard/custom in this portal)
        const reasonProp = String(process.env.HUBSPOT_DEAL_REASON_PROPERTY ?? "reason_for_contact").trim();
        const referralProp = String(process.env.HUBSPOT_DEAL_REFERRAL_PROPERTY ?? "referralsource").trim();
        const leadSourceProp = String(process.env.HUBSPOT_DEAL_LEAD_SOURCE_PROPERTY ?? "lead_source").trim();

        const dealExtraProps: any = {};
        if (reasonProp && reason) dealExtraProps[reasonProp] = reason;
        if (referralProp && referralSource) dealExtraProps[referralProp] = referralSource;
        // lead_source is an enum in this portal. Store a normalized value; keep raw for label when creating option.
        const leadSourceValue = leadSource ? normalizeEnumValue(leadSource) : "";
        if (leadSourceProp && leadSourceValue) dealExtraProps[leadSourceProp] = leadSourceValue;
        // Ensure deal name stays corrected (domain-based) even if previously created with email.
        if (dealname) dealExtraProps.dealname = dealname;
        Object.assign(dealExtraProps, domainProps);

        let dealId = (log?.hubspot_deal_id ? String(log.hubspot_deal_id) : "") || null;
        
        // If deal exists in log for this lead_id, update it
        if (dealId) {
          stats.existing_deals++;
          if (Object.keys(dealExtraProps).length > 0) {
            try {
              await hubspotUpdateDeal(dealId, dealExtraProps);
            } catch (e: any) {
              // If lead source is an enum and option doesn't exist, add it and retry once.
              const msg = String(e?.message ?? e ?? "");
              if (leadSourceProp && leadSourceValue && isInvalidOptionErrorMessage(msg)) {
                await hubspotEnsureDealPropertyHasOption(leadSourceProp, leadSourceValue, leadSource || leadSourceValue);
                await hubspotUpdateDeal(dealId, dealExtraProps);
              } else {
                throw e;
              }
            }
          }
        } 
        // If deal was found from recent leads with same email, skip creating new deal
        // and create a task on the existing deal instead
        else if (recentDealForEmail?.hubspot_deal_id) {
          dealId = String(recentDealForEmail.hubspot_deal_id);
          stats.skipped_duplicate++;
          
          // Create task on existing deal to notify owner about duplicate submission
          const duplicateTaskSummary = [
            `Duplicate submission from same email (#${leadId})`,
            `Previous lead: #${recentDealForEmail.lead_id}`,
            createdAt ? `New submission at: ${createdAt}` : null,
            reason ? `Reason: ${reason}` : null,
            `Email: ${email}`,
            companyName ? `Company: ${companyName}` : null,
            firstName || lastName ? `Name: ${[firstName, lastName].filter(Boolean).join(" ")}` : null,
            "",
            "Check if this is a legitimate follow-up or spam."
          ]
            .filter((v) => v !== null)
            .join("\n");
          
          await hubspotCreateTaskForDeal(dealId, duplicateTaskSummary, taskDueAtMs);
          
          // Log this lead as skipped (duplicate), pointing to the reused deal
          await safeLogUpsert(supabaseAdmin, {
            lead_id: Number(lead.id),
            status: "skipped_duplicate",
            hubspot_contact_id: contactId,
            hubspot_deal_id: dealId,
            error: null,
            synced_at: new Date().toISOString()
          });
          
          // Skip normal deal/task creation flow for this lead
          continue;
        } 
        // Otherwise create new deal
        else {
          const baseProps: any = {
            dealname,
            pipeline: pipelineId,
            dealstage: dealstageId,
            ...(ownerId ? { hubspot_owner_id: ownerId } : {}),
            // Best-effort: store context in deal description (built-in property exists in many portals).
            description: [
              reason ? `Reason: ${reason}` : null,
              createdAt ? `Submitted at: ${createdAt}` : null,
              companyName ? `Company: ${companyName}` : null,
              `Email: ${email}`,
              `Source: Supabase contact_form_leads (#${leadId})`
            ]
              .filter(Boolean)
              .join("\n")
          };
          const props = { ...baseProps, ...dealExtraProps };
          try {
            dealId = await hubspotCreateDeal(props);
          } catch (e: any) {
            const msg = String(e?.message ?? e ?? "");
            if (leadSourceProp && leadSourceValue && isInvalidOptionErrorMessage(msg)) {
              await hubspotEnsureDealPropertyHasOption(leadSourceProp, leadSourceValue, leadSource || leadSourceValue);
              dealId = await hubspotCreateDeal(props);
            } else {
              throw e;
            }
          }
          if (dealId) stats.created_deals++;
        }
        if (!dealId) throw new Error("Failed to get/create HubSpot deal");

        await hubspotAssociateDealToContact(dealId, contactId);

        // Best-effort: associate deal to the same company as contact.
        // If company wasn't found by name, fallback to whatever company is already linked to the contact.
        if (!companyId) {
          const ids = await findHubspotCompanyIdsForContact(contactId, 5).catch(() => []);
          companyId = ids?.[0] ? String(ids[0]) : null;
        }
        if (companyId) {
          await hubspotAssociateDealToCompany(dealId, companyId);
        }

        // Create a TASK on the deal with summary (owner will be assigned by HubSpot automation)
        let taskId: number | null = log?.hubspot_task_id != null ? Number(log.hubspot_task_id) : null;
        if (!taskId || !Number.isFinite(taskId)) {
          const summary = [
            `New website lead (#${leadId})`,
            createdAt ? `Submitted at: ${createdAt}` : null,
            companyName ? `Company: ${companyName}` : null,
            firstName || lastName ? `Name: ${[firstName, lastName].filter(Boolean).join(" ")}` : null,
            `Email: ${email}`,
            reason ? `Reason: ${reason}` : null,
            referralSource ? `Referral source: ${referralSource}` : null,
            leadSource ? `Lead source: ${leadSource}` : null
          ]
            .filter(Boolean)
            .join("\n");
          // Place task due date 1 hour after the deal creation time (derived from lead.created_at).
          // Dedup: if a previous run already created the task, reuse it.
          const existingTaskId = await findExistingHubspotTaskIdForDeal(dealId, leadId);
          taskId = existingTaskId ?? (await hubspotCreateTaskForDeal(dealId, summary, taskDueAtMs));
        }

        await safeLogUpsert(supabaseAdmin, {
          lead_id: Number(lead.id),
          status: "done",
          hubspot_contact_id: contactId,
          hubspot_deal_id: dealId,
          hubspot_task_id: taskId,
          error: null,
          synced_at: new Date().toISOString()
        });
      } catch (e: any) {
        stats.errors++;
        const msg = String(e?.message ?? e ?? "Unknown error");
        errors.push({ lead_id: leadId, error: msg });
        await safeLogUpsert(supabaseAdmin, {
          lead_id: Number(lead.id),
          status: "failed",
          error: msg
        });
      }
    }

    return NextResponse.json({
      ok: true,
      stats,
      log_table: "contact_form_leads_hubspot",
      since: sinceIso,
      dry_run: dryRun,
      errors: errors.slice(0, 50)
    });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: e?.message ?? String(e) }, { status: 500 });
  }
}

export async function GET(req: Request) {
  // Vercel Cron invokes GET requests. Support GET by translating query params into the POST payload.
  const url = new URL(req.url);
  const limit = url.searchParams.get("limit");
  const lookback = url.searchParams.get("lookback_days");
  const dryRun = url.searchParams.get("dry_run");
  const recheckDone = url.searchParams.get("recheck_done");
  const leadId = url.searchParams.get("lead_id");
  const email = url.searchParams.get("email");
  const dealId = url.searchParams.get("hubspot_deal_id");

  const body = {
    limit: limit != null ? Number(limit) : undefined,
    lookback_days: lookback != null ? Number(lookback) : undefined,
    dry_run: dryRun != null ? dryRun === "1" || dryRun.toLowerCase() === "true" : undefined,
    recheck_done: recheckDone != null ? recheckDone === "1" || recheckDone.toLowerCase() === "true" : undefined,
    lead_id: leadId != null ? Number(leadId) : undefined,
    email: email != null ? String(email) : undefined,
    hubspot_deal_id: dealId != null ? String(dealId) : undefined
  };

  // Create a new Request with the same headers so auth works (x-sync-secret OR Authorization: Bearer <secret>)
  const nextReq = new Request(req.url, {
    method: "POST",
    headers: req.headers,
    body: JSON.stringify(body)
  });

  return POST(nextReq);
}


