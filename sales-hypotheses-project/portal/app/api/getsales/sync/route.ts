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

function toMs(v: any): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

function isoNowMinusMs(ms: number) {
  return new Date(Date.now() - ms).toISOString();
}

let lastGetSalesCallAt = 0;
let lastHubspotCallAt = 0;

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

type PostgrestHeaders = { apikey: string; Authorization: string };

function postgrestHeadersFor(authHeader: string, isCron: boolean): PostgrestHeaders {
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
  if (isCron) {
    if (!serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY (required for cron)");
    return { apikey: serviceRoleKey, Authorization: `Bearer ${serviceRoleKey}` };
  }
  if (!supabaseAnonKey) throw new Error("Missing NEXT_PUBLIC_SUPABASE_ANON_KEY");
  if (!authHeader) throw new Error("Missing Authorization");
  return { apikey: supabaseAnonKey, Authorization: authHeader };
}

async function getsalesFetch(path: string, init?: RequestInit, opts?: { minIntervalMs?: number; maxRetries?: number }) {
  const method = init?.method ?? "GET";
  const token = process.env.GETSALES_API_TOKEN ?? process.env.GETSALES_BEARER_TOKEN ?? "";
  if (!token) throw new Error("Missing GETSALES_API_TOKEN");
  const base = (process.env.GETSALES_BASE_URL ?? "https://amazing.getsales.io").replace(/\/+$/, "");

  const minIntervalMs = Math.max(50, Math.min(500, Number(opts?.minIntervalMs ?? 140)));
  const maxRetries = Math.max(0, Math.min(8, Number(opts?.maxRetries ?? 4)));

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const now = Date.now();
    const wait = Math.max(0, lastGetSalesCallAt + minIntervalMs - now);
    if (wait) await sleep(wait);
    lastGetSalesCallAt = Date.now();

    const url = `${base}${path}`;
    console.log(`[GetSales] Fetching ${method} ${url}`);

    // Add 30s timeout
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), 30000);

    try {
      const res = await fetch(url, {
        ...init,
        signal: controller.signal,
        headers: {
          Authorization: `Bearer ${token}`,
          ...(init?.headers ?? {})
        }
      });
      clearTimeout(id);
      if (res.status !== 429 && res.status !== 503) return res;
      console.warn(`[GetSales] Rate limit ${res.status} for ${url}, retrying...`);
    } catch (e: any) {
      clearTimeout(id);
      if (e.name === 'AbortError') {
        throw new Error(`GetSales API timeout (30s) for ${url}`);
      }
      throw e;
    }
    await sleep(Math.min(10_000, 900 + attempt * 700));
  }
  throw new Error("GetSales rate limit / unavailable. Try again later.");
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

async function postgrestJson(h: PostgrestHeaders, method: string, path: string, body?: any, extraHeaders?: Record<string, string>) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const url = `${supabaseUrl}/rest/v1/${path}`;
  const res = await fetch(url, {
    method,
    headers: {
      ...h,
      "Content-Type": "application/json",
      ...(extraHeaders ?? {})
    },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const json = await res.json().catch(() => null);
  if (!res.ok) throw new Error(String(json?.message || json?.error || `Supabase ${method} failed`));
  return json;
}

async function getSyncState(h: PostgrestHeaders, createdBy: string | null) {
  const where = createdBy ? `created_by=eq.${encodeURIComponent(createdBy)}&` : "";
  const rows = await postgrestJson(h, "GET", `sales_getsales_sync_state?select=last_synced_at&${where}limit=1`);
  const row = Array.isArray(rows) ? rows[0] : null;
  return row?.last_synced_at ? String(row.last_synced_at) : null;
}

async function setSyncState(h: PostgrestHeaders, iso: string, createdBy: string | null) {
  const row: any = { last_synced_at: iso };
  if (createdBy) row.created_by = createdBy;
  await postgrestJson(
    h,
    "POST",
    "sales_getsales_sync_state?on_conflict=created_by",
    row,
    { Prefer: "resolution=merge-duplicates,return=minimal" }
  );
}

async function tryInsertEvent(h: PostgrestHeaders, row: any) {
  const inserted = await postgrestJson(
    h,
    "POST",
    "sales_getsales_events?on_conflict=created_by,source,getsales_uuid",
    row,
    { Prefer: "resolution=ignore-duplicates,return=representation" }
  );
  // If duplicate: representation should be [] (best-effort)
  if (Array.isArray(inserted) && inserted.length === 0) return null;
  return inserted;
}

async function updateEvent(h: PostgrestHeaders, source: string, getsalesUuid: string, patch: any, createdBy: string | null) {
  const where = createdBy ? `created_by=eq.${encodeURIComponent(createdBy)}&` : "";
  await postgrestJson(
    h,
    "PATCH",
    `sales_getsales_events?${where}source=eq.${encodeURIComponent(source)}&getsales_uuid=eq.${encodeURIComponent(getsalesUuid)}`,
    patch,
    { Prefer: "return=minimal" }
  );
}

async function findHubspotContactIdByEmail(email: string) {
  const e = String(email || "").trim().toLowerCase();
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

async function findHubspotContactIdByPropertyEq(propertyName: string, value: string) {
  const p = String(propertyName || "").trim();
  const v = String(value || "").trim();
  if (!p || !v) return null;
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/contacts/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filterGroups: [{ filters: [{ propertyName: p, operator: "EQ", value: v }] }],
      properties: [p],
      limit: 1
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot contacts search failed"));
  const r = Array.isArray(json?.results) ? json.results[0] : null;
  return r?.id ? String(r.id) : null;
}

function looksLikeEmail(v: string) {
  const t = String(v || "").trim();
  return !!t && t.includes("@") && !t.includes(" ");
}

/**
 * Best-effort extraction of contact email from a GetSales event payload.
 *
 * Why: LinkedIn message events often don't include `to_email`, and lead fetching
 * can fail for older/archived leads. If we still have an explicit email in the
 * event payload, it's safe to create/update a HubSpot contact (not "empty").
 */
function pickContactEmailFromEventPayload(source: string, payload: any) {
  const lower = (v: any) => String(v ?? "").trim().toLowerCase();

  // Common fields we observed/expect across GetSales endpoints.
  const cands: string[] = [];
  if (source === "email") {
    cands.push(lower(payload?.to_email));
    cands.push(lower(payload?.toEmail));
    cands.push(lower(payload?.email));
    cands.push(lower(payload?.lead_email));
    cands.push(lower(payload?.leadEmail));
  } else {
    cands.push(lower(payload?.email));
    cands.push(lower(payload?.lead_email));
    cands.push(lower(payload?.leadEmail));
    cands.push(lower(payload?.to_email));
    cands.push(lower(payload?.toEmail));
    cands.push(lower(payload?.contact_email));
    cands.push(lower(payload?.contactEmail));
  }

  for (const e of cands) if (looksLikeEmail(e)) return e;
  return "";
}

/**
 * Build a minimal "lead-like" object from event payload when the lead API fetch fails.
 *
 * Goal: still be able to match/create a HubSpot contact with non-empty properties
 * (email OR linkedin OR name), without regressing into "empty contacts".
 */
function pseudoLeadFromEventPayload(payload: any) {
  const lead: any = {};

  // Name candidates (common variations)
  const full =
    String(payload?.lead_name ?? payload?.leadName ?? payload?.name ?? payload?.full_name ?? payload?.fullName ?? "").trim() || "";
  const first = String(payload?.lead_first_name ?? payload?.leadFirstName ?? payload?.first_name ?? payload?.firstName ?? "").trim() || "";
  const last = String(payload?.lead_last_name ?? payload?.leadLastName ?? payload?.last_name ?? payload?.lastName ?? "").trim() || "";
  if (full) lead.name = full;
  if (first) lead.first_name = first;
  if (last) lead.last_name = last;

  // Company/title candidates
  const company =
    String(payload?.company_name ?? payload?.companyName ?? payload?.company ?? payload?.lead_company ?? payload?.leadCompany ?? "").trim() ||
    "";
  const title = String(payload?.title ?? payload?.job_title ?? payload?.jobTitle ?? payload?.lead_title ?? payload?.leadTitle ?? "").trim() || "";
  if (company) lead.company_name = company;
  if (title) lead.title = title;

  // LinkedIn candidates
  const li =
    String(
      payload?.linkedin ??
      payload?.linkedin_url ??
      payload?.linkedinUrl ??
      payload?.lead_linkedin ??
      payload?.leadLinkedin ??
      payload?.profile_url ??
      payload?.profileUrl ??
      ""
    ).trim() || "";
  if (li) lead.linkedin = li;

  // Email candidates (leave both work/personal so pickContactEmailFromLead can find it)
  const email = String(payload?.email ?? payload?.lead_email ?? payload?.leadEmail ?? payload?.to_email ?? payload?.toEmail ?? "").trim() || "";
  if (email) lead.work_email = email;

  return lead;
}

async function hubspotCreateNote(contactId: string, atMs: number, body: string) {
  // Use legacy engagements API because it supports contactIds without association type IDs.
  const res = await hubspotFetch("https://api.hubapi.com/engagements/v1/engagements", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      engagement: { active: true, type: "NOTE", timestamp: atMs },
      associations: { contactIds: [Number(contactId)] },
      metadata: { body }
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot engagement create failed"));
  const id = json?.engagement?.id ?? json?.id;
  // If HubSpot returns 200 but no ID, treat it as an error so we don't silently "push 0 notes".
  if (id == null) throw new Error("HubSpot engagement create returned no id");
  return String(id);
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

async function findHubspotCompanyIdByDomain(domain: string) {
  const d = String(domain || "").trim().toLowerCase();
  if (!d) return null;
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/companies/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filterGroups: [{ filters: [{ propertyName: "domain", operator: "EQ", value: d }] }],
      properties: ["domain", "name"],
      limit: 1
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot companies search failed"));
  const r = Array.isArray(json?.results) ? json.results[0] : null;
  return r?.id ? String(r.id) : null;
}

async function findHubspotCompanyIdByName(name: string) {
  const n = String(name || "").trim();
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

async function findHubspotCompanyIdByPropertyEq(propertyName: string, value: string) {
  const p = String(propertyName || "").trim();
  const v = String(value || "").trim();
  if (!p || !v) return null;
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/companies/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filterGroups: [{ filters: [{ propertyName: p, operator: "EQ", value: v }] }],
      properties: [p, "name"],
      limit: 1
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot companies search failed"));
  const r = Array.isArray(json?.results) ? json.results[0] : null;
  return r?.id ? String(r.id) : null;
}

async function hubspotCreateCompany(props: Record<string, any>) {
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/companies", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties: props })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot company create failed"));
  return json?.id ? String(json.id) : null;
}

async function hubspotAssociateContactToCompany(contactId: string, companyId: string) {
  // Legacy v3 associations API supports the "contact_to_company" association name.
  const res = await hubspotFetch(
    `https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(contactId)}/associations/companies/${encodeURIComponent(companyId)}/contact_to_company`,
    { method: "PUT" }
  );
  if (res.ok) return true;
  const json = await res.json().catch(() => null);
  // Ignore duplicates / already-associated errors (best-effort).
  const msg = String(json?.message || json?.error || "");
  if (msg.toLowerCase().includes("association") && msg.toLowerCase().includes("already")) return true;
  throw new Error(msg || "HubSpot contact->company association failed");
}

async function getLead(leadUuid: string) {
  const res = await getsalesFetch(`/leads/api/leads/${encodeURIComponent(leadUuid)}`);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales lead fetch failed"));
  // GetSales responses are inconsistent across endpoints:
  // - sometimes lead is the root object
  // - sometimes it's returned as { data: {...} }
  // Normalize here so downstream mappers (email/name/linkedin/company) work reliably.
  const data = json?.data ?? json;
  // Common GetSales pattern: response is { lead: { ... } } or { data: { lead: { ... } } }
  return data?.lead ?? data;
}

function pickContactEmailFromLead(lead: any) {
  // Support both snake_case and camelCase keys (GetSales API/UI may differ).
  const w = String(lead?.work_email ?? lead?.workEmail ?? lead?.email ?? "").trim().toLowerCase();
  const p = String(lead?.personal_email ?? lead?.personalEmail ?? "").trim().toLowerCase();
  return w || p || "";
}

function pickLinkedinFromLead(lead: any) {
  // lead.linkedin is often a LinkedIn "nickname" (e.g. "john-smith-123"), sometimes full URL.
  const a = String(lead?.linkedin ?? lead?.linkedin_url ?? lead?.linkedinUrl ?? lead?.linkedIn ?? "").trim();
  if (a) return a;
  // Fallbacks (can be empty):
  const lnId = String(lead?.ln_id ?? lead?.lnId ?? "").trim();
  return lnId || "";
}

function pickCompanyNameFromLead(lead: any) {
  return String(lead?.company_name ?? lead?.companyName ?? lead?.company ?? "").trim();
}

function companyLinkedinCandidatesFromLead(lead: any) {
  const out: string[] = [];

  // 1) Prefer explicit nickname from latest experience (often equals LinkedIn company slug).
  const exp0 = Array.isArray(lead?.experience) && lead.experience.length ? lead.experience[0] : null;
  const nick = String(exp0?.company_nickname ?? "").trim();
  if (nick) out.push(`https://www.linkedin.com/company/${nick}/`);

  // 2) Fallback: numeric LinkedIn company id (GetSales lead.company_ln_id).
  const companyLnId = String(lead?.company_ln_id ?? "").trim();
  if (companyLnId && /^[0-9]+$/.test(companyLnId)) out.push(`https://www.linkedin.com/company/${companyLnId}/`);

  // Normalize
  const norm = new Set<string>();
  for (const u of out) {
    const t = String(u || "").trim();
    if (!t) continue;
    const noHash = t.split("#")[0].split("?")[0];
    const withSlash = noHash.endsWith("/") ? noHash : `${noHash}/`;
    const withoutSlash = withSlash.endsWith("/") ? withSlash.slice(0, -1) : withSlash;
    norm.add(noHash);
    norm.add(withSlash);
    norm.add(withoutSlash);
  }
  return Array.from(norm).filter(Boolean);
}

function pickDomainFromEmail(email: string) {
  const t = String(email || "").trim().toLowerCase();
  const at = t.lastIndexOf("@");
  if (at <= 0) return "";
  return t.slice(at + 1).trim();
}

function isProbablyPersonalDomain(domain: string) {
  const d = String(domain || "").trim().toLowerCase();
  if (!d) return true;
  const personal = new Set([
    "gmail.com",
    "googlemail.com",
    "yahoo.com",
    "yahoo.co.uk",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "icloud.com",
    "mail.ru",
    "yandex.ru",
    "proton.me",
    "protonmail.com"
  ]);
  return personal.has(d);
}

async function resolveAndAttachCompanyForContact(contactId: string, lead: any, email: string) {
  const associateEnabled = String(process.env.GETSALES_ASSOCIATE_COMPANIES ?? "true").toLowerCase() !== "false";
  if (!associateEnabled) return { companyId: null, created: false };

  const allowCreate = String(process.env.GETSALES_CREATE_COMPANIES ?? "true").toLowerCase() !== "false";
  const companyLinkedinProp = String(process.env.HUBSPOT_COMPANY_LINKEDIN_PROPERTY ?? "linkedin_company_page").trim() || "linkedin_company_page";

  const companyName = pickCompanyNameFromLead(lead);
  const domain = pickDomainFromEmail(email);
  const domainOk = domain && !isProbablyPersonalDomain(domain);

  // Prefer domain match (most reliable)
  if (domainOk) {
    const existing = await findHubspotCompanyIdByDomain(domain);
    const companyId = existing || (allowCreate ? await hubspotCreateCompany({ name: companyName || domain, domain }) : null);
    if (companyId) {
      await hubspotAssociateContactToCompany(contactId, companyId);
      return { companyId, created: !existing };
    }
  }

  // Next: match by LinkedIn company URL (standard HubSpot company field).
  const liCands = companyLinkedinCandidatesFromLead(lead);
  for (const c of liCands) {
    const existingId = await findHubspotCompanyIdByPropertyEq(companyLinkedinProp, c);
    const companyId =
      existingId ||
      (allowCreate
        ? await hubspotCreateCompany({
          name: companyName || c,
          [companyLinkedinProp]: c
        })
        : null);
    if (companyId) {
      await hubspotAssociateContactToCompany(contactId, companyId);
      return { companyId, created: !existingId };
    }
  }

  // Fallback by name (less reliable)
  if (companyName) {
    const existing = await findHubspotCompanyIdByName(companyName);
    const companyId = existing || (allowCreate ? await hubspotCreateCompany({ name: companyName }) : null);
    if (companyId) {
      await hubspotAssociateContactToCompany(contactId, companyId);
      return { companyId, created: !existing };
    }
  }

  return { companyId: null, created: false };
}

function linkedinCandidates(v: string) {
  const t = String(v || "").trim();
  if (!t) return [];
  // If it's already a URL, keep as-is and normalized versions.
  if (t.startsWith("http://") || t.startsWith("https://")) {
    const noHash = t.split("#")[0].split("?")[0];
    const withSlash = noHash.endsWith("/") ? noHash : `${noHash}/`;
    const withoutSlash = withSlash.endsWith("/") ? withSlash.slice(0, -1) : withSlash;
    return Array.from(new Set([noHash, withSlash, withoutSlash].filter(Boolean)));
  }
  // If it's a LinkedIn handle/nickname, generate likely URL forms.
  const slug = t.replace(/^\/+|\/+$/g, "");
  if (!slug) return [];
  const url = `https://www.linkedin.com/in/${slug}/`;
  return Array.from(new Set([slug, url, url.slice(0, -1)]));
}

async function resolveHubspotContactIdForLead(lead: any, fallbackEmail?: string) {
  const allowCreate = String(process.env.GETSALES_CREATE_CONTACTS ?? "true").toLowerCase() !== "false";
  const linkedinProp = String(process.env.HUBSPOT_CONTACT_LINKEDIN_PROPERTY ?? "hs_linkedin_url").trim() || "hs_linkedin_url";

  // If lead fetch failed, do NOT create a new HubSpot contact.
  // Otherwise, we end up with hundreds of empty contacts with only one note.
  if (!lead) {
    const email = looksLikeEmail(String(fallbackEmail || "")) ? String(fallbackEmail).trim().toLowerCase() : "";
    if (!email) return { contactId: null, matchedBy: "none" as const, matchedValue: "" };

    // Safe fallback: email-only match/create (never "empty contact").
    const existing = await findHubspotContactIdByEmail(email);
    if (existing) return { contactId: existing, matchedBy: "email" as const, matchedValue: email };
    if (!allowCreate) return { contactId: null, matchedBy: "none" as const, matchedValue: "" };
    const createdId = await hubspotCreateContact({ email });
    return { contactId: createdId, matchedBy: "created" as const, matchedValue: email };
  }

  const emailFromLead = pickContactEmailFromLead(lead);
  const email = looksLikeEmail(emailFromLead) ? emailFromLead : (looksLikeEmail(String(fallbackEmail || "")) ? String(fallbackEmail).trim().toLowerCase() : "");

  if (email) {
    const existing = await findHubspotContactIdByEmail(email);
    if (existing) return { contactId: existing, matchedBy: "email" as const, matchedValue: email };
  }

  const li = pickLinkedinFromLead(lead);
  const liCands = linkedinCandidates(li);
  for (const c of liCands) {
    const existing = await findHubspotContactIdByPropertyEq(linkedinProp, c);
    if (existing) return { contactId: existing, matchedBy: "linkedin" as const, matchedValue: c };
  }

  if (!allowCreate) return { contactId: null, matchedBy: "none" as const, matchedValue: "" };

  // Create a new contact (best-effort). Without email, we risk duplicates, but user explicitly requested this.
  const first = String(lead?.first_name ?? lead?.firstName ?? "").trim();
  const last = String(lead?.last_name ?? lead?.lastName ?? "").trim();
  const full = String(lead?.name ?? lead?.full_name ?? lead?.fullName ?? "").trim();
  const [f2, ...rest] = !first && !last && full ? full.split(" ") : [];
  const newFirst = first || f2 || "";
  const newLast = last || (rest.length ? rest.join(" ") : "");
  const companyName = pickCompanyNameFromLead(lead);
  const title = String(lead?.title ?? lead?.job_title ?? lead?.jobTitle ?? "").trim();

  const props: Record<string, any> = {};
  if (email) props.email = email;
  if (newFirst) props.firstname = newFirst;
  if (newLast) props.lastname = newLast;
  // HubSpot contact "Company name" column maps to the `company` contact property.
  if (companyName) props.company = companyName;
  if (title) props.jobtitle = title;

  // Store LinkedIn if we have it and if property is configured.
  if (liCands.length) props[linkedinProp] = liCands.find((x) => x.startsWith("http")) || liCands[0];

  // Safety: never create a HubSpot contact with empty properties.
  if (Object.keys(props).length === 0) return { contactId: null, matchedBy: "none" as const, matchedValue: "" };

  const createdId = await hubspotCreateContact(props);
  return { contactId: createdId, matchedBy: "created" as const, matchedValue: email || (liCands[0] || "") };
}

async function listEmailsSince(sinceMs: number, max: number) {
  const out: any[] = [];
  const limit = Math.max(1, Math.min(200, max));
  let offset = 0;

  while (out.length < max) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(limit, max - out.length)));
    qs.set("offset", String(offset));
    qs.set("order_field", "updated_at");
    qs.set("order_type", "desc");
    qs.set("filter[updated_at][>=]", new Date(sinceMs).toISOString());

    // Fix: emails endpoint is /emails/api/emails, not /leads/api/emails
    const res = await getsalesFetch(`/emails/api/emails?${qs.toString()}`);
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales emails list failed"));
    const rows = Array.isArray(json) ? json : Array.isArray(json?.data) ? json.data : [];
    if (!rows.length) break;

    out.push(...rows);
    offset += rows.length;

    // Stop early if the last row is older than since (next pages will be even older).
    const last = rows[rows.length - 1];
    const lastMs = toMs(last?.updated_at) ?? toMs(last?.sent_at) ?? toMs(last?.created_at) ?? null;
    if (lastMs != null && lastMs < sinceMs) break;
  }
  return out.slice(0, max);
}

async function listLinkedinMessagesSince(sinceMs: number, max: number) {
  const out: any[] = [];
  const limit = Math.max(1, Math.min(200, max));
  let offset = 0;

  while (out.length < max) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(limit, max - out.length)));
    qs.set("offset", String(offset));
    qs.set("order_field", "sent_at");
    qs.set("order_type", "desc");

    const res = await getsalesFetch(`/flows/api/linkedin-messages?${qs.toString()}`);
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales linkedin messages list failed"));
    const rows = Array.isArray(json?.data) ? json.data : Array.isArray(json) ? json : [];
    if (!rows.length) break;

    out.push(...rows);
    offset += rows.length;

    const last = rows[rows.length - 1];
    const lastMs = toMs(last?.sent_at) ?? toMs(last?.read_at) ?? toMs(last?.updated_at) ?? toMs(last?.created_at) ?? null;
    if (lastMs != null && lastMs < sinceMs) break;

    if (json?.has_more === false) break;
  }
  return out.slice(0, max);
}

/**
 * List automation flows (best-effort, paginated).
 *
 * @param {number} max
 * @returns {Promise<any[]>}
 */
async function listFlows(max: number) {
  const out: any[] = [];
  const limit = Math.max(1, Math.min(200, max));
  let offset = 0;
  while (out.length < max) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(limit, max - out.length)));
    qs.set("offset", String(offset));
    qs.set("order_field", "created_at");
    qs.set("order_type", "desc");
    const res = await getsalesFetch(`/flows/api/flows?${qs.toString()}`);
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales flows list failed"));
    const rows = Array.isArray(json?.data) ? json.data : Array.isArray(json) ? json : [];
    if (!rows.length) break;
    out.push(...rows);
    offset += rows.length;
    if (json?.has_more === false) break;
  }
  return out.slice(0, max);
}

/**
 * Build best-effort flow lookup maps.
 *
 * Why: LinkedIn messages do not include flow uuid in the API response,
 * but flows have workspace/version identifiers that can sometimes match
 * message metadata (task_pipeline_uuid / flow_version_uuid).
 *
 * @param {any[]} flows
 */
function buildFlowLookups(flows: any[]) {
  const byWorkspace = new Map<string, string>();
  const byVersion = new Map<string, string>();
  for (const f of flows || []) {
    const id = String(f?.uuid ?? "").trim();
    const ws = String(f?.flow_workspace_uuid ?? "").trim();
    const ver = String(f?.flow_version_uuid ?? "").trim();
    if (id && ws) byWorkspace.set(ws, id);
    if (id && ver) byVersion.set(ver, id);
  }
  return { byWorkspace, byVersion };
}

/**
 * Resolve flow uuid for a LinkedIn message (best-effort).
 *
 * @param {any} msg
 * @param {{ byWorkspace: Map<string, string>; byVersion: Map<string, string> }} lookups
 * @returns {{ flowUuid: string; source: string }}
 */
function resolveFlowUuidForMessage(
  msg: any,
  lookups: { byWorkspace: Map<string, string>; byVersion: Map<string, string> }
) {
  const fromPayload = String(msg?.flow_uuid ?? msg?.flowUuid ?? msg?.flow?.uuid ?? "").trim();
  if (fromPayload) return { flowUuid: fromPayload, source: "payload" };
  const taskPipeline = String(msg?.task_pipeline_uuid ?? "").trim();
  if (taskPipeline && lookups.byWorkspace.has(taskPipeline)) {
    return { flowUuid: String(lookups.byWorkspace.get(taskPipeline)), source: "task_pipeline_uuid" };
  }
  const flowVersion = String(msg?.flow_version_uuid ?? msg?.flowVersionUuid ?? "").trim();
  if (flowVersion && lookups.byVersion.has(flowVersion)) {
    return { flowUuid: String(lookups.byVersion.get(flowVersion)), source: "flow_version_uuid" };
  }
  return { flowUuid: "", source: "" };
}

async function listActivitiesSince(sinceMs: number, max: number, type?: string) {
  const out: any[] = [];
  const limit = Math.max(1, Math.min(200, max));
  let offset = 0;

  while (out.length < max) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(limit, max - out.length)));
    qs.set("offset", String(offset));
    // NOTE: sorting parameters (order_field/order_type) cause HANGS on GetSales API for this endpoint.
    // We rely on default sorting or client-side handling if needed, but primary goal is to fetch data without timeout.
    // qs.set("order_field", "created_at");
    // qs.set("order_type", "desc");
    qs.set("filter[created_at][>=]", new Date(sinceMs).toISOString());
    if (type) qs.set("filter[type]", type);

    const res = await getsalesFetch(`/leads/api/activities?${qs.toString()}`);
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales activities list failed"));
    const rows = Array.isArray(json?.data) ? json.data : [];
    if (!rows.length) break;

    out.push(...rows);
    offset += rows.length;

    const last = rows[rows.length - 1];
    const lastMs = toMs(last?.created_at) ?? toMs(last?.updated_at) ?? null;
    if (lastMs != null && lastMs < sinceMs) break;
    if (json?.has_more === false) break;
  }
  return out.slice(0, max);
}

function buildEmailNote(email: any, lead: any) {
  const sentAt = String(email?.sent_at ?? "").trim();
  const from = String(email?.from_email ?? "").trim();
  const to = String(email?.to_email ?? "").trim();
  const subj = String(email?.subject ?? "").trim();
  const status = String(email?.status ?? "").trim();
  const type = String(email?.type ?? "").trim();
  const flow = String(email?.flow_uuid ?? "").trim();
  const attempts = email?.sending_attempts != null ? String(email.sending_attempts) : "";
  const leadName = String(lead?.name ?? lead?.first_name ?? "").trim();

  return [
    `[GetSales] Email ${type || ""} ${status ? `(${status})` : ""}`.trim(),
    leadName ? `Lead: ${leadName}` : null,
    subj ? `Subject: ${subj}` : null,
    from ? `From: ${from}` : null,
    to ? `To: ${to}` : null,
    sentAt ? `Sent at: ${sentAt}` : null,
    flow ? `Flow: ${flow}` : null,
    attempts ? `Attempts: ${attempts}` : null,
    `GetSales email uuid: ${String(email?.uuid ?? "")}`
  ]
    .filter(Boolean)
    .join("\n");
}

function buildLinkedinNote(msg: any, lead: any) {
  const sentAt = String(msg?.sent_at ?? "").trim();
  const text = String(msg?.text ?? "").trim();
  const type = String(msg?.type ?? "").trim();
  const status = String(msg?.status ?? "").trim();
  const leadName = String(lead?.name ?? lead?.first_name ?? "").trim();

  return [
    `[GetSales] LinkedIn ${type || ""} ${status ? `(${status})` : ""}`.trim(),
    leadName ? `Lead: ${leadName}` : null,
    sentAt ? `Sent at: ${sentAt}` : null,
    text ? `\n${text}` : null
  ].filter(Boolean).join("\n");
}

function buildLinkedinConnectionNote(act: any, lead: any) {
  const type = String(act?.type ?? "").replace(/_/g, " ").replace(/\b\w/g, (c: string) => c.toUpperCase());
  const leadName = String(lead?.name ?? lead?.first_name ?? "").trim();
  const at = String(act?.created_at ?? "").trim();

  return [
    `[GetSales] ${type}`,
    leadName ? `Lead: ${leadName}` : null,
    at ? `At: ${at}` : null
  ].filter(Boolean).join("\n");
}

export async function POST(req: Request) {
  try {
    const syncSecret = String(process.env.GETSALES_SYNC_SECRET ?? "").trim();
    const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
    const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
    const authHeader = String(req.headers.get("authorization") ?? "").trim();
    const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
    const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
    const isCron = vercelCron === "1" || vercelCron.toLowerCase() === "true" || (!!cronSecret && bearer === cronSecret);
    const secretOk = (!!syncSecret && (gotSecret === syncSecret || bearer === syncSecret)) || isCron;
    const cronUserId = String(process.env.GETSALES_CRON_USER_ID ?? "").trim();
    let createdBy = isCron ? cronUserId : null;

    if (gotSecret) {
      if (!syncSecret) return jsonError(500, "GETSALES_SYNC_SECRET is not configured in Vercel env");
      if (gotSecret !== syncSecret) return jsonError(403, "Bad x-sync-secret");
    }

    if (!secretOk) {
      const user = await getSupabaseUserFromAuthHeader(authHeader);
      if (!user?.email) return jsonError(401, "Not authorized");

      const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
      const email = String(user.email || "").toLowerCase();
      if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");
    }

    if (isCron && !createdBy) return jsonError(400, "Missing GETSALES_CRON_USER_ID (required for cron GetSales sync)");

    const payload = (await req.json().catch(() => ({}))) as {
      since?: string; // ISO or YYYY-MM-DD
      max?: number;
      dry_run?: boolean;
      skip_state_update?: boolean; // when true, do not update sales_getsales_sync_state
      include_cursor?: boolean; // when true, include max_seen cursor info in response
      max_pushes?: number; // optional cap for HubSpot pushes (useful for safe testing)
      max_created_contacts?: number; // optional cap for created contacts (useful for safe testing)
      debug?: boolean; // when true, include debug info in response
    };

    // Allow small runs for testing, but backfill may need a large cap.
    // Keep a hard upper bound to avoid accidental unbounded memory use.
    const max = Math.max(1, Math.min(100000, Number(payload?.max ?? 200)));
    const dryRun = !!payload?.dry_run;
    const skipStateUpdate = !!payload?.skip_state_update;
    const includeCursor = !!payload?.include_cursor;
    const maxPushes = payload?.max_pushes != null ? Math.max(1, Math.min(1000, Number(payload.max_pushes) || 0)) : null;
    const maxCreatedContacts = payload?.max_created_contacts != null ? Math.max(1, Math.min(1000, Number(payload.max_created_contacts) || 0)) : null;
    const debug = !!payload?.debug;
    const pg = postgrestHeadersFor(authHeader, isCron);

    const sinceIso =
      String(payload?.since ?? "").trim() ||
      (await getSyncState(pg, createdBy)) ||
      isoNowMinusMs(7 * 24 * 60 * 60 * 1000);
    const sinceMs = toMs(sinceIso) ?? Date.parse(sinceIso);
    if (!Number.isFinite(sinceMs)) return jsonError(400, "Bad request: invalid since");

    // 1) Emails (disabled: we only keep LinkedIn for GetSales)
    console.log(`[GetSales Sync] Skipping emails (LinkedIn-only mode)`);
    const emailsOut: any[] = [];

    // 2) LinkedIn messages
    console.log(`[GetSales Sync] Fetching LinkedIn messages...`);
    const liOut = await listLinkedinMessagesSince(sinceMs, max);
    console.log(`[GetSales Sync] ✅ Found ${liOut.length} LinkedIn messages`);

    // 2.1) Automations (flows) for best-effort enrichment
    let flowLookups: { byWorkspace: Map<string, string>; byVersion: Map<string, string> } = {
      byWorkspace: new Map(),
      byVersion: new Map()
    };
    try {
      const flows = await listFlows(2000);
      flowLookups = buildFlowLookups(flows);
      console.log(`[GetSales Sync] ✅ Found ${flows.length} flows (for enrichment)`);
    } catch (e: any) {
      console.warn(`[GetSales Sync] ⚠️ Failed to load flows for enrichment: ${String(e?.message || e)}`);
    }

    // 3) Activities (Connection Requests)
    console.log(`[GetSales Sync] Fetching connection requests...`);
    const actSent = await listActivitiesSince(sinceMs, max, "linkedin_connection_request_sent");
    const actAccepted = await listActivitiesSince(sinceMs, max, "linkedin_connection_request_accepted");
    const actOut = [...actSent, ...actAccepted];
    console.log(`[GetSales Sync] ✅ Found ${actOut.length} connection requests`);

    const events: Array<{ source: string; uuid: string; lead_uuid: string; occurred_at_ms: number; payload: any }> = [];

    for (const e of emailsOut) {
      // Email events are ignored in LinkedIn-only mode.
      const uuid = String(e?.uuid ?? "").trim();
      const leadUuid = String(e?.lead_uuid ?? "").trim();
      if (!uuid || !leadUuid) continue;
    }
    /**
     * Check if a LinkedIn message looks like InMail.
     *
     * We exclude InMail from metrics to match GetSales UI.
     */
    function isInmailMessage(msg: any) {
      const status = String(msg?.status ?? "").trim().toLowerCase();
      const text = String(msg?.text ?? "").trim().toLowerCase();
      const type = String(msg?.type ?? "").trim().toLowerCase();
      return status.includes("inmail") || type.includes("inmail") || text.includes("inmail");
    }

    /**
     * Extract message_hash for message-level counting and dedupe.
     */
    function getMessageHash(msg: any) {
      return String(msg?.message_hash || msg?.messageHash || "").trim();
    }

    for (const m of liOut) {
      const uuid = String(m?.uuid ?? "").trim();
      const leadUuid = String(m?.lead_uuid ?? "").trim();
      const sentAt = toMs(m?.sent_at) ?? toMs(m?.updated_at) ?? toMs(m?.created_at) ?? null;
      const readAt = toMs(m?.read_at) ?? null;
      if (!uuid || !leadUuid || !sentAt) continue;
      if (sentAt < sinceMs) continue;
      if (isInmailMessage(m)) continue;
      const messageHash = getMessageHash(m);
      if (!messageHash) continue;
      const flowResolved = resolveFlowUuidForMessage(m, flowLookups);
      const type = String(m?.type ?? "").trim().toLowerCase();
      const isOut = type === "outbox" || type === "outgoing" || type === "sent";
      const isIn = type === "inbox" || type === "incoming" || type === "reply";
      // Create transactional rows per LinkedIn event kind.
      if (isIn) {
        events.push({
          source: "linkedin",
          uuid: `${uuid}:linkedin_message_replied`,
          lead_uuid: leadUuid,
          occurred_at_ms: sentAt,
          payload: {
            ...m,
            event_kind: "linkedin_message_replied",
            event_direction: "inbound",
            ...(flowResolved.flowUuid ? { flow_uuid: flowResolved.flowUuid, flow_uuid_source: flowResolved.source } : {})
          }
        });
      } else {
        events.push({
          source: "linkedin",
          uuid: `${uuid}:linkedin_message_sent`,
          lead_uuid: leadUuid,
          occurred_at_ms: sentAt,
          payload: {
            ...m,
            event_kind: "linkedin_message_sent",
            event_direction: "outbound",
            ...(flowResolved.flowUuid ? { flow_uuid: flowResolved.flowUuid, flow_uuid_source: flowResolved.source } : {})
          }
        });
      }
      if (readAt && isOut) {
        events.push({
          source: "linkedin",
          uuid: `${uuid}:linkedin_message_opened`,
          lead_uuid: leadUuid,
          occurred_at_ms: readAt,
          payload: {
            ...m,
            event_kind: "linkedin_message_opened",
            event_direction: "outbound",
            ...(flowResolved.flowUuid ? { flow_uuid: flowResolved.flowUuid, flow_uuid_source: flowResolved.source } : {})
          }
        });
      }
    }
    const trackedActivityTypes = ["linkedin_connection_request_sent", "linkedin_connection_request_accepted"];
    for (const a of actOut) {
      if (!trackedActivityTypes.includes(a.type)) continue;
      const uuid = String(a.id);
      const leadUuid = String(a.lead_uuid ?? "").trim();
      const at = toMs(a.created_at) ?? toMs(a.updated_at) ?? null;
      if (!uuid || !leadUuid || !at) continue;
      if (at < sinceMs) continue;
      if (a.type === "linkedin_connection_request_sent") {
        events.push({
          source: "linkedin_connection",
          uuid: `${uuid}:linkedin_connection_request_sent`,
          lead_uuid: leadUuid,
          occurred_at_ms: at,
          payload: { ...a, event_kind: "linkedin_connection_request_sent", event_direction: "outbound" }
        });
      } else if (a.type === "linkedin_connection_request_accepted") {
        events.push({
          source: "linkedin_connection",
          uuid: `${uuid}:linkedin_connection_request_accepted`,
          lead_uuid: leadUuid,
          occurred_at_ms: at,
          payload: { ...a, event_kind: "linkedin_connection_request_accepted", event_direction: "inbound" }
        });
      }
    }

    // sort oldest first for stable cursor
    events.sort((a, b) => a.occurred_at_ms - b.occurred_at_ms);

    const leadCache = new Map<string, any>();
    const stats = {
      since: new Date(sinceMs).toISOString(),
      scanned: events.length,
      inserted: 0,
      pushed_to_hubspot: 0,
      created_contacts: 0,
      associated_companies: 0,
      created_companies: 0,
      skipped_duplicates: 0,
      skipped_no_contact: 0,
      errors: 0
    };
    const debugOut = debug ? {
      dry_run: dryRun,
      max,
      max_pushes: maxPushes,
      max_created_contacts: maxCreatedContacts,
      emails: emailsOut.length,
      linkedin: liOut.length,
      activities: actOut.length,
      samples: [] as any[]
    } : null;

    let maxSeen = sinceMs;
    let processed = 0;

    for (const ev of events) {
      processed++;
      maxSeen = Math.max(maxSeen, ev.occurred_at_ms);
      const gsId = `${ev.source}:${ev.uuid}`;

      // Resolve lead
      let lead = leadCache.get(ev.lead_uuid) ?? null;
      let leadFetchError: string | null = null;
      if (!lead) {
        try {
          lead = await getLead(ev.lead_uuid);
          leadCache.set(ev.lead_uuid, lead);
        } catch (e: any) {
          lead = null;
          // Keep a short error string for debugging (do not throw; we still want to process other events).
          leadFetchError = String(e?.message || e || "getLead failed");
        }
      }
      const contactEmail = pickContactEmailFromLead(lead) || (ev.source === "email" ? String(ev.payload?.to_email ?? "").trim().toLowerCase() : "");
      const contactEmailFromPayload = pickContactEmailFromEventPayload(ev.source, ev.payload);
      const emailForContact = contactEmail || contactEmailFromPayload;

      // Dedup insert (skip writes on dry-run)
      if (!dryRun) {
        const inserted = await tryInsertEvent(pg, {
          source: ev.source,
          getsales_uuid: gsId,
          lead_uuid: ev.lead_uuid,
          contact_email: emailForContact || null,
          occurred_at: new Date(ev.occurred_at_ms).toISOString(),
          payload: ev.payload,
          ...(createdBy ? { created_by: createdBy } : {})
        }).catch((e) => {
          stats.errors += 1;
          return null;
        });

        if (!inserted) {
          stats.skipped_duplicates += 1;
          // If we resolved a flow_uuid, backfill it into the existing row.
          if (ev?.payload?.flow_uuid) {
            await updateEvent(pg, ev.source, gsId, { payload: ev.payload }, createdBy).catch(() => { });
          }
          continue;
        }
        stats.inserted += 1;
      }

      // Progress logging every 10 events
      if (processed % 10 === 0) {
        console.log(`[GetSales Sync] ${processed}/${events.length} | Inserted: ${stats.inserted}, Pushed: ${stats.pushed_to_hubspot}, Skipped: ${stats.skipped_duplicates}`);
      }
      if (processed % 50 === 0) {
        console.log(`[GetSales Sync] Full stats:`, stats);
      }

      // HubSpot push
      if (dryRun) continue;
      try {
        // If we couldn't resolve lead and we don't have an email from payload, we must not create a HubSpot contact.
        // Otherwise, we'll create empty contacts with a single note.
        if (!lead && !emailForContact) {
          stats.skipped_no_contact += 1;
          if (debugOut && debugOut.samples.length < 20) {
            debugOut.samples.push({
              gs_id: gsId,
              source: ev.source,
              lead_uuid: ev.lead_uuid,
              skipped_reason: "no_lead_and_no_email",
              lead_fetch_error: leadFetchError,
              payload_keys: Object.keys(ev.payload || {}).slice(0, 30)
            });
          }
          continue;
        }

        // If lead fetch failed, try to build a minimal lead-like object from payload
        // (safe because we still enforce non-empty props on creation).
        const leadForHubspot = lead || pseudoLeadFromEventPayload(ev.payload);
        const r = await resolveHubspotContactIdForLead(leadForHubspot, emailForContact);
        const contactId = r.contactId;
        if (!contactId) {
          stats.skipped_no_contact += 1;
          if (debugOut && debugOut.samples.length < 20) {
            const leadObj = leadForHubspot ?? null;
            const leadKeys = leadObj ? Object.keys(leadObj) : [];
            const leadName = String(leadObj?.name ?? leadObj?.full_name ?? leadObj?.fullName ?? leadObj?.first_name ?? leadObj?.firstName ?? "").trim();
            const leadLinkedin = String(leadObj?.linkedin ?? leadObj?.linkedin_url ?? leadObj?.linkedinUrl ?? leadObj?.ln_id ?? leadObj?.lnId ?? "").trim();
            const leadEmail = String(leadObj?.work_email ?? leadObj?.workEmail ?? leadObj?.personal_email ?? leadObj?.personalEmail ?? leadObj?.email ?? "").trim();
            debugOut.samples.push({
              gs_id: gsId,
              source: ev.source,
              lead_uuid: ev.lead_uuid,
              skipped_reason: "no_contact_id",
              matched_by: r.matchedBy,
              matched_value: r.matchedValue,
              email: emailForContact || null
              ,
              lead_fetch_error: leadFetchError,
              lead_summary: {
                keys: leadKeys.slice(0, 25),
                has_email: !!leadEmail,
                has_linkedin: !!leadLinkedin,
                has_name: !!leadName
              }
            });
          }
          continue;
        }
        if (r.matchedBy === "created") stats.created_contacts += 1;
        if (debugOut && debugOut.samples.length < 20) {
          debugOut.samples.push({
            gs_id: gsId,
            source: ev.source,
            lead_uuid: ev.lead_uuid,
            matched_by: r.matchedBy,
            matched_value: r.matchedValue,
            contact_id: contactId,
            contact_email: emailForContact || null
          });
        }

        // Best-effort company association (and optional company creation).
        try {
          const companyRes = await resolveAndAttachCompanyForContact(contactId, leadForHubspot, emailForContact || "");
          if (companyRes.companyId) stats.associated_companies += 1;
          if (companyRes.created) stats.created_companies += 1;
        } catch {
          // ignore company association errors (do not block activities)
        }

        const body = ev.source === "email" ? buildEmailNote(ev.payload, leadForHubspot) :
          ev.source === "linkedin" ? buildLinkedinNote(ev.payload, leadForHubspot) :
            buildLinkedinConnectionNote(ev.payload, leadForHubspot);
        const engId = await hubspotCreateNote(contactId, ev.occurred_at_ms, body);
        stats.pushed_to_hubspot += 1;

        // Update event with hubspot_contact_id regardless of note creation success.
        // This ensures the event is counted in dashboard stats even if the note push failed.
        const updates: any = { hubspot_contact_id: contactId };
        if (engId) updates.hubspot_engagement_id = Number(engId);

        if (contactId) {
          await updateEvent(pg, ev.source, gsId, updates, createdBy).catch(() => { });
        }

        // Safety caps for testing runs.
        if (maxPushes != null && stats.pushed_to_hubspot >= maxPushes) break;
        if (maxCreatedContacts != null && stats.created_contacts >= maxCreatedContacts) break;
      } catch {
        stats.errors += 1;
      }
    }

    // Update cursor
    if (!dryRun && !skipStateUpdate) {
      await setSyncState(pg, new Date(maxSeen).toISOString(), createdBy).catch(() => { });
    }

    return NextResponse.json({
      ok: true,
      stats,
      ...(debugOut ? { debug: debugOut } : {}),
      ...(includeCursor
        ? {
          cursor: {
            since: new Date(sinceMs).toISOString(),
            max_seen: new Date(maxSeen).toISOString()
          }
        }
        : {})
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


