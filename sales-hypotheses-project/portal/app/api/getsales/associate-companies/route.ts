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

let lastGetSalesCallAt = 0;
let lastHubspotCallAt = 0;
async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function getsalesFetch(path: string, init?: RequestInit) {
  const token = process.env.GETSALES_API_TOKEN ?? process.env.GETSALES_BEARER_TOKEN ?? "";
  if (!token) throw new Error("Missing GETSALES_API_TOKEN");
  const base = (process.env.GETSALES_BASE_URL ?? "https://amazing.getsales.io").replace(/\/+$/, "");

  const minIntervalMs = 120;
  const now = Date.now();
  const wait = Math.max(0, lastGetSalesCallAt + minIntervalMs - now);
  if (wait) await sleep(wait);
  lastGetSalesCallAt = Date.now();

  return fetch(`${base}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(init?.headers ?? {})
    }
  });
}

async function hubspotFetch(url: string, init?: RequestInit) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const minIntervalMs = 140;
  const now = Date.now();
  const wait = Math.max(0, lastHubspotCallAt + minIntervalMs - now);
  if (wait) await sleep(wait);
  lastHubspotCallAt = Date.now();

  return fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(init?.headers ?? {})
    }
  });
}

function looksLikeEmail(v: string) {
  const t = String(v || "").trim();
  return !!t && t.includes("@") && !t.includes(" ");
}

function pickEmailFromLead(lead: any) {
  const w = String(lead?.work_email ?? "").trim().toLowerCase();
  const p = String(lead?.personal_email ?? "").trim().toLowerCase();
  const best = w || p;
  return looksLikeEmail(best) ? best : "";
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

function pickCompanyNameFromLead(lead: any) {
  return String(lead?.company_name ?? "").trim();
}

function companyLinkedinCandidatesFromLead(lead: any) {
  const out: string[] = [];
  const exp0 = Array.isArray(lead?.experience) && lead.experience.length ? lead.experience[0] : null;
  const nick = String(exp0?.company_nickname ?? "").trim();
  if (nick) out.push(`https://www.linkedin.com/company/${nick}/`);

  const companyLnId = String(lead?.company_ln_id ?? "").trim();
  if (companyLnId && /^[0-9]+$/.test(companyLnId)) out.push(`https://www.linkedin.com/company/${companyLnId}/`);

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

async function getsalesListLeadsPage(listUuid: string, offset: number, limit: number) {
  const body = { limit, offset, filter: { list_uuid: listUuid } };
  const res = await getsalesFetch("/leads/api/leads/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales leads search failed"));
  const rows = Array.isArray(json?.data) ? json.data : [];
  const leads = rows.map((r: any) => r?.lead ?? r);
  const total = json?.total != null && Number.isFinite(Number(json.total)) ? Number(json.total) : null;
  const hasMore = !!json?.has_more;
  return { leads, total, hasMore, nextOffset: offset + leads.length };
}

async function hubspotFindContactIdByEmail(email: string) {
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

async function hubspotFindCompanyIdByDomain(domain: string) {
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

async function hubspotFindCompanyIdByName(name: string) {
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

async function hubspotFindCompanyIdByPropertyEq(propertyName: string, value: string) {
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
  const res = await hubspotFetch(
    `https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(contactId)}/associations/companies/${encodeURIComponent(companyId)}/contact_to_company`,
    { method: "PUT" }
  );
  if (res.ok) return true;
  const json = await res.json().catch(() => null);
  const msg = String(json?.message || json?.error || "");
  // ignore "already exists"-like messages
  if (msg.toLowerCase().includes("already")) return true;
  throw new Error(msg || "HubSpot contact->company association failed");
}

async function resolveCompanyIdForLead(lead: any, email: string) {
  const allowCreate = String(process.env.GETSALES_CREATE_COMPANIES ?? "true").toLowerCase() !== "false";
  const companyLinkedinProp = String(process.env.HUBSPOT_COMPANY_LINKEDIN_PROPERTY ?? "linkedin_company_page").trim() || "linkedin_company_page";

  const companyName = pickCompanyNameFromLead(lead);
  const domain = pickDomainFromEmail(email);
  const domainOk = domain && !isProbablyPersonalDomain(domain);

  if (domainOk) {
    const existing = await hubspotFindCompanyIdByDomain(domain);
    if (existing) return { companyId: existing, created: false };
    if (allowCreate) {
      const createdId = await hubspotCreateCompany({ name: companyName || domain, domain });
      return { companyId: createdId, created: true };
    }
  }

  const liCands = companyLinkedinCandidatesFromLead(lead);
  for (const c of liCands) {
    const existing = await hubspotFindCompanyIdByPropertyEq(companyLinkedinProp, c);
    if (existing) return { companyId: existing, created: false };
    if (allowCreate) {
      const createdId = await hubspotCreateCompany({ name: companyName || c, [companyLinkedinProp]: c });
      return { companyId: createdId, created: true };
    }
  }

  if (companyName) {
    const existing = await hubspotFindCompanyIdByName(companyName);
    if (existing) return { companyId: existing, created: false };
    if (allowCreate) {
      const createdId = await hubspotCreateCompany({ name: companyName });
      return { companyId: createdId, created: true };
    }
  }

  return { companyId: null, created: false };
}

export async function POST(req: Request) {
  try {
    const syncSecret = String(process.env.GETSALES_SYNC_SECRET ?? "").trim();
    const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
    const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
    const authHeader = String(req.headers.get("authorization") ?? "").trim();
    const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
    const secretOk = (!!syncSecret && (gotSecret === syncSecret || bearer === syncSecret)) || (!!cronSecret && bearer === cronSecret);

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

    const payload = (await req.json().catch(() => ({}))) as {
      list_uuid?: string;
      max?: number;
      offset?: number;
      batch_limit?: number;
      dry_run?: boolean;
    };

    const listUuid = String(payload?.list_uuid ?? "").trim();
    if (!listUuid) return jsonError(400, "Bad request: list_uuid is required");

    const max = Math.max(1, Math.min(10000, Number(payload?.max ?? 6000)));
    const offset = Math.max(0, Number(payload?.offset ?? 0) || 0);
    const batchLimit = Math.max(1, Math.min(25, Number(payload?.batch_limit ?? 20) || 20));
    const dryRun = !!payload?.dry_run;

    const { leads, total, hasMore, nextOffset } = await getsalesListLeadsPage(listUuid, offset, Math.min(batchLimit, Math.max(0, max - offset) || batchLimit));

    const stats = {
      list_uuid: listUuid,
      list_total: total,
      max_requested: max,
      page_offset: offset,
      page_limit: batchLimit,
      next_offset: nextOffset,
      has_more: hasMore && nextOffset < max,
      scanned: leads.length,
      contacts_found: 0,
      associated: 0,
      created_companies: 0,
      skipped_no_email: 0,
      skipped_no_contact: 0,
      errors: 0
    };

    for (const lead of leads) {
      const email = pickEmailFromLead(lead);
      if (!email) {
        stats.skipped_no_email += 1;
        continue;
      }
      let contactId: string | null = null;
      try {
        contactId = await hubspotFindContactIdByEmail(email);
      } catch {
        contactId = null;
      }
      if (!contactId) {
        stats.skipped_no_contact += 1;
        continue;
      }
      stats.contacts_found += 1;

      try {
        const { companyId, created } = await resolveCompanyIdForLead(lead, email);
        if (!companyId) continue;
        if (!dryRun) await hubspotAssociateContactToCompany(contactId, companyId);
        stats.associated += 1;
        if (created) stats.created_companies += 1;
      } catch {
        stats.errors += 1;
      }
    }

    return NextResponse.json({ ok: true, dry_run: dryRun, stats });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


