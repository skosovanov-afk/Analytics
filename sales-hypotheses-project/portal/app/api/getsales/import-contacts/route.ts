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

async function getsalesFetch(path: string, init?: RequestInit, opts?: { minIntervalMs?: number; maxRetries?: number }) {
  const token = process.env.GETSALES_API_TOKEN ?? process.env.GETSALES_BEARER_TOKEN ?? "";
  if (!token) throw new Error("Missing GETSALES_API_TOKEN");
  const base = (process.env.GETSALES_BASE_URL ?? "https://amazing.getsales.io").replace(/\/+$/, "");

  const minIntervalMs = Math.max(50, Math.min(600, Number(opts?.minIntervalMs ?? 140)));
  const maxRetries = Math.max(0, Math.min(8, Number(opts?.maxRetries ?? 4)));

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const now = Date.now();
    const wait = Math.max(0, lastGetSalesCallAt + minIntervalMs - now);
    if (wait) await sleep(wait);
    lastGetSalesCallAt = Date.now();

    const res = await fetch(`${base}${path}`, {
      ...init,
      headers: {
        Authorization: `Bearer ${token}`,
        ...(init?.headers ?? {})
      }
    });
    if (res.status !== 429 && res.status !== 503) return res;
    await sleep(Math.min(10_000, 900 + attempt * 700));
  }
  throw new Error("GetSales rate limit / unavailable. Try again later.");
}

async function hubspotFetch(url: string, init?: RequestInit, opts?: { minIntervalMs?: number; maxRetries?: number }) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const minIntervalMs = Math.max(50, Math.min(600, Number(opts?.minIntervalMs ?? 160)));
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

function pickContactPropsFromLead(lead: any) {
  const email = pickEmailFromLead(lead);
  const first = String(lead?.first_name ?? "").trim();
  const last = String(lead?.last_name ?? "").trim();
  const full = String(lead?.name ?? "").trim();
  const [f2, ...rest] = !first && !last && full ? full.split(" ") : [];
  const firstname = first || f2 || "";
  const lastname = last || (rest.length ? rest.join(" ") : "");

  const position = String(lead?.position ?? "").trim();
  const companyName = String(lead?.company_name ?? "").trim();

  // Contact LinkedIn URL property: standard HubSpot field is hs_linkedin_url.
  const linkedinProp = String(process.env.HUBSPOT_CONTACT_LINKEDIN_PROPERTY ?? "hs_linkedin_url").trim() || "hs_linkedin_url";
  const linkedin = String(lead?.linkedin ?? "").trim();
  const linkedinUrl =
    linkedin && (linkedin.startsWith("http://") || linkedin.startsWith("https://"))
      ? linkedin
      : linkedin
        ? `https://www.linkedin.com/in/${linkedin.replace(/^\/+|\/+$/g, "")}/`
        : "";

  const props: Record<string, any> = {};
  if (email) props.email = email;
  if (firstname) props.firstname = firstname;
  if (lastname) props.lastname = lastname;
  if (position) props.jobtitle = position;
  if (companyName) props.company = companyName;
  if (linkedinUrl) props[linkedinProp] = linkedinUrl;
  return { email, props, companyName };
}

async function getsalesListLeadsByListUuidPage(args: { listUuid: string; offset: number; limit: number }) {
  const listUuid = String(args.listUuid || "").trim();
  const offset = Math.max(0, Number(args.offset || 0));
  const limit = Math.max(1, Math.min(200, Number(args.limit || 200)));

  const body = {
    limit,
    offset,
    filter: { list_uuid: listUuid }
  };
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
  const nextOffset = offset + leads.length;
  return { leads, total, hasMore, nextOffset };
}

async function hubspotBatchUpsertContactsByEmail(inputs: Array<{ email: string; properties: Record<string, any> }>) {
  // Best-effort: use batch upsert if available. If the endpoint is missing, caller should fallback.
  const idByEmail = new Map<string, string>();
  const rawBatches: any[] = [];

  // HubSpot batch endpoints commonly limit inputs per request; keep it conservative.
  const batchSize = 100;
  for (let i = 0; i < inputs.length; i += batchSize) {
    const batch = inputs.slice(i, i + batchSize);
    const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/contacts/batch/upsert?idProperty=email", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        inputs: batch.map((x) => ({ id: x.email, properties: x.properties }))
      })
    });
    const json = (await res.json()) as any;
    if (!res.ok) {
      const msg = String(json?.message || json?.error || "HubSpot batch upsert failed");
      const err: any = new Error(msg);
      err.status = res.status;
      throw err;
    }
    rawBatches.push(json);
    const results = Array.isArray(json?.results) ? json.results : [];
    for (const r of results) {
      const id = String(r?.id ?? "").trim();
      const email = String(r?.properties?.email ?? "").trim().toLowerCase();
      if (id && email) idByEmail.set(email, id);
    }
  }

  return { idByEmail, raw: rawBatches };
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
  const res = await hubspotFetch(`https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(contactId)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties: props })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot contact update failed"));
  return true;
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
      // Max items total (cap for safety); useful if you want to stop early.
      max?: number;
      // Page controls to avoid Vercel timeouts.
      offset?: number;
      batch_limit?: number;
      dry_run?: boolean;
    };

    const listUuid = String(payload?.list_uuid ?? "").trim();
    if (!listUuid) return jsonError(400, "Bad request: list_uuid is required");

    const max = Math.max(1, Math.min(10000, Number(payload?.max ?? 1000)));
    const dryRun = !!payload?.dry_run;

    const offset = Math.max(0, Number(payload?.offset ?? 0) || 0);
    // Keep it small to avoid Vercel serverless timeouts (HubSpot upsert + GetSales fetch).
    const batchLimit = Math.max(1, Math.min(50, Number(payload?.batch_limit ?? 50) || 50));

    const { leads, total, hasMore, nextOffset } = await getsalesListLeadsByListUuidPage({
      listUuid,
      offset,
      // Respect max: don't request beyond remaining.
      limit: Math.min(batchLimit, Math.max(0, max - offset) || batchLimit)
    });
    const contacts = (leads as any[])
      .map((l: any) => ({ lead: l, ...pickContactPropsFromLead(l) }))
      .filter((x: any) => x.email);

    const stats = {
      list_uuid: listUuid,
      list_total: total,
      max_requested: max,
      leads_scanned: leads.length,
      contacts_with_email: contacts.length,
      upserted: 0,
      created: 0,
      updated: 0,
      skipped: 0,
      errors: 0,
      page_offset: offset,
      page_limit: batchLimit,
      next_offset: nextOffset,
      has_more: hasMore && nextOffset < max
    };

    if (dryRun) {
      return NextResponse.json({
        ok: true,
        dry_run: true,
        stats,
        sample: contacts.slice(0, 3).map((c) => ({ email: c.email, props: c.props }))
      });
    }

    // Try fast path: batch upsert by email
    try {
      const { idByEmail } = await hubspotBatchUpsertContactsByEmail(contacts.map((c) => ({ email: c.email, properties: c.props })));
      stats.upserted = idByEmail.size;
    } catch (e: any) {
      // Fallback: per-contact upsert via search + create/update (slower but reliable)
      const status = Number(e?.status ?? 0) || 0;
      if (status && status !== 404) {
        // Keep going with fallback anyway.
      }
      for (const c of contacts) {
        try {
          const existingId = await hubspotFindContactIdByEmail(c.email);
          if (existingId) {
            await hubspotUpdateContact(existingId, c.props);
            stats.updated += 1;
          } else {
            const id = await hubspotCreateContact(c.props);
            if (id) stats.created += 1;
          }
          stats.upserted += 1;
        } catch {
          stats.errors += 1;
        }
      }
    }

    return NextResponse.json({ ok: true, stats });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


