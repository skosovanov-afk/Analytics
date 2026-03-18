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

  const minIntervalMs = Math.max(50, Math.min(800, Number(opts?.minIntervalMs ?? 140)));
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

  // HubSpot has strict per-second rolling limits; keep spacing conservative here.
  const minIntervalMs = Math.max(80, Math.min(2000, Number(opts?.minIntervalMs ?? 300)));
  const maxRetries = Math.max(0, Math.min(10, Number(opts?.maxRetries ?? 6)));

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
    const backoff = Number.isFinite(retryAfter) ? retryAfter * 1000 : 1200 + attempt * 900;
    await sleep(Math.min(15_000, backoff));
  }

  throw new Error("HubSpot rate limit: too many requests (429). Try again in ~15 seconds.");
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

function buildLinkedinUrlFromLead(lead: any) {
  const linkedin = String(lead?.linkedin ?? "").trim();
  if (!linkedin) return "";
  if (linkedin.startsWith("http://") || linkedin.startsWith("https://")) return linkedin;
  const slug = linkedin.replace(/^\/+|\/+$/g, "");
  return slug ? `https://www.linkedin.com/in/${slug}/` : "";
}

function linkedinCandidates(urlOrSlug: string) {
  const t = String(urlOrSlug || "").trim();
  if (!t) return [];
  if (t.startsWith("http://") || t.startsWith("https://")) {
    const noHash = t.split("#")[0].split("?")[0];
    const withSlash = noHash.endsWith("/") ? noHash : `${noHash}/`;
    const withoutSlash = withSlash.endsWith("/") ? withSlash.slice(0, -1) : withSlash;
    return Array.from(new Set([noHash, withSlash, withoutSlash].filter(Boolean)));
  }
  const slug = t.replace(/^\/+|\/+$/g, "");
  if (!slug) return [];
  const u = `https://www.linkedin.com/in/${slug}/`;
  return Array.from(new Set([slug, u, u.slice(0, -1)]));
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

async function hubspotFindContactByLinkedin(linkedinProp: string, cand: string) {
  const res = await hubspotFetch(
    "https://api.hubapi.com/crm/v3/objects/contacts/search",
    {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filterGroups: [{ filters: [{ propertyName: linkedinProp, operator: "EQ", value: cand }] }],
      properties: ["email", "firstname", "lastname", linkedinProp],
      limit: 1
    })
    },
    { minIntervalMs: 320, maxRetries: 7 }
  );
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot contacts search failed"));
  const r = Array.isArray(json?.results) ? json.results[0] : null;
  if (!r?.id) return null;
  return { id: String(r.id), properties: r?.properties ?? {} };
}

async function hubspotPatchContact(contactId: string, props: Record<string, any>) {
  const res = await hubspotFetch(
    `https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(contactId)}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ properties: props })
    },
    { minIntervalMs: 320, maxRetries: 7 }
  );
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot contact update failed"));
  return true;
}

function pickNamesFromLead(lead: any) {
  const first = String(lead?.first_name ?? "").trim();
  const last = String(lead?.last_name ?? "").trim();
  const full = String(lead?.name ?? "").trim();
  const [f2, ...rest] = !first && !last && full ? full.split(" ") : [];
  const firstname = first || f2 || "";
  const lastname = last || (rest.length ? rest.join(" ") : "");
  return { firstname, lastname };
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
      offset?: number;
      batch_limit?: number;
      max?: number;
      dry_run?: boolean;
    };

    const listUuid = String(payload?.list_uuid ?? "").trim();
    if (!listUuid) return jsonError(400, "Bad request: list_uuid is required");

    const max = Math.max(1, Math.min(10000, Number(payload?.max ?? 6000)));
    const offset = Math.max(0, Number(payload?.offset ?? 0) || 0);
    const batchLimit = Math.max(1, Math.min(50, Number(payload?.batch_limit ?? 20) || 20));
    const dryRun = !!payload?.dry_run;

    const linkedinProp = String(process.env.HUBSPOT_CONTACT_LINKEDIN_PROPERTY ?? "hs_linkedin_url").trim() || "hs_linkedin_url";

    const { leads, total, hasMore, nextOffset } = await getsalesListLeadsPage(listUuid, offset, Math.min(batchLimit, Math.max(0, max - offset) || batchLimit));

    const stats = {
      list_uuid: listUuid,
      list_total: total,
      page_offset: offset,
      page_limit: batchLimit,
      next_offset: nextOffset,
      has_more: hasMore && nextOffset < max,
      scanned: leads.length,
      candidates: 0,
      found_placeholders: 0,
      patched: 0,
      skipped_already_ok: 0,
      errors: 0
    };

    for (const lead of leads) {
      const email = pickEmailFromLead(lead);
      const liUrl = buildLinkedinUrlFromLead(lead);
      if (!email || !liUrl) continue;
      stats.candidates += 1;
      const cands = linkedinCandidates(liUrl);
      let found: { id: string; properties: any } | null = null;
      for (const c of cands) {
        try {
          found = await hubspotFindContactByLinkedin(linkedinProp, c);
          if (found) break;
        } catch {
          stats.errors += 1;
          // If a particular candidate lookup fails, try next candidate or next lead.
        }
      }
      if (!found) continue;

      const props = found.properties ?? {};
      const existingEmail = String(props?.email ?? "").trim();
      const needsEmail = !existingEmail;
      const needsName = !String(props?.firstname ?? "").trim() && !String(props?.lastname ?? "").trim();
      if (!needsEmail && !needsName) {
        stats.skipped_already_ok += 1;
        continue;
      }

      stats.found_placeholders += 1;
      if (dryRun) continue;

      const patch: any = {};
      if (needsEmail) patch.email = email;
      const { firstname, lastname } = pickNamesFromLead(lead);
      if (needsName) {
        if (firstname) patch.firstname = firstname;
        if (lastname) patch.lastname = lastname;
      }
      if (!Object.keys(patch).length) continue;
      try {
        await hubspotPatchContact(found.id, patch);
        stats.patched += 1;
      } catch {
        stats.errors += 1;
      }
    }

    return NextResponse.json({ ok: true, dry_run: dryRun, stats });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


