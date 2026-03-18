import { NextResponse } from "next/server";

type SupabaseUserResponse = { email?: string | null };
type PostgrestHeaders = { apikey: string; Authorization: string };

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

let lastGetSalesCallAt = 0;
async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function getsalesFetch(path: string, init?: RequestInit, opts?: { minIntervalMs?: number; maxRetries?: number }) {
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

async function getLead(leadUuid: string) {
  const uuid = String(leadUuid || "").trim();
  if (!uuid) return null;
  const res = await getsalesFetch(`/leads/api/leads/${encodeURIComponent(uuid)}`, { method: "GET" });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales lead fetch failed"));
  return json?.data ?? json;
}

function looksLikeEmail(v: string) {
  const t = String(v || "").trim();
  return !!t && t.includes("@") && !t.includes(" ");
}

function pickContactEmailFromLead(lead: any): string | null {
  const raw =
    String(lead?.email ?? "").trim() ||
    String(lead?.work_email ?? "").trim() ||
    String(lead?.personal_email ?? "").trim() ||
    (Array.isArray(lead?.emails) ? String(lead.emails.find((e: any) => looksLikeEmail(e)) || "").trim() : "") ||
    (Array.isArray(lead?.contacts)
      ? String(
          lead.contacts
            .flatMap((c: any) => [c?.email, c?.work_email, c?.personal_email].filter(Boolean))
            .find((e: any) => looksLikeEmail(String(e))) || ""
        ).trim()
      : "");
  const e = String(raw || "").trim().toLowerCase();
  return looksLikeEmail(e) ? e : null;
}

export async function POST(req: Request) {
  try {
    const syncSecret = String(process.env.GETSALES_SYNC_SECRET ?? "").trim();
    const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
    const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
    const authHeader = String(req.headers.get("authorization") ?? "").trim();
    const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
    const secretOk = (!!syncSecret && (gotSecret === syncSecret || bearer === syncSecret)) || (!!cronSecret && bearer === cronSecret);
    const isCron = !!cronSecret && bearer === cronSecret;
    const createdBy = isCron ? (String(process.env.GETSALES_CRON_USER_ID ?? "").trim() || null) : null;

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
      limit?: number;
      dry_run?: boolean;
    };
    const limit = Math.max(1, Math.min(500, Number(payload?.limit ?? 200)));
    const dryRun = !!payload?.dry_run;
    const pg = postgrestHeadersFor(authHeader, isCron);

    const where = createdBy ? `created_by=eq.${encodeURIComponent(createdBy)}&` : "";
    const rows = await postgrestJson(
      pg,
      "GET",
      `sales_getsales_events?select=id,lead_uuid,contact_email&${where}contact_email=is.null&lead_uuid=not.is.null&order=occurred_at.asc.nullslast&limit=${limit}`
    );

    const items = Array.isArray(rows) ? rows : [];
    const stats = { scanned: items.length, updated: 0, still_missing: 0, errors: 0 };

    for (const r of items) {
      const id = String(r?.id ?? "").trim();
      const leadUuid = String(r?.lead_uuid ?? "").trim();
      if (!id || !leadUuid) continue;
      try {
        const lead = await getLead(leadUuid);
        const email = pickContactEmailFromLead(lead);
        if (!email) {
          stats.still_missing += 1;
          continue;
        }
        if (!dryRun) {
          await postgrestJson(
            pg,
            "PATCH",
            `sales_getsales_events?${where}id=eq.${encodeURIComponent(id)}`,
            { contact_email: email },
            { Prefer: "return=minimal" }
          );
        }
        stats.updated += 1;
      } catch {
        stats.errors += 1;
      }
    }

    return NextResponse.json({ ok: true, stats });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


