import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

type SupabaseUserResponse = { id?: string | null; email?: string | null };

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function parseHubspotListIdFromUrl(url: string) {
  const t = String(url ?? "").trim();
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  return m?.[1] ? m[1] : null;
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

async function hubspotFetch(url: string, init?: RequestInit, opts?: { minIntervalMs?: number; maxRetries?: number }) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const minIntervalMs = Math.max(60, Math.min(800, Number(opts?.minIntervalMs ?? 160)));
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

  throw new Error("HubSpot rate limit: too many requests (429). Try again later.");
}

function toTs(v: any): number | null {
  if (v == null || v === "") return null;
  const n = Number(String(v).trim());
  if (Number.isFinite(n) && n > 0) return n;
  const ms = Date.parse(String(v));
  return Number.isFinite(ms) ? ms : null;
}

async function hubspotBatchRead(
  object: "companies" | "contacts",
  ids: number[],
  properties: string[]
) {
  const chunkSize = 100;
  const out: any[] = [];
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const res = await hubspotFetch(`https://api.hubapi.com/crm/v3/objects/${object}/batch/read`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ properties, inputs: chunk.map((id) => ({ id: String(id) })) })
    });
    const json = (await res.json().catch(() => null)) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot batch read failed"));
    const results = Array.isArray(json?.results) ? json.results : [];
    out.push(...results);
  }
  return out;
}

function pickLastActivityFromProps(
  props: any,
  opts?: { allowFallback?: boolean }
): { at: number | null; type: string | null } {
  const lastEmail = toTs(props?.hs_last_email_activity_timestamp ?? props?.hs_last_email_activity_date);
  const lastMeeting = toTs(
    props?.hs_last_meeting_activity_timestamp ?? props?.hs_last_meeting_activity_date ?? props?.hs_latest_meeting_activity
  );
  const lastCall = toTs(props?.hs_last_call_activity_timestamp ?? props?.hs_last_call_activity_date);
  const last = Math.max(lastEmail ?? 0, lastMeeting ?? 0, lastCall ?? 0);
  if (!last) {
    if (!opts?.allowFallback) return { at: null, type: null };
    const fallback = toTs(
      props?.hs_lastactivitydate ??
        props?.hs_last_sales_activity_timestamp ??
        props?.hs_last_sales_activity_date
    );
    return fallback ? { at: fallback, type: "ACTIVITY" } : { at: null, type: null };
  }
  if (last === lastEmail) return { at: last, type: "EMAIL" };
  if (last === lastMeeting) return { at: last, type: "MEETING" };
  return { at: last, type: "CALL" };
}

async function processTouchJobSlice(supabaseAdmin: any, job: any, opts: { batchCompanies: number; batchContacts: number }) {
  const jobId = String(job?.id ?? "");
  const listId = String(job?.tal_list_id ?? "").trim();
  if (!jobId || !listId) throw new Error("Invalid job (missing id/tal_list_id)");

  const phase = String(job?.phase ?? "companies");

  if (phase === "deals") {
    // Backward compatibility: treat legacy "deals" phase as "companies".
  }

  if (phase === "companies" || phase === "deals") {
    const listContactsRes = await supabaseAdmin
      .from("sales_hubspot_tal_contacts")
      .select("contact_id", { count: "exact", head: true })
      .eq("tal_list_id", listId);
    if (listContactsRes.error) throw listContactsRes.error;
    const listContactsCount = Number(listContactsRes.count ?? 0) || 0;
    if (listContactsCount > 0) {
      const upd = await supabaseAdmin
        .from("sales_hubspot_tal_touch_jobs")
        .update({ phase: "contacts", last_contact_id: 0, status: "running", updated_at: new Date().toISOString() })
        .eq("id", jobId)
        .select("*")
        .single();
      if (upd.error) throw upd.error;
      return { job: upd.data, done: false };
    }

    const lastCompanyId = Number(job.last_company_id ?? job.last_deal_id ?? 0) || 0;
    const companiesRes = await supabaseAdmin
      .from("sales_hubspot_tal_companies")
      .select("company_id")
      .eq("tal_list_id", listId)
      .gt("company_id", lastCompanyId)
      .order("company_id", { ascending: true })
      .limit(opts.batchCompanies);
    if (companiesRes.error) throw companiesRes.error;
    const companyIds: number[] = (Array.isArray(companiesRes.data) ? companiesRes.data : [])
      .map((r: any) => Number(r?.company_id))
      .filter((n: number) => Number.isFinite(n) && n > 0);
    if (!companyIds.length) {
      const countRes = await supabaseAdmin
        .from("sales_hubspot_tal_companies")
        .select("company_id", { count: "exact", head: true })
        .eq("tal_list_id", listId);
      if (countRes.error) throw countRes.error;
      const companiesCount = Number(countRes.count ?? 0) || 0;
      if (!companiesCount) {
        // TAL companies not loaded yet. Keep waiting in companies phase.
        const upd = await supabaseAdmin
          .from("sales_hubspot_tal_touch_jobs")
          .update({ phase: "companies", status: "running", updated_at: new Date().toISOString() })
          .eq("id", jobId)
          .select("*")
          .single();
        if (upd.error) throw upd.error;
        return { job: upd.data, done: false };
      }

      const upd = await supabaseAdmin
        .from("sales_hubspot_tal_touch_jobs")
        .update({ phase: "contacts", last_contact_id: 0, status: "running" })
        .eq("id", jobId)
        .select("*")
        .single();
      if (upd.error) throw upd.error;
      return { job: upd.data, done: false };
    }

    const rows = await hubspotBatchRead("companies", companyIds, [
      "hs_last_email_activity_timestamp",
      "hs_last_email_activity_date",
      "hs_last_meeting_activity_timestamp",
      "hs_last_meeting_activity_date",
      "hs_latest_meeting_activity",
      "hs_last_call_activity_timestamp",
      "hs_last_call_activity_date",
      "hs_lastactivitydate",
      "hs_last_sales_activity_timestamp",
      "hs_last_sales_activity_date"
    ]);
    let processed = 0;
    let maxId = lastCompanyId;
    for (const r of rows) {
      const id = Number(r?.id ?? r?.properties?.hs_object_id ?? 0);
      if (!Number.isFinite(id) || id <= 0) continue;
      maxId = Math.max(maxId, id);
      const props = r?.properties ?? {};
      const last = pickLastActivityFromProps(props, { allowFallback: true });
      const up = await supabaseAdmin
        .from("sales_hubspot_company_touches")
        .upsert(
          { company_id: id, last_touch_at: last.at ? new Date(last.at).toISOString() : null, last_touch_type: last.type, updated_at: new Date().toISOString() },
          { onConflict: "company_id" }
        );
      if (up.error) throw up.error;
      processed++;
    }

    const upd = await supabaseAdmin
      .from("sales_hubspot_tal_touch_jobs")
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

  if (phase === "contacts") {
    const companiesProcessed = Number(job.companies_processed ?? 0) || 0;
    if (!companiesProcessed) {
      const countRes = await supabaseAdmin
        .from("sales_hubspot_tal_companies")
        .select("company_id", { count: "exact", head: true })
        .eq("tal_list_id", listId);
      if (countRes.error) throw countRes.error;
      const companiesCount = Number(countRes.count ?? 0) || 0;
      if (companiesCount) {
        // Company touches not processed yet; rewind to companies phase.
        const upd = await supabaseAdmin
          .from("sales_hubspot_tal_touch_jobs")
          .update({ phase: "companies", last_company_id: 0, status: "running" })
          .eq("id", jobId)
          .select("*")
          .single();
        if (upd.error) throw upd.error;
        return { job: upd.data, done: false };
      }
    }
    const lastContactId = Number(job.last_contact_id ?? 0) || 0;
    const contactsRes = await supabaseAdmin.rpc("sales_hubspot_tal_next_contacts", {
      p_tal_list_id: listId,
      p_last_contact_id: lastContactId,
      p_limit: opts.batchContacts
    });
    if (contactsRes.error) throw contactsRes.error;
    const contactIds: number[] = (Array.isArray(contactsRes.data) ? contactsRes.data : [])
      .map((r: any) => Number(r?.contact_id))
      .filter((n: number) => Number.isFinite(n) && n > 0);
    if (!contactIds.length) {
      const upd = await supabaseAdmin
        .from("sales_hubspot_tal_touch_jobs")
        .update({ status: "done", phase: "done", finished_at: new Date().toISOString(), updated_at: new Date().toISOString() })
        .eq("id", jobId)
        .select("*")
        .single();
      if (upd.error) throw upd.error;
      return { job: upd.data, done: true };
    }

    const rows = await hubspotBatchRead("contacts", contactIds, [
      "hs_last_email_activity_timestamp",
      "hs_last_email_activity_date",
      "hs_last_meeting_activity_timestamp",
      "hs_last_meeting_activity_date",
      "hs_latest_meeting_activity",
      "hs_last_call_activity_timestamp",
      "hs_last_call_activity_date",
      "hs_lastactivitydate",
      "hs_last_sales_activity_timestamp",
      "hs_last_sales_activity_date"
    ]);
    let processed = 0;
    let maxId = lastContactId;
    for (const r of rows) {
      const id = Number(r?.id ?? r?.properties?.hs_object_id ?? 0);
      if (!Number.isFinite(id) || id <= 0) continue;
      maxId = Math.max(maxId, id);
      const props = r?.properties ?? {};
      const last = pickLastActivityFromProps(props, { allowFallback: true });
      const up = await supabaseAdmin
        .from("sales_hubspot_contact_touches")
        .upsert(
          { contact_id: id, last_touch_at: last.at ? new Date(last.at).toISOString() : null, last_touch_type: last.type, updated_at: new Date().toISOString() },
          { onConflict: "contact_id" }
        );
      if (up.error) throw up.error;
      processed++;
    }

    const upd = await supabaseAdmin
      .from("sales_hubspot_tal_touch_jobs")
      .update({ last_contact_id: maxId, contacts_processed: (Number(job.contacts_processed ?? 0) || 0) + processed, status: "running", updated_at: new Date().toISOString() })
      .eq("id", jobId)
      .select("*")
      .single();
    if (upd.error) throw upd.error;
    return { job: upd.data, done: false };
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
      .from("sales_hubspot_tal_touch_jobs")
      .select("*")
      .in("status", ["queued", "running"])
      .order("updated_at", { ascending: true })
      .limit(1);
    if (jobsRes.error) throw jobsRes.error;
    const jobs = Array.isArray(jobsRes.data) ? jobsRes.data : [];

    const processed: any[] = [];
    for (const j of jobs) {
      if (String(j.status) === "queued") {
        await supabaseAdmin.from("sales_hubspot_tal_touch_jobs").update({ status: "running", started_at: new Date().toISOString() }).eq("id", j.id);
      }
      const started = Date.now();
      const budgetMs = 60_000;
      let cur = j;
      let lastOut: any = null;
      while (Date.now() - started < budgetMs) {
        const phase = String(cur?.phase ?? "companies");
        const batchContacts = phase === "contacts" ? 200 : 0;
        const batchCompanies = phase === "companies" || phase === "deals" ? 200 : 0;
        lastOut = await processTouchJobSlice(supabaseAdmin, cur, {
          batchCompanies: Math.max(1, batchCompanies),
          batchContacts: Math.max(1, batchContacts)
        });
        cur = lastOut.job;
        if (lastOut.done) break;
        if (phase !== String(cur?.phase ?? "")) continue;
        // Safety: if nothing changes phase-wise, we'll still exit on budget.
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

    const payload = (await req.json().catch(() => ({}))) as { talUrl?: string; talListId?: string };
    const talUrl = String(payload?.talUrl ?? "").trim();
    const talListId = String(payload?.talListId ?? "").trim();
    const listId = talListId || parseHubspotListIdFromUrl(talUrl);
    if (!listId) return jsonError(400, "Missing TAL list id. Provide talListId or talUrl.");

    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
    if (!supabaseUrl || !serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });

    const existing = await supabaseAdmin
      .from("sales_hubspot_tal_touch_jobs")
      .select("*")
      .eq("tal_list_id", listId)
      .in("status", ["queued", "running"])
      .order("updated_at", { ascending: false })
      .limit(1);
    let job = (existing.data ?? [])[0] ?? null;
    if (!job) {
      const ins = await supabaseAdmin
        .from("sales_hubspot_tal_touch_jobs")
        .insert({ tal_list_id: listId, status: "running", phase: "companies", started_at: new Date().toISOString(), created_by: userId })
        .select("*")
        .single();
      if (ins.error) throw ins.error;
      job = ins.data;
    }

    const out = await processTouchJobSlice(supabaseAdmin, job, { batchCompanies: 100, batchContacts: 100 });
    return NextResponse.json({ ok: true, job: out.job, done: !!out.done });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


