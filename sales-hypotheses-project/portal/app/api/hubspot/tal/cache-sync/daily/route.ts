import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

/**
 * Extract HubSpot list ID from a TAL URL.
 *
 * We accept both `/lists/<id>` and `/objectLists/<id>` forms.
 */
function parseHubspotListIdFromUrl(url: string) {
  const t = String(url ?? "").trim();
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  return m?.[1] ? m[1] : null;
}

/**
 * Validate cron auth via Vercel header or CRON_SECRET.
 */
function isCronAuthorized(req: Request) {
  const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
  if (vercelCron === "1" || vercelCron.toLowerCase() === "true") return true;
  const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
  if (!cronSecret) return false;
  const authHeader = String(req.headers.get("authorization") ?? "").trim();
  const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
  const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
  return bearer === cronSecret || gotSecret === cronSecret;
}

export async function GET(req: Request) {
  try {
    if (!isCronAuthorized(req)) return jsonError(401, "Not authorized");

    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
    if (!supabaseUrl || !serviceRoleKey) return jsonError(500, "Missing SUPABASE_SERVICE_ROLE_KEY");

    const createdBy = String(process.env.HUBSPOT_CRON_USER_ID ?? "").trim();
    if (!createdBy) return jsonError(400, "Missing HUBSPOT_CRON_USER_ID (required for cron HubSpot sync)");

    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });

    const hypRes = await supabaseAdmin
      .from("sales_hypotheses")
      .select("id, hubspot_tal_url, hubspot_contacts_list_url")
      .not("hubspot_tal_url", "is", null);
    if (hypRes.error) throw hypRes.error;

    const rows = Array.isArray(hypRes.data) ? hypRes.data : [];
    let created = 0;
    let skipped = 0;

    for (const h of rows) {
      const talUrl = String(h?.hubspot_tal_url ?? "").trim();
      const listId = parseHubspotListIdFromUrl(talUrl);
      if (!listId) {
        skipped++;
        continue;
      }

      const existsRes = await supabaseAdmin
        .from("sales_hubspot_tal_cache_jobs")
        .select("id")
        .eq("tal_list_id", listId)
        .in("status", ["queued", "running"])
        .limit(1);
      if (existsRes.error) throw existsRes.error;
      if (Array.isArray(existsRes.data) && existsRes.data.length) {
        skipped++;
        continue;
      }

      const contactsListUrl = String(h?.hubspot_contacts_list_url ?? "").trim();
      const contactsListId = contactsListUrl ? parseHubspotListIdFromUrl(contactsListUrl) : null;

      const ins = await supabaseAdmin.from("sales_hubspot_tal_cache_jobs").insert({
        tal_list_id: listId,
        hypothesis_id: String(h?.id ?? ""),
        contacts_list_id: contactsListId,
        status: "queued",
        phase: "memberships",
        created_by: createdBy,
        updated_at: new Date().toISOString()
      });
      if (ins.error) throw ins.error;
      created++;
    }

    return NextResponse.json({ ok: true, created, skipped, total: rows.length });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}
