import { NextResponse } from "next/server";
import {
  getSupabaseUserFromAuthHeader,
  postgrestHeadersFor,
} from "@/app/lib/supabase-server";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

async function postgrestAllRows(h: any, path: string, pageSize = 1000): Promise<any[]> {
  const sbUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const all: any[] = [];
  let offset = 0;
  while (true) {
    const sep = path.includes("?") ? "&" : "?";
    const url = `${sbUrl}/rest/v1/${path}${sep}limit=${pageSize}&offset=${offset}`;
    const res = await fetch(url, {
      headers: { ...h, "Content-Type": "application/json" },
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`PostgREST error: ${text}`);
    }
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) break;
    all.push(...rows);
    if (rows.length < pageSize) break;
    offset += pageSize;
  }
  return all;
}

/**
 * POST /api/replies/list
 * Body: { tab: "email" | "linkedin", since?: string, until?: string }
 *
 * Fetches replies using service role key (bypasses RLS statement timeout).
 */
export async function POST(req: Request) {
  try {
    // Auth: verify user is signed in, but use service role for actual queries
    const auth = req.headers.get("authorization") ?? "";
    const user = await getSupabaseUserFromAuthHeader(auth);
    if (!user?.email) return jsonError(401, "Not authorized");

    const body = await req.json().catch(() => ({}));
    const tabParam = String(body.tab ?? "email");
    const since: string | null = body.since || null;
    const until: string | null = body.until || null;

    // Always use service role key to avoid RLS statement timeout on heavy views
    const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
    if (!serviceRoleKey) return jsonError(500, "Missing SUPABASE_SERVICE_ROLE_KEY");
    const h = { apikey: serviceRoleKey, Authorization: `Bearer ${serviceRoleKey}` };

    let dateFilter = "";
    if (tabParam === "email") {
      if (since) dateFilter += `&occurred_at=gte.${since}`;
      if (until) dateFilter += `&occurred_at=lte.${until}T23:59:59`;
      const rows = await postgrestAllRows(h,
        `smartlead_replies_v?select=event_id,occurred_at,reply_date,campaign_id,campaign_name,email,lead_first_name,lead_last_name,lead_company,subject,sequence_number,sentiment,is_positive,tal_name&order=occurred_at.desc${dateFilter}`
      );
      return NextResponse.json({ ok: true, rows });
    } else {
      if (since) dateFilter += `&occurred_at=gte.${since}`;
      if (until) dateFilter += `&occurred_at=lte.${until}T23:59:59`;
      const rows = await postgrestAllRows(h,
        `expandi_replies_v?select=message_id,occurred_at,reply_date,messenger_id,account_name,campaign_name,contact_name,contact_email,contact_company_name,contact_job_title,reply_body,tal_name&order=occurred_at.desc${dateFilter}`
      );
      return NextResponse.json({ ok: true, rows });
    }
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}
