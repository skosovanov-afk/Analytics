import { NextResponse } from "next/server";
import { getSupabaseUserFromAuthHeader } from "@/app/lib/supabase-server";
import { smartleadFetch } from "@/app/lib/smartlead";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

/**
 * POST /api/replies/thread
 * Body: { campaign_id: number, email: string }
 *
 * Fetches the full email thread from Smartlead API for a given lead.
 */
export async function POST(req: Request) {
  try {
    const auth = req.headers.get("authorization") ?? "";
    const user = await getSupabaseUserFromAuthHeader(auth);
    if (!user?.email) return jsonError(401, "Not authorized");

    const body = await req.json().catch(() => ({}));
    const campaignId = body.campaign_id;
    const email = String(body.email ?? "").trim().toLowerCase();

    if (!campaignId || !email) {
      return jsonError(400, "campaign_id and email are required");
    }

    // 1. Resolve global lead_id from email
    const leadData = await smartleadFetch(
      `/api/v1/leads/?email=${encodeURIComponent(email)}`
    );

    const globalLeadId = leadData?.id;
    if (!globalLeadId) {
      return NextResponse.json({ ok: true, messages: [] });
    }

    // 2. Fetch message history
    const historyData = await smartleadFetch(
      `/api/v1/campaigns/${encodeURIComponent(campaignId)}/leads/${encodeURIComponent(globalLeadId)}/message-history?show_plain_text_response=true`
    );

    const history = historyData?.history ?? (Array.isArray(historyData) ? historyData : []);

    // 3. Map to common format
    const messages = history.map((m: any) => ({
      id: m.stats_id || m.id || String(Math.random()),
      direction: String(m.type ?? "").toUpperCase() === "REPLY" ? "in" : "out",
      body: m.email_body ?? m.body ?? null,
      timestamp: m.time ?? m.sent_at ?? m.received_at ?? "",
      from: m.from ?? null,
      to: m.to ?? null,
      subject: m.email_subject ?? m.subject ?? null,
    }));

    return NextResponse.json({ ok: true, messages });
  } catch (e: any) {
    console.error("replies/thread error:", e);
    return jsonError(500, String(e?.message || e));
  }
}
