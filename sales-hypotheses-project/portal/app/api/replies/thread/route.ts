import { NextResponse } from "next/server";
import {
  getSupabaseUserFromAuthHeader,
  postgrestHeadersFor,
} from "@/app/lib/supabase-server";
import { smartleadFetch } from "@/app/lib/smartlead";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

async function postgrestGet(h: any, path: string): Promise<any[]> {
  const sbUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const url = `${sbUrl}/rest/v1/${path}`;
  const res = await fetch(url, {
    headers: { ...h, "Content-Type": "application/json" },
  });
  if (!res.ok) return [];
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

async function postgrestPatch(h: any, path: string, body: any): Promise<void> {
  const sbUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const url = `${sbUrl}/rest/v1/${path}`;
  await fetch(url, {
    method: "PATCH",
    headers: { ...h, "Content-Type": "application/json", Prefer: "return=minimal" },
    body: JSON.stringify(body),
  }).catch(() => {});
}

/**
 * POST /api/replies/thread
 * Body: { campaign_id: number, email: string }
 *
 * 1. Try reading reply_body from Supabase (smartlead_events)
 * 2. If not cached, fetch from SmartLead API and cache in Supabase
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

    // Use service role key for DB reads/writes (bypasses RLS)
    const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
    const h = serviceRoleKey
      ? { apikey: serviceRoleKey, Authorization: `Bearer ${serviceRoleKey}` }
      : postgrestHeadersFor(auth, false);

    // 1. Check Supabase for cached reply_body + all events for this lead/campaign
    const events = await postgrestGet(
      h,
      `smartlead_events?select=id,event_type,occurred_at,subject,message_body,reply_body,sequence_number,email,from_email,to_email&campaign_id=eq.${encodeURIComponent(campaignId)}&email=eq.${encodeURIComponent(email)}&order=occurred_at.asc`
    );

    const replyEvents = events.filter((e: any) => e.event_type === "reply");
    const sentEvents = events.filter((e: any) => e.event_type === "sent");
    const hasAllBodies = replyEvents.length > 0 && replyEvents.every((e: any) => e.reply_body);

    if (hasAllBodies) {
      // Build thread from Supabase data
      const messages = [
        ...sentEvents.map((e: any) => ({
          id: String(e.id),
          direction: "out" as const,
          body: e.message_body ?? null,
          timestamp: e.occurred_at ?? "",
          from: e.from_email ?? null,
          to: e.to_email ?? email,
          subject: e.subject ?? null,
        })),
        ...replyEvents.map((e: any) => ({
          id: String(e.id),
          direction: "in" as const,
          body: e.reply_body ?? null,
          timestamp: e.occurred_at ?? "",
          from: email,
          to: e.to_email ?? null,
          subject: e.subject ?? null,
        })),
      ].sort((a, b) => (a.timestamp || "").localeCompare(b.timestamp || ""));

      return NextResponse.json({ ok: true, messages, source: "supabase" });
    }

    // 2. Fallback: fetch from SmartLead API
    const leadData = await smartleadFetch(
      `/api/v1/leads/?email=${encodeURIComponent(email)}`
    );

    const globalLeadId = leadData?.id;
    if (!globalLeadId) {
      return NextResponse.json({ ok: true, messages: [] });
    }

    const historyData = await smartleadFetch(
      `/api/v1/campaigns/${encodeURIComponent(campaignId)}/leads/${encodeURIComponent(globalLeadId)}/message-history?show_plain_text_response=true`
    );

    const history = historyData?.history ?? (Array.isArray(historyData) ? historyData : []);

    const messages = history.map((m: any) => ({
      id: m.stats_id || m.id || String(Math.random()),
      direction: String(m.type ?? "").toUpperCase() === "REPLY" ? "in" : "out",
      body: m.email_body ?? m.body ?? null,
      timestamp: m.time ?? m.sent_at ?? m.received_at ?? "",
      from: m.from ?? null,
      to: m.to ?? null,
      subject: m.email_subject ?? m.subject ?? null,
    }));

    // 3. Cache-on-read: save reply bodies to Supabase (fire-and-forget)
    const replyBodies = history
      .filter((m: any) => String(m.type ?? "").toUpperCase() === "REPLY")
      .map((m: any) => ({
        body: String(m.email_body ?? m.body ?? "").trim(),
        time: m.time ?? m.sent_at ?? m.received_at ?? "",
      }))
      .filter((r: any) => r.body);

    if (replyBodies.length > 0 && replyEvents.length > 0) {
      // Match reply bodies to reply events by timestamp proximity
      for (const replyEvent of replyEvents) {
        if (replyEvent.reply_body) continue; // already cached
        const eventTime = new Date(replyEvent.occurred_at).getTime();
        // Find closest API reply by timestamp
        let bestMatch: { body: string; time: string } | null = null;
        let bestDist = Infinity;
        for (const rb of replyBodies) {
          const dist = Math.abs(new Date(rb.time).getTime() - eventTime);
          if (dist < bestDist) { bestDist = dist; bestMatch = rb; }
        }
        if (bestMatch && bestDist < 86400000) { // within 24h
          postgrestPatch(h, `smartlead_events?id=eq.${replyEvent.id}`, {
            reply_body: bestMatch.body,
          });
        }
      }
    }

    return NextResponse.json({ ok: true, messages, source: "smartlead_api" });
  } catch (e: any) {
    console.error("replies/thread error:", e);
    return jsonError(500, String(e?.message || e));
  }
}
