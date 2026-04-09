import { NextResponse } from "next/server";
import {
  getSupabaseUserFromAuthHeader,
  isCronAuthorized,
} from "@/app/lib/supabase-server";
import { smartleadFetch } from "@/app/lib/smartlead";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

const BATCH_SIZE = 30;
const DELAY_MS = 200; // delay between SmartLead API calls

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * POST /api/replies/backfill-body
 * Body: { limit?: number }
 *
 * Fetches reply body text from SmartLead message-history API
 * for reply events that don't have reply_body cached yet.
 * Auth: cron secret or authenticated user.
 */
export async function POST(req: Request) {
  try {
    const isCron = isCronAuthorized(req);
    if (!isCron) {
      const auth = req.headers.get("authorization") ?? "";
      const user = await getSupabaseUserFromAuthHeader(auth);
      if (!user?.email) return jsonError(401, "Not authorized");
    }

    const body = await req.json().catch(() => ({}));
    const limit = Math.min(Math.max(1, Number(body.limit) || BATCH_SIZE), 100);

    const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
    if (!serviceRoleKey) return jsonError(500, "Missing SUPABASE_SERVICE_ROLE_KEY");
    const h = { apikey: serviceRoleKey, Authorization: `Bearer ${serviceRoleKey}` };
    const sbUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";

    // 1. Find reply events without reply_body
    const findUrl = `${sbUrl}/rest/v1/smartlead_events?select=id,campaign_id,lead_id,email,occurred_at&event_type=eq.reply&reply_body=is.null&lead_id=not.is.null&order=occurred_at.desc&limit=${limit}`;
    const findRes = await fetch(findUrl, { headers: h });
    if (!findRes.ok) {
      const text = await findRes.text();
      return jsonError(500, `Failed to query events: ${text.slice(0, 200)}`);
    }
    const events = (await findRes.json()) as any[];

    if (!events.length) {
      return NextResponse.json({ ok: true, processed: 0, remaining: 0, message: "All reply bodies are cached" });
    }

    // 2. Group by (campaign_id, lead_id) to avoid duplicate API calls
    const grouped = new Map<string, { campaignId: number; leadId: number; events: any[] }>();
    for (const e of events) {
      const key = `${e.campaign_id}:${e.lead_id}`;
      if (!grouped.has(key)) {
        grouped.set(key, { campaignId: e.campaign_id, leadId: e.lead_id, events: [] });
      }
      grouped.get(key)!.events.push(e);
    }

    let filled = 0;
    let failed = 0;
    const errors: string[] = [];

    // 3. For each unique (campaign, lead), fetch message history
    for (const [key, group] of grouped) {
      try {
        // Resolve global lead_id from email (SmartLead uses global IDs in message-history)
        const email = group.events[0]?.email;
        let globalLeadId: number | null = null;

        if (email) {
          const leadData = await smartleadFetch(`/api/v1/leads/?email=${encodeURIComponent(email)}`);
          globalLeadId = leadData?.id ?? null;
        }

        if (!globalLeadId) {
          // Try using lead_id directly (may work if it's the global ID)
          globalLeadId = group.leadId;
        }

        const historyData = await smartleadFetch(
          `/api/v1/campaigns/${group.campaignId}/leads/${globalLeadId}/message-history?show_plain_text_response=true`
        );

        const history = historyData?.history ?? (Array.isArray(historyData) ? historyData : []);
        const replies = history
          .filter((m: any) => String(m.type ?? "").toUpperCase() === "REPLY")
          .map((m: any) => ({
            body: String(m.email_body ?? m.body ?? "").trim(),
            time: m.time ?? m.sent_at ?? m.received_at ?? "",
          }))
          .filter((r: any) => r.body);

        // Match to events by timestamp
        for (const event of group.events) {
          const eventTime = new Date(event.occurred_at).getTime();
          let bestBody: string | null = null;
          let bestDist = Infinity;
          for (const r of replies) {
            const dist = Math.abs(new Date(r.time).getTime() - eventTime);
            if (dist < bestDist) { bestDist = dist; bestBody = r.body; }
          }
          if (bestBody && bestDist < 86400000) {
            const patchUrl = `${sbUrl}/rest/v1/smartlead_events?id=eq.${event.id}`;
            const patchRes = await fetch(patchUrl, {
              method: "PATCH",
              headers: { ...h, "Content-Type": "application/json", Prefer: "return=minimal" },
              body: JSON.stringify({ reply_body: bestBody }),
            });
            if (patchRes.ok) filled++;
            else failed++;
          } else if (replies.length === 0) {
            // No replies in history - mark with empty string to skip next time
            const patchUrl = `${sbUrl}/rest/v1/smartlead_events?id=eq.${event.id}`;
            await fetch(patchUrl, {
              method: "PATCH",
              headers: { ...h, "Content-Type": "application/json", Prefer: "return=minimal" },
              body: JSON.stringify({ reply_body: "" }),
            });
            filled++;
          }
        }

        await sleep(DELAY_MS);
      } catch (e: any) {
        failed += group.events.length;
        errors.push(`${key}: ${String(e?.message || e).slice(0, 100)}`);
      }
    }

    // 4. Count remaining
    const countUrl = `${sbUrl}/rest/v1/smartlead_events?select=id&event_type=eq.reply&reply_body=is.null&lead_id=not.is.null&limit=1`;
    const countRes = await fetch(countUrl, { headers: { ...h, Prefer: "count=exact" } });
    const contentRange = countRes.headers.get("content-range") ?? "";
    const remaining = Number(contentRange.split("/")[1]) || 0;

    return NextResponse.json({
      ok: true,
      processed: events.length,
      filled,
      failed,
      remaining,
      unique_leads: grouped.size,
      errors: errors.length ? errors : undefined,
    });
  } catch (e: any) {
    console.error("backfill-body error:", e);
    return jsonError(500, String(e?.message || e));
  }
}
