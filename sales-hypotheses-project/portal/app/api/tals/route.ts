import { NextResponse } from "next/server";
import { getSupabaseUserFromAuthHeader } from "@/app/lib/supabase-server";

const SB_URL = () => process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
const SB_KEY = () => process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function looksLikeMissingRelation(message: string) {
  const msg = String(message || "").toLowerCase();
  return msg.includes("could not find the table") || msg.includes("schema cache") || (msg.includes("relation") && msg.includes("does not exist"));
}

function talSetupHint(message: string) {
  if (!looksLikeMissingRelation(message)) return message;
  return "TAL schema is not deployed in Supabase yet. Apply `projects/Product/supabase/sql/tals_setup_2026_03_26.sql`, then refresh after PostgREST schema cache updates.";
}

async function sbGet(auth: string, path: string) {
  const res = await fetch(`${SB_URL()}/rest/v1/${path}`, {
    headers: { apikey: SB_KEY(), Authorization: auth, "Content-Type": "application/json" },
  });
  const json = await res.json().catch(() => null);
  if (!res.ok) throw new Error(talSetupHint(String(json?.message || json?.error || "Supabase error")));
  return json;
}

async function sbPost(auth: string, path: string, body: unknown) {
  const res = await fetch(`${SB_URL()}/rest/v1/${path}`, {
    method: "POST",
    headers: {
      apikey: SB_KEY(),
      Authorization: auth,
      "Content-Type": "application/json",
      Prefer: "return=representation",
    },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => null);
  if (!res.ok) throw new Error(talSetupHint(String(json?.message || json?.error || "Supabase error")));
  return Array.isArray(json) ? json[0] : json;
}

// GET /api/tals — список TAL с аналитикой
export async function GET(req: Request) {
  try {
    const auth = req.headers.get("authorization") ?? "";
    const user = await getSupabaseUserFromAuthHeader(auth);
    if (!user?.email) return jsonError(401, "Not authorized");

    const tals = await sbGet(
      auth,
      "tal_analytics_v?select=id,name,description,criteria,created_at,email_sent,email_replies,email_reply_rate,email_meetings,email_held_meetings,li_invited,li_accepted,li_replies,li_accept_rate,li_meetings,li_held_meetings,app_invitations,app_touches,app_replies,app_reply_rate,app_meetings,app_held_meetings,tg_touches,tg_replies,tg_reply_rate,tg_meetings,tg_held_meetings,total_meetings,total_held_meetings&order=created_at.desc"
    );

    return NextResponse.json({ ok: true, tals: Array.isArray(tals) ? tals : [] });
  } catch (e: any) {
    return jsonError(500, talSetupHint(String(e?.message || e)));
  }
}

// POST /api/tals — создать TAL + привязать кампании
export async function POST(req: Request) {
  try {
    const auth = req.headers.get("authorization") ?? "";
    const user = await getSupabaseUserFromAuthHeader(auth);
    if (!user?.email) return jsonError(401, "Not authorized");

    const body = await req.json().catch(() => ({}));
    const { name, description, criteria, campaigns } = body as {
      name: string;
      description?: string;
      criteria?: string;
      campaigns?: Array<{ channel: "smartlead" | "expandi" | "app" | "telegram"; campaign_id?: string; campaign_name: string; source_campaign_key?: string }>;
    };

    if (!name?.trim()) return jsonError(400, "name is required");

    // Создаём TAL
    const tal = await sbPost(auth, "tals", {
      name: name.trim(),
      description: description?.trim() || null,
      criteria: criteria?.trim() || null,
    });

    // Привязываем кампании
    if (Array.isArray(campaigns) && campaigns.length > 0) {
      const rows = campaigns
        .filter((c) => c.campaign_name?.trim() && ["smartlead", "expandi", "app", "telegram"].includes(c.channel))
        .map((c) => ({
          tal_id: tal.id,
          channel: c.channel,
          campaign_id: c.campaign_id || null,
          campaign_name: c.campaign_name.trim(),
          source_campaign_key: c.source_campaign_key?.trim() || null,
        }));

      if (rows.length > 0) {
        await sbPost(auth, "tal_campaigns?on_conflict=tal_id,channel,campaign_name", rows);
      }
    }

    return NextResponse.json({ ok: true, tal });
  } catch (e: any) {
    return jsonError(500, talSetupHint(String(e?.message || e)));
  }
}
