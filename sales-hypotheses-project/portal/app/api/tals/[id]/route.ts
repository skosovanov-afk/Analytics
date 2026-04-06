import { NextResponse } from "next/server";
import { getSupabaseUserFromAuthHeader } from "@/app/lib/supabase-server";

const SB_URL = () => process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
const SB_KEY = () => process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function looksLikeMissingRelation(message: string) {
  const msg = String(message || "").toLowerCase();
  return msg.includes("could not find the table") || msg.includes("schema cache") || (msg.includes("relation") && msg.includes("does not exist"));
}

function looksLikeMissingColumn(message: string, column: string) {
  const msg = String(message || "").toLowerCase();
  return msg.includes("column") && msg.includes(column.toLowerCase()) && msg.includes("does not exist");
}

function talSetupHint(message: string) {
  if (!looksLikeMissingRelation(message)) return message;
  return "TAL schema is not deployed in Supabase yet. Apply `projects/Product/supabase/sql/tals_setup_2026_03_26.sql`, then refresh after PostgREST schema cache updates.";
}

async function sbGet(auth: string, path: string) {
  const res = await fetch(`${SB_URL()}/rest/v1/${path}`, {
    headers: { apikey: SB_KEY(), Authorization: auth },
  });
  const json = await res.json().catch(() => null);
  if (!res.ok) throw new Error(talSetupHint(String(json?.message || json?.error || "Supabase error")));
  return json;
}

async function sbPatch(auth: string, path: string, body: unknown) {
  const res = await fetch(`${SB_URL()}/rest/v1/${path}`, {
    method: "PATCH",
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
  return json;
}

// GET /api/tals/[id] — TAL + кампании + аналитика
export async function GET(req: Request, { params }: { params: { id: string } }) {
  try {
    const auth = req.headers.get("authorization") ?? "";
    const user = await getSupabaseUserFromAuthHeader(auth);
    if (!user?.email) return jsonError(401, "Not authorized");

    const { id } = params;
    if (!UUID_RE.test(id)) return jsonError(400, "Invalid id");

    const talRowsPromise = sbGet(auth, `tal_analytics_v?id=eq.${id}&select=*`);
    const campaignsPromise = sbGet(
      auth,
      `tal_campaigns?tal_id=eq.${id}&select=id,channel,campaign_id,campaign_name,source_campaign_key,match_group&order=channel.asc,campaign_name.asc`
    ).catch(async (error) => {
      if (!looksLikeMissingColumn(String(error?.message || error), "match_group")) throw error;
      const fallback = await sbGet(
        auth,
        `tal_campaigns?tal_id=eq.${id}&select=id,channel,campaign_id,campaign_name,source_campaign_key&order=channel.asc,campaign_name.asc`
      );
      return (Array.isArray(fallback) ? fallback : []).map((row: any) => ({ ...row, match_group: null }));
    });

    const [talRows, campaigns] = await Promise.all([talRowsPromise, campaignsPromise]);

    const tal = Array.isArray(talRows) ? talRows[0] : null;
    if (!tal) return jsonError(404, "TAL not found");

    return NextResponse.json({
      ok: true,
      tal,
      campaigns: Array.isArray(campaigns) ? campaigns : [],
    });
  } catch (e: any) {
    return jsonError(500, talSetupHint(String(e?.message || e)));
  }
}

// PATCH /api/tals/[id] — update cross-channel matching for campaigns inside TAL
export async function PATCH(req: Request, { params }: { params: { id: string } }) {
  try {
    const auth = req.headers.get("authorization") ?? "";
    const user = await getSupabaseUserFromAuthHeader(auth);
    if (!user?.email) return jsonError(401, "Not authorized");

    const { id } = params;
    if (!UUID_RE.test(id)) return jsonError(400, "Invalid id");
    const body = await req.json().catch(() => ({}));
    const campaigns = Array.isArray(body?.campaigns) ? body.campaigns : [];

    if (!campaigns.length) return NextResponse.json({ ok: true, updated: [] });

    const existing = await sbGet(auth, `tal_campaigns?tal_id=eq.${id}&select=id`);
    const allowedIds = new Set((Array.isArray(existing) ? existing : []).map((row: any) => String(row.id)));

    const updates = campaigns
      .map((row: any) => ({
        id: String(row?.id ?? "").trim(),
        match_group: String(row?.match_group ?? "").trim() || null,
      }))
      .filter((row: any) => row.id && allowedIds.has(row.id));

    await Promise.all(
      updates.map((row: any) => sbPatch(auth, `tal_campaigns?id=eq.${row.id}`, { match_group: row.match_group }))
    );

    return NextResponse.json({ ok: true, updated: updates });
  } catch (e: any) {
    const message = String(e?.message || e);
    if (looksLikeMissingColumn(message, "match_group")) {
      return jsonError(500, "TAL match groups are not deployed in Supabase yet. Apply `projects/Product/supabase/sql/tals_match_groups_2026_03_31.sql`, then refresh after PostgREST schema cache updates.");
    }
    return jsonError(500, talSetupHint(message));
  }
}

// DELETE /api/tals/[id]
export async function DELETE(req: Request, { params }: { params: { id: string } }) {
  try {
    const auth = req.headers.get("authorization") ?? "";
    const user = await getSupabaseUserFromAuthHeader(auth);
    if (!user?.email) return jsonError(401, "Not authorized");

    const { id } = params;
    if (!UUID_RE.test(id)) return jsonError(400, "Invalid id");
    const res = await fetch(`${SB_URL()}/rest/v1/tals?id=eq.${id}`, {
      method: "DELETE",
      headers: { apikey: SB_KEY(), Authorization: auth },
    });
    if (!res.ok) {
      const json = await res.json().catch(() => null);
      throw new Error(talSetupHint(String(json?.message || json?.error || "Delete failed")));
    }

    return NextResponse.json({ ok: true });
  } catch (e: any) {
    return jsonError(500, talSetupHint(String(e?.message || e)));
  }
}
