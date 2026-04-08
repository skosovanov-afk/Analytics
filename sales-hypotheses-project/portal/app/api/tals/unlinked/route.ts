import { NextResponse } from "next/server";
import { getSupabaseUserFromAuthHeader, postgrestHeadersFor, postgrestJson } from "@/app/lib/supabase-server";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function normalize(s: string) {
  return s.trim().toLowerCase().replace(/\s+/g, " ");
}

async function postgrestGetAll(h: ReturnType<typeof postgrestHeadersFor>, path: string) {
  const out: any[] = [];
  for (let offset = 0; offset < 50000; offset += 1000) {
    const sep = path.includes("?") ? "&" : "?";
    const rows = await postgrestJson(h, "GET", `${path}${sep}limit=1000&offset=${offset}`);
    const batch = Array.isArray(rows) ? rows : [];
    out.push(...batch);
    if (batch.length < 1000) break;
  }
  return out;
}

type UnlinkedCampaign = {
  channel: "smartlead" | "expandi" | "app" | "telegram";
  name: string;
  source_campaign_key: string;
  campaign_id?: string | null;
};

export async function GET(req: Request) {
  try {
    const auth = req.headers.get("authorization") ?? "";
    const user = await getSupabaseUserFromAuthHeader(auth);
    if (!user?.email) return jsonError(401, "Not authorized");

    const h = postgrestHeadersFor(auth, false);

    // Fetch all data in parallel
    const [talCampaigns, slRows, exRows, appRows, tgRows] = await Promise.all([
      // All linked campaigns
      postgrestGetAll(h, "tal_campaigns?select=source_campaign_key,campaign_name,channel"),
      // SmartLead campaigns from daily stats
      postgrestGetAll(h, "smartlead_stats_daily?select=campaign_id,campaign_name&campaign_name=not.is.null&campaign_id=not.is.null"),
      // Expandi campaigns
      postgrestGetAll(h, "expandi_campaign_instances?select=id,name&name=not.is.null"),
      // App campaigns from manual_stats
      postgrestGetAll(h, "manual_stats?select=account_name,campaign_name&channel=eq.app&metric_name=in.(invitations,total_touches,replies,booked_meetings,held_meetings,qualified_leads)"),
      // Telegram campaigns from manual_stats
      postgrestGetAll(h, "manual_stats?select=account_name,campaign_name&channel=eq.telegram&metric_name=in.(total_touches,replies,booked_meetings,held_meetings,qualified_leads)"),
    ]);

    // Build set of linked source keys + fallback name keys
    const linkedKeys = new Set<string>();
    for (const tc of talCampaigns) {
      if (tc.source_campaign_key) linkedKeys.add(tc.source_campaign_key);
      // Fallback: channel + normalized name
      if (tc.campaign_name && tc.channel) {
        linkedKeys.add(`${tc.channel}:fallback:${normalize(tc.campaign_name)}`);
      }
    }

    const unlinked: UnlinkedCampaign[] = [];

    // SmartLead - dedupe by campaign_id
    const slSeen = new Set<string>();
    for (const row of slRows) {
      const cid = String(row.campaign_id);
      const name = String(row.campaign_name ?? "").trim();
      if (!name || !cid || slSeen.has(cid)) continue;
      slSeen.add(cid);
      const sourceKey = `smartlead:id:${cid}`;
      if (linkedKeys.has(sourceKey)) continue;
      if (linkedKeys.has(`smartlead:fallback:${normalize(name)}`)) continue;
      unlinked.push({ channel: "smartlead", name, source_campaign_key: sourceKey, campaign_id: cid });
    }

    // Expandi - dedupe by normalized name
    const exSeen = new Set<string>();
    for (const row of exRows) {
      const name = String(row.name ?? "").trim();
      if (!name) continue;
      const norm = normalize(name);
      if (exSeen.has(norm)) continue;
      exSeen.add(norm);
      const sourceKey = `expandi:canonical:${norm}`;
      if (linkedKeys.has(sourceKey)) continue;
      if (linkedKeys.has(`expandi:fallback:${norm}`)) continue;
      unlinked.push({ channel: "expandi", name, source_campaign_key: sourceKey });
    }

    // App
    const appSeen = new Set<string>();
    for (const row of appRows) {
      const name = (row.campaign_name?.trim() || row.account_name?.trim() || "");
      if (!name) continue;
      const norm = normalize(name);
      if (appSeen.has(norm)) continue;
      appSeen.add(norm);
      const sourceKey = `app:name:${norm}`;
      if (linkedKeys.has(sourceKey)) continue;
      if (linkedKeys.has(`app:fallback:${norm}`)) continue;
      unlinked.push({ channel: "app", name, source_campaign_key: sourceKey });
    }

    // Telegram
    const tgSeen = new Set<string>();
    for (const row of tgRows) {
      const name = (row.campaign_name?.trim() || row.account_name?.trim() || "");
      if (!name) continue;
      const norm = normalize(name);
      if (tgSeen.has(norm)) continue;
      tgSeen.add(norm);
      const sourceKey = `telegram:name:${norm}`;
      if (linkedKeys.has(sourceKey)) continue;
      if (linkedKeys.has(`telegram:fallback:${norm}`)) continue;
      unlinked.push({ channel: "telegram", name, source_campaign_key: sourceKey });
    }

    return NextResponse.json({
      ok: true,
      unlinked,
      total_linked: talCampaigns.length,
      total_unlinked: unlinked.length,
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}
