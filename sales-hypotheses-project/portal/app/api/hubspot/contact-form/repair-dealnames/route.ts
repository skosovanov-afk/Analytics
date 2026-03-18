import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function env(name: string) {
  const v = String(process.env[name] ?? "").trim();
  if (!v) throw new Error(`Missing ${name}`);
  return v;
}

function normEmail(v: any) {
  return String(v ?? "").trim().toLowerCase();
}

function emailDomain(v: any) {
  const e = normEmail(v);
  if (!e) return "";
  const at = e.lastIndexOf("@");
  if (at < 0) return "";
  return e.slice(at + 1).trim().replace(/\.+$/g, "");
}

function looksLikeEmail(v: any) {
  const t = String(v ?? "").trim();
  return !!t && t.includes("@") && !t.includes(" ");
}

async function hubspotFetch(url: string, init?: RequestInit) {
  const token = env("HUBSPOT_PRIVATE_APP_TOKEN");
  return fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(init?.headers ?? {})
    }
  });
}

async function hubspotGetDealName(dealId: string) {
  const id = String(dealId ?? "").trim();
  if (!id) return "";
  const res = await hubspotFetch(`https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(id)}?properties=dealname`);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot deal get failed"));
  return String(json?.properties?.dealname ?? "").trim();
}

async function hubspotUpdateDealName(dealId: string, dealname: string) {
  const id = String(dealId ?? "").trim();
  const name = String(dealname ?? "").trim();
  if (!id || !name) return false;
  const res = await hubspotFetch(`https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties: { dealname: name } })
  });
  if (res.ok) return true;
  const json = await res.json().catch(() => null);
  throw new Error(String(json?.message || json?.error || "HubSpot deal update failed"));
}

function isAuthorized(req: Request) {
  const syncSecret = String(process.env.CONTACT_FORM_HUBSPOT_SYNC_SECRET ?? "").trim();
  const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
  const gotSecret = String(req.headers.get("x-sync-secret") ?? "").trim();
  const authHeader = String(req.headers.get("authorization") ?? "").trim();
  const bearer = authHeader.toLowerCase().startsWith("bearer ") ? authHeader.slice(7).trim() : "";
  return (!!syncSecret && (gotSecret === syncSecret || bearer === syncSecret)) || (!!cronSecret && bearer === cronSecret);
}

export async function POST(req: Request) {
  try {
    if (!isAuthorized(req)) return jsonError(401, "Not authorized");

    const payload = (await req.json().catch(() => ({}))) as { limit?: number; dry_run?: boolean; only_if_email?: boolean };
    const limit = Math.max(1, Math.min(500, Number(payload?.limit ?? 200)));
    const dryRun = !!payload?.dry_run;
    const onlyIfEmail = payload?.only_if_email !== false; // default true

    const supabaseUrl = env("NEXT_PUBLIC_SUPABASE_URL");
    const serviceRoleKey = env("SUPABASE_SERVICE_ROLE_KEY");
    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });

    const logsRes = await supabaseAdmin
      .from("contact_form_leads_hubspot")
      .select("lead_id,hubspot_deal_id,status,updated_at")
      .eq("status", "done")
      .not("hubspot_deal_id", "is", null)
      .order("updated_at", { ascending: false })
      .limit(limit);
    if (logsRes.error) throw logsRes.error;
    const logs = Array.isArray(logsRes.data) ? logsRes.data : [];

    const leadIds = logs.map((r: any) => Number(r?.lead_id)).filter((x) => Number.isFinite(x));
    const leadsById = new Map<number, any>();
    if (leadIds.length) {
      const leadsRes = await supabaseAdmin
        .from("contact_form_leads")
        .select("id,corporate_email,company")
        .in("id", leadIds);
      if (leadsRes.error) throw leadsRes.error;
      for (const r of Array.isArray(leadsRes.data) ? leadsRes.data : []) {
        leadsById.set(Number(r?.id), r);
      }
    }

    const stats = { scanned: logs.length, updated: 0, skipped: 0, errors: 0, dry_run: dryRun };
    const results: any[] = [];

    for (const r of logs) {
      const leadId = Number(r?.lead_id);
      const dealId = String(r?.hubspot_deal_id ?? "").trim();
      const lead = leadsById.get(leadId) ?? null;
      const email = normEmail(lead?.corporate_email ?? "");
      const domain = emailDomain(email);
      const desired = String(domain || String(lead?.company ?? "").trim() || email).trim();
      if (!dealId || !desired) {
        stats.skipped++;
        continue;
      }

      try {
        const current = await hubspotGetDealName(dealId);
        if (onlyIfEmail && current && !looksLikeEmail(current)) {
          stats.skipped++;
          continue;
        }
        if (current === desired) {
          stats.skipped++;
          continue;
        }
        if (!dryRun) await hubspotUpdateDealName(dealId, desired);
        stats.updated++;
        results.push({ lead_id: leadId, deal_id: dealId, from: current, to: desired });
      } catch (e: any) {
        stats.errors++;
        results.push({ lead_id: leadId, deal_id: dealId, error: String(e?.message || e) });
      }
    }

    return NextResponse.json({ ok: true, stats, results: results.slice(0, 50) });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


