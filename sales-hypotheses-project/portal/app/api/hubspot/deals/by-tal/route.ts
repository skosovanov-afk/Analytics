import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

type SupabaseUserResponse = {
  email?: string | null;
  id?: string | null;
};

// --- Shared Helpers (aligned with cache-sync) ---

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

async function getSupabaseUserFromAuthHeader(authHeader: string | null) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  if (!supabaseUrl || !supabaseAnonKey) return null;
  if (!authHeader?.startsWith("Bearer ")) return null;

  const res = await fetch(`${supabaseUrl}/auth/v1/user`, {
    method: "GET",
    headers: {
      apikey: supabaseAnonKey,
      Authorization: authHeader
    }
  });
  if (!res.ok) return null;
  return (await res.json()) as SupabaseUserResponse;
}

let lastHubspotCallAt = 0;
async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function hubspotFetch(url: string, init?: RequestInit, opts?: { minIntervalMs?: number; maxRetries?: number }) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const minIntervalMs = Math.max(50, Math.min(500, Number(opts?.minIntervalMs ?? 140)));
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

  throw new Error("HubSpot rate limit: too many requests (429). Try again in ~10 seconds.");
}

function parseHubspotListIdFromUrl(url: string) {
  const t = String(url ?? "").trim();
  if (!t) return null;
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  if (m?.[1]) return m[1];
  return null;
}

// Stage Meta & Categorization Logic
type DealStageMeta = { label: string; isClosed: boolean };

async function hubspotFetchDealStageMeta() {
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/pipelines/deals");
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const stageMetaById = new Map<string, DealStageMeta>();
  const results = Array.isArray(json?.results) ? json.results : [];
  for (const p of results) {
    for (const s of Array.isArray(p?.stages) ? p.stages : []) {
      const id = String(s?.id ?? "").trim();
      const label = String(s?.label ?? "").trim();
      const rawClosed = s?.metadata?.isClosed ?? s?.metadata?.closed;
      const isClosed = rawClosed === true || String(rawClosed).toLowerCase() === "true";
      if (id) stageMetaById.set(id, { label: label || id, isClosed });
    }
  }
  return stageMetaById;
}

function stageCategoryFromStage(labelOrId: string, meta?: DealStageMeta | null) {
  const t = String(labelOrId ?? "").trim().toLowerCase();
  if (!t) return "unknown";
  // Strict mapping per user's pipeline:
  // Leads = "Lead", "SQL"
  // Opps = "Evaluate", "Select", "Negotiate", "Purchase"
  // Lost/Customer remain as before
  if (t.includes("closed lost") || t.includes("dormant") || t.includes("churn") || t.includes("lost")) return "lost";
  if (t.includes("closed won") || t.includes("customer") || t.includes("integrat") || t.includes("active")) return "customer";
  if (t.includes("lead") || t.includes("sql")) return "lead";
  if (t.includes("evaluate") || t.includes("select") || t.includes("negotiat") || t.includes("purchase")) return "opportunity";
  if (meta?.isClosed) return "lost";
  return "opportunity";
}

async function hubspotBatchReadDeals(dealIds: number[]) {
  const ids = (dealIds ?? []).filter((n) => Number.isFinite(n) && n > 0);
  if (!ids.length) return [];
  const chunkSize = 100; // HubSpot batch read supports up to 100 inputs
  const out: any[] = [];

  // Properties required for frontend display + categorization + filtering
  const properties = [
    "dealname",
    "amount",
    "dealstage",
    "pipeline",
    "closedate",
    "createdate",
    "hs_lastmodifieddate",
    "channel",
    "hubspot_owner_id",
    "tal_category"
  ];

  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/batch/read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        properties,
        inputs: chunk.map((id) => ({ id: String(id) }))
      })
    });
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    const results = Array.isArray(json?.results) ? json.results : [];
    out.push(...results);
  }
  return out;
}

// --- Main Handler ---

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");

    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) {
      return jsonError(403, "Forbidden");
    }

    const payload = (await req.json()) as {
      talUrl?: string;
      talListId?: string;
      hypothesisId?: string;
      maxDeals?: number;
      metric?: string;
    };
    const talUrl = String(payload?.talUrl ?? "").trim();
    const talListId = String(payload?.talListId ?? "").trim();
    const listId = talListId || parseHubspotListIdFromUrl(talUrl);
    if (!listId) return jsonError(400, "Missing TAL list id. Provide talListId or talUrl containing /lists/<id>.");
    const hypothesisId = String(payload?.hypothesisId ?? "").trim();
    const metric = String(payload?.metric ?? "").trim().toLowerCase();
    const metricStage = metric === "leads" ? "lead" : metric === "opps" ? "opportunity" : "";

    const maxDeals = Math.max(1, Math.min(500, Number(payload?.maxDeals ?? 200)));

    // 1. Initialize Supabase Admin to access cached tables
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
    if (!supabaseUrl || !serviceRoleKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });

    // 2. Fetch Hypothesis Config (Filters)
    let dealTalCategoryVal = "";

    if (hypothesisId) {
      const hRes = await supabaseAdmin
        .from("sales_hypotheses")
        .select("hubspot_deal_tal_category, vertical_name")
        .eq("id", hypothesisId)
        .maybeSingle();

      if (hRes.data) {
        const categoryOverride = String(hRes.data.hubspot_deal_tal_category ?? "").trim();
        const verticalName = String(hRes.data.vertical_name ?? "").trim();
        // Strict rule: use vertical name (or explicit override) for TAL category matching.
        dealTalCategoryVal = String(categoryOverride || verticalName).trim().toLowerCase();
      }
    }

    const pipelineEnv =
      String(process.env.HUBSPOT_WEBSITE_PIPELINE_ID ?? "").trim() ||
      String(process.env.HUBSPOT_FUNNEL_PIPELINE_IDS ?? "").trim();
    const pipelineAllow = new Set(
      pipelineEnv
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    );

    // 3. Fetch deals from TAL cache (no activity requirement).
    const pipelineIds = pipelineAllow.size ? Array.from(pipelineAllow) : null;
    const dealsRes = await supabaseAdmin.rpc("sales_tal_deals_from_cache", {
      p_tal_list_id: listId,
      p_pipeline_ids: pipelineIds,
      p_tal_category: dealTalCategoryVal || null,
      p_limit: maxDeals * 5
    });
    if (dealsRes.error) throw new Error(`Supabase RPC error: ${dealsRes.error.message}`);
    const dealsRaw = Array.isArray(dealsRes.data) ? dealsRes.data : [];

    // 4. Filter & Enrich Deals
    const portalId = process.env.HUBSPOT_PORTAL_ID ?? process.env.NEXT_PUBLIC_HUBSPOT_PORTAL_ID ?? "";

    const validDeals = dealsRaw.filter((d: any) => {
      const pipelineId = String(d?.pipeline_id ?? d?.pipeline ?? "").trim();
      // Pipeline Filter
      if (pipelineAllow.size > 0 && !pipelineAllow.has(pipelineId)) return false;

      return true;
    });

    // Category filter is enforced in the RPC; keep client-side filtering minimal.
    const dealsAfterCategory = validDeals;

    const metricDeals = metricStage
      ? dealsAfterCategory.filter((d: any) => String(d?.stage_category ?? "").toLowerCase() === metricStage)
      : dealsAfterCategory;

    const finalDeals = metricDeals.map((r: any) => {
      const dealId = String(r?.deal_id ?? r?.id ?? "");
      const url = portalId && dealId ? `https://app.hubspot.com/contacts/${portalId}/deal/${dealId}` : null;

      return {
        id: dealId,
        url,
        dealname: r?.dealname,
        createdate: r?.createdate,
        dealstage: r?.dealstage_id,
        dealstage_label: r?.stage_label,
        channel: r?.channel,
        amount: r?.amount,
        pipeline: r?.pipeline_id,
        stage_category: r?.stage_category,
        properties: {
          pipeline: r?.pipeline_id,
          dealstage: r?.dealstage_id,
          dealname: r?.dealname,
          amount: r?.amount,
          channel: r?.channel,
          tal_category: r?.tal_category,
          hubspot_owner_id: r?.owner_id,
          createdate: r?.createdate
        }
      };
    });

    return NextResponse.json({
      ok: true,
      tal_list_id: listId,
      filters: {
        pipeline_restricted: pipelineAllow.size > 0,
        category: dealTalCategoryVal
      },
      deals_found: finalDeals.length,
      deals: finalDeals.slice(0, maxDeals)
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}
