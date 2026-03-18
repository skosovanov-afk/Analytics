import { NextResponse } from "next/server";

type SupabaseUserResponse = { email?: string | null };

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
    headers: { apikey: supabaseAnonKey, Authorization: authHeader }
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

function toMs(v: any): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

function ymd(d: Date) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function startOfWeekUTC(d: Date) {
  const x = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = x.getUTCDay();
  const diff = (day + 6) % 7;
  x.setUTCDate(x.getUTCDate() - diff);
  return x;
}

async function hubspotFetchDealStageLabels() {
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/pipelines/deals");
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const stageLabelById = new Map<string, string>();
  const results = Array.isArray(json?.results) ? json.results : [];
  for (const p of results) {
    for (const s of Array.isArray(p?.stages) ? p.stages : []) {
      const id = String(s?.id ?? "").trim();
      const label = String(s?.label ?? "").trim();
      if (id) stageLabelById.set(id, label || id);
    }
  }
  return stageLabelById;
}

async function hubspotPickDefaultPipelineIds() {
  // Best-effort fallback when HUBSPOT_FUNNEL_PIPELINE_IDS is not configured.
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/pipelines/deals");
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const results = Array.isArray(json?.results) ? json.results : [];
  const scored = results
    .map((p: any) => {
      const id = String(p?.id ?? "").trim();
      const label = String(p?.label ?? "").trim().toLowerCase();
      let score = 0;
      if (label.includes("funnel")) score += 3;
      if (label.includes("sales")) score += 2;
      if (label.includes("pipeline")) score += 1;
      return { id, score };
    })
    .filter((x: any) => x.id);
  scored.sort((a: any, b: any) => b.score - a.score);
  return scored[0]?.id ? [scored[0].id] : [];
}

async function hubspotSearchDealsAllPaged(filters: any[], maxDeals: number) {
  // NOTE: we intentionally include "loss reason" fields in the fetch so the UI can show
  // a single unified "Lost reason" column without needing 3 separate columns.
  // Property internal names are best-effort defaults; if your HubSpot portal uses different names,
  // update the constants below.
  const properties = [
    "dealname",
    "dealstage",
    "pipeline",
    "createdate",
    "hs_lastmodifieddate",
    // Loss/disqualification (best-effort defaults)
    "loss_reason_unified",
    "closed_lost_reason",
    "disqualification_reason"
  ];
  const out: any[] = [];
  let after: string | null = null;

  while (out.length < maxDeals) {
    const body: any = {
      filterGroups: [{ filters }],
      sorts: [{ propertyName: "hs_lastmodifieddate", direction: "DESCENDING" }],
      properties,
      limit: Math.min(200, maxDeals - out.length)
    };
    if (after) body.after = after;

    const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    const results = Array.isArray(json?.results) ? json.results : [];
    out.push(...results);
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after || !results.length) break;
  }

  return out.slice(0, maxDeals);
}

async function hubspotFetchDealStageHistory(dealId: string) {
  const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}?properties=dealname,dealstage,createdate,hs_lastmodifieddate&propertiesWithHistory=dealstage`;
  const res = await hubspotFetch(url);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  const props = json?.properties ?? {};
  const hist = json?.propertiesWithHistory?.dealstage;
  const history = Array.isArray(hist)
    ? hist
        .map((x: any) => ({ value: String(x?.value ?? ""), timestamp: toMs(x?.timestamp) }))
        .filter((x: any) => x.value && x.timestamp)
        .sort((a: any, b: any) => (a.timestamp ?? 0) - (b.timestamp ?? 0))
    : [];
  return { createdate: toMs(props?.createdate), lastmodified: toMs(props?.hs_lastmodifieddate), history };
}

async function hubspotBatchReadDealsWithStageHistory(ids: string[]) {
  const inputs = ids.map((id) => ({ id: String(id) }));
  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/batch/read", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      // Keep this in sync with hubspotSearchDealsAllPaged() to avoid losing props in later processing.
      properties: [
        "dealname",
        "dealstage",
        "pipeline",
        "createdate",
        "hs_lastmodifieddate",
        "loss_reason_unified",
        "closed_lost_reason",
        "disqualification_reason"
      ],
      propertiesWithHistory: ["dealstage"],
      inputs
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  return Array.isArray(json?.results) ? json.results : [];
}

function bucketFromStageLabel(labelOrId: string) {
  const t = String(labelOrId ?? "").trim().toLowerCase();
  if (!t) return "other";
  if (t.includes("lead")) return "leads";
  if (t.includes("sql")) return "sql";
  if (t.includes("evaluate") || t.includes("select") || t.includes("negotiat") || t.includes("purchase")) return "opportunity";
  if (t.includes("integrat") || t.includes("active")) return "clients";
  if (t.includes("lost") || t.includes("dormant")) return "lost";
  return "other";
}

function bucketRank(bucket: string) {
  const b = String(bucket ?? "").toLowerCase();
  if (b === "leads") return 0;
  if (b === "sql") return 1;
  if (b === "opportunity") return 2;
  if (b === "clients") return 3;
  if (b === "lost") return 9; // special: treat as bad sink
  return 99;
}

function inc(map: Record<string, number>, key: string, by = 1) {
  map[key] = (map[key] ?? 0) + by;
}

function normalizeLossReasonValue(v: any) {
  // HubSpot can return long multi-line text. Keep it single-line and trimmed for table rendering.
  return String(v ?? "").replace(/\s+/g, " ").trim();
}

function pickUnifiedLostReason(props: any): { field_label: string; value: string } | null {
  // Priority: unified -> closed lost -> disqualification.
  // "Framework" is intentionally excluded per product requirement.
  const candidates: Array<{ prop: string; label: string }> = [
    { prop: "loss_reason_unified", label: "Loss reason (Unified)" },
    { prop: "closed_lost_reason", label: "Closed Lost Reason" },
    { prop: "disqualification_reason", label: "Disqualification reason" }
  ];

  for (const c of candidates) {
    const value = normalizeLossReasonValue(props?.[c.prop]);
    if (value) return { field_label: c.label, value };
  }
  return null;
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");

    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json()) as { pipeline_ids?: string[]; stage_ids?: string[]; maxDeals?: number; since_ymd?: string };
    const defaultPipelineIds = String(process.env.HUBSPOT_FUNNEL_PIPELINE_IDS ?? "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const pipelineIds = Array.isArray(payload?.pipeline_ids) ? payload.pipeline_ids.map(String).filter(Boolean) : [];
    const stageIds = Array.isArray(payload?.stage_ids) ? payload.stage_ids.map(String).filter(Boolean) : [];
    const maxDeals = Math.max(1, Math.min(10000, Number(payload?.maxDeals ?? 5000)));
    let effectivePipelineIds = pipelineIds.length ? pipelineIds : defaultPipelineIds;
    if (!effectivePipelineIds.length) effectivePipelineIds = await hubspotPickDefaultPipelineIds();
    if (!effectivePipelineIds.length) return jsonError(400, "pipeline_ids is required (or set HUBSPOT_FUNNEL_PIPELINE_IDS)");

    const stageLabelById = await hubspotFetchDealStageLabels();

    const filters: any[] = [{ propertyName: "pipeline", operator: "IN", values: effectivePipelineIds }];
    if (stageIds.length) filters.push({ propertyName: "dealstage", operator: "IN", values: stageIds });

    const deals = await hubspotSearchDealsAllPaged(filters, maxDeals);
    const byStage: Record<string, number> = {};
    const byStageId: Record<string, number> = {};

    // delta baseline: start of current week (UTC) unless overridden
    const sinceYmd = payload?.since_ymd ? String(payload.since_ymd) : ymd(startOfWeekUTC(new Date()));
    const sinceMs = Date.parse(`${sinceYmd}T00:00:00.000Z`);
    const nowMs = Date.now();

    const countsNow: Record<string, number> = { leads: 0, sql: 0, opportunity: 0, clients: 0, lost: 0, other: 0 };
    const countsPrev: Record<string, number> = { leads: 0, sql: 0, opportunity: 0, clients: 0, lost: 0, other: 0 };

    const deltaPlus: Record<string, number> = { leads: 0, sql: 0, opportunity: 0, clients: 0, lost: 0, other: 0 };
    const deltaMinus: Record<string, number> = { leads: 0, sql: 0, opportunity: 0, clients: 0, lost: 0, other: 0 };
    const deltaMinusForward: Record<string, number> = { leads: 0, sql: 0, opportunity: 0, clients: 0, lost: 0, other: 0 };
    const deltaMinusToLost: Record<string, number> = { leads: 0, sql: 0, opportunity: 0, clients: 0, lost: 0, other: 0 };
    const deltaMinusBackward: Record<string, number> = { leads: 0, sql: 0, opportunity: 0, clients: 0, lost: 0, other: 0 };

    type ChangeRow = {
      id: string;
      dealname: string;
      pipeline: string;
      createdate: number | null;
      lastmodified: number | null;
      from_bucket: string;
      to_bucket: string;
      from_stage_id: string;
      to_stage_id: string;
      from_stage_label: string;
      to_stage_label: string;
      kind: "created" | "moved";
      lost_reason_field: string | null;
      lost_reason_value: string | null;
    };
    const changesByBucket: Record<string, { plus: ChangeRow[]; minus: ChangeRow[] }> = {
      leads: { plus: [], minus: [] },
      sql: { plus: [], minus: [] },
      opportunity: { plus: [], minus: [] },
      clients: { plus: [], minus: [] },
      lost: { plus: [], minus: [] },
      other: { plus: [], minus: [] }
    };
    const maxChangeDealsPerBucket = 200;

    const needHistory: Array<{
      id: string;
      props: any;
      sidNow: string;
      lblNow: string;
      bNow: string;
      createdMs: number | null;
      lastmodMs: number | null;
    }> = [];

    for (const d of deals) {
      const id = String(d?.id ?? "");
      const props = d?.properties ?? {};
      const sidNow = String(props?.dealstage ?? "").trim() || "—";
      byStageId[sidNow] = (byStageId[sidNow] ?? 0) + 1;
      const lblNow = stageLabelById.get(sidNow) ?? sidNow;
      byStage[lblNow] = (byStage[lblNow] ?? 0) + 1;

      const bNow = bucketFromStageLabel(lblNow);
      inc(countsNow, bNow);

      // Estimate stage at sinceMs:
      const createdMs = toMs(props?.createdate);
      const lastmodMs = toMs(props?.hs_lastmodifieddate);

      // If deal didn't exist at baseline, it wasn't counted.
      if (createdMs != null && createdMs >= sinceMs) {
        // New deal since baseline -> counts as + in its current bucket.
        inc(deltaPlus, bNow);
        const lostReason = pickUnifiedLostReason(props);
        const row: ChangeRow = {
          id,
          dealname: String(props?.dealname ?? id),
          pipeline: String(props?.pipeline ?? ""),
          createdate: createdMs,
          lastmodified: lastmodMs,
          from_bucket: "__none__",
          to_bucket: bNow,
          from_stage_id: "",
          to_stage_id: sidNow,
          from_stage_label: "",
          to_stage_label: lblNow,
          kind: "created",
          lost_reason_field: lostReason?.field_label ?? null,
          lost_reason_value: lostReason?.value ?? null
        };
        const slot = changesByBucket[bNow]?.plus;
        if (slot && slot.length < maxChangeDealsPerBucket) slot.push(row);
        continue;
      }

      // If not modified since baseline, assume stage unchanged (good enough for weekly delta).
      if (lastmodMs != null && lastmodMs < sinceMs) {
        inc(countsPrev, bNow);
        continue;
      }

      // Otherwise, compute stage at baseline via history. We'll batch-read histories to avoid 429.
      if (id) needHistory.push({ id, props, sidNow, lblNow, bNow, createdMs: createdMs ?? null, lastmodMs: lastmodMs ?? null });
    }

    if (needHistory.length) {
      const idToHistory = new Map<string, any[]>();
      for (let i = 0; i < needHistory.length; i += 50) {
        const batch = needHistory.slice(i, i + 50).map((x) => x.id);
        const rows = await hubspotBatchReadDealsWithStageHistory(batch);
        for (const r of rows) {
          const id = String(r?.id ?? "").trim();
          const hist = Array.isArray(r?.propertiesWithHistory?.dealstage) ? r.propertiesWithHistory.dealstage : [];
          const history = Array.isArray(hist)
            ? hist
                .map((x: any) => ({ value: String(x?.value ?? ""), timestamp: toMs(x?.timestamp) }))
                .filter((x: any) => x.value && x.timestamp)
                .sort((a: any, b: any) => (a.timestamp ?? 0) - (b.timestamp ?? 0))
            : [];
          if (id) idToHistory.set(id, history);
        }
      }

      for (const item of needHistory) {
        const history = idToHistory.get(item.id) || [];
        if (!history.length) {
          inc(countsPrev, item.bNow);
          continue;
        }
        let sidPrev = "";
        for (let i = 0; i < history.length; i++) {
          const h = history[i];
          if ((h.timestamp ?? 0) <= sinceMs) sidPrev = String(h.value);
          else break;
        }
        if (!sidPrev) sidPrev = String(history[0]?.value ?? item.sidNow);
        const lblPrev = stageLabelById.get(sidPrev) ?? sidPrev;
        const bPrev = bucketFromStageLabel(lblPrev);
        inc(countsPrev, bPrev);

        if (bPrev !== item.bNow) {
          inc(deltaMinus, bPrev);
          inc(deltaPlus, item.bNow);

          const lostReason = pickUnifiedLostReason(item.props);
          const row: ChangeRow = {
            id: item.id,
            dealname: String(item.props?.dealname ?? item.id),
            pipeline: String(item.props?.pipeline ?? ""),
            createdate: item.createdMs,
            lastmodified: item.lastmodMs,
            from_bucket: bPrev,
            to_bucket: item.bNow,
            from_stage_id: sidPrev,
            to_stage_id: item.sidNow,
            from_stage_label: lblPrev,
            to_stage_label: item.lblNow,
            kind: "moved",
            lost_reason_field: lostReason?.field_label ?? null,
            lost_reason_value: lostReason?.value ?? null
          };

          const prevSlot = changesByBucket[bPrev]?.minus;
          if (prevSlot && prevSlot.length < maxChangeDealsPerBucket) prevSlot.push(row);
          const nowSlot = changesByBucket[item.bNow]?.plus;
          if (nowSlot && nowSlot.length < maxChangeDealsPerBucket) nowSlot.push(row);

          if (item.bNow === "lost") {
            inc(deltaMinusToLost, bPrev);
          } else {
            const rPrev = bucketRank(bPrev);
            const rNow = bucketRank(item.bNow);
            if (rNow > rPrev) inc(deltaMinusForward, bPrev);
            else inc(deltaMinusBackward, bPrev);
          }
        }
      }
    }

    const topStages = Object.entries(byStage)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 30)
      .map(([label, count]) => ({ label, count }));

    const delta: Record<string, number> = {};
    for (const k of Object.keys(countsNow)) {
      delta[k] = (countsNow[k] ?? 0) - (countsPrev[k] ?? 0);
    }

    return NextResponse.json({
      ok: true,
      filters: { pipeline_ids: pipelineIds, stage_ids: stageIds, maxDeals, since_ymd: sinceYmd },
      deals_count: deals.length,
      top_stages: topStages,
      stage_id_counts: byStageId,
      counts_now: countsNow,
      counts_prev: countsPrev,
      delta,
      delta_details: {
        plus: deltaPlus,
        minus: deltaMinus,
        minus_forward: deltaMinusForward,
        minus_to_lost: deltaMinusToLost,
        minus_backward: deltaMinusBackward
      },
      change_deals: changesByBucket,
      window: { since_ymd: sinceYmd, since_ms: sinceMs, now_ms: nowMs }
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


