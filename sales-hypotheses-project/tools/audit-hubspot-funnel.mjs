#!/usr/bin/env node
/**
 * HubSpot funnel audit (no DB writes).
 *
 * Usage (local):
 *   HUBSPOT_PRIVATE_APP_TOKEN=... node 99-applications/sales/tools/audit-hubspot-funnel.mjs \
 *     --pipeline-id 845719418 \
 *     --max 5000
 *
 * Optional:
 *   --stage-id <id>        (repeatable)
 *   --since 2025-12-22     (week start, UTC)
 *   --until 2025-12-29
 *
 * Prints:
 * - total deals in pipeline (and optionally stage-filtered)
 * - counts by dealstage label (top 20)
 * - sample deals (first 10)
 */

const HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN || "";
if (!HUBSPOT_TOKEN) {
  console.error("Missing HUBSPOT_PRIVATE_APP_TOKEN (set env var).");
  process.exit(2);
}

function parseArgs(argv) {
  const out = { pipelineIds: [], stageIds: [], max: 5000, since: null, until: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--pipeline-id") out.pipelineIds.push(String(argv[++i] || ""));
    else if (a === "--stage-id") out.stageIds.push(String(argv[++i] || ""));
    else if (a === "--max") out.max = Math.max(1, Math.min(20000, Number(argv[++i] || "5000")));
    else if (a === "--since") out.since = String(argv[++i] || "");
    else if (a === "--until") out.until = String(argv[++i] || "");
  }
  out.pipelineIds = out.pipelineIds.filter(Boolean);
  out.stageIds = out.stageIds.filter(Boolean);
  return out;
}

async function hubspotFetch(url, init) {
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${HUBSPOT_TOKEN}`,
      ...(init?.headers || {})
    }
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`HubSpot API error ${res.status}: ${JSON.stringify(json).slice(0, 400)}`);
  }
  return json;
}

async function fetchPipelines() {
  const json = await hubspotFetch("https://api.hubapi.com/crm/v3/pipelines/deals");
  const stageLabelById = new Map();
  for (const p of Array.isArray(json?.results) ? json.results : []) {
    for (const s of Array.isArray(p?.stages) ? p.stages : []) {
      const id = String(s?.id ?? "");
      const label = String(s?.label ?? "");
      if (id) stageLabelById.set(id, label || id);
    }
  }
  return { pipelines: json?.results || [], stageLabelById };
}

async function searchDeals({ pipelineIds, stageIds, sinceMs, untilMs, max }) {
  const properties = ["dealname", "dealstage", "pipeline", "createdate", "hs_lastmodifieddate"];
  const out = [];
  let after = null;

  while (out.length < max) {
    const filters = [];
    if (pipelineIds?.length) filters.push({ propertyName: "pipeline", operator: "IN", values: pipelineIds });
    if (stageIds?.length) filters.push({ propertyName: "dealstage", operator: "IN", values: stageIds });
    if (sinceMs != null && untilMs != null) {
      filters.push({ propertyName: "hs_lastmodifieddate", operator: "GTE", value: String(sinceMs) });
      filters.push({ propertyName: "hs_lastmodifieddate", operator: "LT", value: String(untilMs) });
    }

    const body = {
      filterGroups: [{ filters }],
      sorts: [{ propertyName: "hs_lastmodifieddate", direction: "DESCENDING" }],
      properties,
      limit: Math.min(200, max - out.length),
      ...(after ? { after } : {})
    };

    const json = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });

    const results = Array.isArray(json?.results) ? json.results : [];
    out.push(...results);
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after || !results.length) break;
  }

  return out.slice(0, max);
}

function toMs(v) {
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

(async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.pipelineIds.length) {
    console.error("Please provide at least one --pipeline-id <id>.");
    process.exit(2);
  }

  const sinceMs = args.since ? Date.parse(`${args.since}T00:00:00.000Z`) : null;
  const untilMs = args.until ? Date.parse(`${args.until}T00:00:00.000Z`) : null;
  if ((args.since && !Number.isFinite(sinceMs)) || (args.until && !Number.isFinite(untilMs))) {
    console.error("Invalid --since/--until. Use YYYY-MM-DD.");
    process.exit(2);
  }
  if ((args.since && !args.until) || (!args.since && args.until)) {
    console.error("Provide both --since and --until, or neither.");
    process.exit(2);
  }

  const { stageLabelById } = await fetchPipelines();
  const deals = await searchDeals({ pipelineIds: args.pipelineIds, stageIds: args.stageIds, sinceMs, untilMs, max: args.max });

  const byStage = new Map();
  for (const d of deals) {
    const sid = String(d?.properties?.dealstage ?? "");
    const label = stageLabelById.get(sid) || sid || "—";
    byStage.set(label, (byStage.get(label) || 0) + 1);
  }

  const topStages = Array.from(byStage.entries()).sort((a, b) => b[1] - a[1]).slice(0, 20);

  console.log(JSON.stringify({
    ok: true,
    filters: {
      pipeline_ids: args.pipelineIds,
      stage_ids: args.stageIds,
      since: args.since || null,
      until: args.until || null,
      max: args.max
    },
    deals_count: deals.length,
    top_stages: topStages.map(([label, count]) => ({ label, count })),
    sample_deals: deals.slice(0, 10).map((d) => ({
      id: String(d?.id ?? ""),
      dealname: String(d?.properties?.dealname ?? ""),
      pipeline: String(d?.properties?.pipeline ?? ""),
      dealstage: String(d?.properties?.dealstage ?? ""),
      createdate: toMs(d?.properties?.createdate),
      lastmodified: toMs(d?.properties?.hs_lastmodifieddate)
    }))
  }, null, 2));
})().catch((e) => {
  console.error(String(e?.stack || e?.message || e));
  process.exit(1);
});


