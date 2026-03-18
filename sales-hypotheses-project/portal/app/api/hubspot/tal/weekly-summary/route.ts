import { NextResponse } from "next/server";

type SupabaseUserResponse = {
  email?: string | null;
};

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
  const data = (await res.json()) as SupabaseUserResponse;
  return data;
}

function parseHubspotListIdFromUrl(url: string) {
  const t = String(url ?? "").trim();
  if (!t) return null;
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  if (m?.[1]) return m[1];
  return null;
}

function numMs(v: any): number | null {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function toISO(ms: number) {
  return new Date(ms).toISOString();
}

async function hubspotFetchDealStageLabels() {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");
  const res = await fetch("https://api.hubapi.com/crm/v3/pipelines/deals", {
    headers: { Authorization: `Bearer ${token}` }
  });
  const json = (await res.json()) as any;
  if (!res.ok) {
    const msg = json?.message || json?.error || "HubSpot API error";
    throw new Error(String(msg));
  }
  const stageLabelById = new Map<string, string>();
  const results = Array.isArray(json?.results) ? json.results : [];
  for (const p of results) {
    const stages = Array.isArray(p?.stages) ? p.stages : [];
    for (const s of stages) {
      const id = String(s?.id ?? "").trim();
      const label = String(s?.label ?? "").trim();
      if (id) stageLabelById.set(id, label || id);
    }
  }
  return stageLabelById;
}

function stageCategoryFromLabel(labelOrId: string) {
  const t = String(labelOrId ?? "").trim().toLowerCase();
  if (!t) return "unknown";
  if (t.includes("lead")) return "lead";
  if (t.includes("sql")) return "sql";
  if (t.includes("evaluate")) return "evaluate";
  if (t.includes("purchase")) return "purchase";
  if (t.includes("integrat")) return "integration";
  if (t.includes("dormant")) return "dormant";
  if (t.includes("churn")) return "churn";
  return "unknown";
}

function levenshtein(a: string, b: string) {
  const s = String(a ?? "");
  const t = String(b ?? "");
  const n = s.length;
  const m = t.length;
  if (!n) return m;
  if (!m) return n;
  const dp = new Array<number>(m + 1);
  for (let j = 0; j <= m; j++) dp[j] = j;
  for (let i = 1; i <= n; i++) {
    let prev = dp[0];
    dp[0] = i;
    for (let j = 1; j <= m; j++) {
      const tmp = dp[j];
      const cost = s[i - 1] === t[j - 1] ? 0 : 1;
      dp[j] = Math.min(dp[j] + 1, dp[j - 1] + 1, prev + cost);
      prev = tmp;
    }
  }
  return dp[m];
}

function canonicalizeChannelLabel(raw: string) {
  const s0 = String(raw ?? "").trim();
  if (!s0 || s0 === "--") return "Unknown";
  const s = s0.normalize("NFKC").replace(/\s+/g, " ").trim();

  if (s === s.toUpperCase() && /[A-Z]/.test(s)) return s;

  if (!s.includes(" ") && (s.includes("/") || s.includes("."))) {
    return s.replace(/^https?:\/\//i, "").replace(/\/+$/g, "").toLowerCase();
  }

  const key = s.toLowerCase();
  const keyNoSep = key.replace(/[^a-z0-9]+/g, "");

  if (keyNoSep && levenshtein(keyNoSep, "linkedin") <= 1) return "LinkedIn";
  if (keyNoSep && levenshtein(keyNoSep, "google") <= 1) return "Google";

  const words = key
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean);
  if (!words.length) return s;
  const titled = words.map((w) => (w.length <= 2 ? w.toUpperCase() : w[0].toUpperCase() + w.slice(1))).join(" ");
  return titled || s;
}

function channelFromDealProps(props: any) {
  const referralKey = String(process.env.HUBSPOT_DEAL_REFERRAL_PROPERTY ?? "referralsource").trim();
  const referral = referralKey ? String((props as any)?.[referralKey] ?? "").trim() : "";
  if (referral && referral !== "--") return canonicalizeChannelLabel(referral);
  const a1 = String(props?.hs_analytics_source_data_1 ?? "").trim();
  const a = String(props?.hs_analytics_source ?? "").trim();
  const a2 = String(props?.hs_analytics_source_data_2 ?? "").trim();
  const best = a1 || a || a2;
  return canonicalizeChannelLabel(best || "Unknown");
}

async function hubspotFetchListCompanyIds(listId: string, limit: number) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  // HubSpot CRM Lists API memberships endpoint does NOT include object type in the path:
  // GET /crm/v3/lists/{listId}/memberships
  // Ref: https://developers.hubspot.com/docs/api-reference/crm/lists-v3/lists/get-crm-v3-lists-listId-memberships
  const out: string[] = [];
  let after: string | null = null;
  const pageLimit = Math.max(1, Math.min(500, limit));

  while (out.length < limit) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(pageLimit, limit - out.length)));
    if (after) qs.set("after", after);
    const url = `https://api.hubapi.com/crm/v3/lists/${encodeURIComponent(listId)}/memberships?${qs.toString()}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = (await res.json()) as any;
    if (!res.ok) {
      const msg = json?.message || json?.error || "HubSpot API error";
      throw new Error(String(msg));
    }
    const results = Array.isArray(json?.results) ? json.results : [];
    for (const r of results) {
      const id = String(r?.recordId ?? r?.id ?? "").trim();
      if (id) out.push(id);
    }
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after || !results.length) break;
  }
  return out;
}

async function hubspotFetchDealIdsForCompanies(companyIds: string[], maxDeals: number) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const dealIds = new Set<string>();
  for (const cid of companyIds) {
    if (dealIds.size >= maxDeals) break;
    const url = `https://api.hubapi.com/crm/v3/objects/companies/${encodeURIComponent(cid)}/associations/deals`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = (await res.json()) as any;
    if (!res.ok) {
      const msg = json?.message || json?.error || "HubSpot API error";
      throw new Error(String(msg));
    }
    const results = Array.isArray(json?.results) ? json.results : [];
    for (const r of results) {
      const did = String(r?.id ?? "").trim();
      if (!did) continue;
      dealIds.add(did);
      if (dealIds.size >= maxDeals) break;
    }
  }
  return Array.from(dealIds);
}

async function hubspotBatchReadDeals(dealIds: string[]) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const portalId = process.env.HUBSPOT_PORTAL_ID ?? process.env.NEXT_PUBLIC_HUBSPOT_PORTAL_ID ?? "";
  const properties = [
    "dealname",
    "amount",
    "dealstage",
    "pipeline",
    "closedate",
    "createdate",
    "hs_lastmodifieddate",
    "referralsource",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_analytics_source_data_2",
    "hubspot_owner_id",
    "tal_category"
  ];

  const res = await fetch("https://api.hubapi.com/crm/v3/objects/deals/batch/read", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ properties, inputs: dealIds.map((id) => ({ id })) })
  });
  const json = (await res.json()) as any;
  if (!res.ok) {
    const msg = json?.message || json?.error || "HubSpot API error";
    throw new Error(String(msg));
  }

  const results = Array.isArray(json?.results) ? json.results : [];
  return results.map((r: any) => {
    const dealId = String(r?.id ?? "");
    const url = portalId && dealId ? `https://app.hubspot.com/contacts/${portalId}/deal/${dealId}` : null;
    return { id: dealId, url, properties: r?.properties ?? {} };
  });
}

async function hubspotFetchDealStageHistory(dealId: string) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}?properties=dealname,dealstage,createdate,hs_lastmodifieddate&propertiesWithHistory=dealstage`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = (await res.json()) as any;
  if (!res.ok) {
    const msg = json?.message || json?.error || "HubSpot API error";
    throw new Error(String(msg));
  }

  const props = json?.properties ?? {};
  const hist = json?.propertiesWithHistory?.dealstage;
  const history = Array.isArray(hist)
    ? hist
      .map((x: any) => ({ value: String(x?.value ?? ""), timestamp: numMs(x?.timestamp) }))
      .filter((x: any) => x.value && x.timestamp)
    : [];

  history.sort((a: any, b: any) => (a.timestamp ?? 0) - (b.timestamp ?? 0));
  return { dealId, dealname: String(props.dealname ?? ""), currentStage: String(props.dealstage ?? ""), history };
}

async function hubspotFetchAssociatedIdsForDeal(dealId: string, objectName: string, limit: number) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");
  const out: string[] = [];
  let after: string | null = null;
  while (out.length < limit) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(500, limit - out.length)));
    if (after) qs.set("after", after);
    const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}/associations/${encodeURIComponent(objectName)}?${qs.toString()}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = (await res.json()) as any;
    if (!res.ok) {
      const msg = json?.message || json?.error || "HubSpot API error";
      throw new Error(String(msg));
    }
    const results = Array.isArray(json?.results) ? json.results : [];
    for (const r of results) {
      const id = String(r?.id ?? "").trim();
      if (id) out.push(id);
    }
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after || !results.length) break;
  }
  return out;
}

async function hubspotBatchReadObjects(objectName: string, ids: string[], properties: string[]) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const res = await fetch(`https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(objectName)}/batch/read`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ properties, inputs: ids.map((id) => ({ id })) })
  });
  const json = (await res.json()) as any;
  if (!res.ok) {
    const msg = json?.message || json?.error || "HubSpot API error";
    throw new Error(String(msg));
  }
  return Array.isArray(json?.results) ? json.results : [];
}

function pickBestTimestamp(props: any) {
  const candidates = [
    numMs(props?.hs_timestamp),
    numMs(props?.hs_createdate),
    numMs(props?.createdate),
    numMs(props?.hs_lastmodifieddate)
  ].filter((x) => typeof x === "number") as number[];
  if (!candidates.length) return null;
  return Math.max(...candidates);
}

async function hubspotResolveOwnerIdByEmail(email: string) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");
  const t = String(email ?? "").trim().toLowerCase();
  if (!t) return null;

  let after: string | undefined;
  while (true) {
    const qs = new URLSearchParams();
    qs.set("limit", "100");
    qs.set("archived", "false");
    if (after) qs.set("after", after);

    const url = `https://api.hubapi.com/crm/v3/owners?${qs.toString()}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = (await res.json()) as any;
    if (!res.ok) return null; // fail silently

    const results = Array.isArray(json?.results) ? json.results : [];
    const found = results.find((o: any) => String(o?.email ?? "").trim().toLowerCase() === t);
    if (found) return String(found.id);

    after = json?.paging?.next?.after ? String(json.paging.next.after) : undefined;
    if (!after) break;
  }
  return null;
}

async function maybeLLMSummary(structured: any) {
  const provider = String(process.env.TAL_SUMMARY_LLM_PROVIDER ?? "").trim().toLowerCase();
  if (!provider) return null;

  // Currently supported: anthropic (optional)
  if (provider === "anthropic") {
    const key = process.env.ANTHROPIC_API_KEY ?? "";
    const model = process.env.TAL_SUMMARY_LLM_MODEL ?? "claude-3-5-sonnet-20241022";
    if (!key) throw new Error("Missing ANTHROPIC_API_KEY");

    const prompt = `You are a sales operations analyst.\n\nWrite a short weekly summary for a single hypothesis based on the structured HubSpot TAL data below.\n- Keep it 5-8 bullet points.\n- Mention notable new deals and stage movements.\n- Mention activity volume (emails, meetings, notes, tasks).\n- If data is sparse, explicitly say what is missing.\n\nStructured data (JSON):\n${JSON.stringify(structured).slice(0, 12000)}\n`;

    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
      },
      body: JSON.stringify({
        model,
        max_tokens: 500,
        temperature: 0.2,
        messages: [{ role: "user", content: prompt }]
      })
    });
    const json = (await res.json()) as any;
    if (!res.ok) {
      const msg = json?.error?.message || json?.message || "LLM error";
      throw new Error(String(msg));
    }
    const text = Array.isArray(json?.content) ? json.content.map((c: any) => c?.text ?? "").join("\n") : "";
    return String(text || "").trim() || null;
  }

  throw new Error(`Unsupported TAL_SUMMARY_LLM_PROVIDER: ${provider}`);
}

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
      days?: number;
      since?: string; // ISO string (ms or RFC3339). If provided, overrides "days" window start.
      until?: string; // ISO string (optional). Defaults to now.
      maxCompanies?: number;
      maxDeals?: number;
      maxActivitiesPerType?: number;
      includeLLMSummary?: boolean;
      ownerId?: string;
      ownerEmail?: string;
      talCategory?: string;
    };

    let ownerId = String(payload?.ownerId ?? "").trim();
    const ownerEmail = String(payload?.ownerEmail ?? "").trim();

    if (!ownerId && ownerEmail) {
      const resolved = await hubspotResolveOwnerIdByEmail(ownerEmail);
      if (resolved) ownerId = resolved;
    }

    const talUrl = String(payload?.talUrl ?? "").trim();
    const talListId = String(payload?.talListId ?? "").trim();
    const listId = talListId || parseHubspotListIdFromUrl(talUrl);
    if (!listId) return jsonError(400, "Missing TAL list id. Provide talListId or talUrl containing /lists/<id>.");

    const untilMs = payload?.until ? Date.parse(String(payload.until)) : Date.now();
    if (!Number.isFinite(untilMs)) return jsonError(400, "Invalid until (must be ISO date/time)");

    const days = Math.max(1, Math.min(90, Number(payload?.days ?? 7)));
    const sinceMs = payload?.since ? Date.parse(String(payload.since)) : untilMs - days * 24 * 60 * 60 * 1000;
    if (!Number.isFinite(sinceMs)) return jsonError(400, "Invalid since (must be ISO date/time)");
    if (sinceMs > untilMs) return jsonError(400, "since must be <= until");
    const maxCompanies = Math.max(1, Math.min(500, Number(payload?.maxCompanies ?? 200)));
    const maxDeals = Math.max(1, Math.min(500, Number(payload?.maxDeals ?? 200)));
    const maxActivitiesPerType = Math.max(1, Math.min(500, Number(payload?.maxActivitiesPerType ?? 200)));

    const stageLabelById = await hubspotFetchDealStageLabels();

    const companyIds = await hubspotFetchListCompanyIds(listId, maxCompanies);
    const initialDealIds = await hubspotFetchDealIdsForCompanies(companyIds, maxDeals);
    const deals = initialDealIds.length ? await hubspotBatchReadDeals(initialDealIds) : [];

    const deals_compact = deals.map((d: any) => {
      const p = d?.properties ?? {};
      const stageId = String(p?.dealstage ?? "");
      const stageLabel = stageLabelById.get(stageId) ?? stageId;
      const stageCategory = stageCategoryFromLabel(stageLabel || stageId);
      const createdMs = numMs(p?.createdate);
      const dealOwnerId = String(p?.hubspot_owner_id ?? "").trim();

      return {
        id: String(d?.id ?? ""),
        url: d?.url ?? null,
        dealname: String(p?.dealname ?? ""),
        createdate: createdMs ? toISO(createdMs) : (p?.createdate ?? null),
        dealstage: stageId || null,
        stage_label: stageLabel || null,
        stage_category: stageCategory,
        channel: channelFromDealProps(p),
        hubspot_owner_id: dealOwnerId
      };
    });

    // Filter deals by owner if requested
    let filteredDeals = ownerId
      ? deals_compact.filter((d: any) => d.hubspot_owner_id === ownerId)
      : deals_compact;

    // Filter by tal_category if requested
    const talCategory = String(payload?.talCategory ?? "").trim().toLowerCase();
    if (talCategory) {
      filteredDeals = filteredDeals.filter((d: any) => {
        const cat = String(d.properties?.tal_category ?? "").toLowerCase();
        if (!cat) return false;
        return cat === talCategory || cat.includes(talCategory) || talCategory.includes(cat);
      });
    }

    // Recalculate based on filtered deals
    const finalDealIds = filteredDeals.map((d: any) => d.id);
    const deals_map = new Map(deals.map((d: any) => [d.id, d]));

    const newDeals = filteredDeals
      .map((d_compact: any) => {
        const d = deals_map.get(d_compact.id);
        const created = numMs((d as any)?.properties?.createdate);
        return { ...(d || {}), created_ms: created };
      })
      .filter((d: any) => (d.created_ms ?? 0) >= sinceMs && (d.created_ms ?? 0) <= untilMs)
      .sort((a: any, b: any) => (b.created_ms ?? 0) - (a.created_ms ?? 0))
      .slice(0, 20);

    // Stage movements (best-effort, limited to reduce API calls)
    const stageMoves: Array<{ deal_id: string; dealname: string; at: string; from: string; to: string }> = [];
    for (const did of finalDealIds.slice(0, Math.min(50, finalDealIds.length))) {
      const h = await hubspotFetchDealStageHistory(did);
      const hist = h.history;
      for (let i = 1; i < hist.length; i++) {
        const prev = hist[i - 1];
        const cur = hist[i];
        if (!cur?.timestamp || cur.timestamp < sinceMs || cur.timestamp > untilMs) continue;
        if (prev?.value && cur?.value && prev.value !== cur.value) {
          stageMoves.push({
            deal_id: did,
            dealname: h.dealname || did,
            at: toISO(cur.timestamp),
            from: prev.value,
            to: cur.value
          });
        }
      }
    }
    stageMoves.sort((a, b) => String(b.at).localeCompare(String(a.at)));

    // Activities on deals (emails/meetings/notes/tasks) via associations + batch read.
    const activityTypes = [
      { object: "emails", props: ["hs_timestamp", "hs_email_direction", "hs_email_subject", "hs_email_status", "hs_lastmodifieddate", "hs_createdate"] },
      { object: "meetings", props: ["hs_timestamp", "hs_meeting_title", "hs_meeting_start_time", "hs_meeting_end_time", "hs_lastmodifieddate", "hs_createdate"] },
      { object: "notes", props: ["hs_timestamp", "hs_note_body", "hs_lastmodifieddate", "hs_createdate"] },
      { object: "tasks", props: ["hs_timestamp", "hs_task_subject", "hs_task_status", "hs_lastmodifieddate", "hs_createdate"] }
    ];

    const activitiesSummary: any = {};
    const activitiesTop: any = {};

    for (const t of activityTypes) {
      const ids: string[] = [];
      for (const did of finalDealIds.slice(0, Math.min(50, finalDealIds.length))) {
        if (ids.length >= maxActivitiesPerType) break;
        const assoc = await hubspotFetchAssociatedIdsForDeal(did, t.object, Math.min(50, maxActivitiesPerType - ids.length));
        for (const x of assoc) {
          ids.push(x);
          if (ids.length >= maxActivitiesPerType) break;
        }
      }

      const uniqueIds = Array.from(new Set(ids)).slice(0, maxActivitiesPerType);
      const rows = uniqueIds.length ? await hubspotBatchReadObjects(t.object, uniqueIds, t.props) : [];

      const items = rows
        .map((r: any) => {
          const props = r?.properties ?? {};
          const ts = pickBestTimestamp(props);
          return { id: String(r?.id ?? ""), ts_ms: ts, properties: props };
        })
        .filter((x: any) => (x.ts_ms ?? 0) >= sinceMs && (x.ts_ms ?? 0) <= untilMs)
        .sort((a: any, b: any) => (b.ts_ms ?? 0) - (a.ts_ms ?? 0));

      const top = items.slice(0, 15).map((x: any) => ({
        id: x.id,
        at: x.ts_ms ? toISO(x.ts_ms) : null,
        properties: x.properties
      }));

      activitiesSummary[t.object] = { total: items.length };
      activitiesTop[t.object] = top;

      if (t.object === "emails") {
        const sent = items.filter((x: any) => String(x.properties?.hs_email_direction ?? "").toUpperCase() === "OUTGOING").length;
        const received = items.filter((x: any) => String(x.properties?.hs_email_direction ?? "").toUpperCase() === "INCOMING").length;
        activitiesSummary.emails.sent = sent;
        activitiesSummary.emails.received = received;
      }
    }

    const structured = {
      window_days: days,
      since: toISO(sinceMs),
      until: toISO(untilMs),
      tal_list_id: listId,
      companies_in_tal_count: companyIds.length,
      deals_in_tal_count: deals.length,
      new_deals_count: newDeals.length,
      stage_moves_count: stageMoves.length,
      activities: activitiesSummary
    };

    let llm_summary: string | null = null;
    if (payload?.includeLLMSummary) {
      llm_summary = await maybeLLMSummary({
        ...structured,
        examples: {
          new_deals: newDeals.slice(0, 10).map((d: any) => ({ id: d.id, name: d.properties?.dealname ?? "", stage: d.properties?.dealstage ?? "", amount: d.properties?.amount ?? null, createdate: d.properties?.createdate ?? null })),
          stage_moves: stageMoves.slice(0, 15),
          activities_top: activitiesTop
        }
      });
    }

    return NextResponse.json({
      ok: true,
      ...structured,
      new_deals: newDeals.map((d: any) => ({ id: d.id, url: d.url, properties: d.properties })),
      deals_compact,
      stage_moves: stageMoves.slice(0, 200),
      activities_top: activitiesTop,
      llm_summary
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


