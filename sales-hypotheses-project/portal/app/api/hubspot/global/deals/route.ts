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

function parseHubspotListIdFromUrl(url: string) {
  const t = String(url ?? "").trim();
  const m = t.match(/\/(?:lists|objectLists)\/(\d+)(?:\b|\/|\?|#)/i);
  return m?.[1] ? m[1] : null;
}

function toMs(v: any): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
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

let lastHubspotCallAt = 0;
async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function hubspotFetch(url: string, init?: RequestInit, opts?: { minIntervalMs?: number; maxRetries?: number }) {
  const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
  if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

  const minIntervalMs = Math.max(50, Math.min(500, Number(opts?.minIntervalMs ?? 160)));
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

async function postgrestGet(authHeader: string, path: string) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const url = `${supabaseUrl}/rest/v1/${path}`;
  const res = await fetch(url, { headers: { apikey: supabaseAnonKey, Authorization: authHeader } });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || "Supabase query failed"));
  return json;
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

async function hubspotSearchDealsModifiedBetweenPaged(sinceMs: number, untilMs: number, maxDeals: number) {
  const properties = [
    "dealname",
    "dealstage",
    "pipeline",
    "createdate",
    "hs_lastmodifieddate",
    "referralsource",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_analytics_source_data_2"
  ];

  const out: any[] = [];
  let after: string | null = null;
  while (out.length < maxDeals) {
    const body: any = {
      filterGroups: [
        {
          filters: [
            { propertyName: "hs_lastmodifieddate", operator: "GTE", value: String(sinceMs) },
            { propertyName: "hs_lastmodifieddate", operator: "LT", value: String(untilMs) }
          ]
        }
      ],
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

async function hubspotFetchListCompanyIds(listId: string, limit: number) {
  // HubSpot CRM Lists API memberships endpoint does NOT include object type in the path:
  // GET /crm/v3/lists/{listId}/memberships
  // Ref: https://developers.hubspot.com/docs/api-reference/crm/lists-v3/lists/get-crm-v3-lists-listId-memberships
  const out: string[] = [];
  let after: string | null = null;
  while (out.length < limit) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(500, limit - out.length)));
    if (after) qs.set("after", after);
    const url = `https://api.hubapi.com/crm/v3/lists/${encodeURIComponent(listId)}/memberships?${qs.toString()}`;
    const res = await hubspotFetch(url);
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
    for (const r of Array.isArray(json?.results) ? json.results : []) {
      const id = String(r?.recordId ?? r?.id ?? "").trim();
      if (id) out.push(id);
    }
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after) break;
  }
  return out;
}

async function hubspotFetchAssociatedCompanyIdsForDeal(dealId: string, limit: number) {
  const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}/associations/companies?limit=${encodeURIComponent(
    String(Math.max(1, Math.min(20, limit)))
  )}`;
  const res = await hubspotFetch(url);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot API error"));
  return (Array.isArray(json?.results) ? json.results : []).map((r: any) => String(r?.id ?? "").trim()).filter(Boolean);
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");

    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json()) as { periodStart?: string; maxDeals?: number };
    const maxDeals = Math.max(1, Math.min(1000, Number(payload?.maxDeals ?? 200)));

    const ps = payload?.periodStart ? new Date(String(payload.periodStart)) : startOfWeekUTC(new Date());
    const periodStart = ymd(startOfWeekUTC(ps));
    const periodEnd = ymd(new Date(Date.parse(`${periodStart}T00:00:00.000Z`) + 7 * 24 * 60 * 60 * 1000));
    const sinceMs = Date.parse(`${periodStart}T00:00:00.000Z`);
    const untilMs = Date.parse(`${periodEnd}T00:00:00.000Z`);

    const portalId = process.env.HUBSPOT_PORTAL_ID ?? process.env.NEXT_PUBLIC_HUBSPOT_PORTAL_ID ?? "";

    const stageLabelById = await hubspotFetchDealStageLabels();

    // Map companies -> active hypothesis (for "unassigned")
    const hyps = await postgrestGet(
      authHeader!,
      "sales_hypotheses?select=id,title,status,hubspot_tal_url&status=eq.active&hubspot_tal_url=not.is.null&limit=500"
    );
    const companyToHyp = new Map<string, string>();
    const hypTitleById = new Map<string, string>();
    for (const h of Array.isArray(hyps) ? hyps : []) {
      const hid = String(h?.id ?? "");
      hypTitleById.set(hid, String(h?.title ?? hid));
      const listId = parseHubspotListIdFromUrl(String(h?.hubspot_tal_url ?? "")) || null;
      if (!listId) continue;
      const cids = await hubspotFetchListCompanyIds(listId, 2000);
      for (const cid of cids) if (!companyToHyp.has(cid)) companyToHyp.set(cid, hid);
    }

    const deals = await hubspotSearchDealsModifiedBetweenPaged(sinceMs, untilMs, maxDeals);
    const out = [];
    for (const d of deals) {
      const id = String(d?.id ?? "");
      const props = d?.properties ?? {};
      const stageId = String(props?.dealstage ?? "").trim();
      const stageLabel = stageLabelById.get(stageId) ?? stageId;
      const stageCategory = stageCategoryFromLabel(stageLabel || stageId);
      const channel = channelFromDealProps(props);
      const createdMs = toMs(props?.createdate);
      const lastmodMs = toMs(props?.hs_lastmodifieddate);

      const companyIds = await hubspotFetchAssociatedCompanyIdsForDeal(id, 5);
      let hypId: string | null = null;
      for (const cid of companyIds) {
        const h = companyToHyp.get(cid) ?? null;
        if (h) {
          hypId = h;
          break;
        }
      }
      const hypKey = hypId || "__unassigned__";
      const hypTitle = hypId ? (hypTitleById.get(hypId) ?? hypId) : "Unassigned";

      const url = portalId && id ? `https://app.hubspot.com/contacts/${portalId}/record/0-3/${id}/` : null;
      out.push({
        id,
        url,
        dealname: String(props?.dealname ?? ""),
        createdate: createdMs ? new Date(createdMs).toISOString() : (props?.createdate ?? null),
        lastmodified: lastmodMs ? new Date(lastmodMs).toISOString() : (props?.hs_lastmodifieddate ?? null),
        dealstage_id: stageId || null,
        dealstage_label: stageLabel || null,
        stage_category: stageCategory,
        channel,
        hypothesis_key: hypKey,
        hypothesis_title: hypTitle
      });
    }

    return NextResponse.json({
      ok: true,
      period_start: periodStart,
      period_end: periodEnd,
      deals_count: out.length,
      deals: out
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


