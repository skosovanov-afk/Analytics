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

function parseCsvEnv(name: string) {
  const raw = String(process.env[name] ?? "").trim();
  if (!raw) return [];
  return raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

async function smartleadFetch(path: string, init?: RequestInit) {
  const apiKey = String(process.env.SMARTLEAD_API_KEY ?? "").trim();
  if (!apiKey) throw new Error("Missing SMARTLEAD_API_KEY");
  const baseUrl = String(process.env.SMARTLEAD_BASE_URL ?? "https://server.smartlead.ai").trim().replace(/\/+$/g, "");
  const url = `${baseUrl}${path}${path.includes("?") ? "&" : "?"}api_key=${encodeURIComponent(apiKey)}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      accept: "application/json",
      ...(init?.body != null ? { "content-type": "application/json" } : {}),
      ...(init?.headers ?? {})
    }
  });
  const text = await res.text();
  let json: any = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }
  if (!res.ok) throw new Error(String(json?.message || json?.error || text || "SmartLead API error"));
  return json;
}

function pickArrayFromSmartleadListResponse(json: any): any[] {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json?.data)) return json.data;
  if (Array.isArray(json?.results)) return json.results;
  if (Array.isArray(json?.campaigns)) return json.campaigns;
  return [];
}

function pickCampaignName(raw: any) {
  return (
    String(raw?.name ?? raw?.campaign_name ?? raw?.campaignName ?? raw?.title ?? raw?.campaign_title ?? raw?.campaignTitle ?? "").trim() || null
  );
}

function pickCampaignStatus(raw: any) {
  const s = String(raw?.status ?? raw?.state ?? raw?.campaign_status ?? raw?.campaignStatus ?? raw?.is_active ?? raw?.isActive ?? "").trim().toLowerCase();
  if (!s) return { status: "unknown" as const, raw_status: "" };
  if (s === "1" || s === "true" || s.includes("active") || s.includes("running") || s.includes("live")) return { status: "active" as const, raw_status: s };
  if (s === "0" || s === "false" || s.includes("pause") || s.includes("stopp") || s.includes("disabled") || s.includes("draft")) return { status: "paused" as const, raw_status: s };
  return { status: "unknown" as const, raw_status: s };
}

async function smartleadListCampaignsBestEffort(opts?: { limit?: number; offset?: number }) {
  const limit = Math.max(1, Math.min(200, Number(opts?.limit ?? 100)));
  const offset = Math.max(0, Number(opts?.offset ?? 0));
  const qs = new URLSearchParams();
  qs.set("limit", String(limit));
  qs.set("offset", String(offset));
  const q = qs.toString();
  const attempts = [
    `/api/v1/campaigns?${q}`,
    `/api/v1/campaigns/list?${q}`,
    `/api/v1/campaigns`
  ];
  let lastErr: any = null;
  for (const path of attempts) {
    try {
      const json = await smartleadFetch(path, { method: "GET" });
      return pickArrayFromSmartleadListResponse(json);
    } catch (e: any) {
      lastErr = e;
    }
  }
  throw lastErr ?? new Error("Failed to list campaigns");
}

async function smartleadListAllCampaignsBestEffort(limit: number, maxTotal: number) {
  const out: any[] = [];
  let offset = 0;
  const pageSize = Math.max(1, Math.min(200, Number(limit || 100)));
  const cap = Math.max(pageSize, Math.min(2000, Number(maxTotal || 1000)));
  for (;;) {
    const batch = await smartleadListCampaignsBestEffort({ limit: pageSize, offset });
    const rows = Array.isArray(batch) ? batch : [];
    if (!rows.length) break;
    out.push(...rows);
    if (rows.length < pageSize) break;
    if (out.length >= cap) break;
    offset += pageSize;
  }
  return out.slice(0, cap);
}

function pickCampaignStats(raw: any) {
  // SmartLead fields are not stable across versions; we keep this best-effort and expose raw keys in debug if needed.
  const num = (v: any) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };
  const deepGet = (obj: any, path: string[]) => {
    let cur = obj;
    for (const k of path) {
      if (!cur || typeof cur !== "object") return undefined;
      cur = (cur as any)[k];
    }
    return cur;
  };
  // Try multiple candidate keys for each metric.
  const pick = (candidates: Array<string | string[]>) => {
    for (const c of candidates) {
      const v = Array.isArray(c) ? deepGet(raw, c) : (raw as any)?.[c];
      const n = num(v);
      if (n != null) return n;
    }
    return null;
  };

  // Common shapes: raw.stats.*, raw.statistics.*, raw.data.stats.*
  const baseCandidates = (k: string) => [
    k,
    ["stats", k],
    ["statistics", k],
    ["data", "stats", k],
    ["data", "statistics", k]
  ];

  // SmartLead has both "unique sent" (leads contacted) and "sent count" (messages sent).
  const leadsContacted = pick([
    ...baseCandidates("unique_sent_count"),
    ...baseCandidates("uniqueSentCount"),
    ...baseCandidates("unique_sent"),
    ...baseCandidates("uniqueSent")
  ]);
  const sent = pick([...baseCandidates("sent"), ...baseCandidates("emails_sent"), ...baseCandidates("sent_count"), ...baseCandidates("total_sent")]);
  const opened = pick([
    ...baseCandidates("opened"),
    ...baseCandidates("emails_opened"),
    ...baseCandidates("opened_count"),
    ...baseCandidates("total_opened"),
    ...baseCandidates("open_count"),
    ...baseCandidates("total_open")
  ]);
  const replied = pick([
    ...baseCandidates("replied"),
    ...baseCandidates("emails_replied"),
    ...baseCandidates("replied_count"),
    ...baseCandidates("total_replied"),
    ...baseCandidates("reply_count")
  ]);
  const positiveReply = pick([
    ...baseCandidates("positive_reply"),
    ...baseCandidates("positive_replies"),
    ...baseCandidates("positive_reply_count"),
    ...baseCandidates("positiveReplies"),
    ...baseCandidates("positive_replied"),
    ...baseCandidates("positive_replied_count")
  ]);
  const bounced = pick([...baseCandidates("bounced"), ...baseCandidates("bounce"), ...baseCandidates("bounced_count"), ...baseCandidates("bounce_count"), ...baseCandidates("total_bounced")]);
  const senderBounced = pick([
    ...baseCandidates("sender_bounced"),
    ...baseCandidates("sender_bounce"),
    ...baseCandidates("sender_bounced_count"),
    ...baseCandidates("sender_bounce_count")
  ]);
  const repliedOoo = pick([
    ...baseCandidates("replied_ooo"),
    ...baseCandidates("replied_with_ooo"),
    ...baseCandidates("replied_w_ooo"),
    ...baseCandidates("reply_ooo_count")
  ]);

  return {
    leads_contacted: leadsContacted,
    emails_sent: sent,
    emails_opened: opened,
    emails_replied: replied,
    positive_reply: positiveReply,
    bounced,
    sender_bounced: senderBounced,
    replied_ooo: repliedOoo
  };
}

async function smartleadGetCampaignBestEffort(id: number) {
  const cid = encodeURIComponent(String(id));
  // SmartLead API surface varies; try a few likely endpoints.
  const attempts = [
    `/api/v1/campaigns/${cid}`,
    `/api/v1/campaigns/${cid}/details`,
    // Some accounts expose aggregated stats via dedicated endpoints.
    `/api/v1/campaigns/${cid}/stats`,
    `/api/v1/campaigns/${cid}/analytics`,
    `/api/v1/campaigns/${cid}/analytics/summary`
  ];
  let lastErr: any = null;
  for (const path of attempts) {
    try {
      return await smartleadFetch(path, { method: "GET" });
    } catch (e: any) {
      lastErr = e;
    }
  }
  throw lastErr ?? new Error("Failed to fetch campaign");
}

async function smartleadGetCampaignStatsProbe(id: number) {
  const cid = encodeURIComponent(String(id));
  const attempts = [
    `/api/v1/campaigns/${cid}/stats`,
    `/api/v1/campaigns/${cid}/analytics`,
    `/api/v1/campaigns/${cid}/analytics/summary`,
    `/api/v1/campaigns/${cid}/metrics`,
    `/api/v1/campaigns/${cid}/statistics`,
    `/api/v1/campaigns/${cid}/report`
  ];
  const tried: Array<{ path: string; ok: boolean; error?: string }> = [];
  for (const path of attempts) {
    try {
      const json = await smartleadFetch(path, { method: "GET" });
      tried.push({ path, ok: true });
      return { ok: true as const, path, json, tried };
    } catch (e: any) {
      tried.push({ path, ok: false, error: String(e?.message || e) });
    }
  }
  return { ok: false as const, tried };
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");
    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json().catch(() => ({}))) as {
      campaign_ids?: Array<string | number>;
      include_raw?: boolean;
      include_stats?: boolean;
      limit?: number;
      offset?: number;
      all?: boolean;
    };
    const includeRaw = Boolean(payload?.include_raw);
    const includeStats = Boolean(payload?.include_stats);
    const fromEnv = parseCsvEnv("SMARTLEAD_CAMPAIGN_IDS").map((x) => Number(x)).filter((n) => Number.isFinite(n) && n > 0) as number[];
    const fromReq = Array.isArray(payload?.campaign_ids)
      ? payload.campaign_ids.map((x) => Number(x)).filter((n) => Number.isFinite(n) && n > 0)
      : [];
    const listAll = Boolean(payload?.all);
    const listLimit = Math.max(1, Math.min(200, Number(payload?.limit ?? 100)));
    const listOffset = Math.max(0, Number(payload?.offset ?? 0));

    let ids: number[] = [];
    let source: "request" | "smartlead_api" | "env" = fromReq.length ? "request" : "env";
    if (fromReq.length) ids = fromReq.slice(0, 200);
    else {
      // If env is not configured, auto-discover campaigns from SmartLead API.
      const listed = listAll
        ? await smartleadListAllCampaignsBestEffort(listLimit, 1000)
        : await smartleadListCampaignsBestEffort({ limit: listLimit, offset: listOffset });
      const picked = listed
        .map((c: any) => Number(c?.id ?? c?.campaign_id ?? c?.campaignId))
        .filter((n) => Number.isFinite(n) && n > 0) as number[];
      if (picked.length) {
        source = "smartlead_api";
        ids = picked.slice(0, listAll ? 1000 : 200);
      } else if (fromEnv.length) {
        source = "env";
        ids = fromEnv.slice(0, 200);
      } else {
        return NextResponse.json({
          ok: true,
          skipped: true,
          reason: "No SmartLead campaigns available (set SMARTLEAD_CAMPAIGN_IDS or ensure SmartLead API key can list campaigns)",
          campaigns: [],
          failed: [],
          source
        });
      }
    }

    const campaigns: any[] = [];
    const failed: any[] = [];
    const listMap = new Map<number, any>();
    if (source === "smartlead_api") {
      const listed = listAll
        ? await smartleadListAllCampaignsBestEffort(listLimit, 1000)
        : await smartleadListCampaignsBestEffort({ limit: listLimit, offset: listOffset });
      for (const c of listed) {
        const id = Number(c?.id ?? c?.campaign_id ?? c?.campaignId);
        if (Number.isFinite(id) && id > 0) listMap.set(id, c);
      }
    }
    for (const id of ids) {
      try {
        // For listing, prefer a fast path: if include_stats/raw, fetch details; otherwise return minimal info.
        const raw = includeRaw || includeStats ? await smartleadGetCampaignBestEffort(id) : (listMap.get(id) ?? null);
        const name = raw ? pickCampaignName(raw) : null;
        const st = raw ? pickCampaignStatus(raw) : { status: "unknown" as const, raw_status: "" };
        let statsProbe: any = null;
        if (includeStats) {
          try {
            statsProbe = await smartleadGetCampaignStatsProbe(id);
          } catch (e: any) {
            statsProbe = { ok: false, error: String(e?.message || e) };
          }
        }
        const stats = raw ? (statsProbe?.ok ? pickCampaignStats(statsProbe.json) : pickCampaignStats(raw)) : null;
        campaigns.push({
          id,
          name,
          status: st.status,
          raw_status: st.raw_status,
          stats,
          raw: includeRaw ? { ...(raw ?? {}), stats_probe: statsProbe } : null
        });
      } catch (e: any) {
        failed.push({ id, error: String(e?.message || e) });
        campaigns.push({ id, name: null, status: "unknown", raw_status: "", stats: null, raw: null });
      }
    }

    return NextResponse.json({ ok: true, campaigns, failed, source });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


