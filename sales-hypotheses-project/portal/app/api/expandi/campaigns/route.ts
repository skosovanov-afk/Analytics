import { NextResponse } from "next/server";

type SupabaseUserResponse = { email?: string | null };

type ExpandiCampaignRow = {
  id?: number | string | null;
  li_account_id?: number | string | null;
  campaign_id?: number | string | null;
  name?: string | null;
  active?: boolean | null;
  archived?: boolean | null;
  campaign_status?: string | null;
  step_count?: number | string | null;
  nr_contacts_total?: number | string | null;
  stats_datetime?: string | null;
  synced_at?: string | null;
  updated_at?: string | null;
};

type ExpandiSnapshotRow = {
  campaign_instance_id?: number | string | null;
  snapshot_date?: string | null;
  connected?: number | string | null;
  contacted_people?: number | string | null;
  replied_first_action?: number | string | null;
  replied_other_actions?: number | string | null;
  people_in_campaign?: number | string | null;
  step_count?: number | string | null;
  synced_at?: string | null;
};

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function looksLikeMissingRelation(msg: string) {
  const t = String(msg || "").toLowerCase();
  return t.includes("could not find the table") || t.includes("schema cache") || (t.includes("relation") && t.includes("does not exist"));
}

function toNum(v: unknown) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function asBool(v: string | null | undefined, fallback: boolean) {
  if (v == null || v === "") return fallback;
  const t = String(v).trim().toLowerCase();
  if (t === "1" || t === "true" || t === "yes") return true;
  if (t === "0" || t === "false" || t === "no") return false;
  return fallback;
}

function parseIds(input: unknown): number[] {
  if (Array.isArray(input)) {
    return input.map((x) => Number(x)).filter((n) => Number.isFinite(n) && n > 0);
  }
  const s = String(input ?? "").trim();
  if (!s) return [];
  return s
    .split(",")
    .map((x) => Number(x.trim()))
    .filter((n) => Number.isFinite(n) && n > 0);
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

async function postgrestGetPaged(authHeader: string, pathBase: string, maxRows: number) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const out: any[] = [];
  const limit = Math.max(1, Math.min(50000, Number(maxRows || 50000)));
  for (let offset = 0; offset < limit; offset += 1000) {
    const url = `${supabaseUrl}/rest/v1/${pathBase}&limit=1000&offset=${offset}`;
    const res = await fetch(url, { headers: { apikey: supabaseAnonKey, Authorization: authHeader } });
    const json = (await res.json().catch(() => null)) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "Supabase query failed"));
    const rows = Array.isArray(json) ? json : [];
    out.push(...rows);
    if (rows.length < 1000) break;
  }
  return out.slice(0, limit);
}

async function loadCampaigns(authHeader: string, opts: {
  ids: number[];
  onlyActive: boolean;
  includeArchived: boolean;
  limit: number;
  offset: number;
}) {
  const select = "id,li_account_id,campaign_id,name,active,archived,campaign_status,step_count,nr_contacts_total,stats_datetime,synced_at,updated_at";
  const filters: string[] = [];
  if (opts.ids.length) {
    const csv = opts.ids.map((n) => encodeURIComponent(String(n))).join(",");
    filters.push(`id=in.(${csv})`);
  }
  if (opts.onlyActive) filters.push("active=is.true");
  if (!opts.includeArchived) filters.push("or=(archived.is.false,archived.is.null)");

  const where =
    `expandi_campaign_instances?select=${select}` +
    (filters.length ? `&${filters.join("&")}` : "") +
    `&order=updated_at.desc,id.asc`;

  if (opts.ids.length) {
    const rows = await postgrestGetPaged(authHeader, where, 50000);
    return rows as ExpandiCampaignRow[];
  }

  const pageWhere = `${where}&limit=${opts.limit}&offset=${opts.offset}`;
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const url = `${supabaseUrl}/rest/v1/${pageWhere}`;
  const res = await fetch(url, { headers: { apikey: supabaseAnonKey, Authorization: authHeader } });
  const json = (await res.json().catch(() => null)) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "Supabase query failed"));
  return (Array.isArray(json) ? json : []) as ExpandiCampaignRow[];
}

async function loadLatestSnapshots(authHeader: string, campaignIds: number[]) {
  if (!campaignIds.length) return new Map<number, ExpandiSnapshotRow>();
  const csv = campaignIds.map((n) => encodeURIComponent(String(n))).join(",");
  const where =
    `expandi_campaign_stats_snapshots?select=campaign_instance_id,snapshot_date,connected,contacted_people,replied_first_action,replied_other_actions,people_in_campaign,step_count,synced_at` +
    `&campaign_instance_id=in.(${csv})` +
    `&order=campaign_instance_id.asc,snapshot_date.desc`;

  const rows = (await postgrestGetPaged(authHeader, where, 50000)) as ExpandiSnapshotRow[];
  const out = new Map<number, ExpandiSnapshotRow>();
  for (const r of rows) {
    const cid = Number(r.campaign_instance_id);
    if (!Number.isFinite(cid)) continue;
    if (!out.has(cid)) out.set(cid, r);
  }
  return out;
}

async function handleRequest(req: Request) {
  const authHeader = req.headers.get("authorization");
  const user = await getSupabaseUserFromAuthHeader(authHeader);
  if (!user?.email) return jsonError(401, "Not authorized");

  const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "";
  const email = String(user.email || "").toLowerCase();
  if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

  const payload = req.method === "POST" ? await req.json().catch(() => ({})) : null;
  const url = new URL(req.url);

  const campaignIds = parseIds(payload?.campaign_ids ?? url.searchParams.get("campaign_ids"));
  const includeStats = asBool(
    String(payload?.include_stats ?? url.searchParams.get("include_stats") ?? ""),
    true
  );
  const onlyActive = asBool(
    String(payload?.only_active ?? url.searchParams.get("only_active") ?? ""),
    false
  );
  const includeArchived = asBool(
    String(payload?.include_archived ?? url.searchParams.get("include_archived") ?? ""),
    false
  );

  const limit = Math.max(1, Math.min(1000, Number(payload?.limit ?? url.searchParams.get("limit") ?? 200)));
  const offset = Math.max(0, Number(payload?.offset ?? url.searchParams.get("offset") ?? 0));

  const campaigns = await loadCampaigns(authHeader ?? "", {
    ids: campaignIds,
    onlyActive,
    includeArchived,
    limit,
    offset
  });

  const ids = campaigns
    .map((r) => Number(r.id))
    .filter((n) => Number.isFinite(n) && n > 0);

  let latestSnapshots = new Map<number, ExpandiSnapshotRow>();
  let snapshotStatsDisabledReason: string | null = null;
  if (includeStats) {
    try {
      latestSnapshots = await loadLatestSnapshots(authHeader ?? "", ids);
    } catch (e: any) {
      const msg = String(e?.message || e);
      if (!looksLikeMissingRelation(msg)) throw e;
      snapshotStatsDisabledReason = msg;
    }
  }

  const rows = campaigns.map((c) => {
    const id = Number(c.id);
    const snap = latestSnapshots.get(id);
    return {
      id,
      li_account_id: toNum(c.li_account_id),
      campaign_id: c.campaign_id == null ? null : toNum(c.campaign_id),
      name: c.name ? String(c.name) : null,
      active: c.active == null ? null : Boolean(c.active),
      archived: c.archived == null ? null : Boolean(c.archived),
      campaign_status: c.campaign_status ? String(c.campaign_status) : null,
      step_count: toNum(c.step_count),
      nr_contacts_total: toNum(c.nr_contacts_total),
      stats_datetime: c.stats_datetime ? String(c.stats_datetime) : null,
      synced_at: c.synced_at ? String(c.synced_at) : null,
      updated_at: c.updated_at ? String(c.updated_at) : null,
      latest_snapshot: snap
        ? {
          snapshot_date: snap.snapshot_date ? String(snap.snapshot_date) : null,
          connected: toNum(snap.connected),
          contacted_people: toNum(snap.contacted_people),
          replied_first_action: toNum(snap.replied_first_action),
          replied_other_actions: toNum(snap.replied_other_actions),
          people_in_campaign: toNum(snap.people_in_campaign),
          step_count: toNum(snap.step_count),
          synced_at: snap.synced_at ? String(snap.synced_at) : null
        }
        : null
    };
  });

  return NextResponse.json({
    ok: true,
    source: "expandi_db",
    filters: {
      campaign_ids: campaignIds,
      include_stats: includeStats,
      only_active: onlyActive,
      include_archived: includeArchived,
      limit,
      offset
    },
    snapshot_stats_disabled_reason: snapshotStatsDisabledReason,
    campaigns: rows
  });
}

export async function GET(req: Request) {
  try {
    return await handleRequest(req);
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}

export async function POST(req: Request) {
  try {
    return await handleRequest(req);
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}
