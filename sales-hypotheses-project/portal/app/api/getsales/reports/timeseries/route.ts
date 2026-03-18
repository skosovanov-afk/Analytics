import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

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
  return (await res.json()) as { email?: string | null };
}

function parseYmd(ymd: string) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd || "").trim());
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  if (!Number.isFinite(y) || !Number.isFinite(mo) || !Number.isFinite(d)) return null;
  if (mo < 1 || mo > 12 || d < 1 || d > 31) return null;
  return { y, mo, d };
}

function daysInclusive(sinceYmd: string, untilYmd: string) {
  const out: string[] = [];
  const s = parseYmd(sinceYmd);
  const u = parseYmd(untilYmd);
  if (!s || !u) return out;
  const sMs = Date.UTC(s.y, s.mo - 1, s.d, 0, 0, 0, 0);
  const uMs = Date.UTC(u.y, u.mo - 1, u.d, 0, 0, 0, 0);
  if (uMs < sMs) return out;
  const days = Math.min(370, Math.floor((uMs - sMs) / 86_400_000) + 1);
  for (let i = 0; i < days; i++) {
    const d = new Date(sMs + i * 86_400_000);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    out.push(`${y}-${m}-${dd}`);
  }
  return out;
}

/**
 * Fetch GetSales data with bearer token.
 *
 * @param {string} path
 * @returns {Promise<any>}
 */
async function getsalesFetchJson(path: string) {
  const token = process.env.GETSALES_API_TOKEN ?? process.env.GETSALES_BEARER_TOKEN ?? "";
  if (!token) throw new Error("Missing GETSALES_API_TOKEN");
  const base = (process.env.GETSALES_BASE_URL ?? "https://amazing.getsales.io").replace(/\/+$/, "");
  const res = await fetch(`${base}${path}`, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => null);
  if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales request failed"));
  return json;
}

/**
 * Best-effort list of flows to map flow UUIDs to workspace/version UUIDs.
 *
 * @returns {Promise<any[]>}
 */
async function listFlowsForMapping() {
  const out: any[] = [];
  const limit = 200;
  let offset = 0;
  while (out.length < 2000) {
    const qs = new URLSearchParams();
    qs.set("limit", String(limit));
    qs.set("offset", String(offset));
    qs.set("order_field", "created_at");
    qs.set("order_type", "desc");
    const json = await getsalesFetchJson(`/flows/api/flows?${qs.toString()}`);
    const rows = Array.isArray(json?.data) ? json.data : Array.isArray(json) ? json : [];
    if (!rows.length) break;
    out.push(...rows);
    offset += rows.length;
    if (json?.has_more === false) break;
  }
  return out;
}

/**
 * Read GetSales events in pages to avoid default limits.
 *
 * @param {ReturnType<typeof createClient>} supabaseAdmin
 * @param {string} sinceYmd
 * @param {string} untilYmd
 * @returns {Promise<any[]>}
 */
async function listGetsalesEventsPaged(
  supabaseAdmin: any,
  sinceYmd: string,
  untilYmd: string
) {
  const rows: any[] = [];
  const pageSize = 1000;
  const maxRows = 50000;
  let from = 0;

  while (rows.length < maxRows) {
    const to = from + pageSize - 1;
    const { data, error } = await supabaseAdmin
      .from("sales_getsales_events")
      .select("getsales_uuid, lead_uuid, occurred_at, payload")
      .gte("occurred_at", `${sinceYmd}T00:00:00Z`)
      .lte("occurred_at", `${untilYmd}T23:59:59Z`)
      .in("source", ["linkedin", "linkedin_connection"])
      .range(from, to);

    if (error) throw error;
    const batch = Array.isArray(data) ? data : [];
    rows.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  return rows;
}

export async function POST(req: Request) {
  try {
    const authHeader = req.headers.get("authorization");
    const user = await getSupabaseUserFromAuthHeader(authHeader);
    if (!user?.email) return jsonError(401, "Not authorized");
    const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
    const email = String(user.email || "").toLowerCase();
    if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

    const payload = (await req.json()) as {
      kind?: "linkedin" | "email";
      since: string;
      until: string;
      flow_uuids?: string[];
    };
    const kind = String(payload?.kind ?? "linkedin").toLowerCase();
    const sinceYmd = String(payload?.since ?? "").trim();
    const untilYmd = String(payload?.until ?? "").trim();
    if (!parseYmd(sinceYmd) || !parseYmd(untilYmd)) return jsonError(400, "since/until must be YYYY-MM-DD");

    if (kind !== "linkedin") {
      return NextResponse.json({
        ok: true,
        disabled: true,
        reason: "GetSales email reports are disabled (LinkedIn-only mode).",
        kind,
        since: sinceYmd,
        until: untilYmd,
        days: [],
        series: {}
      });
    }

    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";
    if (!supabaseUrl || !serviceRoleKey) return jsonError(500, "Missing Supabase config");
    const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey);

    const rows = await listGetsalesEventsPaged(supabaseAdmin, sinceYmd, untilYmd);
    console.log("[GetSales Timeseries] rows", {
      since: sinceYmd,
      until: untilYmd,
      count: rows.length
    });

    const days = daysInclusive(sinceYmd, untilYmd);
    const init = () => days.map(() => 0);
    const series: Record<string, number[]> = {
      connections_sent: init(),
      connections_accepted: init(),
      messages_sent: init(),
      messages_opened: init(),
      messages_replied: init()
    };

    const sentSets = days.map(() => new Set<string>());
    const openedSets = days.map(() => new Set<string>());
    const repliedSets = days.map(() => new Set<string>());
    const connSentSets = days.map(() => new Set<string>());
    const connAcceptedSets = days.map(() => new Set<string>());
    const flowSet = new Set((payload?.flow_uuids || []).map((x) => String(x || "").trim()).filter(Boolean));
    const flowWorkspaceSet = new Set<string>();
    const flowVersionSet = new Set<string>();

    if (flowSet.size) {
      try {
        const flows = await listFlowsForMapping();
        for (const f of flows) {
          const id = String(f?.uuid ?? "").trim();
          if (!id || !flowSet.has(id)) continue;
          const ws = String(f?.flow_workspace_uuid ?? "").trim();
          const ver = String(f?.flow_version_uuid ?? "").trim();
          if (ws) flowWorkspaceSet.add(ws);
          if (ver) flowVersionSet.add(ver);
        }
      } catch {
        // If flow mapping fails, keep best-effort filtering by payload flow_uuid only.
      }
    }
    console.log("[GetSales Reports] timeseries query", {
      sinceYmd,
      untilYmd,
      flowUuidsCount: flowSet.size,
      flowWorkspaceCount: flowWorkspaceSet.size,
      flowVersionCount: flowVersionSet.size
    });

    /**
     * Ignore InMail for message counts to match GetSales UI.
     */
    const isInmail = (p: any) => {
      const status = String(p?.status ?? "").trim().toLowerCase();
      const text = String(p?.text ?? "").trim().toLowerCase();
      const type = String(p?.type ?? "").trim().toLowerCase();
      return status.includes("inmail") || type.includes("inmail") || text.includes("inmail");
    };

    /**
     * Prefer message_hash for message-level counting; skip when missing.
     */
    const msgKey = (p: any) => String(p?.message_hash || p?.messageHash || "").trim();

    for (const r of (rows || [])) {
      const d = new Date(r.occurred_at).toISOString().split("T")[0];
      const idx = days.indexOf(d);
      if (idx < 0) continue;

      const kind = String(r?.payload?.event_kind ?? "").trim();
      if (!kind) continue;
      if (flowSet.size) {
        const flowUuid = String(r?.payload?.flow_uuid ?? r?.payload?.flowUuid ?? r?.payload?.flow?.uuid ?? "").trim();
        const taskPipeline = String(r?.payload?.task_pipeline_uuid ?? "").trim();
        const flowVersion = String(r?.payload?.flow_version_uuid ?? r?.payload?.flowVersionUuid ?? "").trim();
        const hasMatch =
          (flowUuid && flowSet.has(flowUuid)) ||
          (taskPipeline && flowWorkspaceSet.has(taskPipeline)) ||
          (flowVersion && flowVersionSet.has(flowVersion));
        const hasAnyId = Boolean(flowUuid || taskPipeline || flowVersion);
        if (hasAnyId && !hasMatch) continue;
      }
      if (kind === "linkedin_connection_request_sent") {
        if (r?.lead_uuid) connSentSets[idx].add(String(r.lead_uuid));
      } else if (kind === "linkedin_connection_request_accepted") {
        if (r?.lead_uuid) connAcceptedSets[idx].add(String(r.lead_uuid));
      } else if (kind === "linkedin_message_sent") {
        if (isInmail(r?.payload)) continue;
        const key = msgKey(r?.payload);
        if (key) sentSets[idx].add(key);
      } else if (kind === "linkedin_message_opened") {
        if (isInmail(r?.payload)) continue;
        const key = msgKey(r?.payload);
        if (key) openedSets[idx].add(key);
      } else if (kind === "linkedin_message_replied") {
        if (isInmail(r?.payload)) continue;
        const key = msgKey(r?.payload);
        if (key) repliedSets[idx].add(key);
      }
    }

    for (let i = 0; i < days.length; i++) {
      series.connections_sent[i] = connSentSets[i].size;
      series.connections_accepted[i] = connAcceptedSets[i].size;
      series.messages_sent[i] = sentSets[i].size;
      series.messages_opened[i] = openedSets[i].size;
      series.messages_replied[i] = repliedSets[i].size;
    }

    const sumArr = (a: number[]) => a.reduce((s, v) => s + (Number(v) || 0), 0);
    console.log("[GetSales Reports] timeseries totals", {
      days: days.length,
      rows: rows.length,
      connections_sent: sumArr(series.connections_sent),
      connections_accepted: sumArr(series.connections_accepted),
      messages_sent: sumArr(series.messages_sent),
      messages_opened: sumArr(series.messages_opened),
      messages_replied: sumArr(series.messages_replied)
    });

    console.log("[GetSales Timeseries] totals", {
      connections_sent: sumArr(series.connections_sent),
      connections_accepted: sumArr(series.connections_accepted),
      messages_sent: sumArr(series.messages_sent),
      messages_opened: sumArr(series.messages_opened),
      messages_replied: sumArr(series.messages_replied)
    });

    return NextResponse.json({
      ok: true,
      kind,
      since: sinceYmd,
      until: untilYmd,
      days,
      series
    });
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}
