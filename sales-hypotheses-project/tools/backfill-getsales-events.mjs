/**
 * Backfill transactional GetSales events into sales_getsales_events.
 *
 * Creates one row per event kind (email_sent, email_replied, email_bounced,
 * linkedin_message_sent, linkedin_message_replied, linkedin_message_opened,
 * linkedin_connection_request_sent, linkedin_connection_request_accepted).
 *
 * Usage:
 *   node backfill-getsales-events.mjs --since 2026-01-01 --until 2026-01-31 --created-by <uuid>
 *   node backfill-getsales-events.mjs --since 2026-01-01 --until 2026-01-31 --created-by <uuid> --only linkedin
 *
 * Env:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 *   GETSALES_API_TOKEN (or GETSALES_BEARER_TOKEN)
 *   GETSALES_BASE_URL (optional, default https://amazing.getsales.io)
 */
import process from "process";

/**
 * Parse CLI args (simple --key value pairs).
 *
 * @returns {Record<string, string>}
 */
function parseArgs() {
  const out = {};
  const args = process.argv.slice(2);
  for (let i = 0; i < args.length; i++) {
    const k = args[i];
    if (!k.startsWith("--")) continue;
    const key = k.slice(2);
    const val = args[i + 1];
    if (!val || val.startsWith("--")) {
      out[key] = "true";
      continue;
    }
    out[key] = val;
    i++;
  }
  return out;
}

/**
 * Sleep helper for rate limiting.
 *
 * @param {number} ms
 */
async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

/**
 * Parse timestamps to epoch ms.
 *
 * @param {any} v
 * @returns {number|null}
 */
function toMs(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}

/**
 * Validate YYYY-MM-DD.
 *
 * @param {string} ymd
 * @returns {boolean}
 */
function isYmd(ymd) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(ymd || "").trim());
}

/**
 * Convert YYYY-MM-DD to ISO start of day (UTC).
 *
 * @param {string} ymd
 * @returns {string}
 */
function ymdStartIso(ymd) {
  return `${String(ymd).trim()}T00:00:00.000Z`;
}

/**
 * Convert YYYY-MM-DD to ISO end of day (UTC).
 *
 * @param {string} ymd
 * @returns {string}
 */
function ymdEndIso(ymd) {
  return `${String(ymd).trim()}T23:59:59.999Z`;
}

/**
 * Pick a reasonable contact email from a lead or payload.
 *
 * @param {any} lead
 * @param {any} payload
 * @returns {string}
 */
function pickContactEmail(lead, payload) {
  const fromLead =
    String(lead?.work_email ?? lead?.workEmail ?? lead?.personal_email ?? lead?.personalEmail ?? lead?.email ?? "")
      .trim()
      .toLowerCase();
  if (fromLead) return fromLead;
  const fromPayload =
    String(payload?.to_email ?? payload?.toEmail ?? payload?.email ?? payload?.lead_email ?? "")
      .trim()
      .toLowerCase();
  return fromPayload || "";
}

/**
 * GetSales API fetch with retry/backoff.
 *
 * @param {string} path
 * @param {RequestInit} init
 * @param {{ minIntervalMs?: number; maxRetries?: number }} opts
 * @returns {Promise<Response>}
 */
async function getsalesFetch(path, init, opts) {
  const token = process.env.GETSALES_API_TOKEN ?? process.env.GETSALES_BEARER_TOKEN ?? "";
  if (!token) throw new Error("Missing GETSALES_API_TOKEN");
  const base = (process.env.GETSALES_BASE_URL ?? "https://amazing.getsales.io").replace(/\/+$/, "");
  const minIntervalMs = Math.max(50, Math.min(500, Number(opts?.minIntervalMs ?? 140)));
  const maxRetries = Math.max(0, Math.min(8, Number(opts?.maxRetries ?? 4)));
  let lastCallAt = 0;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const now = Date.now();
    const wait = Math.max(0, lastCallAt + minIntervalMs - now);
    if (wait) await sleep(wait);
    lastCallAt = Date.now();

    const url = `${base}${path}`;
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), 30000);
    try {
      const res = await fetch(url, {
        ...init,
        signal: controller.signal,
        headers: {
          Authorization: `Bearer ${token}`,
          ...(init?.headers ?? {})
        }
      });
      clearTimeout(id);
      if (res.status !== 429 && res.status !== 503) return res;
    } catch (e) {
      clearTimeout(id);
      if (e?.name === "AbortError") throw new Error(`GetSales API timeout (30s) for ${url}`);
      throw e;
    }
    await sleep(Math.min(10_000, 900 + attempt * 700));
  }
  throw new Error("GetSales rate limit / unavailable. Try again later.");
}

/**
 * Supabase REST insert (ignore duplicates).
 *
 * @param {string} path
 * @param {any} body
 * @returns {Promise<any>}
 */
async function supabaseInsert(path, body) {
  const supabaseUrl = process.env.SUPABASE_URL ?? process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";
  if (!supabaseUrl || !serviceKey) throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  const res = await fetch(`${supabaseUrl}/rest/v1/${path}`, {
    method: "POST",
    headers: {
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
      "Content-Type": "application/json",
      Prefer: "resolution=ignore-duplicates,return=representation"
    },
    body: JSON.stringify(body)
  });
  const json = await res.json().catch(() => null);
  if (!res.ok) throw new Error(String(json?.message || json?.error || "Supabase insert failed"));
  return json;
}

/**
 * Supabase REST update (patch existing row).
 *
 * @param {string} path
 * @param {any} body
 * @returns {Promise<any>}
 */
async function supabaseUpdate(path, body) {
  const supabaseUrl = process.env.SUPABASE_URL ?? process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";
  if (!supabaseUrl || !serviceKey) throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  const res = await fetch(`${supabaseUrl}/rest/v1/${path}`, {
    method: "PATCH",
    headers: {
      apikey: serviceKey,
      Authorization: `Bearer ${serviceKey}`,
      "Content-Type": "application/json",
      Prefer: "return=minimal"
    },
    body: JSON.stringify(body)
  });
  const json = await res.json().catch(() => null);
  if (!res.ok) throw new Error(String(json?.message || json?.error || "Supabase update failed"));
  return json;
}

/**
 * List emails since a timestamp.
 *
 * @param {number} sinceMs
 * @param {number} max
 * @returns {Promise<any[]>}
 */
async function listEmailsSince(sinceMs, max) {
  const out = [];
  const limit = Math.max(1, Math.min(200, max));
  let offset = 0;
  console.log(`[GetSales Txn] emails: start since=${new Date(sinceMs).toISOString()}`);
  while (out.length < max) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(limit, max - out.length)));
    qs.set("offset", String(offset));
    qs.set("order_field", "updated_at");
    qs.set("order_type", "desc");
    qs.set("filter[updated_at][>=]", new Date(sinceMs).toISOString());
    const res = await getsalesFetch(`/emails/api/emails?${qs.toString()}`, { method: "GET" });
    const json = await res.json();
    if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales emails list failed"));
    const rows = Array.isArray(json) ? json : Array.isArray(json?.data) ? json.data : [];
    console.log(`[GetSales Txn] emails: page offset=${offset} rows=${rows.length} total=${out.length + rows.length}`);
    if (!rows.length) break;
    out.push(...rows);
    offset += rows.length;
    const last = rows[rows.length - 1];
    const lastMs = toMs(last?.updated_at) ?? toMs(last?.sent_at) ?? toMs(last?.created_at) ?? null;
    if (lastMs != null && lastMs < sinceMs) break;
  }
  return out.slice(0, max);
}

/**
 * List LinkedIn messages since a timestamp.
 *
 * @param {number} sinceMs
 * @param {number} max
 * @returns {Promise<any[]>}
 */
async function listLinkedinMessagesSince(sinceMs, max) {
  const out = [];
  const limit = Math.max(1, Math.min(200, max));
  let offset = 0;
  console.log(`[GetSales Txn] linkedin: start since=${new Date(sinceMs).toISOString()}`);
  while (out.length < max) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(limit, max - out.length)));
    qs.set("offset", String(offset));
    qs.set("order_field", "sent_at");
    qs.set("order_type", "desc");
    const res = await getsalesFetch(`/flows/api/linkedin-messages?${qs.toString()}`, { method: "GET" });
    const json = await res.json();
    if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales linkedin messages list failed"));
    const rows = Array.isArray(json?.data) ? json.data : Array.isArray(json) ? json : [];
    console.log(`[GetSales Txn] linkedin: page offset=${offset} rows=${rows.length} total=${out.length + rows.length}`);
    if (!rows.length) break;
    out.push(...rows);
    offset += rows.length;
    const last = rows[rows.length - 1];
    const lastMs = toMs(last?.sent_at) ?? toMs(last?.read_at) ?? toMs(last?.updated_at) ?? toMs(last?.created_at) ?? null;
    if (lastMs != null && lastMs < sinceMs) break;
    if (json?.has_more === false) break;
  }
  return out.slice(0, max);
}

/**
 * List automation flows (best-effort, paginated).
 *
 * @param {number} max
 * @returns {Promise<any[]>}
 */
async function listFlows(max) {
  const out = [];
  const limit = Math.max(1, Math.min(200, max));
  let offset = 0;
  while (out.length < max) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(limit, max - out.length)));
    qs.set("offset", String(offset));
    qs.set("order_field", "created_at");
    qs.set("order_type", "desc");
    const res = await getsalesFetch(`/flows/api/flows?${qs.toString()}`, { method: "GET" });
    const json = await res.json();
    if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales flows list failed"));
    const rows = Array.isArray(json?.data) ? json.data : Array.isArray(json) ? json : [];
    if (!rows.length) break;
    out.push(...rows);
    offset += rows.length;
    if (json?.has_more === false) break;
  }
  return out.slice(0, max);
}

/**
 * Build best-effort flow lookup maps.
 *
 * @param {any[]} flows
 */
function buildFlowLookups(flows) {
  const byWorkspace = new Map();
  const byVersion = new Map();
  for (const f of flows || []) {
    const id = String(f?.uuid ?? "").trim();
    const ws = String(f?.flow_workspace_uuid ?? "").trim();
    const ver = String(f?.flow_version_uuid ?? "").trim();
    if (id && ws) byWorkspace.set(ws, id);
    if (id && ver) byVersion.set(ver, id);
  }
  return { byWorkspace, byVersion };
}

/**
 * Resolve flow uuid for a LinkedIn message (best-effort).
 *
 * @param {any} msg
 * @param {{ byWorkspace: Map<string, string>; byVersion: Map<string, string> }} lookups
 * @returns {{ flowUuid: string; source: string }}
 */
function resolveFlowUuidForMessage(msg, lookups) {
  const fromPayload = String(msg?.flow_uuid ?? msg?.flowUuid ?? msg?.flow?.uuid ?? "").trim();
  if (fromPayload) return { flowUuid: fromPayload, source: "payload" };
  const taskPipeline = String(msg?.task_pipeline_uuid ?? "").trim();
  if (taskPipeline && lookups.byWorkspace.has(taskPipeline)) {
    return { flowUuid: String(lookups.byWorkspace.get(taskPipeline)), source: "task_pipeline_uuid" };
  }
  const flowVersion = String(msg?.flow_version_uuid ?? msg?.flowVersionUuid ?? "").trim();
  if (flowVersion && lookups.byVersion.has(flowVersion)) {
    return { flowUuid: String(lookups.byVersion.get(flowVersion)), source: "flow_version_uuid" };
  }
  return { flowUuid: "", source: "" };
}

/**
 * List connection request activities since a timestamp.
 *
 * @param {number} sinceMs
 * @param {number} max
 * @param {string} type
 * @returns {Promise<any[]>}
 */
async function listActivitiesSince(sinceMs, max, type) {
  const out = [];
  const limit = Math.max(1, Math.min(200, max));
  let offset = 0;
  console.log(`[GetSales Txn] activities(${type || "all"}): start since=${new Date(sinceMs).toISOString()}`);
  while (out.length < max) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(limit, max - out.length)));
    qs.set("offset", String(offset));
    qs.set("filter[created_at][>=]", new Date(sinceMs).toISOString());
    if (type) qs.set("filter[type]", type);
    const res = await getsalesFetch(`/leads/api/activities?${qs.toString()}`, { method: "GET" });
    const json = await res.json();
    if (!res.ok) throw new Error(String(json?.message || json?.error || "GetSales activities list failed"));
    const rows = Array.isArray(json?.data) ? json.data : [];
    console.log(`[GetSales Txn] activities(${type || "all"}): page offset=${offset} rows=${rows.length} total=${out.length + rows.length}`);
    if (!rows.length) break;
    out.push(...rows);
    offset += rows.length;
    const last = rows[rows.length - 1];
    const lastMs = toMs(last?.created_at) ?? toMs(last?.updated_at) ?? null;
    if (lastMs != null && lastMs < sinceMs) break;
    if (json?.has_more === false) break;
  }
  return out.slice(0, max);
}

/**
 * Compute event kinds for email row.
 *
 * @param {any} email
 * @returns {Array<{ kind: string; occurred_at_ms: number }>}
 */
function buildEmailEvents(email) {
  const out = [];
  const sentAt = toMs(email?.sent_at) ?? toMs(email?.updated_at) ?? toMs(email?.created_at) ?? null;
  if (sentAt) {
    const status = String(email?.status ?? "").toLowerCase();
    const isReply = Boolean(email?.replied_to_uuid);
    const isBounced = Boolean(email?.bounced_by_uuid) || status.includes("bounce");
    if (isReply) out.push({ kind: "email_replied", occurred_at_ms: sentAt });
    else if (isBounced) out.push({ kind: "email_bounced", occurred_at_ms: sentAt });
    else out.push({ kind: "email_sent", occurred_at_ms: sentAt });
  }
  return out;
}

/**
 * Compute event kinds for LinkedIn message row.
 *
 * @param {any} msg
 * @returns {Array<{ kind: string; occurred_at_ms: number }>}
 */
function buildLinkedinEvents(msg) {
  /**
   * Ignore InMail to match GetSales UI counts.
   *
   * @param {any} m
   * @returns {boolean}
   */
  function isInmail(m) {
    const status = String(m?.status ?? "").trim().toLowerCase();
    const text = String(m?.text ?? "").trim().toLowerCase();
    const type = String(m?.type ?? "").trim().toLowerCase();
    return status.includes("inmail") || type.includes("inmail") || text.includes("inmail");
  }

  /**
   * Extract message_hash for message-level counting.
   *
   * @param {any} m
   * @returns {string}
   */
  function getMessageHash(m) {
    return String(m?.message_hash || m?.messageHash || "").trim();
  }

  const out = [];
  const sentAt = toMs(msg?.sent_at) ?? toMs(msg?.updated_at) ?? toMs(msg?.created_at) ?? null;
  const readAt = toMs(msg?.read_at);
  const type = String(msg?.type ?? "").toLowerCase();
  const isOut = type === "outbox" || type === "outgoing" || type === "sent";
  const isIn = type === "inbox" || type === "incoming" || type === "reply";
  if (isInmail(msg)) return out;
  if (!getMessageHash(msg)) return out;
  if (sentAt) {
    if (isIn) out.push({ kind: "linkedin_message_replied", occurred_at_ms: sentAt });
    else out.push({ kind: "linkedin_message_sent", occurred_at_ms: sentAt });
  }
  if (readAt && isOut) out.push({ kind: "linkedin_message_opened", occurred_at_ms: readAt });
  return out;
}

/**
 * Compute event kinds for LinkedIn connection activity.
 *
 * @param {any} act
 * @returns {Array<{ kind: string; occurred_at_ms: number }>}
 */
function buildConnectionEvents(act) {
  const out = [];
  const at = toMs(act?.created_at) ?? toMs(act?.updated_at) ?? null;
  const type = String(act?.type ?? "").toLowerCase();
  if (!at) return out;
  if (type === "linkedin_connection_request_sent") out.push({ kind: "linkedin_connection_request_sent", occurred_at_ms: at });
  if (type === "linkedin_connection_request_accepted") out.push({ kind: "linkedin_connection_request_accepted", occurred_at_ms: at });
  return out;
}

/**
 * Main backfill logic.
 */
async function main() {
  const args = parseArgs();
  const since = String(args.since || "").trim();
  const until = String(args.until || "").trim();
  const createdBy = String(args["created-by"] || process.env.GETSALES_CRON_USER_ID || "").trim();
  const max = Math.max(1, Math.min(200000, Number(args.max ?? 50000)));
  const dryRun = String(args["dry-run"] || "").trim() === "true";
  const only = String(args.only || "linkedin").trim().toLowerCase();
  const onlyLinkedin = only === "linkedin";
  const logEvery = Math.max(1, Number(args["log-every"] ?? 500));

  if (!isYmd(since)) throw new Error("Missing or invalid --since (YYYY-MM-DD)");
  if (until && !isYmd(until)) throw new Error("Invalid --until (YYYY-MM-DD)");
  if (!createdBy) throw new Error("Missing --created-by (uuid) or GETSALES_CRON_USER_ID");

  const sinceMs = Date.parse(ymdStartIso(since));
  const untilMs = until ? Date.parse(ymdEndIso(until)) : null;

  console.log(`[GetSales Txn] since=${since} until=${until || "now"} max=${max} dryRun=${dryRun}`);

  const emails = onlyLinkedin ? [] : await listEmailsSince(sinceMs, max);
  const linkedin = await listLinkedinMessagesSince(sinceMs, max);
  const actSent = await listActivitiesSince(sinceMs, max, "linkedin_connection_request_sent");
  const actAccepted = await listActivitiesSince(sinceMs, max, "linkedin_connection_request_accepted");
  const activities = [...actSent, ...actAccepted];

  let flowLookups = { byWorkspace: new Map(), byVersion: new Map() };
  try {
    const flows = await listFlows(2000);
    flowLookups = buildFlowLookups(flows);
    console.log(`[GetSales Txn] flows: loaded ${flows.length} for enrichment`);
  } catch (e) {
    console.warn(`[GetSales Txn] flows: failed to load (${String(e?.message || e)})`);
  }

  let inserted = 0;
  let skipped = 0;
  let processed = 0;
  let lastSeenIso = "";
  const logProgress = (label) => {
    console.log(
      `[GetSales Txn] ${label} processed=${processed} inserted=${inserted} skipped=${skipped} last=${lastSeenIso || "—"}`
    );
  };

  for (const e of emails) {
    const uuid = String(e?.uuid ?? "").trim();
    const leadUuid = String(e?.lead_uuid ?? "").trim();
    const events = buildEmailEvents(e);
    for (const ev of events) {
      if (!uuid || !leadUuid || !ev?.occurred_at_ms) continue;
      if (ev.occurred_at_ms < sinceMs) continue;
      if (untilMs && ev.occurred_at_ms > untilMs) continue;
      const row = {
        source: "email",
        getsales_uuid: `${uuid}:${ev.kind}`,
        lead_uuid: leadUuid,
        contact_email: pickContactEmail(null, e) || null,
        occurred_at: new Date(ev.occurred_at_ms).toISOString(),
        payload: {
          ...e,
          event_kind: ev.kind,
          event_direction: ev.kind.includes("replied") ? "inbound" : "outbound"
        },
        created_by: createdBy
      };
      if (dryRun) {
        inserted += 1;
      } else {
        const res = await supabaseInsert("sales_getsales_events?on_conflict=created_by,source,getsales_uuid", row);
        if (Array.isArray(res) && res.length === 0) skipped += 1;
        else inserted += 1;
      }
      processed += 1;
      lastSeenIso = new Date(ev.occurred_at_ms).toISOString();
      if (processed % logEvery === 0) logProgress("email");
    }
  }

  for (const m of linkedin) {
    const uuid = String(m?.uuid ?? "").trim();
    const leadUuid = String(m?.lead_uuid ?? "").trim();
    const events = buildLinkedinEvents(m);
    const flowResolved = resolveFlowUuidForMessage(m, flowLookups);
    for (const ev of events) {
      if (!uuid || !leadUuid || !ev?.occurred_at_ms) continue;
      if (ev.occurred_at_ms < sinceMs) continue;
      if (untilMs && ev.occurred_at_ms > untilMs) continue;
      const row = {
        source: "linkedin",
        getsales_uuid: `${uuid}:${ev.kind}`,
        lead_uuid: leadUuid,
        contact_email: pickContactEmail(null, m) || null,
        occurred_at: new Date(ev.occurred_at_ms).toISOString(),
        payload: {
          ...m,
          event_kind: ev.kind,
          event_direction: ev.kind.includes("replied") ? "inbound" : "outbound",
          ...(flowResolved.flowUuid ? { flow_uuid: flowResolved.flowUuid, flow_uuid_source: flowResolved.source } : {})
        },
        created_by: createdBy
      };
      if (dryRun) {
        inserted += 1;
      } else {
        const res = await supabaseInsert("sales_getsales_events?on_conflict=created_by,source,getsales_uuid", row);
        if (Array.isArray(res) && res.length === 0) {
          skipped += 1;
          // Backfill flow_uuid into existing rows when possible.
          if (flowResolved.flowUuid) {
            await supabaseUpdate(
              `sales_getsales_events?created_by=eq.${encodeURIComponent(createdBy)}&source=eq.linkedin&getsales_uuid=eq.${encodeURIComponent(`${uuid}:${ev.kind}`)}`,
              { payload: row.payload }
            );
          }
        } else inserted += 1;
      }
      processed += 1;
      lastSeenIso = new Date(ev.occurred_at_ms).toISOString();
      if (processed % logEvery === 0) logProgress("linkedin");
    }
  }

  for (const a of activities) {
    const uuid = String(a?.id ?? "").trim();
    const leadUuid = String(a?.lead_uuid ?? "").trim();
    const events = buildConnectionEvents(a);
    for (const ev of events) {
      if (!uuid || !leadUuid || !ev?.occurred_at_ms) continue;
      if (ev.occurred_at_ms < sinceMs) continue;
      if (untilMs && ev.occurred_at_ms > untilMs) continue;
      const row = {
        source: "linkedin_connection",
        getsales_uuid: `${uuid}:${ev.kind}`,
        lead_uuid: leadUuid,
        contact_email: pickContactEmail(null, a) || null,
        occurred_at: new Date(ev.occurred_at_ms).toISOString(),
        payload: {
          ...a,
          event_kind: ev.kind,
          event_direction: ev.kind.includes("accepted") ? "inbound" : "outbound"
        },
        created_by: createdBy
      };
      if (dryRun) {
        inserted += 1;
      } else {
        const res = await supabaseInsert("sales_getsales_events?on_conflict=created_by,source,getsales_uuid", row);
        if (Array.isArray(res) && res.length === 0) skipped += 1;
        else inserted += 1;
      }
      processed += 1;
      lastSeenIso = new Date(ev.occurred_at_ms).toISOString();
      if (processed % logEvery === 0) logProgress("connections");
    }
  }

  logProgress("done");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
