import { NextResponse } from "next/server";
import { parseCsvEnv } from "@/app/lib/smartlead";
import {
  getSupabaseUserFromAuthHeader,
  getBearer,
  isCronAuthorized,
  postgrestHeadersFor,
  postgrestJson,
  type PostgrestHeaders
} from "@/app/lib/supabase-server";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function ymdUtc(ms: number) {
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function toMs(v: any): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  if (Number.isFinite(n)) return n;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? t : null;
}


async function getSmartleadSyncState(h: PostgrestHeaders, createdBy: string | null) {
  const where = createdBy ? `created_by=eq.${encodeURIComponent(createdBy)}&` : "";
  const rows = await postgrestJson(
    h,
    "GET",
    `sales_smartlead_sync_state?${where}select=created_by,last_sql_synced_at,last_completed_synced_at&limit=1`
  );
  const r = Array.isArray(rows) && rows[0] ? rows[0] : null;
  return {
    lastSql: r?.last_sql_synced_at ? String(r.last_sql_synced_at) : null,
    lastCompleted: r?.last_completed_synced_at ? String(r.last_completed_synced_at) : null
  };
}

async function upsertSmartleadSyncState(h: PostgrestHeaders, row: any) {
  await postgrestJson(h, "POST", "sales_smartlead_sync_state?on_conflict=created_by", row, {
    Prefer: "resolution=merge-duplicates,return=minimal"
  });
}

async function findEnrollment(
  h: PostgrestHeaders,
  createdBy: string | null,
  campaignId: number,
  dealId: string,
  contactId: string
) {
  const where = createdBy ? `created_by=eq.${encodeURIComponent(createdBy)}&` : "";
  const path =
    `sales_smartlead_enrollments?${where}` +
    `smartlead_campaign_id=eq.${encodeURIComponent(String(campaignId))}` +
    `&hubspot_deal_id=eq.${encodeURIComponent(String(dealId))}` +
    `&hubspot_contact_id=eq.${encodeURIComponent(String(contactId))}` +
    `&select=id,status,contact_email,smartlead_lead_map_id&limit=1`;
  const rows = await postgrestJson(h, "GET", path);
  return Array.isArray(rows) && rows[0] ? rows[0] : null;
}

async function insertEnrollmentIfNew(h: PostgrestHeaders, row: any) {
  const inserted = await postgrestJson(h, "POST", "sales_smartlead_enrollments", row, {
    Prefer: "resolution=ignore-duplicates,return=representation"
  });
  if (Array.isArray(inserted) && inserted.length === 0) return null;
  return inserted;
}

async function updateEnrollment(h: PostgrestHeaders, id: string, patch: any, createdBy: string | null) {
  const where = createdBy ? `created_by=eq.${encodeURIComponent(createdBy)}&` : "";
  await postgrestJson(
    h,
    "PATCH",
    `sales_smartlead_enrollments?${where}id=eq.${encodeURIComponent(id)}`,
    patch,
    { Prefer: "return=minimal" }
  );
}

// -----------------
// HubSpot helpers
// -----------------

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

async function hubspotSearchDealsModifiedSince(sinceMs: number, pipelineIds: string[], limit: number) {
  const properties = [
    "dealname",
    "dealstage",
    "pipeline",
    "hs_lastmodifieddate",
    String(process.env.HUBSPOT_DEAL_SMARTLEAD_CAMPAIGN_PROPERTY ?? "smartlead_campaign_id").trim() || "smartlead_campaign_id"
  ];

  const filters: any[] = [{ propertyName: "hs_lastmodifieddate", operator: "GTE", value: String(sinceMs) }];
  if (pipelineIds.length) filters.push({ propertyName: "pipeline", operator: "IN", values: pipelineIds });

  const body: any = {
    filterGroups: [{ filters }],
    sorts: [{ propertyName: "hs_lastmodifieddate", direction: "ASCENDING" }],
    properties,
    limit: Math.max(1, Math.min(200, limit))
  };

  const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/deals/search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot deals search failed"));
  return Array.isArray(json?.results) ? json.results : [];
}

async function hubspotFetchDealStageHistory(dealId: string) {
  const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}?properties=dealname,dealstage,pipeline,hs_lastmodifieddate&propertiesWithHistory=dealstage`;
  const res = await hubspotFetch(url);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot deal fetch failed"));
  const hist = json?.propertiesWithHistory?.dealstage;
  const history = Array.isArray(hist)
    ? hist
        .map((x: any) => ({ value: String(x?.value ?? ""), timestamp: toMs(x?.timestamp) }))
        .filter((x: any) => x.value && x.timestamp)
        .sort((a: any, b: any) => (a.timestamp ?? 0) - (b.timestamp ?? 0))
    : [];
  return { properties: json?.properties ?? {}, history };
}

async function hubspotFetchAssociatedContactIdsForDeal(dealId: string, limit: number) {
  const out: string[] = [];
  let after: string | null = null;
  while (out.length < limit) {
    const qs = new URLSearchParams();
    qs.set("limit", String(Math.min(100, limit - out.length)));
    if (after) qs.set("after", after);
    const url = `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}/associations/contacts?${qs.toString()}`;
    const res = await hubspotFetch(url);
    const json = (await res.json()) as any;
    if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot deal->contacts associations failed"));
    for (const r of Array.isArray(json?.results) ? json.results : []) {
      const id = String(r?.id ?? "").trim();
      if (id) out.push(id);
    }
    after = json?.paging?.next?.after ? String(json.paging.next.after) : null;
    if (!after) break;
  }
  return out;
}

async function hubspotFetchContact(contactId: string) {
  const props = ["email", "firstname", "lastname", "company", "website", "hs_linkedin_url"];
  const url = `https://api.hubapi.com/crm/v3/objects/contacts/${encodeURIComponent(contactId)}?properties=${encodeURIComponent(props.join(","))}`;
  const res = await hubspotFetch(url);
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot contact fetch failed"));
  return json;
}

async function hubspotCreateNoteForDealAndContact(dealId: string, contactId: string, atMs: number, body: string) {
  const did = Number(dealId);
  const cid = Number(contactId);
  if (!Number.isFinite(did) || !Number.isFinite(cid)) return null;
  const res = await hubspotFetch("https://api.hubapi.com/engagements/v1/engagements", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      engagement: { active: true, type: "NOTE", timestamp: atMs },
      associations: { dealIds: [did], contactIds: [cid] },
      metadata: { body }
    })
  });
  const json = (await res.json()) as any;
  if (!res.ok) throw new Error(String(json?.message || json?.error || "HubSpot engagement create failed"));
  const id = json?.engagement?.id ?? json?.id;
  return id != null ? String(id) : null;
}

// -----------------
// SmartLead helpers
// -----------------

async function smartleadFetch(path: string, init?: RequestInit) {
  const apiKey = String(process.env.SMARTLEAD_API_KEY ?? "").trim();
  if (!apiKey) throw new Error("Missing SMARTLEAD_API_KEY");
  const baseUrl = String(process.env.SMARTLEAD_BASE_URL ?? "https://server.smartlead.ai").trim().replace(/\/+$/g, "");
  const url = `${baseUrl}${path}${path.includes("?") ? "&" : "?"}api_key=${encodeURIComponent(apiKey)}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      accept: "application/json",
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

function extractLeadIdForEmail(responseJson: any, emailLower: string): string | null {
  const target = String(emailLower || "").trim().toLowerCase();
  if (!target) return null;

  const seen = new Set<any>();
  const stack: any[] = [responseJson];
  while (stack.length) {
    const cur = stack.pop();
    if (!cur || typeof cur !== "object") continue;
    if (seen.has(cur)) continue;
    seen.add(cur);

    if (!Array.isArray(cur)) {
      const v = (cur as any)[target];
      if (v != null && (typeof v === "string" || typeof v === "number")) return String(v);
      for (const [k, vv] of Object.entries(cur)) {
        if (k && typeof k === "string" && k.toLowerCase() === target && (typeof vv === "string" || typeof vv === "number")) return String(vv);
      }
    }

    if (Array.isArray(cur)) for (const x of cur) stack.push(x);
    else for (const v of Object.values(cur)) stack.push(v);
  }
  return null;
}

async function smartleadAddLeadToCampaign(campaignId: number, lead: any, settings: any) {
  return await smartleadFetch(`/api/v1/campaigns/${encodeURIComponent(String(campaignId))}/leads`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lead_list: [lead], settings })
  });
}

async function smartleadListLeadsByCampaign(
  campaignId: number,
  opts: { limit?: number; offset?: number; status?: string; event_time_gt?: string; created_at_gt?: string; last_sent_time_gt?: string }
) {
  const qs = new URLSearchParams();
  if (opts.limit != null) qs.set("limit", String(opts.limit));
  if (opts.offset != null) qs.set("offset", String(opts.offset));
  if (opts.status) qs.set("status", String(opts.status));
  if (opts.event_time_gt) qs.set("event_time_gt", String(opts.event_time_gt));
  if (opts.created_at_gt) qs.set("created_at_gt", String(opts.created_at_gt));
  if (opts.last_sent_time_gt) qs.set("last_sent_time_gt", String(opts.last_sent_time_gt));
  const q = qs.toString();
  return await smartleadFetch(`/api/v1/campaigns/${encodeURIComponent(String(campaignId))}/leads${q ? `?${q}` : ""}`, { method: "GET" });
}

async function smartleadGetSequenceDetails(leadMapId: string) {
  const id = String(leadMapId || "").trim();
  if (!id) return null;
  return await smartleadFetch(`/api/v1/leads/${encodeURIComponent(id)}/sequence-details`, { method: "GET" });
}

function pickArrayFromSmartleadListResponse(json: any): any[] {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json?.data)) return json.data;
  if (Array.isArray(json?.results)) return json.results;
  if (Array.isArray(json?.leads)) return json.leads;
  return [];
}

function stageCategoryFromLabel(labelOrId: string) {
  const t = String(labelOrId ?? "").trim().toLowerCase();
  if (!t) return "unknown";
  if (t.includes("sql")) return "sql";
  return "unknown";
}

export async function POST(req: Request) {
  try {
    const syncSecret = String(process.env.SMARTLEAD_SYNC_SECRET ?? "").trim();
    const cronSecret = String(process.env.CRON_SECRET ?? "").trim();
    const { authHeader, bearer, gotSecret } = getBearer(req);

    const vercelCron = String(req.headers.get("x-vercel-cron") ?? "").trim();
    const isCron = vercelCron === "1" || vercelCron.toLowerCase() === "true" || (!!cronSecret && bearer === cronSecret);
    const secretOk = (!!syncSecret && (gotSecret === syncSecret || bearer === syncSecret)) || isCron;
    const createdBy = isCron ? (String(process.env.SMARTLEAD_CRON_USER_ID ?? "").trim() || null) : null;

    if (gotSecret) {
      if (!syncSecret) return jsonError(500, "SMARTLEAD_SYNC_SECRET is not configured in env");
      if (gotSecret !== syncSecret) return jsonError(403, "Bad x-sync-secret");
    }

    if (!secretOk) {
      const user = await getSupabaseUserFromAuthHeader(authHeader);
      if (!user?.email) return jsonError(401, "Not authorized");
      const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
      const email = String(user.email || "").toLowerCase();
      if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");
    }

    if (isCron && !createdBy) return jsonError(400, "Missing SMARTLEAD_CRON_USER_ID (required for cron SmartLead sync)");

    const payload = (await req.json().catch(() => ({}))) as {
      since_sql?: string;
      since_completed?: string;
      max_deals?: number;
      max_contacts_per_deal?: number;
      max_completed_leads?: number;
      retry_failed?: boolean;
      dry_run?: boolean;
      pipeline_ids?: string[];
    };
    const dryRun = !!payload?.dry_run;
    const retryFailed = !!payload?.retry_failed;
    const maxDeals = Math.max(1, Math.min(300, Number(payload?.max_deals ?? 120)));
    const maxContactsPerDeal = Math.max(1, Math.min(50, Number(payload?.max_contacts_per_deal ?? 10)));
    const maxCompletedLeads = Math.max(1, Math.min(1000, Number(payload?.max_completed_leads ?? 200)));

    const pg = postgrestHeadersFor(authHeader, isCron);
    const state = await getSmartleadSyncState(pg, createdBy);

    const nowMs = Date.now();
    const defaultSinceIso = new Date(nowMs - 7 * 24 * 60 * 60 * 1000).toISOString();
    const sinceSqlIso = String(payload?.since_sql || state.lastSql || defaultSinceIso).trim();
    const sinceCompletedIso = String(payload?.since_completed || state.lastCompleted || defaultSinceIso).trim();
    const sinceSqlMs = toMs(sinceSqlIso) ?? Date.parse(sinceSqlIso);
    const sinceCompletedMs = toMs(sinceCompletedIso) ?? Date.parse(sinceCompletedIso);
    if (!Number.isFinite(sinceSqlMs)) return jsonError(400, "Bad request: invalid since_sql");
    if (!Number.isFinite(sinceCompletedMs)) return jsonError(400, "Bad request: invalid since_completed");

    const pipelineIds =
      Array.isArray(payload?.pipeline_ids) && payload.pipeline_ids.length
        ? payload.pipeline_ids.map(String).filter(Boolean)
        : parseCsvEnv("HUBSPOT_FUNNEL_PIPELINE_IDS");

    const sqlStageIds = new Set(parseCsvEnv("HUBSPOT_SQL_STAGE_IDS").map(String));
    const dealCampaignProp = String(process.env.HUBSPOT_DEAL_SMARTLEAD_CAMPAIGN_PROPERTY ?? "smartlead_campaign_id").trim() || "smartlead_campaign_id";
    const defaultCampaignIdRaw = String(process.env.SMARTLEAD_DEFAULT_CAMPAIGN_ID ?? "").trim();
    const defaultCampaignId = defaultCampaignIdRaw ? Number(defaultCampaignIdRaw) : NaN;

    const results: any = {
      ok: true,
      dry_run: dryRun,
      mode: isCron ? "cron" : secretOk ? "secret" : "user",
      cursors: { since_sql: sinceSqlIso, since_completed: sinceCompletedIso },
      enrolled: { scanned_deals: 0, sql_deals: 0, contacts_scanned: 0, enrolled: 0, skipped_existing: 0, skipped_no_email: 0, skipped_no_campaign: 0, failed: 0 },
      completed: { campaigns_scanned: 0, leads_scanned: 0, completed_matched: 0, notes_created: 0, skipped_unmatched: 0, failed: 0 }
    };

    // -----------------
    // (A) HubSpot SQL -> SmartLead enroll
    // -----------------
    const stageLabelById = await hubspotFetchDealStageLabels();

    // If pipelines are not configured, it's too risky to scan entire portal.
    if (!pipelineIds.length) {
      results.enrolled.skipped = true;
      results.enrolled.reason = "Missing HUBSPOT_FUNNEL_PIPELINE_IDS (configure pipeline filter for SQL trigger)";
    } else {
      const deals = await hubspotSearchDealsModifiedSince(sinceSqlMs, pipelineIds, maxDeals);
      results.enrolled.scanned_deals = deals.length;

      let maxSqlTs = sinceSqlMs;
      for (const d of deals) {
        const dealId = String(d?.id ?? "").trim();
        if (!dealId) continue;
        const hist = await hubspotFetchDealStageHistory(dealId);
        const props = hist.properties ?? {};
        const history = Array.isArray(hist.history) ? hist.history : [];

        // Find SQL transition(s) since sinceSqlMs
        let sqlEnteredAtMs: number | null = null;
        for (let i = 0; i < history.length; i++) {
          const cur = history[i];
          const ts = Number(cur?.timestamp ?? 0);
          if (!ts || ts < sinceSqlMs) continue;
          const stageId = String(cur?.value ?? "").trim();
          const label = stageLabelById.get(stageId) ?? stageId;
          const isSql = (sqlStageIds.size > 0 && sqlStageIds.has(stageId)) || stageCategoryFromLabel(label) === "sql";
          if (isSql) {
            sqlEnteredAtMs = ts;
            break;
          }
        }
        if (!sqlEnteredAtMs) continue;
        results.enrolled.sql_deals++;
        if (sqlEnteredAtMs > maxSqlTs) maxSqlTs = sqlEnteredAtMs;

        // campaign id: deal property (preferred) or env default
        const dealCampaignRaw = String((props as any)?.[dealCampaignProp] ?? "").trim();
        const dealCampaignId = dealCampaignRaw ? Number(dealCampaignRaw) : NaN;
        const campaignId = Number.isFinite(dealCampaignId) ? dealCampaignId : defaultCampaignId;
        if (!Number.isFinite(campaignId)) {
          results.enrolled.skipped_no_campaign++;
          continue;
        }

        const contactIds = await hubspotFetchAssociatedContactIdsForDeal(dealId, maxContactsPerDeal);
        for (const contactId of contactIds) {
          results.enrolled.contacts_scanned++;
          const existing = await findEnrollment(pg, createdBy, campaignId, dealId, contactId);
          if (existing && String(existing.status || "").toLowerCase() !== "failed") {
            results.enrolled.skipped_existing++;
            continue;
          }
          if (existing && String(existing.status || "").toLowerCase() === "failed" && !retryFailed) {
            results.enrolled.skipped_existing++;
            continue;
          }

          const contact = await hubspotFetchContact(contactId);
          const cprops = contact?.properties ?? {};
          const email = String(cprops?.email ?? "").trim().toLowerCase();
          if (!email) {
            results.enrolled.skipped_no_email++;
            continue;
          }

          const rowBase = {
            smartlead_campaign_id: campaignId,
            hubspot_deal_id: dealId,
            hubspot_contact_id: contactId,
            contact_email: email,
            sql_entered_at: new Date(sqlEnteredAtMs).toISOString(),
            status: existing ? "failed" : "enrolled", // will be overwritten below
            created_by: createdBy ?? undefined
          };

          let rowId: string | null = existing?.id ? String(existing.id) : null;
          if (!rowId) {
            const inserted = await insertEnrollmentIfNew(pg, { ...rowBase, status: "enrolled" });
            if (!inserted) {
              results.enrolled.skipped_existing++;
              continue;
            }
            rowId = String(Array.isArray(inserted) ? inserted[0]?.id : inserted?.id ?? "");
          }
          if (!rowId) {
            results.enrolled.failed++;
            continue;
          }

          if (dryRun) {
            // Mark as skipped in dry run so next real run still enrolls (manual cleanup may be needed).
            await updateEnrollment(pg, rowId, { status: "skipped", error: "dry_run", enrolled_at: null }, createdBy);
            continue;
          }

          try {
            const leadPayload = {
              first_name: String(cprops?.firstname ?? "").trim() || undefined,
              last_name: String(cprops?.lastname ?? "").trim() || undefined,
              email,
              company_name: String(cprops?.company ?? "").trim() || undefined,
              website: String(cprops?.website ?? "").trim() || undefined,
              linkedin_profile: String(cprops?.hs_linkedin_url ?? "").trim() || undefined,
              custom_fields: {
                HubSpotDealId: dealId,
                HubSpotContactId: contactId,
                HubSpotPipelineId: String(props?.pipeline ?? ""),
                HubSpotDealName: String(props?.dealname ?? "")
              }
            };

            const settings = {
              ignore_global_block_list: true,
              ignore_unsubscribe_list: true,
              ignore_community_bounce_list: true,
              ignore_duplicate_leads_in_other_campaign: String(process.env.SMARTLEAD_IGNORE_DUPLICATE_LEADS_IN_OTHER_CAMPAIGN ?? "true").trim().toLowerCase() !==
                "false",
              return_lead_ids: true
            };

            const resp = await smartleadAddLeadToCampaign(campaignId, leadPayload, settings);
            const leadMapId = extractLeadIdForEmail(resp, email);
            await updateEnrollment(
              pg,
              rowId,
              {
                status: "enrolled",
                enrolled_at: new Date().toISOString(),
                smartlead_lead_map_id: leadMapId,
                error: null,
                raw_enroll_response: resp
              },
              createdBy
            );
            results.enrolled.enrolled++;
          } catch (e: any) {
            await updateEnrollment(
              pg,
              rowId,
              { status: "failed", error: String(e?.message || e), raw_enroll_response: { error: String(e?.message || e) } },
              createdBy
            );
            results.enrolled.failed++;
          }
        }
      }

      if (!dryRun && maxSqlTs > sinceSqlMs) {
        await upsertSmartleadSyncState(pg, {
          created_by: createdBy ?? undefined,
          last_sql_synced_at: new Date(maxSqlTs).toISOString()
        });
      }
    }

    // -----------------
    // (B) SmartLead COMPLETED -> HubSpot notes
    // -----------------
    const campaignIdsFromEnv = parseCsvEnv("SMARTLEAD_CAMPAIGN_IDS")
      .map((x) => Number(x))
      .filter((n) => Number.isFinite(n)) as number[];
    const campaignIdsToScan = new Set<number>();
    for (const id of campaignIdsFromEnv) campaignIdsToScan.add(id);
    if (Number.isFinite(defaultCampaignId)) campaignIdsToScan.add(defaultCampaignId);

    // As a fallback, infer campaign ids from recent enrollments.
    if (!campaignIdsToScan.size) {
      const where = createdBy ? `created_by=eq.${encodeURIComponent(createdBy)}&` : "";
      const rows = await postgrestJson(
        pg,
        "GET",
        `sales_smartlead_enrollments?${where}select=smartlead_campaign_id&order=updated_at.desc&limit=200`
      );
      for (const r of Array.isArray(rows) ? rows : []) {
        const n = Number((r as any)?.smartlead_campaign_id);
        if (Number.isFinite(n)) campaignIdsToScan.add(n);
      }
    }

    const completedSinceYmd = ymdUtc(sinceCompletedMs);
    let maxCompletedTs = sinceCompletedMs;

    for (const campaignId of Array.from(campaignIdsToScan.values())) {
      results.completed.campaigns_scanned++;
      let offset = 0;
      const limit = 100;
      while (results.completed.leads_scanned < maxCompletedLeads) {
        const listJson = await smartleadListLeadsByCampaign(campaignId, {
          limit,
          offset,
          status: "COMPLETED",
          event_time_gt: completedSinceYmd
        });
        const leads = pickArrayFromSmartleadListResponse(listJson);
        if (!leads.length) break;

        for (const lead of leads) {
          results.completed.leads_scanned++;
          if (results.completed.leads_scanned > maxCompletedLeads) break;

          const email = String((lead as any)?.email ?? (lead as any)?.email_id ?? (lead as any)?.emailId ?? "").trim().toLowerCase();
          const leadMapId = String(
            (lead as any)?.leadMapId ?? (lead as any)?.lead_map_id ?? (lead as any)?.lead_mapid ?? (lead as any)?.id ?? ""
          ).trim();

          const eventAtMs = toMs((lead as any)?.event_time ?? (lead as any)?.completed_at ?? (lead as any)?.updated_at ?? null) ?? null;
          if (eventAtMs != null && eventAtMs > maxCompletedTs) maxCompletedTs = eventAtMs;

          if (!email) {
            results.completed.skipped_unmatched++;
            continue;
          }

          const where = createdBy ? `created_by=eq.${encodeURIComponent(createdBy)}&` : "";
          const rows = await postgrestJson(
            pg,
            "GET",
            `sales_smartlead_enrollments?${where}smartlead_campaign_id=eq.${encodeURIComponent(
              String(campaignId)
            )}&contact_email=eq.${encodeURIComponent(email)}&select=id,status,hubspot_deal_id,hubspot_contact_id,smartlead_lead_map_id,hubspot_engagement_id&limit=5`
          );
          const r0 = Array.isArray(rows) && rows[0] ? rows[0] : null;
          if (!r0) {
            results.completed.skipped_unmatched++;
            continue;
          }
          results.completed.completed_matched++;

          const curStatus = String(r0?.status ?? "").toLowerCase();
          if (curStatus === "completed" && r0?.hubspot_engagement_id) continue;

          const enrollmentId = String(r0.id);
          const dealId = String(r0.hubspot_deal_id ?? "").trim();
          const contactId = String(r0.hubspot_contact_id ?? "").trim();
          const effectiveLeadMapId = String(r0.smartlead_lead_map_id ?? leadMapId ?? "").trim();

          try {
            const seqDetails = effectiveLeadMapId ? await smartleadGetSequenceDetails(effectiveLeadMapId) : null;

            const lines: string[] = [];
            lines.push("[SmartLead] Sequence completed");
            lines.push(`Campaign ID: ${campaignId}`);
            if (effectiveLeadMapId) lines.push(`LeadMapId: ${effectiveLeadMapId}`);
            lines.push(`Lead email: ${email}`);
            lines.push(`Captured at: ${new Date().toISOString()}`);
            lines.push("");
            if (seqDetails != null) {
              lines.push("Sequence details (raw JSON):");
              lines.push(JSON.stringify(seqDetails, null, 2));
              lines.push("");
            }
            lines.push("Lead (raw JSON):");
            lines.push(JSON.stringify(lead, null, 2));
            const noteBody = lines.join("\n");

            if (dryRun) {
              await updateEnrollment(
                pg,
                enrollmentId,
                { raw_completed_payload: lead, raw_sequence_details: seqDetails ?? {}, status: "completed", completed_at: new Date().toISOString() },
                createdBy
              );
              continue;
            }

            const engagementId = await hubspotCreateNoteForDealAndContact(dealId, contactId, Date.now(), noteBody);
            await updateEnrollment(
              pg,
              enrollmentId,
              {
                status: "completed",
                completed_at: new Date().toISOString(),
                hubspot_engagement_id: engagementId ? Number(engagementId) : null,
                raw_completed_payload: lead,
                raw_sequence_details: seqDetails ?? {}
              },
              createdBy
            );
            results.completed.notes_created++;
          } catch (e: any) {
            await updateEnrollment(pg, enrollmentId, { error: String(e?.message || e), raw_completed_payload: lead }, createdBy);
            results.completed.failed++;
          }
        }

        if (leads.length < limit) break;
        offset += leads.length;
        if (offset > 10_000) break;
      }
    }

    if (!dryRun && maxCompletedTs > sinceCompletedMs) {
      await upsertSmartleadSyncState(pg, {
        created_by: createdBy ?? undefined,
        last_completed_synced_at: new Date(maxCompletedTs).toISOString()
      });
    }

    return NextResponse.json(results);
  } catch (e: any) {
    return jsonError(500, String(e?.message || e));
  }
}


