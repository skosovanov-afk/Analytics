// Seed demo data for Sales portal (2 hypotheses + 3 weekly check-ins each).
//
// Usage:
//   node 99-applications/sales/tools/seed-demo.mjs
//
// Auth:
// - Uses 02-calls/_private_cache/auth.json by default (same Supabase project) or SALES_AUTH_FILE/CALLS_AUTH_FILE.
//
// Idempotency:
// - Deletes existing hypotheses with title starting "[DEMO]" (owned by current user), then recreates.
// - Channels/Metrics are upserted by slug (demo slugs are prefixed "demo_").
//
// NOTE: This is meant for testing UI in a sandbox/prod Supabase project. Remove demo rows manually if needed.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadAuth, refreshIfNeeded, supabaseHeaders, getDefaultAuthFile } from "../../calls/tools/supabase-auth.mjs";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function must(v, msg) {
  if (!v) throw new Error(msg);
  return v;
}

function todayISO() {
  return new Date().toISOString().slice(0, 10);
}

function startOfWeekISO(d = new Date()) {
  const x = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
  // Monday start (ISO). JS: 0=Sun..6=Sat. Convert to 0..6 where 0=Mon.
  const day = x.getUTCDay();
  const diff = (day + 6) % 7;
  x.setUTCDate(x.getUTCDate() - diff);
  return x.toISOString().slice(0, 10);
}

function addDaysISO(isoDate, days) {
  const d = new Date(`${isoDate}T00:00:00.000Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function toNum(v) {
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : Number(String(v).trim());
  return Number.isFinite(n) ? n : null;
}

async function rest(auth, { method, table, qs = "", body, prefer }) {
  const url = `${auth.supabase_url}/rest/v1/${table}${qs ? `?${qs}` : ""}`;
  const res = await fetch(url, {
    method,
    headers: {
      ...supabaseHeaders(auth),
      ...(prefer ? { Prefer: prefer } : {})
    },
    body: body == null ? undefined : JSON.stringify(body)
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${table} failed: ${res.status} ${text}`);
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function restSelect(auth, table, select, filters = {}) {
  const params = new URLSearchParams();
  params.set("select", select);
  for (const [k, v] of Object.entries(filters)) params.set(k, v);
  return await rest(auth, { method: "GET", table, qs: params.toString() });
}

async function restUpsertOne(auth, table, row, onConflict, select = "*") {
  const params = new URLSearchParams();
  params.set("on_conflict", onConflict);
  params.set("select", select);
  const data = await rest(auth, {
    method: "POST",
    table,
    qs: params.toString(),
    body: row,
    prefer: "resolution=merge-duplicates,return=representation"
  });
  if (!Array.isArray(data) || !data[0]) throw new Error(`Upsert ${table} returned empty`);
  return data[0];
}

async function restInsertOne(auth, table, row, select = "*") {
  const params = new URLSearchParams();
  params.set("select", select);
  const data = await rest(auth, {
    method: "POST",
    table,
    qs: params.toString(),
    body: row,
    prefer: "return=representation"
  });
  if (!Array.isArray(data) || !data[0]) throw new Error(`Insert ${table} returned empty`);
  return data[0];
}

async function restInsertMany(auth, table, rows) {
  if (!rows.length) return { ok: true, inserted: 0 };
  await rest(auth, { method: "POST", table, body: rows, prefer: "return=minimal" });
  return { ok: true, inserted: rows.length };
}

async function getCurrentUser(auth) {
  const res = await fetch(`${auth.supabase_url}/auth/v1/user`, {
    method: "GET",
    headers: {
      apikey: auth.supabase_anon_key,
      authorization: `Bearer ${auth.access_token}`
    }
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`GET auth user failed: ${res.status} ${text}`);
  const json = JSON.parse(text);
  return {
    user_id: String(json.id),
    email: String(json.email ?? "")
  };
}

async function getOrCreateRole(auth, name) {
  const existing = await restSelect(auth, "sales_icp_roles", "id,name", { "name": `eq.${name}` });
  if (Array.isArray(existing) && existing[0]) return existing[0];
  return await restInsertOne(auth, "sales_icp_roles", { name, decision_roles: ["DecisionMaker"], seniority: "Senior", titles: [name] }, "id,name");
}

async function getOrCreateCompany(auth, row) {
  // Best-effort de-dup by (vertical_name, sub_vertical, region, size_bucket)
  const filters = {
    "vertical_name": row.vertical_name ? `eq.${row.vertical_name}` : "is.null",
    "sub_vertical": row.sub_vertical ? `eq.${row.sub_vertical}` : "is.null",
    "region": row.region ? `eq.${row.region}` : "is.null",
    "size_bucket": row.size_bucket ? `eq.${row.size_bucket}` : "is.null"
  };
  const existing = await restSelect(auth, "sales_icp_company_profiles", "id,vertical_name,sub_vertical,region,size_bucket", filters);
  if (Array.isArray(existing) && existing[0]) return existing[0];
  return await restInsertOne(auth, "sales_icp_company_profiles", row, "id,vertical_name,sub_vertical,region,size_bucket");
}

async function upsertChannel(auth, { slug, name, sort_order = 0 }) {
  return await restUpsertOne(auth, "sales_channels", { slug, name, sort_order, is_active: true }, "slug", "id,slug,name");
}

async function upsertMetric(auth, { slug, name, input_type = "number", unit = "count", sort_order = 0 }) {
  return await restUpsertOne(
    auth,
    "sales_metrics",
    { slug, name, input_type, unit, sort_order, is_active: true },
    "slug",
    "id,slug,name,unit,input_type"
  );
}

async function deleteDemoHypotheses(auth) {
  // Delete only hypotheses owned by the current user (owner_user_id = auth.uid by RLS anyway, but keep safe).
  const hyps = await restSelect(auth, "sales_hypotheses", "id,title", { "title": "ilike.[DEMO]%" });
  const ids = (Array.isArray(hyps) ? hyps : []).map((h) => String(h.id));
  if (!ids.length) return { deleted: 0 };

  // PostgREST doesn't support IN delete with multiple ids easily via URLSearchParams
  // (it does via `id=in.(a,b)`).
  const qs = new URLSearchParams();
  qs.set("id", `in.(${ids.join(",")})`);
  await rest(auth, { method: "DELETE", table: "sales_hypotheses", qs: qs.toString() });
  return { deleted: ids.length };
}

async function createHypothesis(auth, userEmail, hyp) {
  const created = await restInsertOne(
    auth,
    "sales_hypotheses",
    {
      title: hyp.title,
      status: hyp.status ?? "active",
      priority: hyp.priority ?? 50,
      timebox_days: hyp.timebox_days ?? 28,
      win_criteria: hyp.win_criteria,
      kill_criteria: hyp.kill_criteria,
      vertical_name: hyp.vertical_name ?? "Demo vertical",
      vertical_hubspot_url: hyp.vertical_hubspot_url ?? null,
      hubspot_deals_view_url: hyp.hubspot_deals_view_url ?? null,
      hubspot_tal_url: hyp.hubspot_tal_url ?? null,
      opps_in_progress_count: hyp.opps_in_progress_count ?? 0,
      tal_companies_count_baseline: hyp.tal_companies_count_baseline ?? 50,
      contacts_count_baseline: hyp.contacts_count_baseline ?? 250,
      one_sentence_pitch: hyp.one_sentence_pitch ?? null,
      product_description: hyp.product_description ?? null,
      owner_email: userEmail || null,
      cjm_json: { channels: hyp.channels }
    },
    "id,title"
  );
  return created;
}

async function seed() {
  const authFile = process.env.SALES_AUTH_FILE || process.env.CALLS_AUTH_FILE || getDefaultAuthFile();
  must(fs.existsSync(authFile), `Missing auth file: ${authFile}`);
  let auth = loadAuth(authFile);
  auth = await refreshIfNeeded(authFile, auth);

  const me = await getCurrentUser(auth);
  const email = String(me.email || "").toLowerCase();
  if (!email) throw new Error("Current user email is missing (auth.jwt email).");

  console.log(`[seed-demo] user: ${email}`);
  console.log(`[seed-demo] today: ${todayISO()}`);

  const del = await deleteDemoHypotheses(auth);
  if (del.deleted) console.log(`[seed-demo] deleted old demo hypotheses: ${del.deleted}`);

  // Channels
  const channels = await Promise.all([
    upsertChannel(auth, { slug: "demo_outbound_email", name: "Demo: Outbound email", sort_order: 10 }),
    upsertChannel(auth, { slug: "demo_linkedin_outbound", name: "Demo: LinkedIn outbound", sort_order: 20 }),
    upsertChannel(auth, { slug: "demo_partnerships", name: "Demo: Partnerships", sort_order: 30 })
  ]);
  const channelBySlug = new Map(channels.map((c) => [String(c.slug), c]));

  // Metrics (library)
  const metrics = await Promise.all([
    upsertMetric(auth, { slug: "demo_calls_held", name: "Demo: Calls held", unit: "count", sort_order: 10 }),
    upsertMetric(auth, { slug: "demo_opps_created", name: "Demo: Opps created", unit: "count", sort_order: 20 }),
    upsertMetric(auth, { slug: "demo_messages_sent", name: "Demo: Messages sent", unit: "count", sort_order: 30 }),
    upsertMetric(auth, { slug: "demo_replies", name: "Demo: Replies", unit: "count", sort_order: 40 }),
    upsertMetric(auth, { slug: "demo_reply_rate_pct", name: "Demo: Reply rate", unit: "%", sort_order: 50 }),
    upsertMetric(auth, { slug: "demo_meetings_booked", name: "Demo: Meetings booked", unit: "count", sort_order: 60 })
  ]);
  const metricBySlug = new Map(metrics.map((m) => [String(m.slug), m]));

  // ICP library (minimal)
  const roleCiso = await getOrCreateRole(auth, "CISO");
  const roleSecEng = await getOrCreateRole(auth, "Security Engineer");

  const coFintech = await getOrCreateCompany(auth, {
    vertical_name: "Fintech",
    sub_vertical: "Payments",
    region: "US/EU",
    size_bucket: "Series B+",
    tech_stack: ["Kotlin", "Swift"],
    constraints_json: { compliance: ["SOC2"], mobile_first: true }
  });
  const coConsumer = await getOrCreateCompany(auth, {
    vertical_name: "Consumer apps",
    sub_vertical: "Social",
    region: "Global",
    size_bucket: "10M+ MAU",
    tech_stack: ["React Native", "Kotlin"],
    constraints_json: { high_release_velocity: true }
  });

  // Hypotheses (2)
  const hyp1 = await createHypothesis(auth, email, {
    title: "[DEMO] Fintech CISO via outbound email",
    status: "active",
    priority: 90,
    timebox_days: 28,
    win_criteria: ">= 3 opps created in HubSpot from outbound sequence",
    kill_criteria: "< 1 opp created OR reply rate < 2% after 4 weeks",
    vertical_name: "Fintech (Payments)",
    one_sentence_pitch: "CISOs in fintech respond when we lead with mobile attack surface + compliance risk.",
    product_description: "Demo hypothesis for UI validation. Numbers are synthetic.",
    opps_in_progress_count: 2,
    tal_companies_count_baseline: 60,
    contacts_count_baseline: 300,
    channels: ["demo_outbound_email", "demo_linkedin_outbound"]
  });

  const hyp2 = await createHypothesis(auth, email, {
    title: "[DEMO] Consumer apps Security Eng via LinkedIn",
    status: "active",
    priority: 70,
    timebox_days: 21,
    win_criteria: ">= 2 opps created from LinkedIn outbound within 3 weeks",
    kill_criteria: "No meetings booked after 3 weeks OR conversion < 3%",
    vertical_name: "Consumer apps (Social)",
    one_sentence_pitch: "Security Engineers engage when we show fast 'developer-first' remediation on mobile findings.",
    product_description: "Demo hypothesis for UI validation. Numbers are synthetic.",
    opps_in_progress_count: 1,
    tal_companies_count_baseline: 80,
    contacts_count_baseline: 420,
    channels: ["demo_linkedin_outbound", "demo_partnerships"]
  });

  const hypothesisIds = [String(hyp1.id), String(hyp2.id)];

  // Link Roles/Company Profiles to hypotheses (for VP matrix on hypothesis page)
  await restInsertMany(auth, "sales_hypothesis_roles", [
    { hypothesis_id: hyp1.id, role_id: roleCiso.id },
    { hypothesis_id: hyp1.id, role_id: roleSecEng.id },
    { hypothesis_id: hyp2.id, role_id: roleSecEng.id },
    { hypothesis_id: hyp2.id, role_id: roleCiso.id }
  ]);
  await restInsertMany(auth, "sales_hypothesis_company_profiles", [
    { hypothesis_id: hyp1.id, company_profile_id: coFintech.id },
    { hypothesis_id: hyp1.id, company_profile_id: coConsumer.id },
    { hypothesis_id: hyp2.id, company_profile_id: coConsumer.id },
    { hypothesis_id: hyp2.id, company_profile_id: coFintech.id }
  ]);

  // Seed VP statements per hypothesis intersection (Role x Company)
  const vp = (s) => ({ value_proposition: s });
  await restInsertMany(auth, "sales_hypothesis_vps", [
    { hypothesis_id: hyp1.id, role_id: roleCiso.id, company_profile_id: coFintech.id, vp_json: vp("Reduce mobile breach + compliance risk with clear, board-ready evidence in weeks.") },
    { hypothesis_id: hyp1.id, role_id: roleSecEng.id, company_profile_id: coFintech.id, vp_json: vp("Get actionable mobile findings with repro steps and CI-friendly fixes, not noise.") },
    { hypothesis_id: hyp1.id, role_id: roleCiso.id, company_profile_id: coConsumer.id, vp_json: vp("Lower brand-impacting mobile incidents with continuous validation before releases ship.") },
    { hypothesis_id: hyp1.id, role_id: roleSecEng.id, company_profile_id: coConsumer.id, vp_json: vp("Catch mobile issues early with fast feedback loops that devs actually adopt.") },

    { hypothesis_id: hyp2.id, role_id: roleSecEng.id, company_profile_id: coConsumer.id, vp_json: vp("Developer-first mobile security with minimal friction and fast remediation.") },
    { hypothesis_id: hyp2.id, role_id: roleCiso.id, company_profile_id: coConsumer.id, vp_json: vp("Reduce mobile security program risk without slowing releases.") },
    { hypothesis_id: hyp2.id, role_id: roleSecEng.id, company_profile_id: coFintech.id, vp_json: vp("Ship compliance-safe mobile releases with fewer regressions and clear ownership.") },
    { hypothesis_id: hyp2.id, role_id: roleCiso.id, company_profile_id: coFintech.id, vp_json: vp("Turn mobile security into measurable risk reduction tied to audit/compliance goals.") }
  ]);

  // Hypothesis metrics (hypothesis-wide)
  await restInsertMany(auth, "sales_hypothesis_metrics", [
    { hypothesis_id: hyp1.id, metric_id: metricBySlug.get("demo_opps_created").id },
    { hypothesis_id: hyp1.id, metric_id: metricBySlug.get("demo_reply_rate_pct").id },
    { hypothesis_id: hyp2.id, metric_id: metricBySlug.get("demo_meetings_booked").id },
    { hypothesis_id: hyp2.id, metric_id: metricBySlug.get("demo_opps_created").id }
  ]);

  // Channel metrics per hypothesis
  const cm = [];
  function addCM(hid, channelSlug, metricSlug) {
    cm.push({
      hypothesis_id: hid,
      channel_id: channelBySlug.get(channelSlug).id,
      metric_id: metricBySlug.get(metricSlug).id
    });
  }
  // hyp1 channels
  addCM(hyp1.id, "demo_outbound_email", "demo_messages_sent");
  addCM(hyp1.id, "demo_outbound_email", "demo_replies");
  addCM(hyp1.id, "demo_outbound_email", "demo_reply_rate_pct");
  addCM(hyp1.id, "demo_outbound_email", "demo_calls_held");
  addCM(hyp1.id, "demo_outbound_email", "demo_opps_created");
  addCM(hyp1.id, "demo_linkedin_outbound", "demo_messages_sent");
  addCM(hyp1.id, "demo_linkedin_outbound", "demo_replies");
  addCM(hyp1.id, "demo_linkedin_outbound", "demo_meetings_booked");

  // hyp2 channels
  addCM(hyp2.id, "demo_linkedin_outbound", "demo_messages_sent");
  addCM(hyp2.id, "demo_linkedin_outbound", "demo_replies");
  addCM(hyp2.id, "demo_linkedin_outbound", "demo_meetings_booked");
  addCM(hyp2.id, "demo_partnerships", "demo_meetings_booked");
  addCM(hyp2.id, "demo_partnerships", "demo_opps_created");
  addCM(hyp2.id, "demo_partnerships", "demo_calls_held");

  await restInsertMany(auth, "sales_hypothesis_channel_metrics", cm);

  // Owners (so /checkins/new wizard sees the hypotheses and allows editing)
  const ownerRows = [];
  const metricOwnerRows = [];
  function addOwnersForHyp(hyp, channelSlugs) {
    for (const ch of channelSlugs) {
      const channelId = channelBySlug.get(ch).id;
      ownerRows.push({ hypothesis_id: hyp.id, channel_id: channelId, owner_email: email });
      for (const m of metrics) {
        // only assign metric-owner if that metric is actually linked to this hypothesis+channel
        const exists = cm.find((x) => x.hypothesis_id === hyp.id && x.channel_id === channelId && x.metric_id === m.id);
        if (exists) metricOwnerRows.push({ hypothesis_id: hyp.id, channel_id: channelId, metric_id: m.id, owner_email: email });
      }
    }
  }
  addOwnersForHyp(hyp1, hyp1.cjm_json?.channels ?? ["demo_outbound_email", "demo_linkedin_outbound"]);
  addOwnersForHyp(hyp2, hyp2.cjm_json?.channels ?? ["demo_linkedin_outbound", "demo_partnerships"]);

  await restInsertMany(auth, "sales_hypothesis_channel_owners", ownerRows);
  await restInsertMany(auth, "sales_hypothesis_channel_metric_owners", metricOwnerRows);

  // Check-ins (3 per hypothesis)
  const w0 = startOfWeekISO(new Date());
  const w1 = addDaysISO(w0, -7);
  const w2 = addDaysISO(w0, -14);
  const weeks = [w2, w1, w0];

  function perChannel(chSlug, data) {
    return {
      activity: data.activity,
      results: data.results,
      metrics: data.metrics
    };
  }

  const checkinRows = [];

  function addCheckinsForHyp(hyp, spec) {
    for (let i = 0; i < weeks.length; i++) {
      const wk = weeks[i];
      const s = spec[i];
      checkinRows.push({
        hypothesis_id: hyp.id,
        week_start: wk,
        opps_in_progress_count: s.opps_in_progress_count,
        tal_companies_count: s.tal_companies_count,
        contacts_count: s.contacts_count,
        notes: s.notes,
        blockers: s.blockers,
        next_steps: s.next_steps,
        channel_activity_json: {
          channels: s.channels,
          per_channel: s.per_channel
        },
        metrics_snapshot_json: { metrics: s.hypothesis_metrics }
      });
    }
  }

  addCheckinsForHyp(hyp1, [
    {
      opps_in_progress_count: 1,
      tal_companies_count: 60,
      contacts_count: 300,
      notes: "Week 1: baseline outreach, validate messaging.",
      blockers: "List quality uncertain.",
      next_steps: "Tighten ICP and A/B subject lines.",
      channels: ["demo_outbound_email", "demo_linkedin_outbound"],
      per_channel: {
        demo_outbound_email: perChannel("demo_outbound_email", {
          activity: "Sent first 2 sequences, iterated on subject line.",
          results: "Some replies, 1 meeting.",
          metrics: { demo_messages_sent: 220, demo_replies: 6, demo_reply_rate_pct: 2.7, demo_calls_held: 1, demo_opps_created: 0 }
        }),
        demo_linkedin_outbound: perChannel("demo_linkedin_outbound", {
          activity: "Connection requests + 1st follow-up.",
          results: "Warm intros, 1 meeting booked.",
          metrics: { demo_messages_sent: 80, demo_replies: 9, demo_meetings_booked: 1 }
        })
      },
      hypothesis_metrics: { demo_reply_rate_pct: 2.7, demo_opps_created: 0 }
    },
    {
      opps_in_progress_count: 2,
      tal_companies_count: 62,
      contacts_count: 315,
      notes: "Week 2: second sequence, tighten compliance angle.",
      blockers: "Long cycles; hard to qualify quickly.",
      next_steps: "Add case-study snippet; focus on CISOs.",
      channels: ["demo_outbound_email", "demo_linkedin_outbound"],
      per_channel: {
        demo_outbound_email: perChannel("demo_outbound_email", {
          activity: "Rolled out compliance-first email variant.",
          results: "More replies, 2 calls held, 1 opp created.",
          metrics: { demo_messages_sent: 260, demo_replies: 10, demo_reply_rate_pct: 3.8, demo_calls_held: 2, demo_opps_created: 1 }
        }),
        demo_linkedin_outbound: perChannel("demo_linkedin_outbound", {
          activity: "Follow-ups + short loom.",
          results: "1 additional meeting.",
          metrics: { demo_messages_sent: 95, demo_replies: 11, demo_meetings_booked: 1 }
        })
      },
      hypothesis_metrics: { demo_reply_rate_pct: 3.8, demo_opps_created: 1 }
    },
    {
      opps_in_progress_count: 3,
      tal_companies_count: 64,
      contacts_count: 330,
      notes: "Week 3: quality improving; keep volume steady.",
      blockers: "",
      next_steps: "Double down on segments converting; prep talk track.",
      channels: ["demo_outbound_email", "demo_linkedin_outbound"],
      per_channel: {
        demo_outbound_email: perChannel("demo_outbound_email", {
          activity: "Continued sequences + refined CTA.",
          results: "3 calls held, 1 opp created.",
          metrics: { demo_messages_sent: 240, demo_replies: 9, demo_reply_rate_pct: 3.6, demo_calls_held: 3, demo_opps_created: 1 }
        }),
        demo_linkedin_outbound: perChannel("demo_linkedin_outbound", {
          activity: "Targeted CISOs in payments.",
          results: "2 meetings booked.",
          metrics: { demo_messages_sent: 110, demo_replies: 14, demo_meetings_booked: 2 }
        })
      },
      hypothesis_metrics: { demo_reply_rate_pct: 3.6, demo_opps_created: 1 }
    }
  ]);

  addCheckinsForHyp(hyp2, [
    {
      opps_in_progress_count: 0,
      tal_companies_count: 80,
      contacts_count: 420,
      notes: "Week 1: start LinkedIn + line up 1 partnership intro.",
      blockers: "Hard to identify the right SecEng persona.",
      next_steps: "Refine targeting + add technical hook.",
      channels: ["demo_linkedin_outbound", "demo_partnerships"],
      per_channel: {
        demo_linkedin_outbound: perChannel("demo_linkedin_outbound", {
          activity: "Technical hook (CI/CD) in first message.",
          results: "Some replies; 1 meeting booked.",
          metrics: { demo_messages_sent: 140, demo_replies: 16, demo_meetings_booked: 1 }
        }),
        demo_partnerships: perChannel("demo_partnerships", {
          activity: "Reached out to 3 potential partners.",
          results: "1 intro call scheduled.",
          metrics: { demo_meetings_booked: 1, demo_calls_held: 0, demo_opps_created: 0 }
        })
      },
      hypothesis_metrics: { demo_meetings_booked: 2, demo_opps_created: 0 }
    },
    {
      opps_in_progress_count: 1,
      tal_companies_count: 82,
      contacts_count: 430,
      notes: "Week 2: better responses after adding 'bug hunter' angle (demo).",
      blockers: "",
      next_steps: "Convert meetings into opps, test 2nd hook.",
      channels: ["demo_linkedin_outbound", "demo_partnerships"],
      per_channel: {
        demo_linkedin_outbound: perChannel("demo_linkedin_outbound", {
          activity: "Follow-ups + shared short demo clip.",
          results: "2 meetings booked.",
          metrics: { demo_messages_sent: 150, demo_replies: 22, demo_meetings_booked: 2 }
        }),
        demo_partnerships: perChannel("demo_partnerships", {
          activity: "Partner intro call happened.",
          results: "1 opp created via intro.",
          metrics: { demo_meetings_booked: 1, demo_calls_held: 1, demo_opps_created: 1 }
        })
      },
      hypothesis_metrics: { demo_meetings_booked: 3, demo_opps_created: 1 }
    },
    {
      opps_in_progress_count: 2,
      tal_companies_count: 85,
      contacts_count: 445,
      notes: "Week 3: scaling what works.",
      blockers: "",
      next_steps: "Document playbook; keep partnerships warm.",
      channels: ["demo_linkedin_outbound", "demo_partnerships"],
      per_channel: {
        demo_linkedin_outbound: perChannel("demo_linkedin_outbound", {
          activity: "Increased volume slightly.",
          results: "2 meetings booked, 1 opp created.",
          metrics: { demo_messages_sent: 180, demo_replies: 26, demo_meetings_booked: 2, demo_opps_created: 1 }
        }),
        demo_partnerships: perChannel("demo_partnerships", {
          activity: "2 more partners pinged.",
          results: "1 more meeting booked.",
          metrics: { demo_meetings_booked: 1, demo_calls_held: 1, demo_opps_created: 0 }
        })
      },
      hypothesis_metrics: { demo_meetings_booked: 3, demo_opps_created: 1 }
    }
  ]);

  // Upsert checkins (onConflict hypothesis_id,week_start)
  for (const row of checkinRows) {
    const params = new URLSearchParams();
    params.set("on_conflict", "hypothesis_id,week_start");
    params.set("select", "id,hypothesis_id,week_start");
    await rest(auth, {
      method: "POST",
      table: "sales_hypothesis_checkins",
      qs: params.toString(),
      body: row,
      prefer: "resolution=merge-duplicates,return=representation"
    });
    // tiny throttle to avoid hitting any edge limits
    await sleep(40);
  }

  console.log("");
  console.log("[seed-demo] done.");
  console.log(`- hypotheses: ${hyp1.title} (${hyp1.id}), ${hyp2.title} (${hyp2.id})`);
  console.log(`- weeks seeded: ${weeks.join(", ")}`);
  console.log("- open Sales portal:");
  console.log("  - /hypotheses");
  console.log("  - /dashboard");
  console.log("  - /checkins/new");
}

seed().catch((e) => {
  console.error(e?.stack || String(e));
  process.exit(1);
});

