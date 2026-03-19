"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { isoDate, startOfWeekISO } from "../../lib/utils";
import { CHANNEL_OPTIONS, parseCjm, parseVp, type CjmJson, type VpJson } from "../_lib/hypothesisJson";
import { AppTopbar } from "../../components/AppTopbar";
import { StackedBars } from "../../components/StackedBars";
import { ActivityLines } from "../../components/ActivityLines";
import { ProgressChart } from "./ProgressChart";

type Bundle = {
  hypothesis: any;
  checkins: any[];
  calls: Array<{ call_id: string; title: string | null; occurred_at: string | null; owner_email: string | null; tag?: string | null; notes?: string | null }>;
} | null;

type UserDirectoryRow = {
  user_id: string;
  email: string;
  display_name: string | null;
};

type PainJson = {
  pain_points?: string; // multiline
  product_solution?: string; // multiline
};

export default function HypothesisDetailPage({ params }: { params: { id: string } }) {
  const id = String(params?.id ?? "");
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [status, setStatus] = useState("");
  const [sessionEmail, setSessionEmail] = useState<string | null>(null);
  const [bundle, setBundle] = useState<Bundle>(null);
  const [editing, setEditing] = useState<any>(null);
  const [cjm, setCjm] = useState<CjmJson>(() => parseCjm({ channels: [] }));

  /**
   * "Last saved" snapshot for the hypothesis edit form.
   *
   * We use this to avoid writing to Supabase when the user blurs a field
   * but the value didn't actually change.
   */
  const lastSavedHypRef = useRef<any>(null);

  /**
   * Autosave queue:
   * - onBlur enqueues a small patch (single field + derived fields)
   * - we flush updates sequentially to avoid race conditions and UI jitter
   */
  const autosavePendingPatchRef = useRef<Record<string, any>>({});
  const autosaveInFlightRef = useRef<boolean>(false);

  // Check-in form
  const [ciWeekStart, setCiWeekStart] = useState<string>(startOfWeekISO(new Date()));
  const [ciOpps, setCiOpps] = useState<number | "">("");
  const [ciTal, setCiTal] = useState<number | "">("");
  const [ciContacts, setCiContacts] = useState<number | "">("");
  const [ciNotes, setCiNotes] = useState("");
  const [ciBlockers, setCiBlockers] = useState("");
  const [ciNextSteps, setCiNextSteps] = useState("");
  const [ciPerChannel, setCiPerChannel] = useState<
    Record<
      string,
      { activity: string; results: string; metrics?: Record<string, string> }
    >
  >({});
  const [ciMetricValues, setCiMetricValues] = useState<Record<string, string>>({});

  // Call linking
  const [callIdToLink, setCallIdToLink] = useState("");
  const [callTag, setCallTag] = useState("");
  const [callNotes, setCallNotes] = useState("");
  const [recentCalls, setRecentCalls] = useState<Array<{ id: string; title: string | null; occurred_at: string | null; owner_email: string | null }>>([]);
  const [recentCallsQ, setRecentCallsQ] = useState("");
  const [recentLinkMeta, setRecentLinkMeta] = useState<Record<string, { tag: string; notes: string }>>({});
  const [linkedEditMeta, setLinkedEditMeta] = useState<Record<string, { tag: string; notes: string }>>({});

  // VP per hypothesis (Roles + Company profiles + VP matrix)
  const [allRoles, setAllRoles] = useState<any[]>([]);
  const [allCompanies, setAllCompanies] = useState<any[]>([]);
  const [selectedRoles, setSelectedRoles] = useState<any[]>([]);
  const [selectedCompanies, setSelectedCompanies] = useState<any[]>([]);
  const [roleToAdd, setRoleToAdd] = useState<string>("");
  const [companyToAdd, setCompanyToAdd] = useState<string>("");
  const [vpByKey, setVpByKey] = useState<Record<string, any>>({});
  const [vpEditKey, setVpEditKey] = useState<string | null>(null);
  const [vpDraft, setVpDraft] = useState<VpJson>(() => parseVp({ value_proposition: "" }));
  const [selectedMetrics, setSelectedMetrics] = useState<Array<{ metric_id: string; metric: any }>>([]);
  const [allMetrics, setAllMetrics] = useState<any[]>([]);
  const [metricToAdd, setMetricToAdd] = useState<string>("");

  // Pain points per hypothesis (Role x Company profiles)
  const [painByKey, setPainByKey] = useState<Record<string, PainJson>>({});
  const [painEditKey, setPainEditKey] = useState<string | null>(null);
  const [painDraft, setPainDraft] = useState<PainJson>({ pain_points: "", product_solution: "" });

  /**
   * Split multiline text into items (1 line = 1 item).
   *
   * We use this for pairing Pain points and How we solve by index,
   * so item #1 corresponds to item #1, etc.
   */
  function splitMultilineItems(text: unknown): string[] {
    return String(text ?? "")
      .split("\n")
      .map((s) => String(s).trim())
      .filter(Boolean);
  }

  /**
   * Render paired list (pain[i] ↔ solution[i]) as a numbered list.
   */
  function renderPairedPains(painText: string, solveText: string) {
    const pains = splitMultilineItems(painText);
    const solves = splitMultilineItems(solveText);
    const n = Math.max(pains.length, solves.length);

    if (!n) return <span className="muted2">(not set)</span>;

    return (
      <ol style={{ margin: 0, paddingLeft: 18, display: "grid", gap: 10 }}>
        {Array.from({ length: n }).map((_, i) => {
          const p = pains[i] ?? "";
          const s = solves[i] ?? "";
          return (
            <li key={i} style={{ margin: 0 }}>
              <div style={{ whiteSpace: "pre-wrap" }}>
                <b>Pain:</b> {p || <span className="muted2">(missing)</span>}
              </div>
              <div style={{ whiteSpace: "pre-wrap", marginTop: 6 }}>
                <b>How we solve:</b> {s || <span className="muted2">(missing)</span>}
              </div>
            </li>
          );
        })}
      </ol>
    );
  }

  // Users directory (for owner pickers)
  const [allUsers, setAllUsers] = useState<UserDirectoryRow[]>([]);
  const [userQ, setUserQ] = useState<string>("");

  // Daily Activity (from RPC)
  const [dailyActivity, setDailyActivity] = useState<any[]>([]);
  const [dailyActivityLoading, setDailyActivityLoading] = useState<boolean>(false);

  // SmartLead API data for real activity chart
  const [smartleadReport, setSmartleadReport] = useState<any>(null);
  const [smartleadReportErr, setSmartleadReportErr] = useState<string>("");
  const [smartleadReportLoading, setSmartleadReportLoading] = useState<boolean>(false);
  const [smartleadCampaigns, setSmartleadCampaigns] = useState<any[]>([]);
  const [smartleadCampaignsErr, setSmartleadCampaignsErr] = useState<string>("");
  const [smartleadCampaignsLoading, setSmartleadCampaignsLoading] = useState<boolean>(false);
  const [smartleadCampaignIds, setSmartleadCampaignIds] = useState<string[]>([]);
  const [smartleadCampaignDraft, setSmartleadCampaignDraft] = useState<string[]>([]);
  const smartleadCampaignTouchedRef = useRef(false);
  const smartleadCampaignDraftTouchedRef = useRef(false);

  // Prepare Activity Graph data (Daily - last 30 days) using real SmartLead data
  const activityChartData = useMemo(() => {
    // Generate last 30 days (YYYY-MM-DD + label)
    const daysYmd: string[] = [];
    const labels: string[] = [];
    const now = new Date();
    for (let i = 29; i >= 0; i--) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      const ymd = d.toISOString().slice(0, 10);
      daysYmd.push(ymd);
      labels.push(ymd.slice(5)); // MM-DD format
    }

    /**
     * Map a timeseries report into a per-day lookup for the requested series key.
     */
    function mapReportSeries(report: any, seriesKey: string) {
      const out = new Map<string, number>();
      const days = Array.isArray(report?.days) ? report.days : [];
      const series = report?.series && typeof report.series === "object" ? report.series : {};
      const values = Array.isArray(series?.[seriesKey]) ? series[seriesKey] : [];
      for (let i = 0; i < days.length && i < values.length; i++) {
        const day = String(days[i] ?? "").slice(0, 10);
        if (day) out.set(day, Number(values[i] ?? 0) || 0);
      }
      return out;
    }

    const smartleadHasSeries = Array.isArray(smartleadReport?.days);

    // Prefer SmartLead report when available (most accurate).
    if (smartleadHasSeries) {
      const emailSentMap = mapReportSeries(smartleadReport, "emails_sent");
      const emailReplyMap = mapReportSeries(smartleadReport, "emails_replied");

      const emails = daysYmd.map((day) => emailSentMap.get(day) ?? 0);
      const linkedin = daysYmd.map(() => 0);
      const replies = daysYmd.map((day) => emailReplyMap.get(day) ?? 0);

      return {
        weeks: labels,
        series: [
          { key: "emails", label: "Emails", color: "#60a5fa", values: emails },
          { key: "linkedin", label: "LinkedIn", color: "#818cf8", values: linkedin },
          { key: "replies", label: "Replies", color: "#34d399", values: replies }
        ],
        totals: {
          emails: emails.reduce((a, b) => a + b, 0),
          linkedin: linkedin.reduce((a, b) => a + b, 0),
          replies: replies.reduce((a, b) => a + b, 0)
        }
      };
    }

    // Use real RPC data if available
    if (dailyActivity.length > 0) {
      const emails = labels.map((day) => dailyActivity.find((r) => String(r.day).endsWith(day))?.emails_sent_count || 0);
      const linkedin = labels.map((day) => dailyActivity.find((r) => String(r.day).endsWith(day))?.linkedin_sent_count || 0);
      const replies = labels.map((day) => dailyActivity.find((r) => String(r.day).endsWith(day))?.replies_count || 0);

      return {
        weeks: labels,
        series: [
          { key: "emails", label: "Emails", color: "#60a5fa", values: emails },
          { key: "linkedin", label: "LinkedIn", color: "#818cf8", values: linkedin },
          { key: "replies", label: "Replies", color: "#34d399", values: replies }
        ],
        totals: {
          emails: emails.reduce((a, b) => a + b, 0),
          linkedin: linkedin.reduce((a, b) => a + b, 0),
          replies: replies.reduce((a, b) => a + b, 0)
        }
      };
    }

    // Fallback: no data
    const emails = labels.map(() => 0);
    const linkedin = labels.map(() => 0);
    const replies = labels.map(() => 0);

    return {
      weeks: labels,
      series: [
        { key: "emails", label: "Emails", color: "#60a5fa", values: emails },
        { key: "linkedin", label: "LinkedIn", color: "#818cf8", values: linkedin },
        { key: "replies", label: "Replies", color: "#34d399", values: replies }
      ],
      totals: { emails: 0, linkedin: 0, replies: 0 }
    };
  }, [dailyActivity, smartleadReport, smartleadCampaignIds]);

  // Channels library
  const [channelOptions, setChannelOptions] = useState<Array<{ slug: string; name: string }>>(() =>
    ([...CHANNEL_OPTIONS] as unknown as string[]).map((slug) => ({ slug, name: slug }))
  );
  const [channelToAdd, setChannelToAdd] = useState<string>("");

  // Channel metrics (per hypothesis)
  const [channelBySlug, setChannelBySlug] = useState<Map<string, { id: string; slug: string; name: string }>>(new Map());
  const [channelMetricIdsBySlug, setChannelMetricIdsBySlug] = useState<Record<string, string[]>>({});
  const [channelMetricAddBySlug, setChannelMetricAddBySlug] = useState<Record<string, string>>({});
  const [channelOwnerEmailsBySlug, setChannelOwnerEmailsBySlug] = useState<Record<string, string[]>>({});
  const [channelOwnerToAddBySlug, setChannelOwnerToAddBySlug] = useState<Record<string, string>>({});
  const [channelMetricOwnerEmailsBySlug, setChannelMetricOwnerEmailsBySlug] = useState<Record<string, Record<string, string[]>>>({});
  const [channelMetricOwnerToAddBySlug, setChannelMetricOwnerToAddBySlug] = useState<Record<string, Record<string, string>>>({});

  // Weekly stats view
  const [weeklyExpanded, setWeeklyExpanded] = useState<string | null>(null);

  // Collapsible sections (default collapsed)
  const [sections, setSections] = useState<{ description: boolean; weekly: boolean; calls: boolean }>({
    description: false,
    weekly: false,
    calls: false
  });

  function toggleSection(key: "description" | "weekly" | "calls") {
    setSections((prev) => ({ ...prev, [key]: !prev[key] }));
  }

  function formatCET(iso: string | null | undefined) {
    const t = String(iso ?? "").trim();
    const ms = Date.parse(t);
    if (!t || !Number.isFinite(ms)) return "";
    try {
      return new Intl.DateTimeFormat("en-GB", {
        timeZone: "Europe/Paris",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false
      }).format(new Date(ms));
    } catch {
      return new Date(ms).toISOString().slice(0, 19);
    }
  }

  function processingNum(n: unknown) {
    if (n == null || n === "") return "—";
    const num = Number(n);
    if (!Number.isFinite(num)) return "—";
    return num.toLocaleString();
  }


  async function loadSmartleadReport() {
    if (!supabase) return;
    setSmartleadReportLoading(true);
    setSmartleadReportErr("");
    try {
      const sess = await supabase.auth.getSession();
      const token = sess.data.session?.access_token ?? "";
      if (!token) throw new Error("Not signed in.");

      // Get last 30 days
      const until = new Date();
      const since = new Date();
      since.setDate(since.getDate() - 30);
      const campaignIds = normalizeSmartleadCampaignIds(smartleadCampaignIds);

      const res = await fetch("/api/smartlead/reports/timeseries", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({
          since: since.toISOString().slice(0, 10),
          until: until.toISOString().slice(0, 10),
          bucketSizeDays: 1,
          maxBuckets: 30,
          campaign_ids: campaignIds.length ? campaignIds : undefined
        })
      });
      const json = (await res.json().catch(() => null)) as any;
      if (!res.ok || !json?.ok) throw new Error(String(json?.error ?? `SmartLead request failed (status ${res.status})`));
      setSmartleadReport(json);
    } catch (e: any) {
      setSmartleadReportErr(String(e?.message || e));
    } finally {
      setSmartleadReportLoading(false);
    }
  }

  /**
   * Load SmartLead campaign list for hypothesis-specific filtering.
   */
  async function loadSmartleadCampaigns() {
    if (!supabase) return;
    setSmartleadCampaignsLoading(true);
    setSmartleadCampaignsErr("");
    try {
      const sess = await supabase.auth.getSession();
      const token = sess.data.session?.access_token ?? "";
      if (!token) throw new Error("Not signed in.");
      const res = await fetch("/api/smartlead/campaigns", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({ all: true, limit: 200, offset: 0 })
      });
      const json = (await res.json().catch(() => null)) as any;
      if (!res.ok || !json?.ok) throw new Error(String(json?.error ?? "Failed to load campaigns"));
      setSmartleadCampaigns(Array.isArray(json?.campaigns) ? json.campaigns : []);
    } catch (e: any) {
      setSmartleadCampaignsErr(String(e?.message || e));
    } finally {
      setSmartleadCampaignsLoading(false);
    }
  }

  const userById = useMemo(() => {
    const m = new Map<string, UserDirectoryRow>();
    for (const u of allUsers) m.set(String(u.user_id), u);
    return m;
  }, [allUsers]);

  const userByEmail = useMemo(() => {
    const m = new Map<string, UserDirectoryRow>();
    for (const u of allUsers) {
      const e = String(u.email ?? "").trim().toLowerCase();
      if (e) m.set(e, u);
    }
    return m;
  }, [allUsers]);

  function userLabel(u: UserDirectoryRow | null) {
    if (!u) return "—";
    const name = String(u.display_name ?? "").trim();
    const email = String(u.email ?? "").trim();
    if (name && email) return `${name} <${email}>`;
    return email || name || "—";
  }

  /**
   * Add a channel owner (auto-save UX).
   *
   * Rationale:
   * - In the UI, "Save" at the top saves the hypothesis form, but owners are a separate table.
   * - The previous UX required selecting a user and then clicking "Add", which was easy to miss.
   * - We now persist immediately on selection to make the action obvious.
   *
   * Notes:
   * - RLS: the table has INSERT/DELETE policies (no UPDATE), so we avoid upsert.
   * - Duplicate inserts are treated as success (unique constraint code 23505).
   */
  async function addHypothesisChannelOwner(opts: { channelId: string; channelSlug: string; ownerUserId: string }) {
    if (!supabase) return;
    const uid = String(opts.ownerUserId || "").trim();
    const u = uid ? (userById.get(uid) ?? null) : null;
    const email = String(u?.email ?? "").trim().toLowerCase();
    if (!uid || !u || !email) {
      setStatus("Cannot add owner: selected user has no email (or users list not loaded).");
      return;
    }

    setStatus("Adding channel owner...");
    const res = await supabase.from("sales_hypothesis_channel_owners").insert({
      hypothesis_id: id,
      channel_id: String(opts.channelId),
      owner_email: email
    });
    if (res?.error && String((res.error as any).code ?? "") !== "23505") {
      setStatus(`add channel owner error: ${res.error.message}`);
      return;
    }

    // UX: clear picker immediately so user can add another owner quickly.
    setChannelOwnerToAddBySlug((prev) => ({ ...prev, [String(opts.channelSlug)]: "" }));
    await load();
    setStatus("Channel owner added.");
  }

  /**
   * Add a metric owner (auto-save UX).
   *
   * Notes:
   * - RLS: INSERT/DELETE only (no UPDATE), so we avoid upsert.
   * - Duplicate inserts are treated as success (unique constraint code 23505).
   */
  async function addHypothesisChannelMetricOwner(opts: {
    channelId: string;
    channelSlug: string;
    metricId: string;
    ownerUserId: string;
  }) {
    if (!supabase) return;
    const uid = String(opts.ownerUserId || "").trim();
    const u = uid ? (userById.get(uid) ?? null) : null;
    const email = String(u?.email ?? "").trim().toLowerCase();
    if (!uid || !u || !email) {
      setStatus("Cannot add metric owner: selected user has no email (or users list not loaded).");
      return;
    }

    setStatus("Adding metric owner...");
    const res = await supabase.from("sales_hypothesis_channel_metric_owners").insert({
      hypothesis_id: id,
      channel_id: String(opts.channelId),
      metric_id: String(opts.metricId),
      owner_email: email
    });
    if (res?.error && String((res.error as any).code ?? "") !== "23505") {
      setStatus(`add metric owner error: ${res.error.message}`);
      return;
    }

    // UX: clear picker immediately so user can add another owner quickly.
    setChannelMetricOwnerToAddBySlug((prev) => ({
      ...prev,
      [String(opts.channelSlug)]: { ...(prev?.[String(opts.channelSlug)] ?? {}), [String(opts.metricId)]: "" }
    }));
    await load();
    setStatus("Metric owner added.");
  }

  useEffect(() => {
    if (!supabase) return;
    supabase.auth.getSession().then(({ data }) => setSessionEmail(data.session?.user?.email ?? null));
  }, [supabase]);

  function isoSinceDaysAgo(days: number) {
    const d = new Date();
    d.setDate(d.getDate() - days);
    return d.toISOString();
  }

  async function loadRecentCalls(email: string) {
    if (!supabase) return;
    const since = isoSinceDaysAgo(7);
    // 1) calls where user was a participant
    const partsRes = await supabase
      .from("call_participants")
      .select("call_id,calls:call_id(id,title,occurred_at,owner_email)")
      .eq("email", email)
      .gte("calls.occurred_at", since)
      .limit(200);

    // 2) calls where user is owner (covers cases where participants weren't captured)
    const ownedRes = await supabase
      .from("calls")
      .select("id,title,occurred_at,owner_email")
      .eq("owner_email", email)
      .gte("occurred_at", since)
      .order("occurred_at", { ascending: false })
      .limit(200);

    if (partsRes.error) {
      // don't block whole page if this fails; user can still link by ID
      console.warn("recent calls participants error", partsRes.error);
    }
    if (ownedRes.error) {
      console.warn("recent calls owned error", ownedRes.error);
    }

    const byId = new Map<string, { id: string; title: string | null; occurred_at: string | null; owner_email: string | null }>();
    for (const r of (partsRes.data ?? []) as any[]) {
      const c = r?.calls ?? null;
      if (!c?.id) continue;
      byId.set(String(c.id), {
        id: String(c.id),
        title: c.title ?? null,
        occurred_at: c.occurred_at ?? null,
        owner_email: c.owner_email ?? null
      });
    }
    for (const c of (ownedRes.data ?? []) as any[]) {
      if (!c?.id) continue;
      byId.set(String(c.id), {
        id: String(c.id),
        title: c.title ?? null,
        occurred_at: c.occurred_at ?? null,
        owner_email: c.owner_email ?? null
      });
    }

    const merged = Array.from(byId.values()).sort((a, b) => String(b.occurred_at ?? "").localeCompare(String(a.occurred_at ?? "")));
    setRecentCalls(merged);
  }

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) {
      setStatus("Not signed in. Go back to / and sign in.");
      return;
    }
    const email = sess.data.session.user.email ?? null;
    if (email) loadRecentCalls(email);
    const [hRes, ciRes, linksRes, hrRes, hcRes, vpsRes, painsRes, rRes, cRes, chRes, hcoRes, hcmRes, hmRes, mRes, usersRes, hcmOwnersRes] = await Promise.all([
      supabase.from("sales_hypotheses").select("*").eq("id", id).single(),
      supabase.from("sales_hypothesis_checkins").select("*").eq("hypothesis_id", id).order("week_start", { ascending: false }),
      supabase.from("sales_hypothesis_calls").select("call_id,tag,notes").eq("hypothesis_id", id),
      supabase
        .from("sales_hypothesis_roles")
        .select("role_id,role:sales_icp_roles(id,name)")
        .eq("hypothesis_id", id),
      supabase
        .from("sales_hypothesis_company_profiles")
        .select("company_profile_id,company:sales_icp_company_profiles(id,vertical_name,sub_vertical,region,size_bucket)")
        .eq("hypothesis_id", id),
      supabase
        .from("sales_hypothesis_vps")
        .select("role_id,company_profile_id,vp_json,updated_at")
        .eq("hypothesis_id", id)
        .order("updated_at", { ascending: false })
        .limit(2000),
      supabase
        .from("sales_hypothesis_pains")
        .select("role_id,company_profile_id,pain_json,updated_at")
        .eq("hypothesis_id", id)
        .order("updated_at", { ascending: false })
        .limit(2000),
      supabase
        .from("sales_icp_roles")
        .select("id,name")
        .order("name", { ascending: true })
        .limit(500),
      supabase
        .from("sales_icp_company_profiles")
        .select("id,vertical_name,sub_vertical,region,size_bucket")
        .order("updated_at", { ascending: false })
        .limit(500),
      supabase
        .from("sales_channels")
        .select("id,slug,is_active,sort_order,name")
        .eq("is_active", true)
        .order("sort_order", { ascending: true })
        .order("name", { ascending: true })
        .limit(200),
      supabase
        .from("sales_hypothesis_channel_owners")
        .select("channel_id,owner_email")
        .eq("hypothesis_id", id)
        .limit(5000),
      supabase
        .from("sales_hypothesis_channel_metrics")
        .select("channel_id,metric_id")
        .eq("hypothesis_id", id)
        .limit(2000),
      supabase
        .from("sales_hypothesis_metrics")
        .select("metric_id,metric:sales_metrics(id,slug,name,input_type,unit,sort_order,is_active)")
        .eq("hypothesis_id", id),
      supabase
        .from("sales_metrics")
        .select("id,slug,name,input_type,unit,sort_order,is_active")
        .eq("is_active", true)
        .order("sort_order", { ascending: true })
        .order("name", { ascending: true })
        .limit(200),
      supabase.rpc("sales_list_users"),
      supabase
        .from("sales_hypothesis_channel_metric_owners")
        .select("channel_id,metric_id,owner_email")
        .eq("hypothesis_id", id)
        .limit(10000)
    ]);
    if (hRes.error) return setStatus(`sales_hypotheses error: ${hRes.error.message}`);
    if (ciRes.error) return setStatus(`checkins error: ${ciRes.error.message}`);
    if (linksRes.error) return setStatus(`call links error: ${linksRes.error.message}`);
    if (hrRes.error) return setStatus(`hypothesis roles error: ${hrRes.error.message}`);
    if (hcRes.error) return setStatus(`hypothesis companies error: ${hcRes.error.message}`);
    if (vpsRes.error) return setStatus(`hypothesis vps error: ${vpsRes.error.message}`);
    if (painsRes.error) return setStatus(`hypothesis pains error: ${painsRes.error.message}`);
    if (rRes.error) return setStatus(`roles error: ${rRes.error.message}`);
    if (cRes.error) return setStatus(`companies error: ${cRes.error.message}`);
    if (hcoRes.error) return setStatus(`hypothesis channel owners error: ${hcoRes.error.message}`);
    if (hcmRes.error) return setStatus(`hypothesis channel metrics error: ${hcmRes.error.message}`);
    if (hmRes.error) return setStatus(`hypothesis metrics error: ${hmRes.error.message}`);
    if (mRes.error) return setStatus(`metrics error: ${mRes.error.message}`);
    if (hcmOwnersRes.error) return setStatus(`hypothesis channel metric owners error: ${hcmOwnersRes.error.message}`);

    if (usersRes?.error) {
      console.warn("sales_list_users error", usersRes.error);
    } else {
      setAllUsers(
        ((usersRes?.data ?? []) as any[]).map((x: any) => ({
          user_id: String(x.user_id),
          email: String(x.email ?? "").trim(),
          display_name: x.display_name ?? null
        }))
      );
    }
    if (!chRes.error) {
      const opts = (chRes.data ?? [])
        .map((x: any) => ({ id: String(x.id ?? "").trim(), slug: String(x.slug ?? "").trim(), name: String(x.name ?? "").trim() }))
        .filter((x: any) => x.slug && x.id);
      if (opts.length) setChannelOptions(opts.map((o: any) => ({ slug: o.slug, name: o.name || o.slug })));
      const m = new Map<string, { id: string; slug: string; name: string }>();
      for (const o of opts) m.set(o.slug, { id: o.id, slug: o.slug, name: o.name || o.slug });
      setChannelBySlug(m);
    }

    const callIds = (linksRes.data ?? []).map((x: any) => String(x.call_id)).filter(Boolean);
    let calls: any[] = [];
    if (callIds.length) {
      const callsRes = await supabase
        .from("calls")
        .select("id,title,occurred_at,owner_email")
        .in("id", callIds);
      if (callsRes.error) return setStatus(`calls error: ${callsRes.error.message}`);
      const byId = new Map<string, any>(((callsRes.data ?? []) as any[]).map((c) => [String(c.id), c]));
      calls = (linksRes.data ?? []).map((l: any) => {
        const c = byId.get(String(l.call_id));
        return {
          call_id: String(l.call_id),
          tag: l.tag ?? null,
          notes: l.notes ?? null,
          title: c?.title ?? null,
          occurred_at: c?.occurred_at ?? null,
          owner_email: c?.owner_email ?? null
        };
      });
    }

    const b: Bundle = {
      hypothesis: hRes.data,
      checkins: (ciRes.data ?? []) as any[],
      calls
    };
    const parsedCjm = parseCjm(hRes.data?.cjm_json ?? {});

    setBundle(b);
    setEditing(hRes.data ? { ...hRes.data } : null);
    // Keep a "last saved" snapshot for autosave diffing.
    lastSavedHypRef.current = hRes.data ? { ...hRes.data } : null;
    setCjm(parsedCjm);
    setAllRoles((rRes.data ?? []) as any[]);
    setAllCompanies((cRes.data ?? []) as any[]);
    setSelectedRoles(((hrRes.data ?? []) as any[]).map((x) => ({ role_id: String(x.role_id), role: x.role })));
    setSelectedCompanies(((hcRes.data ?? []) as any[]).map((x) => ({ company_profile_id: String(x.company_profile_id), company: x.company })));
    setVpByKey(() => {
      const next: Record<string, any> = {};
      for (const x of (vpsRes.data ?? []) as any[]) {
        const rk = String(x.role_id ?? "");
        const ck = String(x.company_profile_id ?? "");
        if (!rk || !ck) continue;
        const key = `${rk}:${ck}`;
        if (next[key] != null) continue; // keep latest (query is ordered by updated_at desc)
        next[key] = x.vp_json ?? {};
      }
      return next;
    });
    setPainByKey(() => {
      const next: Record<string, PainJson> = {};
      for (const x of (painsRes.data ?? []) as any[]) {
        const rk = String(x.role_id ?? "");
        const ck = String(x.company_profile_id ?? "");
        if (!rk || !ck) continue;
        const key = `${rk}:${ck}`;
        if (next[key] != null) continue; // keep latest (query is ordered by updated_at desc)
        const pj = (x.pain_json && typeof x.pain_json === "object") ? x.pain_json : {};
        next[key] = {
          pain_points: String(pj?.pain_points ?? "").trim(),
          product_solution: String(pj?.product_solution ?? "").trim()
        };
      }
      return next;
    });
    setSelectedMetrics(((hmRes.data ?? []) as any[]).map((x) => ({ metric_id: String(x.metric_id), metric: x.metric })));
    setAllMetrics((mRes.data ?? []) as any[]);

    // Channel metrics selections (slug -> metric_ids)
    // NOTE: compute from loaded DB rows, not from React state (state updates are async).
    const chMapNow = (() => {
      const m = new Map<string, { id: string; slug: string; name: string }>();
      for (const x of (chRes.data ?? []) as any[]) {
        const id = String(x?.id ?? "").trim();
        const slug = String(x?.slug ?? "").trim();
        const name = String(x?.name ?? "").trim();
        if (!id || !slug) continue;
        m.set(slug, { id, slug, name: name || slug });
      }
      return m;
    })();

    setChannelOwnerEmailsBySlug(() => {
      const byChannelId = new Map<string, string[]>();
      for (const x of (hcoRes.data ?? []) as any[]) {
        const ch = String(x.channel_id ?? "");
        const email = String(x.owner_email ?? "").trim().toLowerCase();
        if (!ch || !email) continue;
        const arr = byChannelId.get(ch) ?? [];
        if (!arr.includes(email)) arr.push(email);
        byChannelId.set(ch, arr);
      }
      const bySlug: Record<string, string[]> = {};
      for (const slug of (parsedCjm.channels ?? [])) {
        const ch = chMapNow.get(String(slug)) ?? null;
        if (!ch) continue;
        bySlug[String(slug)] = (byChannelId.get(ch.id) ?? []).slice().sort();
      }
      return bySlug;
    });
    setChannelMetricIdsBySlug(() => {
      const byChannelId = new Map<string, string[]>();
      for (const x of (hcmRes.data ?? []) as any[]) {
        const ch = String(x.channel_id ?? "");
        const mid = String(x.metric_id ?? "");
        if (!ch || !mid) continue;
        const arr = byChannelId.get(ch) ?? [];
        if (!arr.includes(mid)) arr.push(mid);
        byChannelId.set(ch, arr);
      }
      const bySlug: Record<string, string[]> = {};
      for (const slug of (parsedCjm.channels ?? [])) {
        const ch = chMapNow.get(String(slug)) ?? null;
        if (!ch) continue;
        bySlug[String(slug)] = (byChannelId.get(ch.id) ?? []).slice();
      }
      return bySlug;
    });

    // Channel metric owners (slug -> metric_id -> owner_emails)
    // NOTE: compute from loaded DB rows, not from React state (state updates are async).
    setChannelMetricOwnerEmailsBySlug(() => {
      const byChannelMetric = new Map<string, string[]>(); // `${channel_id}:${metric_id}` -> emails
      for (const x of (hcmOwnersRes.data ?? []) as any[]) {
        const ch = String(x.channel_id ?? "");
        const mid = String(x.metric_id ?? "");
        const email = String(x.owner_email ?? "").trim().toLowerCase();
        if (!ch || !mid || !email) continue;
        const key = `${ch}:${mid}`;
        const arr = byChannelMetric.get(key) ?? [];
        if (!arr.includes(email)) arr.push(email);
        byChannelMetric.set(key, arr);
      }

      const channelIdToSlug = new Map<string, string>();
      for (const [slug, ch] of chMapNow.entries()) channelIdToSlug.set(String(ch.id), String(slug));

      const metricIdsBySlugLocal: Record<string, string[]> = {};
      for (const x of (hcmRes.data ?? []) as any[]) {
        const chId = String(x.channel_id ?? "");
        const mid = String(x.metric_id ?? "");
        if (!chId || !mid) continue;
        const slug = channelIdToSlug.get(chId) ?? null;
        if (!slug) continue;
        const arr = metricIdsBySlugLocal[slug] ?? [];
        if (!arr.includes(mid)) arr.push(mid);
        metricIdsBySlugLocal[slug] = arr;
      }

      const bySlug: Record<string, Record<string, string[]>> = {};
      for (const slug of (parsedCjm.channels ?? [])) {
        const ch = chMapNow.get(String(slug)) ?? null;
        if (!ch) continue;
        const metricIds = metricIdsBySlugLocal[String(slug)] ?? [];
        const byMetric: Record<string, string[]> = {};
        for (const mid of metricIds) {
          byMetric[String(mid)] = (byChannelMetric.get(`${ch.id}:${String(mid)}`) ?? []).slice().sort();
        }
        bySlug[String(slug)] = byMetric;
      }
      return bySlug;
    });

    // Default check-in snapshot to current hypothesis values (so weekly update is quick)
    const hh = hRes.data ?? null;
    if (hh) {
      setCiOpps(typeof hh.opps_in_progress_count === "number" ? hh.opps_in_progress_count : "");
      setCiTal(typeof hh.tal_companies_count_baseline === "number" ? hh.tal_companies_count_baseline : "");
      setCiContacts(typeof hh.contacts_count_baseline === "number" ? hh.contacts_count_baseline : "");
    }

    // Default metric inputs from latest check-in (best-effort)
    const latest = (ciRes.data ?? [])[0] as any;
    const latestMetrics = latest?.metrics_snapshot_json?.metrics ?? {};
    if (latestMetrics && typeof latestMetrics === "object") {
      const next: Record<string, string> = {};
      for (const [k, v] of Object.entries(latestMetrics)) next[String(k)] = v == null ? "" : String(v);
      setCiMetricValues((prev) => ({ ...next, ...prev }));
    }

    // initialize editable tag/notes for linked calls (don't clobber user edits in-flight)
    setLinkedEditMeta((prev) => {
      const next: Record<string, { tag: string; notes: string }> = { ...prev };
      for (const c of calls) {
        const id = String(c.call_id);
        if (!next[id]) next[id] = { tag: String(c.tag ?? ""), notes: String(c.notes ?? "") };
      }
      return next;
    });
    setStatus("");
  }

  async function persistCjm(next: any) {
    if (!supabase) return;
    const res = await supabase.from("sales_hypotheses").update({ cjm_json: next }).eq("id", id);
    if (res.error) setStatus(`Hypothesis update (cjm_json) error: ${res.error.message}`);
  }

  async function ensureChannelInLibrary(slug: string) {
    const s = String(slug ?? "").trim();
    if (!s || !supabase) return;
    if (channelBySlug.get(s)) return;
    // Auto-create channel in Library so we can link per-channel metrics via channel_id.
    const ins = await supabase
      .from("sales_channels")
      .upsert({ slug: s, name: s, is_active: true, sort_order: 0 }, { onConflict: "slug" })
      .select("id,slug,name")
      .single();
    if (ins.error) {
      setStatus(`Create channel in Library failed: ${ins.error.message}`);
      return;
    }
    // Refresh mappings
    await load();
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase, id]);

  function normalizeUrl(v: string) {
    const t = String(v ?? "").trim();
    if (!t) return null;
    if (t === "-") return null;
    return t;
  }

  function normalizeEmail(v: string) {
    const t = String(v ?? "").trim().toLowerCase();
    return t || null;
  }

  /**
   * Normalize SmartLead campaign ids from UI values.
   */
  function normalizeSmartleadCampaignIds(values: string[]) {
    return (Array.isArray(values) ? values : [])
      .map((x) => Number(x))
      .filter((n) => Number.isFinite(n) && n > 0);
  }

  /**
   * Normalize a selection for stable equality checks.
   */
  function normalizeSmartleadSelection(values: string[]) {
    return (Array.isArray(values) ? values : [])
      .map((x) => String(x ?? "").trim())
      .filter(Boolean)
      .sort((a, b) => {
        const na = Number(a);
        const nb = Number(b);
        if (Number.isFinite(na) && Number.isFinite(nb) && na !== nb) return na - nb;
        return a.localeCompare(b);
      });
  }

  /**
   * Compare two selections by normalized id list.
   */
  function smartleadSelectionSame(a: string[], b: string[]) {
    const na = normalizeSmartleadSelection(a).join(",");
    const nb = normalizeSmartleadSelection(b).join(",");
    return na === nb;
  }

  /**
   * Normalize a small autosave patch for `sales_hypotheses`.
   *
   * NOTE: This intentionally does NOT run the full validation (required fields),
   * because we want to autosave drafts while the user is editing.
   */
  function normalizeHypothesisAutosavePatch(patch: Record<string, any>) {
    const out: Record<string, any> = {};
    for (const [k, v] of Object.entries(patch ?? {})) {
      if (k === "priority") out[k] = Number(v ?? 0) || 0;
      else if (k === "opps_in_progress_count") out[k] = Number(v ?? 0) || 0;
      else if (k === "timebox_days") out[k] = Number(v ?? 0) || 0;
      else if (k === "tal_companies_count_baseline") out[k] = v === "" || v == null ? null : Number(v);
      else if (k === "contacts_count_baseline") out[k] = v === "" || v == null ? null : Number(v);
      else if (k === "owner_email") out[k] = normalizeEmail(String(v ?? ""));
      else if (
        [
          "title",
          "status",
          "owner_user_id",
          "vertical_name",
          "pricing_model",
          "win_criteria",
          "kill_criteria",
          "one_sentence_pitch",
          "product_description",
          "company_profile_text"
        ].includes(k)
      ) {
        const t = String(v ?? "").trim();
        // Keep required fields as strings even if empty; optional ones become NULL.
        if (["title", "win_criteria", "kill_criteria", "status", "owner_user_id"].includes(k)) out[k] = t;
        else out[k] = t || null;
      } else {
        out[k] = v;
      }
    }

    return out;
  }

  /**
   * Apply SmartLead campaign selection and persist it as a hypothesis setting.
   */
  function applySmartleadCampaignSelection(next: string[]) {
    const normalized = normalizeSmartleadCampaignIds(next);
    smartleadCampaignTouchedRef.current = true;
    setSmartleadCampaignIds(next);
    if (editing) {
      setEditing({ ...editing, smartlead_campaign_ids: normalized });
      autosaveHypothesisOnBlur({ smartlead_campaign_ids: normalized });
    }
  }

  /**
   * Toggle a campaign in the draft selection (checkbox UX).
   */
  function toggleSmartleadCampaignDraft(id: string) {
    const key = String(id ?? "").trim();
    if (!key) return;
    smartleadCampaignDraftTouchedRef.current = true;
    setSmartleadCampaignDraft((prev) => {
      const set = new Set(prev.map((x) => String(x)));
      if (set.has(key)) set.delete(key);
      else set.add(key);
      return Array.from(set);
    });
  }

  function shallowSame(a: any, b: any) {
    if (a == null && b == null) return true;
    if (typeof a === "number" || typeof b === "number") return Number(a) === Number(b);
    return String(a ?? "") === String(b ?? "");
  }

  function pickChangedFromLastSaved(patch: Record<string, any>) {
    const saved = lastSavedHypRef.current ?? {};
    const out: Record<string, any> = {};
    for (const [k, v] of Object.entries(patch ?? {})) {
      if (!shallowSame(saved?.[k], v)) out[k] = v;
    }
    return out;
  }

  async function flushHypothesisAutosaveQueue() {
    if (!supabase) return;
    if (autosaveInFlightRef.current) return;
    const patch = { ...(autosavePendingPatchRef.current ?? {}) };
    autosavePendingPatchRef.current = {};
    if (!Object.keys(patch).length) return;

    autosaveInFlightRef.current = true;
    try {
      const res = await supabase.from("sales_hypotheses").update(patch).eq("id", id);
      if (res.error) {
        setStatus(`Autosave error: ${res.error.message}`);
      } else {
        // Update last-saved snapshot so future blurs are no-ops if nothing changed.
        lastSavedHypRef.current = { ...(lastSavedHypRef.current ?? {}), ...patch };
        // Ensure derived fields show up immediately in the form UI.
        setEditing((prev: any) => (prev ? ({ ...prev, ...patch } as any) : prev));
      }
    } finally {
      autosaveInFlightRef.current = false;
      // If something was queued while saving, flush again.
      if (Object.keys(autosavePendingPatchRef.current ?? {}).length) {
        // no await: keep UI responsive; next tick flushes.
        void flushHypothesisAutosaveQueue();
      }
    }
  }

  /**
   * Enqueue an autosave patch and flush immediately.
   * Use this from `onBlur` handlers (low frequency).
   */
  function autosaveHypothesisOnBlur(patch: Record<string, any>) {
    if (!supabase || !editing) return;
    const normalized = normalizeHypothesisAutosavePatch(patch);
    const changed = pickChangedFromLastSaved(normalized);
    if (!Object.keys(changed).length) return;
    autosavePendingPatchRef.current = { ...(autosavePendingPatchRef.current ?? {}), ...changed };
    void flushHypothesisAutosaveQueue();
  }

  async function readJsonResponse(res: Response, label: string) {
    const contentType = String(res.headers.get("content-type") ?? "");
    const txt = await res.text();
    try {
      return JSON.parse(txt || "null");
    } catch {
      const snippet = String(txt || "")
        .slice(0, 220)
        .replace(/\s+/g, " ")
        .trim();
      throw new Error(`${label} returned non-JSON (status ${res.status}). content-type=${contentType || "?"}. body="${snippet}"`);
    }
  }

  function validateHypothesis(h: any) {
    if (!h?.title?.trim()) return "title is required";
    if (!h?.win_criteria?.trim()) return "win criteria is required";
    if (!h?.kill_criteria?.trim()) return "kill criteria is required";
    if (!Number.isFinite(Number(h?.timebox_days ?? 0)) || Number(h.timebox_days) <= 0) return "timebox_days must be > 0";
    return null;
  }

  async function saveHypothesis() {
    if (!supabase || !editing) return;
    const err = validateHypothesis(editing);
    if (err) return setStatus(`Validation: ${err}`);

    setStatus("Saving...");
    const payload: any = {
      title: String(editing.title ?? "").trim(),
      status: String(editing.status ?? "draft"),
      priority: Number(editing.priority ?? 0) || 0,
      owner_user_id: String(editing.owner_user_id ?? "").trim(),
      owner_email: String(editing.owner_email ?? "").trim().toLowerCase() || null,
      vertical_name: String(editing.vertical_name ?? "").trim() || null,
      pricing_model: String(editing.pricing_model ?? "").trim() || null,
      opps_in_progress_count: Number(editing.opps_in_progress_count ?? 0) || 0,
      timebox_days: Number(editing.timebox_days ?? 28) || 28,
      win_criteria: String(editing.win_criteria ?? "").trim(),
      kill_criteria: String(editing.kill_criteria ?? "").trim(),
      smartlead_campaign_ids: normalizeSmartleadCampaignIds(smartleadCampaignIds),
      tal_companies_count_baseline:
        editing.tal_companies_count_baseline === null || editing.tal_companies_count_baseline === "" ? null : Number(editing.tal_companies_count_baseline),
      contacts_count_baseline:
        editing.contacts_count_baseline === null || editing.contacts_count_baseline === "" ? null : Number(editing.contacts_count_baseline),
      one_sentence_pitch: String(editing.one_sentence_pitch ?? "").trim() || null,
      product_description: String(editing.product_description ?? "").trim() || null,
      company_profile_text: String(editing.company_profile_text ?? "").trim() || null,
      cjm_json: cjm
    };

    const res = await supabase.from("sales_hypotheses").update(payload).eq("id", id);
    if (res.error) return setStatus(`Update error: ${res.error.message}`);
    await load();
    setStatus("Saved.");
  }

  // Initialize SmartLead campaign selection from the hypothesis record.
  useEffect(() => {
    if (!editing || smartleadCampaignTouchedRef.current || smartleadCampaignDraftTouchedRef.current) return;
    const fromDb = Array.isArray(editing?.smartlead_campaign_ids) ? editing.smartlead_campaign_ids : [];
    const next = fromDb.map((x: any) => String(x)).filter(Boolean);
    setSmartleadCampaignIds(next);
    setSmartleadCampaignDraft(next);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [editing?.id, editing?.smartlead_campaign_ids]);

  // Load SmartLead report for real activity data
  useEffect(() => {
    if (!supabase) return;
    const t = setTimeout(async () => {
      try {
        await loadSmartleadReport();
      } catch (e) {
        console.warn("SmartLead report load failed:", e);
      }
    }, 1500);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase, smartleadCampaignIds.join(",")]);

  // Load Daily Activity Stats
  useEffect(() => {
    if (!supabase) return;
    const t = setTimeout(() => void loadDailyActivityStats(), 1000);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase]);

  async function loadDailyActivityStats() {
    if (!supabase) return;
    if (!id) return;

    setDailyActivityLoading(true);
    try {
      const sess = await supabase.auth.getSession();
      const token = sess.data.session?.access_token ?? "";
      if (!token) throw new Error("Not signed in.");

      const res = await fetch("/api/analytics/daily-stats", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({ hypothesisId: id, days: 30 })
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || "Failed to fetch daily stats");

      setDailyActivity(Array.isArray(json.stats) ? json.stats : []);
    } catch (e) {
      console.error("loadDailyActivityStats error:", e);
    } finally {
      setDailyActivityLoading(false);
    }
  }

  async function deleteHypothesis() {
    if (!supabase) return;
    const title = String(h?.title ?? "").trim() || id;
    if (!confirm(`Delete hypothesis "${title}"?\n\nThis will delete the hypothesis and all related rows (VP, roles, companies, check-ins, calls links).`)) return;
    setStatus("Deleting hypothesis...");
    const res = await supabase.from("sales_hypotheses").delete().eq("id", id);
    if (res.error) return setStatus(`Delete error: ${res.error.message}`);
    setStatus("Deleted. Redirecting...");
    window.location.href = "/hypotheses";
  }

  async function createCheckin() {
    if (!supabase) return;
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) return setStatus("Not signed in.");
    if (!ciWeekStart) return setStatus("week_start is required");

    setStatus("Saving check-in...");
    // Fix: channels are owned by the hypothesis; keep them persisted even if user only submits a check-in.
    // This prevents "channels disappear after refresh" when cjm_json wasn't saved explicitly.
    const cjmToPersist: any = { ...(cjm as any) };
    const hypSave = await supabase.from("sales_hypotheses").update({ cjm_json: cjmToPersist }).eq("id", id);
    if (hypSave.error) return setStatus(`Hypothesis update (channels) error: ${hypSave.error.message}`);

    const channels = (cjm.channels ?? []).filter(Boolean);
    const per_channel: any = {};
    for (const ch of channels) {
      const v = ciPerChannel[ch] ?? { activity: "", results: "", metrics: {} };

      // Channel-specific metrics
      const channelMetricIds = channelMetricIdsBySlug[ch] ?? [];
      const channelMetrics: Record<string, any> = {};
      for (const mid of channelMetricIds) {
        const m = allMetrics.find((x: any) => String(x.id) === String(mid)) ?? null;
        const slug = String(m?.slug ?? "");
        if (!slug) continue;
        const raw = String(v.metrics?.[slug] ?? "").trim();
        if (!raw) {
          channelMetrics[slug] = null;
          continue;
        }
        if (String(m?.input_type ?? "number") === "number") {
          const n = Number(raw);
          channelMetrics[slug] = Number.isFinite(n) ? n : raw;
        } else {
          channelMetrics[slug] = raw;
        }
      }

      per_channel[ch] = {
        activity: String(v.activity ?? ""),
        results: String(v.results ?? ""),
        metrics: channelMetrics
      };
    }

    const selectedBySlug = new Map<string, any>();
    for (const x of selectedMetrics) {
      const m = x?.metric ?? null;
      if (!m?.slug) continue;
      selectedBySlug.set(String(m.slug), m);
    }
    const metrics: any = {};
    for (const [slug, m] of selectedBySlug.entries()) {
      const raw = String(ciMetricValues[slug] ?? "").trim();
      if (!raw) {
        metrics[slug] = null;
        continue;
      }
      if (String(m.input_type ?? "number") === "number") {
        const n = Number(raw);
        metrics[slug] = Number.isFinite(n) ? n : raw;
      } else {
        metrics[slug] = raw;
      }
    }
    const payload: any = {
      hypothesis_id: id,
      week_start: ciWeekStart,
      opps_in_progress_count: ciOpps === "" ? null : Number(ciOpps),
      tal_companies_count: ciTal === "" ? null : Number(ciTal),
      contacts_count: ciContacts === "" ? null : Number(ciContacts),
      notes: ciNotes.trim() || null,
      blockers: ciBlockers.trim() || null,
      next_steps: ciNextSteps.trim() || null,
      channel_activity_json: {
        channels,
        per_channel
      },
      metrics_snapshot_json: {
        metrics
      }
    };
    const res = await supabase.from("sales_hypothesis_checkins").upsert(payload, { onConflict: "hypothesis_id,week_start" });
    if (res.error) return setStatus(`Check-in error: ${res.error.message}`);
    await load();
    setStatus("Check-in saved.");
  }

  async function deleteCheckin(weekStart: string) {
    if (!supabase) return;
    if (!confirm(`Delete check-in for week_start=${weekStart}?`)) return;
    setStatus("Deleting check-in...");
    const res = await supabase.from("sales_hypothesis_checkins").delete().match({ hypothesis_id: id, week_start: weekStart });
    if (res.error) return setStatus(`Delete error: ${res.error.message}`);
    await load();
    setStatus("Deleted.");
  }

  function editCheckin(weekStart: string) {
    const c = (checkins ?? []).find((x: any) => String(x.week_start) === String(weekStart)) ?? null;
    if (!c) return;
    setCiWeekStart(String(c.week_start ?? ""));
    setCiOpps(typeof c.opps_in_progress_count === "number" ? c.opps_in_progress_count : "");
    setCiTal(typeof c.tal_companies_count === "number" ? c.tal_companies_count : "");
    setCiContacts(typeof c.contacts_count === "number" ? c.contacts_count : "");
    setCiNotes(String(c.notes ?? ""));
    setCiBlockers(String(c.blockers ?? ""));
    setCiNextSteps(String(c.next_steps ?? ""));

    const per = c?.channel_activity_json?.per_channel ?? {};
    const nextPer: any = {};
    for (const slug of (cjm.channels ?? [])) {
      const v = per?.[slug] ?? {};
      nextPer[slug] = {
        activity: String(v?.activity ?? ""),
        results: String(v?.results ?? ""),
        metrics: (() => {
          const m = v?.metrics ?? {};
          const out: Record<string, string> = {};
          if (m && typeof m === "object") {
            for (const [k, vv] of Object.entries(m)) out[String(k)] = vv == null ? "" : String(vv);
          }
          return out;
        })()
      };
    }
    setCiPerChannel(nextPer);

    const hypMetrics = c?.metrics_snapshot_json?.metrics ?? {};
    if (hypMetrics && typeof hypMetrics === "object") {
      const next: Record<string, string> = {};
      for (const [k, v] of Object.entries(hypMetrics)) next[String(k)] = v == null ? "" : String(v);
      setCiMetricValues(next);
    } else {
      setCiMetricValues({});
    }

    // open details panel to make it obvious which week is being edited
    setWeeklyExpanded(String(c.week_start ?? ""));
  }

  async function linkCall() {
    if (!supabase) return;
    const cid = callIdToLink.trim();
    if (!cid) return setStatus("call_id is required");
    setStatus("Linking call...");
    const res = await supabase.from("sales_hypothesis_calls").upsert(
      { hypothesis_id: id, call_id: cid, tag: callTag.trim() || null, notes: callNotes.trim() || null },
      { onConflict: "hypothesis_id,call_id" }
    );
    if (res.error) return setStatus(`Link error: ${res.error.message}`);
    setCallIdToLink("");
    setCallTag("");
    setCallNotes("");
    await load();
    setStatus("Call linked.");
  }

  async function quickLinkCall(callId: string) {
    if (!supabase) return;
    const meta = recentLinkMeta[String(callId)] ?? { tag: "", notes: "" };
    setStatus("Linking call...");
    const res = await supabase.from("sales_hypothesis_calls").upsert(
      { hypothesis_id: id, call_id: callId, tag: meta.tag.trim() || null, notes: meta.notes.trim() || null },
      { onConflict: "hypothesis_id,call_id" }
    );
    if (res.error) return setStatus(`Link error: ${res.error.message}`);
    await load();
    setStatus("Call linked.");
  }

  async function saveLinkedCallMeta(callId: string) {
    if (!supabase) return;
    const meta = linkedEditMeta[String(callId)] ?? { tag: "", notes: "" };
    setStatus("Saving call meta...");
    const res = await supabase
      .from("sales_hypothesis_calls")
      .upsert(
        { hypothesis_id: id, call_id: callId, tag: meta.tag.trim() || null, notes: meta.notes.trim() || null },
        { onConflict: "hypothesis_id,call_id" }
      );
    if (res.error) return setStatus(`Save meta error: ${res.error.message}`);
    await load();
    setStatus("Saved.");
  }

  async function unlinkCall(callId: string) {
    if (!supabase) return;
    if (!confirm("Unlink call from this hypothesis?")) return;
    setStatus("Unlinking call...");
    const res = await supabase.from("sales_hypothesis_calls").delete().match({ hypothesis_id: id, call_id: callId });
    if (res.error) return setStatus(`Unlink error: ${res.error.message}`);
    await load();
    setStatus("Unlinked.");
  }

  function roleLabel(r: any) {
    return String(r?.name ?? "—");
  }

  function companyLabel(c: any) {
    const v = c?.vertical_name ?? "—";
    const sv = c?.sub_vertical ? ` / ${c.sub_vertical}` : "";
    const reg = c?.region ? ` · ${c.region}` : "";
    const size = c?.size_bucket ? ` · ${c.size_bucket}` : "";
    return `${v}${sv}${reg}${size}`;
  }

  function vpKey(roleId: string, companyId: string) {
    return `${String(roleId)}:${String(companyId)}`;
  }

  function parseLines(v: string): string[] {
    return String(v ?? "")
      .split("\n")
      .map((x) => x.trim())
      .filter(Boolean);
  }

  function renderVpPreview(v: any) {
    const value = String(v?.value_proposition ?? v?.statement ?? "").trim();
    return (
      <div>
        {value ? (
          <div style={{ fontSize: 13, lineHeight: 1.35, whiteSpace: "pre-wrap" }}>{value}</div>
        ) : (
          <div className="muted2" style={{ fontSize: 12, marginBottom: 8 }}>No VP statement yet.</div>
        )}
      </div>
    );
  }

  async function addRole() {
    if (!supabase) return;
    if (!roleToAdd) return setStatus("Pick a role to add");
    setStatus("Adding role...");
    const res = await supabase.from("sales_hypothesis_roles").upsert({ hypothesis_id: id, role_id: roleToAdd });
    if (res.error) return setStatus(`add role error: ${res.error.message}`);
    setRoleToAdd("");
    await load();
    setStatus("Role added.");
  }

  async function removeRole(roleId: string) {
    if (!supabase) return;
    if (!confirm("Remove role from this hypothesis?")) return;
    setStatus("Removing role...");
    const res = await supabase.from("sales_hypothesis_roles").delete().match({ hypothesis_id: id, role_id: roleId });
    if (res.error) return setStatus(`remove role error: ${res.error.message}`);
    await load();
    setStatus("Role removed.");
  }

  async function addCompany() {
    if (!supabase) return;
    if (!companyToAdd) return setStatus("Pick a company profile to add");
    setStatus("Adding company...");
    const res = await supabase
      .from("sales_hypothesis_company_profiles")
      .upsert({ hypothesis_id: id, company_profile_id: companyToAdd });
    if (res.error) return setStatus(`add company error: ${res.error.message}`);
    setCompanyToAdd("");
    await load();
    setStatus("Company added.");
  }

  async function removeCompany(companyId: string) {
    if (!supabase) return;
    if (!confirm("Remove company profile from this hypothesis?")) return;
    setStatus("Removing company...");
    const res = await supabase
      .from("sales_hypothesis_company_profiles")
      .delete()
      .match({ hypothesis_id: id, company_profile_id: companyId });
    if (res.error) return setStatus(`remove company error: ${res.error.message}`);
    await load();
    setStatus("Company removed.");
  }

  function startEditVp(roleId: string, companyId: string) {
    const key = vpKey(roleId, companyId);
    setVpEditKey(key);
    setVpDraft(parseVp(vpByKey[key] ?? {}));
  }

  function startEditPain(roleId: string, companyId: string) {
    const key = vpKey(roleId, companyId);
    setPainEditKey(key);
    const v = painByKey[key] ?? {};
    setPainDraft({
      pain_points: String(v?.pain_points ?? "").trim(),
      product_solution: String(v?.product_solution ?? "").trim()
    });
  }

  async function savePain() {
    if (!supabase) return;
    if (!painEditKey) return;
    const [roleId, companyId] = painEditKey.split(":");
    if (!roleId || !companyId) return;
    setStatus("Saving pains...");
    const payload: any = {
      hypothesis_id: id,
      role_id: roleId,
      company_profile_id: companyId,
      pain_json: {
        pain_points: String(painDraft?.pain_points ?? "").trim(),
        product_solution: String(painDraft?.product_solution ?? "").trim()
      }
    };
    const res = await supabase.from("sales_hypothesis_pains").upsert(payload);
    if (res.error) return setStatus(`save pains error: ${res.error.message}`);
    setPainByKey((prev) => ({ ...prev, [painEditKey]: payload.pain_json }));
    setStatus("Pains saved.");
  }

  async function saveVp() {
    if (!supabase) return;
    if (!vpEditKey) return;
    const [roleId, companyId] = vpEditKey.split(":");
    if (!roleId || !companyId) return;
    if (!String(vpDraft.value_proposition ?? "").trim()) {
      setStatus("VP statement is required. Fill 'Value proposition statement' before saving.");
      return;
    }
    setStatus("Saving VP...");
    const payload: any = {
      hypothesis_id: id,
      role_id: roleId,
      company_profile_id: companyId,
      vp_json: vpDraft
    };
    const res = await supabase.from("sales_hypothesis_vps").upsert(payload);
    if (res.error) return setStatus(`save VP error: ${res.error.message}`);
    setVpByKey((prev) => ({ ...prev, [vpEditKey]: vpDraft }));
    setStatus("VP saved.");
  }

  async function addMetric() {
    if (!supabase) return;
    if (!metricToAdd) return setStatus("Pick a metric to add");
    setStatus("Adding metric...");
    const res = await supabase.from("sales_hypothesis_metrics").upsert({ hypothesis_id: id, metric_id: metricToAdd });
    if (res.error) return setStatus(`add metric error: ${res.error.message}`);
    setMetricToAdd("");
    await load();
    setStatus("Metric added.");
  }

  async function removeMetric(metricId: string) {
    if (!supabase) return;
    if (!confirm("Remove metric from this hypothesis?")) return;
    setStatus("Removing metric...");
    const res = await supabase.from("sales_hypothesis_metrics").delete().match({ hypothesis_id: id, metric_id: metricId });
    if (res.error) return setStatus(`remove metric error: ${res.error.message}`);
    await load();
    setStatus("Metric removed.");
  }

  const checkins = Array.isArray(bundle?.checkins) ? bundle!.checkins : [];
  const calls = Array.isArray(bundle?.calls) ? bundle!.calls : [];
  const h = bundle?.hypothesis ?? null;

  const ownerDisplay = useMemo(() => {
    const uid = String(h?.owner_user_id ?? "").trim();
    const email = String(h?.owner_email ?? "").trim().toLowerCase();
    if (uid && userById.has(uid)) return userLabel(userById.get(uid) ?? null);
    if (email && userByEmail.has(email)) return userLabel(userByEmail.get(email) ?? null);
    return email || uid || "—";
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [h?.owner_user_id, h?.owner_email, userById, userByEmail]);

  const filteredUsers = useMemo(() => {
    const needle = userQ.trim().toLowerCase();
    if (!needle) return allUsers.slice();
    return allUsers.filter((u) => {
      const e = String(u.email ?? "").toLowerCase();
      const n = String(u.display_name ?? "").toLowerCase();
      return e.includes(needle) || n.includes(needle) || String(u.user_id).toLowerCase().includes(needle);
    });
  }, [allUsers, userQ]);

  // Ensure check-in per-channel inputs always include current hypothesis channels
  useEffect(() => {
    const channels = (cjm.channels ?? []).filter(Boolean);
    setCiPerChannel((prev) => {
      const next: any = { ...prev };
      for (const ch of channels) {
        if (!next[ch]) next[ch] = { activity: "", results: "" };
      }
      // don't delete old keys automatically (keeps drafts stable)
      return next;
    });
  }, [cjm.channels]);

  const recentFiltered = useMemo(() => {
    const needle = recentCallsQ.trim().toLowerCase();
    const linked = new Set<string>(calls.map((c) => String(c.call_id)));
    const xs = recentCalls.filter((c) => !linked.has(String(c.id)));
    if (!needle) return xs;
    return xs.filter((c) => {
      const t = String(c.title ?? "").toLowerCase();
      const o = String(c.owner_email ?? "").toLowerCase();
      return t.includes(needle) || o.includes(needle) || String(c.id).toLowerCase().includes(needle);
    });
  }, [recentCalls, recentCallsQ, calls]);

  const channelNameBySlug = useMemo(() => {
    const m = new Map<string, string>();
    for (const c of channelOptions) m.set(c.slug, c.name || c.slug);
    return m;
  }, [channelOptions]);

  return (
    <main>
      <AppTopbar
        title={h?.title || "Hypothesis"}
        subtitle={`${ownerDisplay && ownerDisplay !== "—" ? `Owner: ${ownerDisplay} · ` : ""}${id}`}
      />

      <div className="page" style={{ marginTop: 12 }}>
        <div className="btnRow" style={{ justifyContent: "flex-end" }}>
          <a className="btn" href="/hypotheses">Back to hypotheses</a>
          <button
            className="btn"
            onClick={deleteHypothesis}
            style={{ borderColor: "rgba(255,80,80,0.6)", color: "rgba(255,160,160,0.95)" }}
          >
            Delete
          </button>
          <button className="btn btnPrimary" onClick={saveHypothesis}>Save</button>
        </div>
      </div>

      <div className="page grid">
        {status ? (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardBody">
              <div className="notice">{status}</div>
            </div>
          </div>
        ) : null}

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">1) Performance</div>
              <div className="cardDesc">
                Funnel metrics and activity trend (synced from SmartLead).
              </div>
            </div>
            <div className="btnRow">
            </div>
          </div>
          <div className="cardBody">
            {/* Activity Graph */}
            <div style={{ marginTop: 24 }}>
              <ActivityLines
                title="Activity (Daily)"
                weeks={activityChartData?.weeks || []}
                series={activityChartData?.series || []}
                right={
                  <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
                    <span className="tag">Emails: {activityChartData?.totals?.emails ?? 0}</span>
                    <span className="tag">LinkedIn: {activityChartData?.totals?.linkedin ?? 0}</span>
                    <span className="tag">Replies: {activityChartData?.totals?.replies ?? 0}</span>
                  </div>
                }
              />
            </div>

            {/* Progress Chart removed - was duplicate/unwanted visualization */}
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">2) Description</div>
              <div className="cardDesc">
                High-level setup (owner, scope, channels, metrics). {sections.description ? "Expanded." : "Collapsed."}
              </div>
            </div>
            <div className="btnRow">
              <button className="btn" onClick={() => toggleSection("description")}>
                {sections.description ? "Collapse" : "Expand"}
              </button>
              <a className="btn" href="/icp/matrix">VP matrix</a>
              <details className="popover">
                <summary className="btn">Actions</summary>
                <div className="card popoverPanel">
                  <div className="cardBody">
                  </div>
                </div>
              </details>
            </div>
          </div>
          <div className="cardBody">
            {!sections.description ? (
              <div className="grid">
                <div style={{ gridColumn: "span 12" }}>
                  <table className="table">
                    <tbody>
                      <tr><td><b>Status</b></td><td className="mono">{String(h?.status ?? "—")}</td></tr>
                      <tr><td><b>Priority</b></td><td className="mono">{String(h?.priority ?? "—")}</td></tr>
                      <tr><td><b>Owner</b></td><td>{ownerDisplay}</td></tr>
                      <tr><td><b>Timebox</b></td><td className="mono">{String(h?.timebox_days ?? "—")}d</td></tr>
                      <tr><td><b>Pricing model</b></td><td className="mono">{String(h?.pricing_model ?? "—")}</td></tr>
                      <tr><td><b>Roles × Companies</b></td><td className="mono">{selectedRoles.length} × {selectedCompanies.length}</td></tr>
                      <tr><td><b>Channels</b></td><td>{(cjm.channels ?? []).length ? (cjm.channels ?? []).map((x) => channelNameBySlug.get(String(x)) ?? String(x)).join(", ") : "—"}</td></tr>
                      <tr><td><b>Hypothesis metrics</b></td><td className="mono">{selectedMetrics.length}</td></tr>
                      <tr><td><b>Linked calls</b></td><td className="mono">{calls.length}</td></tr>
                      <tr><td><b>Last updated</b></td><td className="mono">{h?.updated_at ? isoDate(h.updated_at) : "—"}</td></tr>
                    </tbody>
                  </table>
                  <div className="muted2" style={{ fontSize: 12, marginTop: 10 }}>
                    Expand this section to edit.
                  </div>
                </div>
              </div>
            ) : !editing ? <div className="muted2">Loading...</div> : (
              <div className="grid formGridTight">
                <div style={{ gridColumn: "span 7" }} className="subcard">
                  <div className="subcardTitle">Core</div>
                  <div className="grid formGridTight">
                    <div style={{ gridColumn: "span 12" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Title *</label>
                      <input
                        className="input"
                        value={editing.title ?? ""}
                        onChange={(e) => setEditing({ ...editing, title: e.target.value })}
                        onBlur={() => autosaveHypothesisOnBlur({ title: editing.title })}
                      />
                      <div className="helpInline">
                        v<span className="mono">{editing.version ?? 1}</span> · parent <span className="mono">{editing.parent_hypothesis_id ?? "—"}</span>
                      </div>
                    </div>
                    <div style={{ gridColumn: "span 6" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Status</label>
                      <select
                        className="select"
                        value={editing.status ?? "draft"}
                        onChange={(e) => setEditing({ ...editing, status: e.target.value })}
                        onBlur={() => autosaveHypothesisOnBlur({ status: editing.status })}
                      >
                        <option value="draft">draft</option>
                        <option value="active">active</option>
                        <option value="paused">paused</option>
                        <option value="won">won</option>
                        <option value="lost">lost</option>
                      </select>
                    </div>
                    <div style={{ gridColumn: "span 6" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Priority</label>
                      <input
                        className="input"
                        type="number"
                        value={editing.priority ?? 0}
                        onChange={(e) => setEditing({ ...editing, priority: Number(e.target.value || 0) })}
                        onBlur={() => autosaveHypothesisOnBlur({ priority: editing.priority })}
                      />
                    </div>
                    <div style={{ gridColumn: "span 12" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Owner</label>
                      {!allUsers.length ? (
                        <div className="notice" style={{ marginBottom: 8 }}>
                          Users list is not available yet. Apply SQL from <span className="mono">99-applications/sales/supabase/schema-hypotheses.sql</span> (function <span className="mono">sales_list_users()</span>).
                        </div>
                      ) : null}
                      <div className="grid formGridTight">
                        <div style={{ gridColumn: "span 6" }}>
                          <input
                            className="input"
                            value={userQ}
                            onChange={(e) => setUserQ(e.target.value)}
                            placeholder="Search users by name/email..."
                          />
                        </div>
                        <div style={{ gridColumn: "span 6" }}>
                          <select
                            className="select"
                            disabled={!allUsers.length}
                            value={String(editing.owner_user_id ?? "")}
                            onChange={(e) => {
                              const uid = String(e.target.value || "");
                              const u = userById.get(uid) ?? null;
                              if (!uid || !u) return;
                              setEditing({ ...editing, owner_user_id: uid, owner_email: u.email ?? null });
                            }}
                            onBlur={() => autosaveHypothesisOnBlur({ owner_user_id: editing.owner_user_id, owner_email: editing.owner_email })}
                          >
                            {(() => {
                              const currentUid = String(editing.owner_user_id ?? "");
                              const current = currentUid ? (userById.get(currentUid) ?? null) : null;
                              const rest = filteredUsers.filter((u) => String(u.user_id) !== currentUid);
                              return (
                                <>
                                  {current ? (
                                    <option value={currentUid}>{userLabel(current)}</option>
                                  ) : (
                                    <option value={currentUid}>—</option>
                                  )}
                                  {rest.map((u) => (
                                    <option key={String(u.user_id)} value={String(u.user_id)}>
                                      {userLabel(u)}
                                    </option>
                                  ))}
                                </>
                              );
                            })()}
                          </select>
                        </div>
                      </div>
                      <div className="helpInline">
                        Collaborative editing: any authenticated user can edit hypotheses. Owner is for attribution; only admins can change{" "}
                        <span className="mono">owner_user_id</span>.
                      </div>
                    </div>
                  </div>
                </div>

                <div style={{ gridColumn: "span 5" }} className="subcard">
                  <div className="subcardTitle">Experiment</div>
                  <div className="grid formGridTight">
                    <div style={{ gridColumn: "span 12" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Pricing model</label>
                      <textarea
                        className="textarea textareaAutoGrow"
                        value={editing.pricing_model ?? ""}
                        onChange={(e) => setEditing({ ...editing, pricing_model: e.target.value })}
                        onInput={(e) => {
                          const el = e.currentTarget;
                          el.style.height = "auto";
                          el.style.height = `${el.scrollHeight}px`;
                        }}
                        onBlur={() => autosaveHypothesisOnBlur({ pricing_model: editing.pricing_model })}
                        placeholder="e.g. per-app subscription / usage-based / per-scan"
                        style={{ minHeight: 44 }}
                      />
                    </div>
                    <div style={{ gridColumn: "span 5" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Timebox (days) *</label>
                      <input
                        className="input"
                        type="number"
                        value={editing.timebox_days ?? 28}
                        onChange={(e) => setEditing({ ...editing, timebox_days: Number(e.target.value || 0) })}
                        onBlur={() => autosaveHypothesisOnBlur({ timebox_days: editing.timebox_days })}
                      />
                    </div>
                    <div style={{ gridColumn: "span 7" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Win criteria *</label>
                      <textarea
                        className="textarea textareaAutoGrow"
                        value={editing.win_criteria ?? ""}
                        onChange={(e) => setEditing({ ...editing, win_criteria: e.target.value })}
                        onInput={(e) => {
                          const el = e.currentTarget;
                          el.style.height = "auto";
                          el.style.height = `${el.scrollHeight}px`;
                        }}
                        onBlur={() => autosaveHypothesisOnBlur({ win_criteria: editing.win_criteria })}
                        style={{ minHeight: 44 }}
                      />
                    </div>
                    <div style={{ gridColumn: "span 12" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Kill criteria *</label>
                      <textarea
                        className="textarea textareaAutoGrow"
                        value={editing.kill_criteria ?? ""}
                        onChange={(e) => setEditing({ ...editing, kill_criteria: e.target.value })}
                        onInput={(e) => {
                          const el = e.currentTarget;
                          el.style.height = "auto";
                          el.style.height = `${el.scrollHeight}px`;
                        }}
                        onBlur={() => autosaveHypothesisOnBlur({ kill_criteria: editing.kill_criteria })}
                        style={{ minHeight: 44 }}
                      />
                    </div>
                  </div>
                </div>
                <div style={{ gridColumn: "span 12" }} className="subcard">
                  <div className="subcardTitle">Messaging</div>
                  <div className="grid formGridTight">
                    <div style={{ gridColumn: "span 12" }}>
                      <label className="muted" style={{ fontSize: 13 }}>One sentence pitch</label>
                      <textarea
                        className="textarea"
                        style={{ minHeight: 44 }}
                        value={editing.one_sentence_pitch ?? ""}
                        onChange={(e) => setEditing({ ...editing, one_sentence_pitch: e.target.value })}
                        onBlur={() => autosaveHypothesisOnBlur({ one_sentence_pitch: editing.one_sentence_pitch })}
                        placeholder="One sentence (you can resize if it needs to be longer)."
                      />
                    </div>
                    <div style={{ gridColumn: "span 12" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Product description</label>
                      <textarea
                        className="textarea"
                        value={editing.product_description ?? ""}
                        onChange={(e) => setEditing({ ...editing, product_description: e.target.value })}
                        onBlur={() => autosaveHypothesisOnBlur({ product_description: editing.product_description })}
                      />
                    </div>
                  </div>
                </div>

                <div style={{ gridColumn: "span 12" }} className="subcard">
                  <div className="subcardTitle">Scope (VP matrix)</div>
                  <div className="helpInline" style={{ marginTop: -4, marginBottom: 10 }}>
                    Pick Roles and Company Profiles, then edit VP per intersection. Library <a href="/icp/matrix">VP matrix</a> shows aggregated outputs.
                  </div>

                  <div className="grid formGridTight" style={{ gridTemplateColumns: "repeat(12,1fr)", alignItems: "end" }}>
                    <div style={{ gridColumn: "span 5" }}>
                      <label className="muted2" style={{ fontSize: 12 }}>Add role</label>
                      <select className="select" value={roleToAdd} onChange={(e) => setRoleToAdd(e.target.value)}>
                        <option value="">—</option>
                        {allRoles
                          .filter((r: any) => !selectedRoles.some((x: any) => String(x.role_id) === String(r.id)))
                          .map((r: any) => (
                            <option key={String(r.id)} value={String(r.id)}>
                              {roleLabel(r)}
                            </option>
                          ))}
                      </select>
                    </div>
                    <div style={{ gridColumn: "span 1" }}>
                      <button className="btn btnPrimary" onClick={addRole}>Add</button>
                    </div>

                    <div style={{ gridColumn: "span 5" }}>
                      <label className="muted2" style={{ fontSize: 12 }}>Add company profile</label>
                      <select className="select" value={companyToAdd} onChange={(e) => setCompanyToAdd(e.target.value)}>
                        <option value="">—</option>
                        {allCompanies
                          .filter((c: any) => !selectedCompanies.some((x: any) => String(x.company_profile_id) === String(c.id)))
                          .map((c: any) => (
                            <option key={String(c.id)} value={String(c.id)}>
                              {companyLabel(c)}
                            </option>
                          ))}
                      </select>
                    </div>
                    <div style={{ gridColumn: "span 1" }}>
                      <button className="btn btnPrimary" onClick={addCompany}>Add</button>
                    </div>
                  </div>

                  <div className="grid formGridTight" style={{ gridTemplateColumns: "repeat(12,1fr)", marginTop: 12 }}>
                    <div style={{ gridColumn: "span 6" }}>
                      <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Selected roles</div>
                      <div className="btnRow" style={{ flexWrap: "wrap" }}>
                        {selectedRoles.map((x: any) => (
                          <span key={String(x.role_id)} className="pill" style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                            <span>{roleLabel(x.role)}</span>
                            <button className="btn" style={{ padding: "4px 8px" }} onClick={() => removeRole(String(x.role_id))}>×</button>
                          </span>
                        ))}
                        {!selectedRoles.length ? <span className="muted2">No roles selected yet.</span> : null}
                      </div>
                    </div>
                    <div style={{ gridColumn: "span 6" }}>
                      <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Selected company profiles</div>
                      <div className="btnRow" style={{ flexWrap: "wrap" }}>
                        {selectedCompanies.map((x: any) => (
                          <span key={String(x.company_profile_id)} className="pill" style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                            <span>{companyLabel(x.company)}</span>
                            <button className="btn" style={{ padding: "4px 8px" }} onClick={() => removeCompany(String(x.company_profile_id))}>×</button>
                          </span>
                        ))}
                        {!selectedCompanies.length ? <span className="muted2">No company profiles selected yet.</span> : null}
                      </div>
                    </div>
                  </div>

                  <div className="subcard" style={{ overflowX: "auto", marginTop: 12 }}>
                    <div className="subcardTitle" style={{ marginBottom: 8 }}>VP grid</div>
                    <table className="table" style={{ minWidth: 900 }}>
                      <thead>
                        <tr>
                          <th style={{ width: 320 }}>Company profile</th>
                          {selectedRoles.map((r: any) => (
                            <th key={String(r.role_id)} style={{ minWidth: 220 }}>
                              {roleLabel(r.role)}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {selectedCompanies.map((c: any) => (
                          <tr key={String(c.company_profile_id)}>
                            <td>
                              <b>{companyLabel(c.company)}</b>
                              <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{String(c.company_profile_id)}</div>
                            </td>
                            {selectedRoles.map((r: any) => {
                              const key = vpKey(String(r.role_id), String(c.company_profile_id));
                              const v = vpByKey[key] ?? {};
                              const hasVp = !!String(v?.value_proposition ?? v?.statement ?? "").trim();
                              return (
                                <td
                                  key={key}
                                  style={
                                    hasVp
                                      ? undefined
                                      : {
                                        border: "1px solid rgba(255, 99, 71, 0.35)",
                                        background: "rgba(255, 99, 71, 0.06)"
                                      }
                                  }
                                >
                                  {renderVpPreview(v)}
                                  <div className="btnRow" style={{ marginTop: 8, justifyContent: "flex-start" }}>
                                    <button className="btn" onClick={() => startEditVp(String(r.role_id), String(c.company_profile_id))}>Edit</button>
                                  </div>
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                        {!selectedCompanies.length || !selectedRoles.length ? (
                          <tr>
                            <td colSpan={1 + Math.max(1, selectedRoles.length)} className="muted2">
                              Select at least 1 role and 1 company profile to edit VP.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>

                  <div className="subcard" style={{ overflowX: "auto", marginTop: 12 }}>
                    <div className="subcardTitle" style={{ marginBottom: 8 }}>Pains (problem → solution)</div>
                    <div className="helpInline" style={{ marginTop: -4, marginBottom: 10 }}>
                      One cell = 2 fields: pain points + how we solve. Click Edit to update both.
                    </div>
                    <table className="table" style={{ minWidth: 980 }}>
                      <thead>
                        <tr>
                          <th style={{ width: 320 }}>Company profile</th>
                          {selectedRoles.map((r: any) => (
                            <th key={String(r.role_id)} style={{ minWidth: 260 }}>
                              {roleLabel(r.role)}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {selectedCompanies.map((c: any) => (
                          <tr key={`pain:${String(c.company_profile_id)}`}>
                            <td>
                              <b>{companyLabel(c.company)}</b>
                              <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{String(c.company_profile_id)}</div>
                            </td>
                            {selectedRoles.map((r: any) => {
                              const key = vpKey(String(r.role_id), String(c.company_profile_id));
                              const v = painByKey[key] ?? {};
                              const p = String(v?.pain_points ?? "").trim();
                              const s = String(v?.product_solution ?? "").trim();
                              const has = !!p || !!s;
                              return (
                                <td
                                  key={`paincell:${key}`}
                                  style={
                                    has
                                      ? undefined
                                      : {
                                        border: "1px solid rgba(255, 99, 71, 0.35)",
                                        background: "rgba(255, 99, 71, 0.06)"
                                      }
                                  }
                                >
                                  {renderPairedPains(p, s)}
                                  <div className="btnRow" style={{ marginTop: 10, justifyContent: "flex-start" }}>
                                    <button className="btn" onClick={() => startEditPain(String(r.role_id), String(c.company_profile_id))}>Edit</button>
                                  </div>
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                        {!selectedCompanies.length || !selectedRoles.length ? (
                          <tr>
                            <td colSpan={1 + Math.max(1, selectedRoles.length)} className="muted2">
                              Select at least 1 role and 1 company profile to edit pains.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>

                  {vpEditKey ? (
                    <div className="subcard" style={{ marginTop: 12 }}>
                      <div className="subcardTitle">Edit VP</div>
                      <div className="helpInline">Cell: <span className="mono">{vpEditKey}</span></div>
                      <div className="grid formGridTight" style={{ marginTop: 10 }}>
                        <div style={{ gridColumn: "span 12" }}>
                          <label className="muted" style={{ fontSize: 13 }}>Value proposition statement *</label>
                          <textarea
                            className="textarea"
                            value={String(vpDraft.value_proposition ?? "")}
                            onChange={(e) => setVpDraft({ ...vpDraft, value_proposition: e.target.value })}
                            placeholder="One clear sentence (or 2 lines max): what unique value this role gets in this segment, and why they should care."
                          />
                          {!String(vpDraft.value_proposition ?? "").trim() ? (
                            <div className="helpInline">Required: write a single VP statement for this cell before saving.</div>
                          ) : null}
                        </div>
                        <div style={{ gridColumn: "span 12", justifyContent: "flex-end" }} className="btnRow">
                          <button className="btn" onClick={() => setVpEditKey(null)}>Close</button>
                          <button
                            className="btn btnPrimary"
                            onClick={saveVp}
                            disabled={!String(vpDraft.value_proposition ?? "").trim()}
                            title={!String(vpDraft.value_proposition ?? "").trim() ? "VP statement is required" : "Save VP"}
                          >
                            Save VP
                          </button>
                        </div>
                      </div>
                    </div>
                  ) : null}

                  {painEditKey ? (
                    <div className="subcard" style={{ marginTop: 12 }}>
                      <div className="subcardTitle">Edit pains</div>
                      <div className="helpInline">Cell: <span className="mono">{painEditKey}</span></div>
                      <div className="grid formGridTight" style={{ marginTop: 10 }}>
                        <div style={{ gridColumn: "span 12" }}>
                          <label className="muted" style={{ fontSize: 13 }}>Pain points (multiline)</label>
                          <textarea
                            className="textarea"
                            value={String(painDraft.pain_points ?? "")}
                            onChange={(e) => setPainDraft((p) => ({ ...p, pain_points: e.target.value }))}
                            placeholder="List pains, one per line."
                          />
                        </div>
                        <div style={{ gridColumn: "span 12" }}>
                          <label className="muted" style={{ fontSize: 13 }}>How product closes pains (multiline)</label>
                          <textarea
                            className="textarea"
                            value={String(painDraft.product_solution ?? "")}
                            onChange={(e) => setPainDraft((p) => ({ ...p, product_solution: e.target.value }))}
                            placeholder="Describe how the product addresses each pain."
                          />
                        </div>
                        <div style={{ gridColumn: "span 12", justifyContent: "flex-end" }} className="btnRow">
                          <button className="btn" onClick={() => setPainEditKey(null)}>Close</button>
                          <button className="btn btnPrimary" onClick={savePain}>Save</button>
                        </div>
                      </div>
                    </div>
                  ) : null}
                </div>

                <div style={{ gridColumn: "span 12" }} className="subcard">
                  <div className="subcardTitle">Channels</div>
                  <div className="helpInline" style={{ marginTop: -4, marginBottom: 10 }}>
                    Managed in <a href="/icp/channels">Library → Channels</a>. Weekly check-ins prompt per selected channel.
                  </div>

                  <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10, alignItems: "end" }}>
                    <div style={{ gridColumn: "span 10" }}>
                      <label className="muted2" style={{ fontSize: 12 }}>Add channel</label>
                      <select className="select" value={channelToAdd} onChange={(e) => setChannelToAdd(e.target.value)}>
                        <option value="">—</option>
                        {channelOptions
                          .filter((ch) => !(cjm.channels ?? []).includes(ch.slug))
                          .map((ch) => (
                            <option key={ch.slug} value={ch.slug}>
                              {ch.name || ch.slug} ({ch.slug})
                            </option>
                          ))}
                      </select>
                    </div>
                    <div style={{ gridColumn: "span 2" }}>
                      <button
                        className="btn btnPrimary"
                        onClick={async () => {
                          if (!channelToAdd) return;
                          await ensureChannelInLibrary(channelToAdd);
                          const next = new Set<string>(cjm.channels ?? []);
                          next.add(channelToAdd);
                          const nextCjm = { ...(cjm as any), channels: Array.from(next) };
                          setCjm(nextCjm);
                          await persistCjm(nextCjm);
                          setChannelToAdd("");
                        }}
                      >
                        Add
                      </button>
                    </div>
                  </div>

                  <div className="subcard" style={{ marginTop: 12, overflowX: "auto" }}>
                    <div className="subcardTitle" style={{ marginBottom: 8 }}>Selected channels</div>
                    <table className="table" style={{ minWidth: 980 }}>
                      <thead>
                        <tr>
                          <th>Selected channels</th>
                          <th style={{ width: 420 }}>Owners</th>
                          <th style={{ width: 380 }}>Channel metrics</th>
                          <th style={{ width: 120 }}></th>
                        </tr>
                      </thead>
                      <tbody>
                        {(cjm.channels ?? []).map((slug) => (
                          <tr key={slug}>
                            <td>
                              <b>{channelNameBySlug.get(slug) ?? slug}</b>
                              <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{slug}</div>
                            </td>
                            <td>
                              {(() => {
                                const ch = channelBySlug.get(String(slug)) ?? null;
                                const owners = channelOwnerEmailsBySlug[slug] ?? [];
                                const inputVal = channelOwnerToAddBySlug[slug] ?? "";
                                if (!ch) return <span className="muted2">Channel not found in Library yet.</span>;
                                return (
                                  <div>
                                    <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 8, alignItems: "end" }}>
                                      <div style={{ gridColumn: "span 8" }}>
                                        <select
                                          className="select"
                                          value={inputVal}
                                          disabled={!allUsers.length}
                                          onChange={(e) => {
                                            const uid = String(e.target.value || "");
                                            // Keep controlled input predictable.
                                            setChannelOwnerToAddBySlug((prev) => ({ ...prev, [slug]: "" }));
                                            if (!uid) return;
                                            // Auto-save on selection (no extra button click).
                                            void addHypothesisChannelOwner({ channelId: ch.id, channelSlug: slug, ownerUserId: uid });
                                          }}
                                        >
                                          <option value="">— select owner —</option>
                                          {allUsers
                                            .filter((u) => !owners.includes(String(u.email ?? "").trim().toLowerCase()))
                                            .map((u) => (
                                              <option key={String(u.user_id)} value={String(u.user_id)}>
                                                {userLabel(u)}
                                              </option>
                                            ))}
                                        </select>
                                      </div>
                                    </div>
                                    <div className="btnRow" style={{ flexWrap: "wrap", gap: 8, marginTop: 8, justifyContent: "flex-start" }}>
                                      {owners.map((email: string) => (
                                        <span key={email} className="tag" style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                                          <span className="mono">{userByEmail.get(String(email).toLowerCase()) ? userLabel(userByEmail.get(String(email).toLowerCase()) ?? null) : email}</span>
                                          <button
                                            className="btn"
                                            style={{ padding: "2px 8px" }}
                                            onClick={async () => {
                                              setStatus("Removing channel owner...");
                                              const res = await supabase
                                                ?.from("sales_hypothesis_channel_owners")
                                                .delete()
                                                .match({ hypothesis_id: id, channel_id: ch.id, owner_email: email });
                                              if (res?.error) return setStatus(`remove channel owner error: ${res.error.message}`);
                                              await load();
                                              setStatus("Channel owner removed.");
                                            }}
                                          >
                                            ×
                                          </button>
                                        </span>
                                      ))}
                                      {!owners.length ? <span className="muted2">—</span> : null}
                                    </div>
                                  </div>
                                );
                              })()}
                            </td>
                            <td>
                              {(() => {
                                const ch = channelBySlug.get(String(slug)) ?? null;
                                if (!ch) return <span className="muted2">Channel not found in Library yet.</span>;
                                const selected = channelMetricIdsBySlug[slug] ?? [];
                                const addVal = channelMetricAddBySlug[slug] ?? "";
                                const selectedLabels = selected
                                  .map((mid: string) => {
                                    const m = allMetrics.find((x: any) => String(x.id) === String(mid)) ?? null;
                                    return String(m?.slug ?? m?.name ?? mid);
                                  })
                                  .filter(Boolean);
                                const preview = selectedLabels.slice(0, 6);
                                const more = Math.max(0, selectedLabels.length - preview.length);
                                return (
                                  <div>
                                    <div className="btnRow" style={{ flexWrap: "wrap", gap: 6, justifyContent: "flex-start" }}>
                                      {preview.length ? (
                                        preview.map((t) => (
                                          <span key={`${slug}:preview:${t}`} className="tag" style={{ padding: "4px 8px", fontSize: 12 }}>
                                            {t}
                                          </span>
                                        ))
                                      ) : (
                                        <span className="muted2">—</span>
                                      )}
                                      {more ? <span className="muted2">+{more} more</span> : null}
                                    </div>

                                    <details style={{ marginTop: 10 }}>
                                      <summary className="muted2" style={{ cursor: "pointer" }}>
                                        Manage metrics {selected.length ? `(${selected.length})` : ""}
                                      </summary>
                                      <div style={{ marginTop: 10 }}>
                                        <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 8, alignItems: "end" }}>
                                          <div style={{ gridColumn: "span 8" }}>
                                            <select
                                              className="select"
                                              value={addVal}
                                              onChange={(e) => setChannelMetricAddBySlug({ ...channelMetricAddBySlug, [slug]: e.target.value })}
                                            >
                                              <option value="">— add metric —</option>
                                              {allMetrics
                                                .filter((m: any) => !selected.includes(String(m.id)))
                                                .map((m: any) => (
                                                  <option key={String(m.id)} value={String(m.id)}>
                                                    {String(m.name ?? m.slug ?? "Metric")} ({String(m.slug ?? "")})
                                                  </option>
                                                ))}
                                            </select>
                                          </div>
                                          <div style={{ gridColumn: "span 4" }}>
                                            <button
                                              className="btn"
                                              onClick={async () => {
                                                const mid = String(addVal || "");
                                                if (!mid) return;
                                                setStatus("Adding channel metric...");
                                                const res = await supabase?.from("sales_hypothesis_channel_metrics").upsert({
                                                  hypothesis_id: id,
                                                  channel_id: ch.id,
                                                  metric_id: mid
                                                });
                                                if (res?.error) return setStatus(`add channel metric error: ${res.error.message}`);
                                                setChannelMetricAddBySlug((prev) => ({ ...prev, [slug]: "" }));
                                                await load();
                                                setStatus("Channel metric added.");
                                              }}
                                            >
                                              Add
                                            </button>
                                          </div>
                                        </div>

                                        <div className="btnRow" style={{ flexWrap: "wrap", gap: 6, marginTop: 10, justifyContent: "flex-start" }}>
                                          {selected.map((mid: string) => {
                                            const m = allMetrics.find((x: any) => String(x.id) === String(mid)) ?? null;
                                            const label = String(m?.slug ?? m?.name ?? mid);
                                            return (
                                              <span
                                                key={mid}
                                                className="tag"
                                                style={{ display: "inline-flex", gap: 8, alignItems: "center", padding: "4px 8px", fontSize: 12 }}
                                              >
                                                <span>{label}</span>
                                                <button
                                                  className="btn"
                                                  style={{ padding: "2px 8px" }}
                                                  onClick={async () => {
                                                    setStatus("Removing channel metric...");
                                                    const res = await supabase
                                                      ?.from("sales_hypothesis_channel_metrics")
                                                      .delete()
                                                      .match({ hypothesis_id: id, channel_id: ch.id, metric_id: mid });
                                                    if (res?.error) return setStatus(`remove channel metric error: ${res.error.message}`);
                                                    await load();
                                                    setStatus("Channel metric removed.");
                                                  }}
                                                >
                                                  ×
                                                </button>
                                              </span>
                                            );
                                          })}
                                          {!selected.length ? <span className="muted2">—</span> : null}
                                        </div>

                                        {selected.length ? (
                                          <details style={{ marginTop: 10 }}>
                                            <summary className="muted2" style={{ cursor: "pointer" }}>
                                              Metric owners (weekly reports)
                                            </summary>
                                            <div style={{ marginTop: 10 }}>
                                              <table className="table">
                                                <thead>
                                                  <tr>
                                                    <th>Metric</th>
                                                    <th>Owners</th>
                                                    <th style={{ width: 260 }}>Add owner (auto)</th>
                                                  </tr>
                                                </thead>
                                                <tbody>
                                                  {selected.map((mid: string) => {
                                                    const m = allMetrics.find((x: any) => String(x.id) === String(mid)) ?? null;
                                                    const label = String(m?.name ?? m?.slug ?? mid);
                                                    const owners = channelMetricOwnerEmailsBySlug?.[slug]?.[mid] ?? [];
                                                    const addUid = channelMetricOwnerToAddBySlug?.[slug]?.[mid] ?? "";
                                                    return (
                                                      <tr key={mid}>
                                                        <td>
                                                          <b>{label}</b>
                                                          <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>
                                                            {String(m?.slug ?? "")}
                                                          </div>
                                                        </td>
                                                        <td>
                                                          <div className="btnRow" style={{ flexWrap: "wrap", gap: 6, justifyContent: "flex-start" }}>
                                                            {owners.map((email: string) => (
                                                              <span
                                                                key={email}
                                                                className="tag"
                                                                style={{
                                                                  display: "inline-flex",
                                                                  gap: 8,
                                                                  alignItems: "center",
                                                                  padding: "4px 8px",
                                                                  fontSize: 12
                                                                }}
                                                              >
                                                                <span className="mono">
                                                                  {userByEmail.get(String(email).toLowerCase())
                                                                    ? userLabel(userByEmail.get(String(email).toLowerCase()) ?? null)
                                                                    : email}
                                                                </span>
                                                                <button
                                                                  className="btn"
                                                                  style={{ padding: "2px 8px" }}
                                                                  onClick={async () => {
                                                                    setStatus("Removing metric owner...");
                                                                    const res = await supabase
                                                                      ?.from("sales_hypothesis_channel_metric_owners")
                                                                      .delete()
                                                                      .match({
                                                                        hypothesis_id: id,
                                                                        channel_id: ch.id,
                                                                        metric_id: mid,
                                                                        owner_email: String(email).toLowerCase()
                                                                      });
                                                                    if (res?.error) return setStatus(`remove metric owner error: ${res.error.message}`);
                                                                    await load();
                                                                    setStatus("Metric owner removed.");
                                                                  }}
                                                                >
                                                                  ×
                                                                </button>
                                                              </span>
                                                            ))}
                                                            {!owners.length ? <span className="muted2">—</span> : null}
                                                          </div>
                                                        </td>
                                                        <td>
                                                          <div className="btnRow" style={{ justifyContent: "flex-start" }}>
                                                            <select
                                                              className="select"
                                                              value={addUid}
                                                              disabled={!allUsers.length}
                                                              onChange={(e) => {
                                                                const uid = String(e.target.value || "");
                                                                // Clear selection immediately for better UX.
                                                                setChannelMetricOwnerToAddBySlug((prev) => ({
                                                                  ...prev,
                                                                  [slug]: { ...(prev?.[slug] ?? {}), [mid]: "" }
                                                                }));
                                                                if (!uid) return;
                                                                // Auto-save on selection (no extra button click).
                                                                void addHypothesisChannelMetricOwner({
                                                                  channelId: ch.id,
                                                                  channelSlug: slug,
                                                                  metricId: mid,
                                                                  ownerUserId: uid
                                                                });
                                                              }}
                                                            >
                                                              <option value="">— select owner —</option>
                                                              {allUsers
                                                                .filter((u) => !owners.includes(String(u.email ?? "").trim().toLowerCase()))
                                                                .map((u) => (
                                                                  <option key={String(u.user_id)} value={String(u.user_id)}>
                                                                    {userLabel(u)}
                                                                  </option>
                                                                ))}
                                                            </select>
                                                          </div>
                                                        </td>
                                                      </tr>
                                                    );
                                                  })}
                                                </tbody>
                                              </table>
                                            </div>
                                          </details>
                                        ) : null}
                                      </div>
                                    </details>
                                  </div>
                                );
                              })()}
                            </td>
                            <td>
                              <button
                                className="btn"
                                onClick={async () => {
                                  const next = new Set<string>(cjm.channels ?? []);
                                  next.delete(slug);
                                  const nextCjm = { ...(cjm as any), channels: Array.from(next) };
                                  setCjm(nextCjm);
                                  await persistCjm(nextCjm);
                                }}
                              >
                                Remove
                              </button>
                            </td>
                          </tr>
                        ))}
                        {!(cjm.channels ?? []).length ? (
                          <tr>
                            <td colSpan={4} className="muted2">No channels selected yet.</td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div style={{ gridColumn: "span 12" }} className="subcard">
                  <div className="subcardTitle">Metrics</div>
                  <div className="helpInline" style={{ marginTop: -4, marginBottom: 10 }}>
                    Pick metrics from <a href="/icp/metrics">Library → Metrics</a>. Weekly check-ins will include inputs for selected metrics.
                  </div>
                  <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10, alignItems: "end" }}>
                    <div style={{ gridColumn: "span 10" }}>
                      <label className="muted2" style={{ fontSize: 12 }}>Add metric</label>
                      <select className="select" value={metricToAdd} onChange={(e) => setMetricToAdd(e.target.value)}>
                        <option value="">—</option>
                        {allMetrics.map((m: any) => (
                          <option key={String(m.id)} value={String(m.id)}>
                            {String(m.name ?? m.slug ?? "Metric")} ({String(m.slug ?? "")})
                          </option>
                        ))}
                      </select>
                    </div>
                    <div style={{ gridColumn: "span 2" }}>
                      <button className="btn btnPrimary" onClick={addMetric}>Add</button>
                    </div>
                  </div>

                  <div className="subcard" style={{ marginTop: 12 }}>
                    <div className="subcardTitle" style={{ marginBottom: 8 }}>Selected metrics</div>
                    <div className="btnRow" style={{ flexWrap: "wrap", gap: 6, justifyContent: "flex-start" }}>
                      {selectedMetrics.map((x) => {
                        const slug = String(x.metric?.slug ?? x.metric_id);
                        const label = String(x.metric?.slug ?? x.metric?.name ?? x.metric_id);
                        return (
                          <span key={x.metric_id} className="tag" style={{ display: "inline-flex", gap: 8, alignItems: "center", padding: "4px 8px", fontSize: 12 }}>
                            <span>{label}</span>
                            <span className="muted2 mono">{slug}</span>
                            <button className="btn" style={{ padding: "2px 8px" }} onClick={() => removeMetric(String(x.metric_id))}>×</button>
                          </span>
                        );
                      })}
                      {!selectedMetrics.length ? <span className="muted2">No metrics selected yet.</span> : null}
                    </div>

                    {selectedMetrics.length ? (
                      <details style={{ marginTop: 10 }}>
                        <summary className="muted2" style={{ cursor: "pointer" }}>Show details</summary>
                        <div style={{ marginTop: 10 }}>
                          <table className="table">
                            <thead>
                              <tr>
                                <th>Metric</th>
                                <th style={{ width: 140 }}>Type</th>
                                <th style={{ width: 140 }}>Unit</th>
                              </tr>
                            </thead>
                            <tbody>
                              {selectedMetrics.map((x) => (
                                <tr key={`detail:${x.metric_id}`}>
                                  <td>
                                    <b>{String(x.metric?.name ?? x.metric?.slug ?? "Metric")}</b>
                                    <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{String(x.metric?.slug ?? "")}</div>
                                  </td>
                                  <td className="mono">{String(x.metric?.input_type ?? "number")}</td>
                                  <td className="mono">{String(x.metric?.unit ?? "—")}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </details>
                    ) : null}
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">3) Weekly check-ins</div>
              <div className="cardDesc">Baseline + weekly snapshots (counts can stay unchanged). {sections.weekly ? "Expanded." : "Collapsed."}</div>
            </div>
            <div className="btnRow">
              <button className="btn" onClick={() => toggleSection("weekly")}>
                {sections.weekly ? "Collapse" : "Expand"}
              </button>
            </div>
          </div>
          <div className="cardBody">
            {!sections.weekly ? (
              <div className="grid">
                <div style={{ gridColumn: "span 12" }}>
                  <table className="table">
                    <tbody>
                      <tr><td><b>Total check-ins</b></td><td className="mono">{checkins.length}</td></tr>
                      <tr>
                        <td><b>Latest</b></td>
                        <td className="mono">{checkins[0]?.week_start ? String(checkins[0].week_start) : "—"}</td>
                      </tr>
                      <tr><td><b>Channels</b></td><td>{(cjm.channels ?? []).length ? (cjm.channels ?? []).map((x) => channelNameBySlug.get(String(x)) ?? String(x)).join(", ") : "—"}</td></tr>
                    </tbody>
                  </table>
                  <div className="muted2" style={{ fontSize: 12, marginTop: 10 }}>
                    Expand this section to view/edit weekly check-ins.
                  </div>
                </div>
              </div>
            ) : (
              <>
                <div className="card" style={{ marginBottom: 14 }}>
                  <div className="cardHeader">
                    <div>
                      <div className="cardTitle" style={{ fontSize: 14 }}>Weekly channel stats</div>
                      <div className="cardDesc">Read-only summary built from submitted weekly check-ins.</div>
                    </div>
                  </div>
                  <div className="cardBody">
                    <table className="table">
                      <thead>
                        <tr>
                          <th style={{ width: 120 }}>Week</th>
                          <th>Channels</th>
                          <th style={{ width: 220 }}></th>
                        </tr>
                      </thead>
                      <tbody>
                        {(checkins ?? []).slice(0, 8).map((c: any) => {
                          const wk = String(c.week_start);
                          const per = c?.channel_activity_json?.per_channel ?? {};
                          const chKeys = Object.keys(per || {}).filter(Boolean);
                          const active = chKeys.filter((k) => {
                            const v = per?.[k] ?? {};
                            return String(v?.activity ?? "").trim() || String(v?.results ?? "").trim();
                          });
                          const open = weeklyExpanded === wk;
                          return (
                            <tr key={wk}>
                              <td className="mono">{wk}</td>
                              <td>
                                {active.length ? (
                                  <div className="btnRow" style={{ flexWrap: "wrap", gap: 8 }}>
                                    {active.slice(0, 6).map((x: string) => (
                                      <span key={x} className="tag">{channelNameBySlug.get(x) ?? x}</span>
                                    ))}
                                    {active.length > 6 ? <span className="muted2">+{active.length - 6} more</span> : null}
                                  </div>
                                ) : (
                                  <span className="muted2">—</span>
                                )}
                              </td>
                              <td>
                                <div className="btnRow" style={{ justifyContent: "flex-end" }}>
                                  <button className="btn" onClick={() => setWeeklyExpanded(open ? null : wk)}>
                                    {open ? "Hide" : "Details"}
                                  </button>
                                  <button className="btn" onClick={() => editCheckin(wk)}>Edit</button>
                                  <button className="btn" onClick={() => deleteCheckin(wk)}>Delete</button>
                                </div>
                              </td>
                            </tr>
                          );
                        })}
                        {!((checkins ?? []).length) ? (
                          <tr>
                            <td colSpan={3} className="muted2">No weekly check-ins yet.</td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>

                    {weeklyExpanded ? (
                      <div style={{ marginTop: 12 }}>
                        {(() => {
                          const c = (checkins ?? []).find((x: any) => String(x.week_start) === weeklyExpanded) ?? null;
                          const per = c?.channel_activity_json?.per_channel ?? {};
                          const keys = Object.keys(per || {}).filter(Boolean);
                          const active = keys.filter((k) => {
                            const v = per?.[k] ?? {};
                            return String(v?.activity ?? "").trim() || String(v?.results ?? "").trim();
                          });
                          return active.length ? (
                            <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10 }}>
                              {active.map((k) => {
                                const v = per?.[k] ?? {};
                                const activity = String(v?.activity ?? "").trim();
                                const results = String(v?.results ?? "").trim();
                                return (
                                  <div key={k} style={{ gridColumn: "span 6" }} className="card">
                                    <div className="cardBody">
                                      <div className="tag" style={{ marginBottom: 10 }}>{channelNameBySlug.get(k) ?? k}</div>
                                      {activity ? (
                                        <>
                                          <div className="muted2" style={{ fontSize: 12, marginBottom: 4 }}>Activity</div>
                                          <div style={{ fontSize: 13, whiteSpace: "pre-wrap" }}>{activity}</div>
                                        </>
                                      ) : null}
                                      {results ? (
                                        <>
                                          <div className="muted2" style={{ fontSize: 12, marginTop: 10, marginBottom: 4 }}>Results</div>
                                          <div style={{ fontSize: 13, whiteSpace: "pre-wrap" }}>{results}</div>
                                        </>
                                      ) : null}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          ) : (
                            <div className="muted2">No per-channel details in this week.</div>
                          );
                        })()}
                      </div>
                    ) : null}
                  </div>
                </div>

                <div className="grid" style={{ marginBottom: 12 }}>
                  <div style={{ gridColumn: "span 3" }}>
                    <label className="muted" style={{ fontSize: 13 }}>Week start (Mon) *</label>
                    <input className="input" value={ciWeekStart} onChange={(e) => setCiWeekStart(e.target.value)} placeholder="YYYY-MM-DD" />
                  </div>
                  <div style={{ gridColumn: "span 3" }}>
                    <label className="muted" style={{ fontSize: 13 }}>Opps in progress</label>
                    <input className="input" type="number" value={ciOpps} onChange={(e) => setCiOpps(e.target.value === "" ? "" : Number(e.target.value))} />
                  </div>
                  <div style={{ gridColumn: "span 3" }}>
                    <label className="muted" style={{ fontSize: 13 }}>TAL companies</label>
                    <input className="input" type="number" value={ciTal} onChange={(e) => setCiTal(e.target.value === "" ? "" : Number(e.target.value))} />
                  </div>
                  <div style={{ gridColumn: "span 3" }}>
                    <label className="muted" style={{ fontSize: 13 }}>Contacts</label>
                    <input className="input" type="number" value={ciContacts} onChange={(e) => setCiContacts(e.target.value === "" ? "" : Number(e.target.value))} />
                  </div>

                  <div style={{ gridColumn: "span 4" }}>
                    <label className="muted" style={{ fontSize: 13 }}>Notes</label>
                    <textarea className="textarea" value={ciNotes} onChange={(e) => setCiNotes(e.target.value)} />
                  </div>
                  <div style={{ gridColumn: "span 4" }}>
                    <label className="muted" style={{ fontSize: 13 }}>Blockers</label>
                    <textarea className="textarea" value={ciBlockers} onChange={(e) => setCiBlockers(e.target.value)} />
                  </div>
                  <div style={{ gridColumn: "span 4" }}>
                    <label className="muted" style={{ fontSize: 13 }}>Next steps</label>
                    <textarea className="textarea" value={ciNextSteps} onChange={(e) => setCiNextSteps(e.target.value)} />
                  </div>

                  <div style={{ gridColumn: "span 12" }}>
                    <div className="muted" style={{ fontSize: 13, marginBottom: 6 }}>Per-channel results</div>
                    <div className="muted2" style={{ fontSize: 12, marginBottom: 10 }}>
                      Channels are taken from hypothesis settings above:{" "}
                      <b>
                        {(cjm.channels ?? []).length
                          ? (cjm.channels ?? []).map((x) => channelNameBySlug.get(String(x)) ?? String(x)).join(", ")
                          : "—"}
                      </b>.
                      To change channels, edit the hypothesis (not the check-in).
                    </div>

                    {(cjm.channels ?? []).length ? (
                      <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10 }}>
                        {(cjm.channels ?? []).map((ch) => (
                          <div key={ch} style={{ gridColumn: "span 6" }} className="subcard">
                            <div className="subcardTitle" style={{ marginBottom: 10, textTransform: "none", letterSpacing: 0 }}>
                              {channelNameBySlug.get(String(ch)) ?? String(ch)}
                            </div>

                            <label className="muted2" style={{ fontSize: 12 }}>What did we do?</label>
                            <textarea
                              className="textarea"
                              value={ciPerChannel[ch]?.activity ?? ""}
                              onChange={(e) =>
                                setCiPerChannel({
                                  ...ciPerChannel,
                                  [ch]: { ...(ciPerChannel[ch] ?? { activity: "", results: "", metrics: {} }), activity: e.target.value }
                                })
                              }
                              placeholder="Activity this week (messages, spend, experiments, etc.)"
                            />
                            <label className="muted2" style={{ fontSize: 12, marginTop: 8, display: "block" }}>What happened?</label>
                            <textarea
                              className="textarea"
                              value={ciPerChannel[ch]?.results ?? ""}
                              onChange={(e) =>
                                setCiPerChannel({
                                  ...ciPerChannel,
                                  [ch]: { ...(ciPerChannel[ch] ?? { activity: "", results: "", metrics: {} }), results: e.target.value }
                                })
                              }
                              placeholder="Results (replies, meetings, learnings, negatives)"
                            />

                            {(() => {
                              const metricIds = channelMetricIdsBySlug[ch] ?? [];
                              if (!metricIds.length) return null;
                              return (
                                <div style={{ marginTop: 12 }}>
                                  <div className="muted2" style={{ fontSize: 12, marginBottom: 8 }}>Metrics</div>
                                  <table className="table">
                                    <thead>
                                      <tr>
                                        <th>Metric</th>
                                        <th style={{ width: 220 }}>Value</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {metricIds.map((mid) => {
                                        const m = allMetrics.find((x: any) => String(x.id) === String(mid)) ?? null;
                                        const slug = String(m?.slug ?? "");
                                        const name = String((m?.name ?? slug) || "Metric");
                                        const unit = String(m?.unit ?? "");
                                        const t = String(m?.input_type ?? "number");
                                        if (!slug) return null;
                                        const v = ciPerChannel[ch]?.metrics?.[slug] ?? "";
                                        return (
                                          <tr key={mid}>
                                            <td>
                                              <b>{name}</b>
                                              <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{slug}{unit ? ` · ${unit}` : ""}</div>
                                            </td>
                                            <td>
                                              <input
                                                className="input"
                                                type={t === "number" ? "number" : "text"}
                                                value={v}
                                                onChange={(e) =>
                                                  setCiPerChannel({
                                                    ...ciPerChannel,
                                                    [ch]: {
                                                      ...(ciPerChannel[ch] ?? { activity: "", results: "", metrics: {} }),
                                                      metrics: { ...(ciPerChannel[ch]?.metrics ?? {}), [slug]: e.target.value }
                                                    }
                                                  })
                                                }
                                              />
                                            </td>
                                          </tr>
                                        );
                                      })}
                                    </tbody>
                                  </table>
                                </div>
                              );
                            })()}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="muted2">No channels selected in hypothesis yet.</div>
                    )}
                  </div>

                  <div style={{ gridColumn: "span 12" }}>
                    <div className="muted" style={{ fontSize: 13, marginBottom: 6 }}>Weekly totals</div>
                    <div className="muted2" style={{ fontSize: 12, marginBottom: 10 }}>
                      Hypothesis metrics (if selected) are saved per week. Channel metrics are saved inside each channel block above.
                    </div>
                    {selectedMetrics.length ? (
                      <div className="subcard" style={{ marginTop: 8 }}>
                        <div className="subcardTitle">Totals</div>
                        <table className="table">
                          <thead>
                            <tr>
                              <th>Metric</th>
                              <th style={{ width: 220 }}>Value</th>
                            </tr>
                          </thead>
                          <tbody>
                            {selectedMetrics.map((x) => {
                              const slug = String(x.metric?.slug ?? "");
                              const name = String(x.metric?.name ?? slug);
                              const unit = String(x.metric?.unit ?? "");
                              const t = String(x.metric?.input_type ?? "number");
                              return (
                                <tr key={x.metric_id}>
                                  <td>
                                    <b>{name}</b>
                                    <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{slug}{unit ? ` · ${unit}` : ""}</div>
                                  </td>
                                  <td>
                                    <input
                                      className="input"
                                      type={t === "number" ? "number" : "text"}
                                      value={ciMetricValues[slug] ?? ""}
                                      onChange={(e) => setCiMetricValues({ ...ciMetricValues, [slug]: e.target.value })}
                                      placeholder={t === "number" ? "0" : "text"}
                                    />
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="muted2" style={{ marginTop: 10, fontSize: 12 }}>
                        No metrics selected for this hypothesis.
                      </div>
                    )}
                  </div>
                </div>
                <div className="btnRow" style={{ justifyContent: "flex-end" }}>
                  <button className="btn btnPrimary" onClick={createCheckin}>Upsert check-in</button>
                </div>

                {/* Removed: duplicate weekly check-ins table (top summary is the single source of truth UI). */}
              </>
            )}
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">4) Calls</div>
              <div className="cardDesc">Attach calls to this hypothesis (and assign from the call page too). {sections.calls ? "Expanded." : "Collapsed."}</div>
            </div>
            <div className="btnRow">
              <button className="btn" onClick={() => toggleSection("calls")}>
                {sections.calls ? "Collapse" : "Expand"}
              </button>
            </div>
          </div>
          <div className="cardBody">
            {!sections.calls ? (
              <div className="grid">
                <div style={{ gridColumn: "span 12" }}>
                  <table className="table">
                    <tbody>
                      <tr><td><b>Linked calls</b></td><td className="mono">{calls.length}</td></tr>
                      <tr>
                        <td><b>Latest</b></td>
                        <td>{calls.length ? calls.slice(0, 3).map((c) => c.title || "Untitled").join(" · ") : "—"}</td>
                      </tr>
                    </tbody>
                  </table>
                  <div className="muted2" style={{ fontSize: 12, marginTop: 10 }}>
                    Expand this section to link/unlink calls and edit tag/notes.
                  </div>
                </div>
              </div>
            ) : (
              <>
                <div className="subcard" style={{ marginBottom: 12 }}>
                  <div className="subcardTitle">Quick link (last 7 days)</div>
                  <div className="btnRow" style={{ marginBottom: 10 }}>
                    <input
                      className="input"
                      style={{ width: 320 }}
                      placeholder="Search by title/email/id…"
                      value={recentCallsQ}
                      onChange={(e) => setRecentCallsQ(e.target.value)}
                    />
                  </div>
                  <table className="table">
                    <thead>
                      <tr>
                        <th>Call</th>
                        <th>Owner</th>
                        <th style={{ width: 180 }}>Tag</th>
                        <th style={{ width: 360 }}>Notes</th>
                        <th style={{ width: 120 }}></th>
                      </tr>
                    </thead>
                    <tbody>
                      {recentFiltered.slice(0, 30).map((c) => (
                        <tr key={c.id}>
                          <td>
                            <b>{c.title || "Untitled"}</b>
                            <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>
                              {c.occurred_at ? isoDate(c.occurred_at) : "—"} · {c.id}
                            </div>
                          </td>
                          <td className="mono">{c.owner_email || "—"}</td>
                          <td>
                            <input
                              className="input"
                              value={recentLinkMeta[c.id]?.tag ?? ""}
                              onChange={(e) => setRecentLinkMeta({ ...recentLinkMeta, [c.id]: { ...(recentLinkMeta[c.id] ?? { tag: "", notes: "" }), tag: e.target.value } })}
                              placeholder="discovery/demo/..."
                            />
                          </td>
                          <td>
                            <input
                              className="input"
                              value={recentLinkMeta[c.id]?.notes ?? ""}
                              onChange={(e) => setRecentLinkMeta({ ...recentLinkMeta, [c.id]: { ...(recentLinkMeta[c.id] ?? { tag: "", notes: "" }), notes: e.target.value } })}
                              placeholder="why this call matters"
                            />
                          </td>
                          <td>
                            <button className="btn btnPrimary" onClick={() => quickLinkCall(c.id)}>Link</button>
                          </td>
                        </tr>
                      ))}
                      {!recentFiltered.length ? (
                        <tr>
                          <td colSpan={5} className="muted2">No recent calls found (or all are already linked).</td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                  {recentFiltered.length > 30 ? (
                    <div className="muted2" style={{ fontSize: 12, marginTop: 8 }}>
                      Showing first 30. Use search to narrow down.
                    </div>
                  ) : null}
                </div>

                <div className="subcard" style={{ marginBottom: 12 }}>
                  <div className="subcardTitle">Manual link</div>
                  <div className="grid formGridTight">
                    <div style={{ gridColumn: "span 4" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Call ID</label>
                      <input className="input" value={callIdToLink} onChange={(e) => setCallIdToLink(e.target.value)} placeholder="uuid" />
                      <div className="helpInline">Tip: open the call and copy the Call ID.</div>
                    </div>
                    <div style={{ gridColumn: "span 3" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Tag</label>
                      <input className="input" value={callTag} onChange={(e) => setCallTag(e.target.value)} placeholder="discovery/demo/..." />
                    </div>
                    <div style={{ gridColumn: "span 5" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Notes</label>
                      <input className="input" value={callNotes} onChange={(e) => setCallNotes(e.target.value)} placeholder="why this call matters for the hypothesis" />
                    </div>
                    <div style={{ gridColumn: "span 12", justifyContent: "flex-end" }} className="btnRow">
                      <button className="btn btnPrimary" onClick={linkCall}>Link call</button>
                    </div>
                  </div>
                </div>

                <div className="subcard">
                  <div className="subcardTitle">Linked calls</div>
                  <table className="table">
                    <thead>
                      <tr>
                        <th>Call</th>
                        <th>Tag</th>
                        <th>Notes</th>
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {calls.map((c) => (
                        <tr key={c.call_id}>
                          <td>
                            <a href={`/calls/${c.call_id}`} style={{ textDecoration: "none" }}>
                              <b>{c.title || "Untitled"}</b>
                            </a>
                            <div className="muted2" style={{ fontSize: 12, marginTop: 2 }}>
                              {c.occurred_at ? <span className="mono">{isoDate(c.occurred_at)}</span> : null}
                              {" "}· <span className="mono">{c.call_id}</span>
                            </div>
                          </td>
                          <td>
                            <input
                              className="input"
                              value={linkedEditMeta[c.call_id]?.tag ?? String(c.tag ?? "")}
                              onChange={(e) => setLinkedEditMeta({ ...linkedEditMeta, [c.call_id]: { ...(linkedEditMeta[c.call_id] ?? { tag: "", notes: "" }), tag: e.target.value } })}
                              placeholder="discovery/demo/..."
                            />
                          </td>
                          <td>
                            <input
                              className="input"
                              value={linkedEditMeta[c.call_id]?.notes ?? String(c.notes ?? "")}
                              onChange={(e) => setLinkedEditMeta({ ...linkedEditMeta, [c.call_id]: { ...(linkedEditMeta[c.call_id] ?? { tag: "", notes: "" }), notes: e.target.value } })}
                              placeholder="why this call matters"
                            />
                          </td>
                          <td style={{ width: 140 }}>
                            <div className="btnRow" style={{ justifyContent: "flex-end" }}>
                              <button className="btn btnPrimary" onClick={() => saveLinkedCallMeta(c.call_id)}>Save</button>
                              <button className="btn" onClick={() => unlinkCall(c.call_id)}>Unlink</button>
                            </div>
                          </td>
                        </tr>
                      ))}
                      {!calls.length ? (
                        <tr>
                          <td colSpan={4} className="muted2">No calls linked.</td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </main>
  );
}


