"use client";

import { useEffect, useMemo, useState } from "react";
import { AppTopbar } from "../components/AppTopbar";
import { CountUp } from "../components/CountUp";
import { FadeIn } from "../components/FadeIn";
import { SpotlightCard } from "../components/SpotlightCard";
import { getSupabase } from "../lib/supabase";
// ─── Types ────────────────────────────────────────────────────────────────────

type Period = "7d" | "30d" | "90d" | "all";
type ChartBucket = "day" | "week" | "month";

type DailyRow = {
  date: string;
  campaign_name: string;
  sent_count: number;
  reply_count: number;
  touch_number: number;
  unique_leads_count: number;
};

type ManualStatRow = {
  record_date: string;
  campaign_name: string | null;
  metric_name: string;
  value: number;
};

type CampaignAliasRow = {
  alias: string;
  canonical: string;
};

type ReplyCategoryEventRow = {
  date: string;
  campaign_id: number;
  campaign_name: string | null;
  lead_key: string;
  lead_category_id: number | null;
  lead_category_name: string;
};

type ReplyEventRow = {
  occurred_at: string;
  campaign_id: number;
  campaign_name: string | null;
  lead_id: number | null;
  email: string | null;
};

type LeadCategoryLookupRow = {
  campaign_id: number;
  lead_id: number | null;
  email: string | null;
  lead_category_id: number | null;
  updated_at_source?: string | null;
  synced_at?: string | null;
  created_at_source?: string | null;
};

type Tab = "campaign" | "touch" | "month";

const COLORS = ["#7dd3fc", "#a7f3d0", "#fca5a5", "#c4b5fd", "#fde68a", "#fdba74", "#86efac", "#f9a8d4"];
const PAGE_SIZE = 1000;

// ─── Helpers ──────────────────────────────────────────────────────────────────

function n(v: unknown): number {
  const x = Number(v);
  return Number.isFinite(x) ? x : 0;
}

function pctStr(num: number, den: number): string {
  if (!den) return "—";
  return `${((num / den) * 100).toFixed(1)}%`;
}

function pctValueStr(v: number): string {
  if (!Number.isFinite(v)) return "—";
  return `${v.toFixed(1)}%`;
}

function toWeek(dateStr: string): string {
  const d = new Date(`${dateStr}T00:00:00Z`);
  const day = d.getUTCDay();
  const diff = (day === 0 ? -6 : 1) - day;
  d.setUTCDate(d.getUTCDate() + diff);
  return d.toISOString().slice(0, 10);
}

function toMonth(dateStr: string): string {
  return `${String(dateStr).slice(0, 7)}-01`;
}

function bucketForPeriod(period: Period): ChartBucket {
  if (period === "all") return "month";
  if (period === "90d") return "week";
  return "day";
}

function bucketStart(dateStr: string, bucket: ChartBucket): string {
  if (bucket === "month") return toMonth(dateStr);
  if (bucket === "week") return toWeek(dateStr);
  return String(dateStr).slice(0, 10);
}

function enumerateBuckets(since: string, until: string, bucket: ChartBucket): string[] {
  if (!since || !until) return [];
  const out: string[] = [];
  const cur = new Date(`${bucketStart(since, bucket)}T00:00:00Z`);
  const end = new Date(`${bucketStart(until, bucket)}T00:00:00Z`);
  if (!Number.isFinite(cur.getTime()) || !Number.isFinite(end.getTime())) return [];
  while (cur.getTime() <= end.getTime() && out.length < 4000) {
    out.push(cur.toISOString().slice(0, 10));
    if (bucket === "month") cur.setUTCMonth(cur.getUTCMonth() + 1);
    else if (bucket === "week") cur.setUTCDate(cur.getUTCDate() + 7);
    else cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

function formatBucketLabel(value: string, bucket: ChartBucket): string {
  if (bucket === "month") {
    const d = new Date(`${value}T00:00:00Z`);
    return d.toLocaleString("en-US", { month: "short", year: "2-digit", timeZone: "UTC" });
  }
  return value.slice(5);
}

function bucketTitle(bucket: ChartBucket): string {
  if (bucket === "month") return "Monthly trend";
  if (bucket === "week") return "Weekly trend";
  return "Daily trend";
}

function bucketUnit(bucket: ChartBucket): string {
  if (bucket === "month") return "months";
  if (bucket === "week") return "weeks";
  return "days";
}

// Fetch all rows with pagination (bypasses 1000-row API limit)
async function fetchAllRows(
  supabase: any,
  table: string,
  select: string
): Promise<DailyRow[]> {
  const all: DailyRow[] = [];
  for (let offset = 0; ; offset += PAGE_SIZE) {
    const { data, error } = await supabase
      .from(table)
      .select(select)
      .order("date", { ascending: true })
      .range(offset, offset + PAGE_SIZE - 1);
    if (error) throw new Error(error.message);
    all.push(...((data ?? []) as DailyRow[]));
    if ((data?.length ?? 0) < PAGE_SIZE) break;
  }
  return all;
}

async function fetchManualEmailRows(
  supabase: any
): Promise<ManualStatRow[]> {
  const all: ManualStatRow[] = [];
  for (let offset = 0; ; offset += PAGE_SIZE) {
    const { data, error } = await supabase
      .from("manual_stats")
      .select("record_date,campaign_name,metric_name,value")
      .eq("channel", "email")
      .in("metric_name", ["sent_count", "reply_count", "booked_meetings", "held_meetings"])
      .order("record_date", { ascending: true })
      .range(offset, offset + PAGE_SIZE - 1);
    if (error) throw new Error(error.message);
    all.push(...((data ?? []) as ManualStatRow[]));
    if ((data?.length ?? 0) < PAGE_SIZE) break;
  }
  return all;
}

async function fetchEmailAliases(
  supabase: any
): Promise<CampaignAliasRow[]> {
  const { data, error } = await supabase
    .from("campaign_name_aliases")
    .select("alias,canonical")
    .eq("channel", "email");
  if (error) throw new Error(error.message);
  return (data ?? []) as CampaignAliasRow[];
}

function normalizeCampaignName(
  rawName: string | null,
  aliasMap: Map<string, string>
): string | null {
  const trimmed = (rawName ?? "").trim();
  if (!trimmed) return null;
  return aliasMap.get(trimmed.toLowerCase()) ?? trimmed;
}

async function fetchReplyCategoryRows(
  supabase: any,
  since?: string,
  until?: string
): Promise<ReplyCategoryEventRow[]> {
  const eventRows: ReplyEventRow[] = [];
  for (let offset = 0; ; offset += PAGE_SIZE) {
    let query = supabase
      .from("smartlead_events")
      .select("occurred_at,campaign_id,campaign_name,lead_id,email")
      .eq("event_type", "reply");
    if (since) query = query.gte("occurred_at", since);
    if (until) query = query.lte("occurred_at", `${until}T23:59:59`);
    const { data, error } = await query
      .order("occurred_at", { ascending: true })
      .range(offset, offset + PAGE_SIZE - 1);
    if (error) throw new Error(error.message);
    eventRows.push(...((data ?? []) as ReplyEventRow[]));
    if ((data?.length ?? 0) < PAGE_SIZE) break;
  }

  const leadIds = Array.from(new Set(
    eventRows
      .map((row) => row.lead_id)
      .filter((value): value is number => value !== null && value !== undefined && Number.isFinite(Number(value)))
  ));
  const emails = Array.from(new Set(
    eventRows
      .filter((row) => !Number.isFinite(Number(row.lead_id)))
      .map((row) => (row.email ?? "").trim().toLowerCase())
      .filter(Boolean)
  ));

  const leadRows: LeadCategoryLookupRow[] = [];
  const chunkSize = 50;

  for (let i = 0; i < leadIds.length; i += chunkSize) {
    const chunk = leadIds.slice(i, i + chunkSize);
    const { data, error } = await supabase
      .from("smartlead_leads")
      .select("campaign_id,lead_id,email,lead_category_id,updated_at_source,synced_at,created_at_source")
      .in("lead_id", chunk);
    if (error) throw new Error(error.message);
    leadRows.push(...((data ?? []) as LeadCategoryLookupRow[]));
  }

  for (let i = 0; i < emails.length; i += chunkSize) {
    const chunk = emails.slice(i, i + chunkSize);
    const { data, error } = await supabase
      .from("smartlead_leads")
      .select("campaign_id,lead_id,email,lead_category_id,updated_at_source,synced_at,created_at_source")
      .in("email", chunk);
    if (error) throw new Error(error.message);
    leadRows.push(...((data ?? []) as LeadCategoryLookupRow[]));
  }

  const byCampaignLeadId = new Map<string, LeadCategoryLookupRow>();
  const byCampaignEmail = new Map<string, LeadCategoryLookupRow>();

  function rowFreshness(row: LeadCategoryLookupRow): [number, number, number, number] {
    return [
      Date.parse(row.updated_at_source ?? "") || 0,
      Date.parse(row.synced_at ?? "") || 0,
      Date.parse(row.created_at_source ?? "") || 0,
      Number(row.lead_id ?? 0) || 0,
    ];
  }

  function isFresher(next: LeadCategoryLookupRow, prev: LeadCategoryLookupRow | undefined) {
    if (!prev) return true;
    const a = rowFreshness(next);
    const b = rowFreshness(prev);
    for (let i = 0; i < a.length; i += 1) {
      if (a[i] !== b[i]) return a[i] > b[i];
    }
    return false;
  }

  for (const row of leadRows) {
    if (Number.isFinite(Number(row.lead_id))) {
      const key = `${row.campaign_id}|lead:${row.lead_id}`;
      if (isFresher(row, byCampaignLeadId.get(key))) byCampaignLeadId.set(key, row);
    }
    const email = (row.email ?? "").trim().toLowerCase();
    if (email) {
      const key = `${row.campaign_id}|email:${email}`;
      if (isFresher(row, byCampaignEmail.get(key))) byCampaignEmail.set(key, row);
    }
  }

  function categoryName(categoryId: number | null) {
    if (categoryId === 1) return "Interested";
    if (categoryId === 2) return "Meeting Request";
    if (categoryId === 3) return "Not Interested";
    if (categoryId === 4) return "Do Not Contact";
    if (categoryId === 5) return "Information Request";
    if (categoryId === 6) return "Out Of Office";
    if (categoryId === 7) return "Wrong Person";
    if (categoryId === 8) return "Uncategorizable by AI";
    if (categoryId === 9) return "Sender Originated Bounce";
    if (categoryId === 121483) return "Didn't Attend (Ask for Referral)";
    return "Uncategorized";
  }

  return eventRows.map((row) => {
    const leadMatch = Number.isFinite(Number(row.lead_id))
      ? byCampaignLeadId.get(`${row.campaign_id}|lead:${row.lead_id}`)
      : undefined;
    const emailKey = `${row.campaign_id}|email:${(row.email ?? "").trim().toLowerCase()}`;
    const emailMatch = (row.email ?? "").trim() ? byCampaignEmail.get(emailKey) : undefined;
    const categoryId = leadMatch?.lead_category_id ?? emailMatch?.lead_category_id ?? null;
    const email = (row.email ?? "").trim().toLowerCase();

    return {
      date: String(row.occurred_at).slice(0, 10),
      campaign_id: Number(row.campaign_id),
      campaign_name: row.campaign_name,
      lead_key: email || (Number.isFinite(Number(row.lead_id)) ? `lead:${row.lead_id}` : `${row.campaign_id}|${row.occurred_at}`),
      lead_category_id: categoryId,
      lead_category_name: categoryName(categoryId),
    };
  });
}

function groupReplyCategory(row: ReplyCategoryEventRow): string {
  const categoryId = Number(row.lead_category_id);
  if (categoryId === 1 || categoryId === 2 || categoryId === 5 || categoryId === 121483) return "Positive intent";
  if (categoryId === 6) return "Out of Office";
  if (categoryId === 7) return "Wrong Person";
  if (categoryId === 8) return "AI Uncategorized";
  if (categoryId === 9) return "Sender Bounce";
  if (categoryId === 3) return "Not Interested";
  if (categoryId === 4) return "Do Not Contact";
  return row.lead_category_name || "Uncategorized";
}

// ─── SVG Line Chart ───────────────────────────────────────────────────────────

function lineSegments(pts: Array<[number, number] | null>): string[] {
  const segments: string[] = [];
  let current: string[] = [];
  for (const pt of pts) {
    if (!pt) {
      if (current.length) segments.push(current.join(" "));
      current = [];
      continue;
    }
    const [x, y] = pt;
    current.push(`${current.length === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`);
  }
  if (current.length) segments.push(current.join(" "));
  return segments;
}

function LineChart({
  buckets,
  bucket,
  series,
}: {
  buckets: string[];
  bucket: ChartBucket;
  series: Array<{ label: string; color: string; values: Array<number | null> }>;
}) {
  const W = 960, H = 220, pL = 46, pR = 12, pT = 16, pB = 28;
  const iW = W - pL - pR;
  const iH = H - pT - pB;

  const allVals = series.flatMap((s) => s.values).filter((v): v is number => typeof v === "number" && Number.isFinite(v));
  const maxVal = allVals.length ? Math.max(...allVals, 1) : 1;
  const span = maxVal || 1;

  const xFor = (i: number) =>
    buckets.length <= 1 ? pL + iW / 2 : pL + (i * iW) / (buckets.length - 1);
  const yFor = (v: number) => pT + ((maxVal - v) * iH) / span;

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((t) => maxVal * t);
  const step = Math.max(1, Math.floor(buckets.length / 8));

  return (
    <div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: "block" }}>
        {yTicks.map((t, i) => (
          <g key={i}>
            <line x1={pL} y1={yFor(t)} x2={W - pR} y2={yFor(t)} stroke="var(--chartGrid)" strokeWidth={1} />
            <text x={pL - 6} y={yFor(t) + 4} textAnchor="end" fontSize={10} fill="var(--chartAxis)">
              {t >= 1000 ? `${(t / 1000).toFixed(0)}k` : t.toFixed(0)}
            </text>
          </g>
        ))}
        {buckets.map((value, i) => {
          if (i % step !== 0 && i !== buckets.length - 1) return null;
          return (
            <text key={value} x={xFor(i)} y={H - 6} textAnchor="middle" fontSize={10} fill="var(--chartAxis)">
              {formatBucketLabel(value, bucket)}
            </text>
          );
        })}
        {series.map((s, si) => {
          if (!s.values.length) return null;
          const pts = s.values.map((v, i) => (typeof v === "number" ? [xFor(i), yFor(v)] as [number, number] : null));
          const segments = lineSegments(pts);
          return (
            <g key={si}>
              {segments.map((d, i) => (
                <path key={i} d={d} fill="none" stroke={s.color} strokeWidth={2.2} opacity={0.9} />
              ))}
              {pts.map((pt, i) => (
                pt ? <circle key={i} cx={pt[0]} cy={pt[1]} r={3} fill={s.color} /> : null
              ))}
            </g>
          );
        })}
      </svg>
      <div className="chartLegend">
        {series.map((s, i) => (
          <span key={i} className="chartLegendItem">
            <span className="chartLegendSwatch" style={{ width: 12, height: 3, background: s.color }} />
            {s.label}
          </span>
        ))}
      </div>
    </div>
  );
}

// ─── Sort icon ────────────────────────────────────────────────────────────────

function SortIcon({ col, sortKey, sortDir }: { col: string; sortKey: string; sortDir: "asc" | "desc" }) {
  if (sortKey !== col) return <span style={{ opacity: 0.28, marginLeft: 3 }}>↕</span>;
  return <span style={{ marginLeft: 3 }}>{sortDir === "desc" ? "↓" : "↑"}</span>;
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function SmartleadPage() {
  const supabase = useMemo(() => getSupabase(), []);

  const [status, setStatus] = useState("");
  const [loading, setLoading] = useState(false);
  const [rows, setRows] = useState<DailyRow[]>([]);
  const [manualRows, setManualRows] = useState<ManualStatRow[]>([]);
  const [replyCategoryRows, setReplyCategoryRows] = useState<ReplyCategoryEventRow[]>([]);
  const [replyCategoryStatus, setReplyCategoryStatus] = useState("");

  const [period, setPeriod] = useState<Period>("all");
  const [dateRange, setDateRange] = useState<{ since: string; until: string }>({ since: "", until: "" });
  const [tab, setTab] = useState<Tab>("campaign");

  useEffect(() => {
    if (period === "all") {
      setDateRange({ since: "", until: "" });
    } else {
      const days = period === "7d" ? 7 : period === "30d" ? 30 : 90;
      const until = new Date();
      const since = new Date();
      since.setDate(since.getDate() - (days - 1));
      setDateRange({
        since: since.toISOString().slice(0, 10),
        until: until.toISOString().slice(0, 10),
      });
    }
  }, [period]);
  const [chartMetric, setChartMetric] = useState<"sent_count" | "reply_count">("sent_count");
  const [sortKey, setSortKey] = useState<string>("sent");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [tablePage, setTablePage] = useState(0);
  const [tablePageSize, setTablePageSize] = useState<number | "all">(25);
  const [replyBreakdownOpen, setReplyBreakdownOpen] = useState(false);

  // ─── Load (once, all data) ──────────────────────────────────────────────────

  const [loadTick, setLoadTick] = useState(0);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      if (!supabase) return;
      setLoading(true);
      setStatus("Loading…");
      const sess = await supabase.auth.getSession();
      if (!sess.data.session) {
        if (!cancelled) setStatus("Not signed in. Go back to / and sign in.");
        return;
      }
      try {
        const [data, manual, aliases] = await Promise.all([
          fetchAllRows(supabase, "smartlead_stats_daily", "date,campaign_name,sent_count,reply_count,touch_number,unique_leads_count"),
          fetchManualEmailRows(supabase),
          fetchEmailAliases(supabase),
        ]);
        if (cancelled) return;
        const aliasMap = new Map(
          aliases.filter((row: { alias: string; canonical: string }) => row.alias && row.canonical).map((row: { alias: string; canonical: string }) => [row.alias.trim().toLowerCase(), row.canonical.trim()])
        );
        setRows(data);
        setManualRows(
          manual.map((row: ManualStatRow) => ({
            ...row,
            campaign_name: normalizeCampaignName(row.campaign_name, aliasMap),
          }))
        );
        setReplyCategoryStatus("Loading reply categories…");
        const defaultSince = new Date();
        defaultSince.setDate(defaultSince.getDate() - 90);
        fetchReplyCategoryRows(supabase, defaultSince.toISOString().slice(0, 10))
          .then((replyCategories) => {
            if (cancelled) return;
            setReplyCategoryRows(
              replyCategories.map((row) => ({
                ...row,
                campaign_name: normalizeCampaignName(row.campaign_name, aliasMap),
              }))
            );
            setReplyCategoryStatus("");
          })
          .catch((error: unknown) => {
            if (cancelled) return;
            setReplyCategoryRows([]);
            setReplyCategoryStatus(`Reply categories unavailable: ${(error as Error).message}`);
          });
      } catch (e: unknown) {
        if (!cancelled) { setStatus(`Error: ${(e as Error).message}`); setLoading(false); }
        return;
      }
      if (!cancelled) { setStatus(""); setLoading(false); }
    })();
    return () => { cancelled = true; };
  }, [supabase, loadTick]); // eslint-disable-line react-hooks/exhaustive-deps

  // ─── Available date range ─────────────────────────────────────────────────

  // ─── Client-side filtering by date range ─────────────────────────────────

  const filteredRows = useMemo(() => {
    if (!dateRange.since && !dateRange.until) return rows;
    return rows.filter((r) => {
      if (dateRange.since && r.date < dateRange.since) return false;
      if (dateRange.until && r.date > dateRange.until) return false;
      return true;
    });
  }, [rows, dateRange]);

  const filteredManualRows = useMemo(() => {
    if (!dateRange.since && !dateRange.until) return manualRows;
    return manualRows.filter((r) => {
      if (dateRange.since && r.record_date < dateRange.since) return false;
      if (dateRange.until && r.record_date > dateRange.until) return false;
      return true;
    });
  }, [manualRows, dateRange]);

  const filteredReplyCategoryRows = useMemo(() => {
    if (!dateRange.since && !dateRange.until) return replyCategoryRows;
    return replyCategoryRows.filter((r) => {
      if (dateRange.since && r.date < dateRange.since) return false;
      if (dateRange.until && r.date > dateRange.until) return false;
      return true;
    });
  }, [replyCategoryRows, dateRange]);

  // ─── Derived ───────────────────────────────────────────────────────────────

  const chartBucket = useMemo(() => bucketForPeriod(period), [period]);

  const chartBuckets = useMemo(() => {
    if (!filteredRows.length) return [];
    const since = dateRange.since || filteredRows[0]?.date || "";
    const until = dateRange.until || filteredRows[filteredRows.length - 1]?.date || "";
    const generated = enumerateBuckets(since, until, chartBucket);
    if (generated.length) return generated;
    return Array.from(new Set(filteredRows.map((r) => bucketStart(r.date, chartBucket)))).sort();
  }, [filteredRows, dateRange, chartBucket]);

  const totals = useMemo(() => {
    let sentSmartlead = 0, replySmartlead = 0;
    for (const r of filteredRows) {
      sentSmartlead += n(r.sent_count);
      replySmartlead += n(r.reply_count);
    }

    let sentManual = 0, replyManual = 0, booked = 0, held = 0;
    for (const r of filteredManualRows) {
      const value = n(r.value);
      if (r.metric_name === "sent_count") sentManual += value;
      else if (r.metric_name === "reply_count") replyManual += value;
      // Email named campaign rows are all-time backfill facts.
      // Top KPI must use only monthly NULL-campaign totals to avoid double-counting.
      else if ((r.campaign_name ?? "").trim()) continue;
      else if (r.metric_name === "booked_meetings") booked += value;
      else if (r.metric_name === "held_meetings") held += value;
    }

    const sent = sentSmartlead > 0 ? sentSmartlead : sentManual;
    const reply = replySmartlead > 0 ? replySmartlead : replyManual;
    return {
      sent,
      reply,
      booked_meetings: booked,
      held_meetings: held,
      cr_booked: reply > 0 ? (booked / reply) * 100 : 0,
      cr_held: booked > 0 ? (held / booked) * 100 : 0,
    };
  }, [filteredRows, filteredManualRows]);

  const manualCampaignCount = useMemo(() => {
    return new Set(
      filteredManualRows
        .map((r) => (r.campaign_name ?? "").trim())
        .filter(Boolean)
    ).size;
  }, [filteredManualRows]);

  const replyCategorySummary = useMemo(() => {
    const byGroup = new Map<string, Set<string>>();
    for (const row of filteredReplyCategoryRows) {
      const label = groupReplyCategory(row);
      if (!byGroup.has(label)) byGroup.set(label, new Set<string>());
      byGroup.get(label)!.add(row.lead_key);
    }
    return Array.from(byGroup.entries())
      .map(([label, keys]) => ({ label, count: keys.size }))
      .sort((a, b) => b.count - a.count);
  }, [filteredReplyCategoryRows]);

  const replySemantics = useMemo(() => {
    const uniqueLeadKeys = new Set<string>();
    const cleanedLeadKeys = new Set<string>();

    for (const row of filteredReplyCategoryRows) {
      uniqueLeadKeys.add(row.lead_key);
      const categoryId = Number(row.lead_category_id);
      if (categoryId === 6 || categoryId === 8 || categoryId === 9) continue;
      cleanedLeadKeys.add(row.lead_key);
    }

    return {
      raw_reply_events: totals.reply,
      dedup_replied_leads: uniqueLeadKeys.size,
      dedup_replied_minus_ooo_ai_bounce: cleanedLeadKeys.size,
    };
  }, [filteredReplyCategoryRows, totals.reply]);

  // Trend chart series by campaign (top 8 by selected metric)
  const chartSeries = useMemo(() => {
    const byCampaign = new Map<string, Map<string, number>>();
    for (const r of filteredRows) {
      const key = bucketStart(r.date, chartBucket);
      if (!byCampaign.has(r.campaign_name)) byCampaign.set(r.campaign_name, new Map());
      const wm = byCampaign.get(r.campaign_name)!;
      wm.set(key, (wm.get(key) ?? 0) + n(r[chartMetric]));
    }
    const totByC = Array.from(byCampaign.entries()).map(([name, wm]) => ({
      name,
      total: Array.from(wm.values()).reduce((a, b) => a + b, 0),
      wm,
    }));
    totByC.sort((a, b) => b.total - a.total);
    return totByC.slice(0, 8).map(({ name, wm }, i) => ({
      label: name.length > 35 ? name.slice(0, 33) + "…" : name,
      color: COLORS[i % COLORS.length],
      values: chartBuckets.map((key) => wm.has(key) ? (wm.get(key) ?? 0) : null),
    }));
  }, [filteredRows, chartBuckets, chartBucket, chartMetric]);

  // By-campaign table
  const campaignRows = useMemo(() => {
    const map = new Map<string, { sent: number; reply: number; booked: number; held: number }>();
    for (const r of filteredRows) {
      if (!map.has(r.campaign_name)) map.set(r.campaign_name, { sent: 0, reply: 0, booked: 0, held: 0 });
      const d = map.get(r.campaign_name)!;
      d.sent += n(r.sent_count);
      d.reply += n(r.reply_count);
    }
    for (const r of filteredManualRows) {
      const name = (r.campaign_name ?? "").trim();
      if (!name) continue;
      if (!map.has(name)) map.set(name, { sent: 0, reply: 0, booked: 0, held: 0 });
      const d = map.get(name)!;
      if (r.metric_name === "booked_meetings") d.booked += n(r.value);
      else if (r.metric_name === "held_meetings") d.held += n(r.value);
    }
    const arr = Array.from(map.entries()).map(([name, d]) => ({
      name,
      sent: d.sent,
      reply: d.reply,
      reply_rate: d.sent > 0 ? (d.reply / d.sent) * 100 : 0,
      booked_meetings: d.booked,
      held_meetings: d.held,
      cr_booked: d.reply > 0 ? (d.booked / d.reply) * 100 : null,
      cr_held: d.booked > 0 ? (d.held / d.booked) * 100 : null,
    }));
    return arr.sort((a, b) => {
      const av = n(a[sortKey as keyof typeof a]);
      const bv = n(b[sortKey as keyof typeof b]);
      return sortDir === "desc" ? bv - av : av - bv;
    });
  }, [filteredRows, filteredManualRows, sortKey, sortDir]);

  // By-touch table
  const touchRows = useMemo(() => {
    const map = new Map<number, { sent: number; reply: number }>();
    for (const r of filteredRows) {
      const t = n(r.touch_number);
      if (!t) continue;
      if (!map.has(t)) map.set(t, { sent: 0, reply: 0 });
      const d = map.get(t)!;
      d.sent += n(r.sent_count);
      d.reply += n(r.reply_count);
    }
    return Array.from(map.entries())
      .map(([touch, d]) => ({ touch, sent: d.sent, reply: d.reply }))
      .sort((a, b) => a.touch - b.touch);
  }, [filteredRows]);

  const monthSummaryRows = useMemo(() => {
    const map = new Map<string, {
      sent: number;
      reply: number;
      booked: number;
      held: number;
    }>();

    for (const r of filteredRows) {
      const month = toMonth(r.date);
      if (!map.has(month)) {
        map.set(month, {
          sent: 0,
          reply: 0,
          booked: 0,
          held: 0,
        });
      }
      const d = map.get(month)!;
      d.sent += n(r.sent_count);
      d.reply += n(r.reply_count);
    }

    for (const r of filteredManualRows) {
      if (r.metric_name !== "booked_meetings" && r.metric_name !== "held_meetings") continue;
      if ((r.campaign_name ?? "").trim()) continue;
      const month = toMonth(r.record_date);
      if (!map.has(month)) {
        map.set(month, {
          sent: 0,
          reply: 0,
          booked: 0,
          held: 0,
        });
      }
      const d = map.get(month)!;
      const value = n(r.value);
      if (r.metric_name === "booked_meetings") {
        d.booked += value;
      } else {
        d.held += value;
      }
    }

    return Array.from(map.entries())
      .map(([month, d]) => ({
        month,
        sent: d.sent,
        reply: d.reply,
        reply_rate: d.sent > 0 ? (d.reply / d.sent) * 100 : 0,
        booked: d.booked,
        held: d.held,
      }))
      .sort((a, b) => b.month.localeCompare(a.month));
  }, [filteredRows, filteredManualRows]);

  function toggleSort(key: string) {
    setTablePage(0);
    if (sortKey === key) setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    else { setSortKey(key); setSortDir("desc"); }
  }

  const rangeLabel = period === "all" ? "All time" : `Last ${period}`;

  // ─── Render ────────────────────────────────────────────────────────────────

  return (
    <main>
      <AppTopbar title="Email Analytics" subtitle="Smartlead outreach performance" />

      <div className="page grid">
        {status && (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardBody"><div className="notice">{status}</div></div>
          </div>
        )}

        {/* ── Filters ──────────────────────────────────────────────────────── */}
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle" style={{ fontFamily: "var(--sans)", fontWeight: 600 }}>Email Outreach Analytics</div>
              <div className="cardDesc">
                {rangeLabel} ·{" "}
                {Math.max(campaignRows.length, manualCampaignCount)} campaigns
              </div>
            </div>
            <div className="btnRow">
              <button className="btn" onClick={() => setLoadTick((t) => t + 1)} disabled={loading} title="Refresh data">↻</button>
            </div>
          </div>
          <div className="cardBody">
            <div className="btnRow">
              {(["7d", "30d", "90d", "all"] as Period[]).map((p) => (
                <button
                  key={p}
                  className={`btn${period === p ? " btnPrimary" : ""}`}
                  onClick={() => setPeriod(p)}
                  disabled={loading}
                >
                  {p === "all" ? "All" : p.toUpperCase()}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* ── KPI cards ────────────────────────────────────────────────────── */}
        <FadeIn delay={60} style={{ gridColumn: "span 12" }}>
          <div className="kpiRow kpiRowFive">
            {[
              { label: "Emails sent", numVal: totals.sent, strVal: null, sub: null },
              {
                label: "Replies",
                numVal: totals.reply,
                strVal: null,
                sub: `${pctStr(totals.reply, totals.sent)} of sent · click for breakdown`,
                onClick: () => setReplyBreakdownOpen(true),
              },
              {
                label: "Official replied candidate",
                numVal: replySemantics.dedup_replied_minus_ooo_ai_bounce,
                strVal: null,
                sub: "dedup minus OOO / AI / sender bounce",
                onClick: () => setReplyBreakdownOpen(true),
              },
              { label: "Booked meetings", numVal: totals.booked_meetings, strVal: null, sub: pctValueStr(totals.cr_booked) + " of replies" },
              { label: "Held meetings", numVal: totals.held_meetings, strVal: null, sub: pctValueStr(totals.cr_held) + " of booked" },
              { label: "Reply rate", numVal: null, strVal: pctStr(totals.reply, totals.sent), sub: "replies / sent" },
              { label: "CR → Booked", numVal: null, strVal: pctValueStr(totals.cr_booked), sub: "booked / replies" },
              { label: "CR → Held", numVal: null, strVal: pctValueStr(totals.cr_held), sub: "held / booked" },
            ].map((kpi) => (
              <SpotlightCard
                key={kpi.label}
                className="card kpiCard"
                style={{ gridColumn: "auto", cursor: kpi.onClick ? "pointer" : "default" }}
                onClick={kpi.onClick}
              >
                <div className="cardBody">
                  <div className="muted2" style={{ fontSize: 11, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                    {kpi.label}
                  </div>
                  <div style={{ fontSize: 26, fontWeight: 700, letterSpacing: "-0.02em", lineHeight: 1 }}>
                    {kpi.numVal !== null
                      ? <CountUp to={kpi.numVal} duration={1.2} />
                      : kpi.strVal}
                  </div>
                  {kpi.sub && (
                    <div className="muted2" style={{ fontSize: 11, marginTop: 5 }}>{kpi.sub}</div>
                  )}
                </div>
              </SpotlightCard>
            ))}
          </div>
        </FadeIn>

        {/* ── Trend chart ──────────────────────────────────────────────────── */}
        <FadeIn delay={120} style={{ gridColumn: "span 12" }}>
          <div className="card">
            <div className="cardHeader">
              <div>
                <div className="cardTitle">{bucketTitle(chartBucket)}</div>
                <div className="cardDesc">{chartBuckets.length} {bucketUnit(chartBucket)} · top campaigns</div>
              </div>
              <div className="btnRow">
                <button
                  className={`btn${chartMetric === "sent_count" ? " btnPrimary" : ""}`}
                  onClick={() => setChartMetric("sent_count")}
                >
                  Sent
                </button>
                <button
                  className={`btn${chartMetric === "reply_count" ? " btnPrimary" : ""}`}
                  onClick={() => setChartMetric("reply_count")}
                >
                  Replies
                </button>
              </div>
            </div>
            <div className="cardBody">
              {!chartBuckets.length
                ? <div className="muted2">No data for selected period.</div>
                : <LineChart buckets={chartBuckets} bucket={chartBucket} series={chartSeries} />
              }
            </div>
          </div>
        </FadeIn>

        {/* ── Tables ───────────────────────────────────────────────────────── */}
        <FadeIn delay={200} style={{ gridColumn: "span 12" }}>
        <div className="card">
          <div className="cardHeader">
            <div className="btnRow">
              <button
                className={`btn${tab === "campaign" ? " btnPrimary" : ""}`}
                onClick={() => { setTab("campaign"); setSortKey("sent"); setSortDir("desc"); setTablePage(0); }}
              >
                By Campaign
              </button>
              <button
                className={`btn${tab === "touch" ? " btnPrimary" : ""}`}
                onClick={() => { setTab("touch"); setTablePage(0); }}
              >
                By Touch
              </button>
              <button
                className={`btn${tab === "month" ? " btnPrimary" : ""}`}
                onClick={() => { setTab("month"); setTablePage(0); }}
              >
                By Month
              </button>
            </div>
            <div className="muted2" style={{ fontSize: 12 }}>
              {tab === "campaign"
                ? `${campaignRows.length} campaigns`
                : tab === "touch"
                ? `${touchRows.length} touches in sequence`
                : `${monthSummaryRows.length} months`
              }
            </div>
          </div>

          <div className="cardBody" style={{ overflowX: "auto" }}>
            {tab === "campaign" ? (() => {
              const ps = tablePageSize === "all" ? campaignRows.length : tablePageSize;
              const totalPages = campaignRows.length === 0 ? 1 : Math.ceil(campaignRows.length / ps);
              const visibleRows = tablePageSize === "all" ? campaignRows : campaignRows.slice(tablePage * ps, (tablePage + 1) * ps);
              const from = Math.min(tablePage * ps + 1, campaignRows.length);
              const to = Math.min((tablePage + 1) * ps, campaignRows.length);
              return (
              <>
              <table className="table">
                <thead>
                  <tr>
                    <th>Campaign</th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("sent")}>
                      Sent <SortIcon col="sent" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("reply")}>
                      Replies <SortIcon col="reply" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("booked_meetings")}>
                      Booked <SortIcon col="booked_meetings" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("held_meetings")}>
                      Held <SortIcon col="held_meetings" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("reply_rate")}>
                      Reply rate <SortIcon col="reply_rate" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th>CR Booked</th>
                    <th>CR Held</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleRows.map((r) => (
                    <tr key={r.name}>
                      <td><b>{r.name}</b></td>
                      <td className="mono">{r.sent.toLocaleString()}</td>
                      <td className="mono">{r.reply}</td>
                      <td className="mono">{r.booked_meetings || "—"}</td>
                      <td className="mono">{r.held_meetings || "—"}</td>
                      <td className="mono">{pctStr(r.reply, r.sent)}</td>
                      <td className="mono">{r.cr_booked != null ? pctValueStr(r.cr_booked) : "—"}</td>
                      <td className="mono">{r.cr_held != null ? pctValueStr(r.cr_held) : "—"}</td>
                    </tr>
                  ))}
                  {!campaignRows.length && (
                    <tr><td colSpan={8} className="muted2">No data.</td></tr>
                  )}
                </tbody>
              </table>
              {campaignRows.length > 10 && (
                <div className="tableFooter">
                  <span className="muted2">
                    {tablePageSize === "all" ? `All ${campaignRows.length} campaigns` : `${from}–${to} of ${campaignRows.length}`}
                  </span>
                  <div className="tableFooterControls">
                    <span className="muted2">Rows per page:</span>
                    <select
                      className="select paginationSelect"
                      value={tablePageSize}
                      onChange={(e) => { const v = e.target.value === "all" ? "all" : Number(e.target.value); setTablePageSize(v as number | "all"); setTablePage(0); }}
                    >
                      <option value={10}>10</option>
                      <option value={25}>25</option>
                      <option value={50}>50</option>
                      <option value="all">All</option>
                    </select>
                    {tablePageSize !== "all" && totalPages > 1 && (
                      <div className="paginationControls">
                        <button className="btn" onClick={() => setTablePage((p) => Math.max(0, p - 1))} disabled={tablePage === 0} style={{ padding: "2px 8px" }}>‹</button>
                        {Array.from({ length: totalPages }, (_, i) => i).filter(i => Math.abs(i - tablePage) <= 2).map(i => (
                          <button key={i} className={`btn${i === tablePage ? " btnPrimary" : ""}`} onClick={() => setTablePage(i)} style={{ padding: "2px 8px", minWidth: 32 }}>{i + 1}</button>
                        ))}
                        <button className="btn" onClick={() => setTablePage((p) => Math.min(totalPages - 1, p + 1))} disabled={tablePage >= totalPages - 1} style={{ padding: "2px 8px" }}>›</button>
                      </div>
                    )}
                  </div>
                </div>
              )}
              </>
              );
            })() : tab === "touch" ? (
              <table className="table">
                <thead>
                  <tr>
                    <th>Touch #</th>
                    <th>Sent</th>
                    <th>Replies</th>
                    <th>Reply rate</th>
                  </tr>
                </thead>
                <tbody>
                  {touchRows.map((r) => (
                    <tr key={r.touch}>
                      <td><b>Touch {r.touch}</b></td>
                      <td className="mono">{r.sent.toLocaleString()}</td>
                      <td className="mono">{r.reply}</td>
                      <td className="mono">{pctStr(r.reply, r.sent)}</td>
                    </tr>
                  ))}
                  {!touchRows.length && (
                    <tr><td colSpan={4} className="muted2">No data.</td></tr>
                  )}
                </tbody>
              </table>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Month</th>
                    <th>Sent</th>
                    <th>Replies</th>
                    <th>Reply rate</th>
                    <th>Booked</th>
                    <th>Held</th>
                  </tr>
                </thead>
                <tbody>
                  {monthSummaryRows.map((r) => {
                    const ym = r.month.slice(0, 7);
                    const [yr, mo] = ym.split("-");
                    const label = new Date(Number(yr), Number(mo) - 1, 1).toLocaleString("en-US", { month: "short", year: "numeric" });
                    return (
                      <tr key={r.month}>
                        <td><b>{label}</b></td>
                        <td className="mono">{r.sent.toLocaleString()}</td>
                        <td className="mono">{r.reply.toLocaleString()}</td>
                        <td className="mono">{pctStr(r.reply, r.sent)}</td>
                        <td className="mono">{r.booked != null ? r.booked : "—"}</td>
                        <td className="mono">{r.held != null ? r.held : "—"}</td>
                      </tr>
                    );
                  })}
                  {!monthSummaryRows.length && (
                    <tr><td colSpan={6} className="muted2">No data.</td></tr>
                  )}
                </tbody>
              </table>
            )}
          </div>
        </div>
        </FadeIn>
      </div>

      {replyBreakdownOpen && (
        <div
          onClick={() => setReplyBreakdownOpen(false)}
          className="dialogScrim"
        >
          <div
            className="card dialogCard"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="cardHeader">
              <div>
                <div className="cardTitle" style={{ fontFamily: "var(--sans)", fontWeight: 700 }}>Reply Breakdown</div>
                <div className="cardDesc">Category split and candidate semantics for replied leads</div>
              </div>
              <button className="btn" onClick={() => setReplyBreakdownOpen(false)}>Close</button>
            </div>
            <div className="cardBody">
              {replyCategoryStatus ? (
                <div className="muted2">{replyCategoryStatus}</div>
              ) : !replyCategorySummary.length ? (
                <div className="muted2">No categorized replies for selected period.</div>
              ) : (
                <>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 12, marginBottom: 18 }}>
                    {[
                      { label: "Raw reply events", value: replySemantics.raw_reply_events, sub: "current top KPI" },
                      { label: "Dedup replied leads", value: replySemantics.dedup_replied_leads, sub: "1 lead counted once" },
                      { label: "Official replied candidate", value: replySemantics.dedup_replied_minus_ooo_ai_bounce, sub: "dedup minus OOO / AI / sender bounce" },
                    ].map((item) => (
                      <div
                        key={item.label}
                        style={{
                          border: "1px solid var(--border)",
                          borderRadius: 16,
                          padding: "16px 18px",
                          background: "var(--bg2)",
                        }}
                      >
                        <div className="muted2" style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 8 }}>
                          {item.label}
                        </div>
                        <div style={{ fontSize: 30, fontWeight: 700, lineHeight: 1.05, fontFamily: "var(--sans)" }}>
                          {item.value.toLocaleString()}
                        </div>
                        <div className="muted2" style={{ fontSize: 12, marginTop: 8 }}>
                          {item.sub}
                        </div>
                      </div>
                    ))}
                  </div>

                  <div className="card" style={{ boxShadow: "none", border: "1px solid var(--border)" }}>
                    <div className="cardHeader">
                      <div>
                        <div className="cardTitle" style={{ fontFamily: "var(--sans)", fontWeight: 600, fontSize: 15 }}>Category Split</div>
                        <div className="cardDesc">Unique replied leads by SmartLead category</div>
                      </div>
                    </div>
                    <div className="cardBody">
                      <div className="btnRow" style={{ gap: 10, flexWrap: "wrap" }}>
                        {replyCategorySummary.map((item) => (
                          <span
                            key={item.label}
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              gap: 8,
                              padding: "9px 13px",
                              borderRadius: 999,
                              border: "1px solid var(--border)",
                              background: "var(--bg2)",
                              fontSize: 13,
                              fontFamily: "var(--sans)",
                            }}
                          >
                            <span className="muted">{item.label}</span>
                            <span style={{ color: "var(--text)", fontWeight: 700 }}>{item.count}</span>
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
