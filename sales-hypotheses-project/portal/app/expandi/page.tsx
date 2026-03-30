"use client";

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { AppTopbar } from "../components/AppTopbar";
import { CountUp } from "../components/CountUp";
import { FadeIn } from "../components/FadeIn";
import { SpotlightCard } from "../components/SpotlightCard";
// ─── Types ────────────────────────────────────────────────────────────────────

type Period = "7d" | "30d" | "90d" | "all";
type ChartBucket = "day" | "week" | "month";

type DayRow = {
  day: string;
  account_name: string;
  campaign_name: string | null;
  connection_req: number;
  accepted: number;
  sent_messages: number;
  replies: number;
};

type AlltimeRow = {
  account_name: string;
  campaign_name: string;
  li_account_id?: number | null;
  current_instances?: number;
  campaign_missing_in_live_api?: boolean;
  connection_req: number;
  accepted: number;
  sent_messages: number;
  replies: number;
  booked_meetings: number;
  held_meetings: number;
  cr_to_accept_pct: number | null;
  cr_to_reply_pct: number | null;
};

type MeetingRow = {
  record_date: string;
  account_name: string | null;
  campaign_name: string | null;
  metric_name: string;
  value: number;
};

type CampaignAliasRow = {
  alias: string;
  canonical: string;
};

type CampaignStat = {
  account_name: string;
  campaign_name: string;
  connection_req: number;
  accepted: number;
  sent_messages: number;
  replies: number;
  booked_meetings: number;
  held_meetings: number;
  cr_to_accept_pct: number | null;
  cr_to_reply_pct: number | null;
};

type Metric = "connection_req" | "accepted" | "sent_messages" | "replies";
type ConversionMetric = "cr_accept" | "cr_reply";
type Tab = "account" | "campaign" | "month";

type MonthlyRow = {
  month: string;
  li_account_id: number;
  account_name: string;
  campaign_name: string;
  connection_req: number;
  accepted: number;
  sent_messages: number;
  replies: number;
  booked_meetings: number;
  held_meetings: number;
  month_total_booked_meetings?: number;
  month_total_held_meetings?: number;
};

type CampaignBucketStats = {
  connection_req: number;
  accepted: number;
  sent_messages: number;
  replies: number;
};

type ChartPoint = {
  value: number | null;
  tooltip?: string;
};

type LineChartSeries = {
  label: string;
  color: string;
  values: ChartPoint[];
  emphasis?: "normal" | "total";
};

const METRIC_LABELS: Record<Metric, string> = {
  connection_req: "Connections",
  accepted: "Accepted",
  sent_messages: "Messages",
  replies: "Replies",
};

const CONVERSION_METRIC_LABELS: Record<ConversionMetric, string> = {
  cr_accept: "CR Accept",
  cr_reply: "CR Reply",
};

const COLORS = ["#7dd3fc", "#a7f3d0", "#fca5a5", "#c4b5fd", "#fde68a", "#fdba74"];
const PAGE_SIZE = 1000;
const TOP_CAMPAIGN_LINES = 5;
const MIN_CONVERSION_DENOMINATOR = 20;
const LINKEDIN_UI_EXCLUDED_ACCOUNTS = new Set(["Legacy / manual supplement"]);

// ─── Helpers ──────────────────────────────────────────────────────────────────

function n(v: unknown): number {
  const x = Number(v);
  return Number.isFinite(x) ? x : 0;
}

function pctStr(v: number | null, decimals = 1): string {
  if (v == null) return "—";
  return `${v.toFixed(decimals)}%`;
}

function weekStart(dateStr: string): string {
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
  return period === "90d" ? "week" : "day";
}

function bucketStart(dateStr: string, bucket: ChartBucket): string {
  if (bucket === "month") return toMonth(dateStr);
  return bucket === "week" ? weekStart(dateStr) : String(dateStr).slice(0, 10);
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
    else cur.setUTCDate(cur.getUTCDate() + (bucket === "week" ? 7 : 1));
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
  return bucket === "week" ? "Weekly trend" : "Daily trend";
}

function bucketUnit(bucket: ChartBucket): string {
  if (bucket === "month") return "months";
  return bucket === "week" ? "weeks" : "days";
}

function emptyCampaignBucketStats(): CampaignBucketStats {
  return { connection_req: 0, accepted: 0, sent_messages: 0, replies: 0 };
}

async function fetchLinkedinAliases(
  supabase: ReturnType<typeof createClient> | any
): Promise<CampaignAliasRow[]> {
  const { data, error } = await supabase
    .from("campaign_name_aliases")
    .select("alias,canonical")
    .eq("channel", "linkedin");
  if (error) throw new Error(error.message);
  return (data ?? []) as CampaignAliasRow[];
}

function normalizeLinkedinMeetingCampaign(
  campaignName: string | null,
  accountName: string | null,
  aliasMap: Map<string, string>
): string | null {
  const raw = (campaignName ?? "").trim() || (accountName ?? "").trim();
  if (!raw) return null;
  return aliasMap.get(raw.toLowerCase()) ?? raw;
}

function displayLinkedinAccount(accounts: Set<string>): string {
  const special = new Set(["Manual adjustment", "Manual import"]);
  const realAccounts = Array.from(accounts).filter((name) => name && !special.has(name));
  if (realAccounts.length === 1) return realAccounts[0];
  if (realAccounts.length > 1) return `${realAccounts.length} accounts`;
  if (accounts.has("Manual adjustment")) return "Manual / unassigned";
  if (accounts.has("Manual import")) return "Manual import";
  return "Unspecified";
}

function mapLinkedinAlltimeV2Row(row: Record<string, unknown>): AlltimeRow {
  const apiConnectionReq = n(row.api_connection_req);
  const apiAccepted = n(row.api_accepted);
  const apiReplies = n(row.api_replies);
  const historySentMessages = n(row.history_sent_messages);
  const manualConnectionReq = n(row.manual_connection_req);
  const manualAccepted = n(row.manual_accepted);
  const manualSentMessages = n(row.manual_sent_messages);
  const manualReplies = n(row.manual_replies);
  const currentInstances = n(row.current_instances);
  const missingInLiveApi = Boolean(row.campaign_missing_in_live_api);

  const isLegacyOnly =
    missingInLiveApi ||
    (
      currentInstances === 0 &&
      apiConnectionReq === 0 &&
      apiAccepted === 0 &&
      apiReplies === 0 &&
      (manualConnectionReq > 0 || manualAccepted > 0 || manualSentMessages > 0 || manualReplies > 0)
    );

  const connectionReq = isLegacyOnly ? manualConnectionReq : apiConnectionReq;
  const accepted = isLegacyOnly ? manualAccepted : apiAccepted;
  const sentMessages = isLegacyOnly ? manualSentMessages : historySentMessages;
  const replies = isLegacyOnly ? manualReplies : apiReplies;

  return {
    account_name: String(row.account_name ?? "Legacy / manual supplement"),
    campaign_name: String(row.campaign_name ?? "Unspecified"),
    li_account_id: row.li_account_id == null ? null : n(row.li_account_id),
    current_instances: currentInstances,
    campaign_missing_in_live_api: missingInLiveApi,
    connection_req: connectionReq,
    accepted,
    sent_messages: sentMessages,
    replies,
    booked_meetings: n(row.booked_meetings),
    held_meetings: n(row.held_meetings),
    cr_to_accept_pct: connectionReq > 0 ? round2(Math.min((accepted / connectionReq) * 100, 100)) : null,
    cr_to_reply_pct: sentMessages > 0 ? round2((replies / sentMessages) * 100) : null,
  };
}

function formatPercentTick(v: number): string {
  const digits = v >= 10 ? 0 : 1;
  return `${v.toFixed(digits)}%`;
}

function formatMetricValue(v: number): string {
  return v >= 1000 ? `${(v / 1000).toFixed(1)}k` : `${v.toFixed(0)}`;
}

function round2(v: number): number {
  return Math.round(v * 100) / 100;
}

async function fetchAllPaginated<T>(
  fetcher: (from: number, to: number) => PromiseLike<{ data: T[] | null; count: number | null; error: any }>
): Promise<T[]> {
  const first = await fetcher(0, PAGE_SIZE - 1);
  if (first.error) throw first.error;
  const rows: T[] = [...(first.data ?? [])];
  const total = first.count ?? rows.length;
  if (total <= PAGE_SIZE) return rows;
  const extraPages = Math.ceil((total - PAGE_SIZE) / PAGE_SIZE);
  const rest = await Promise.all(
    Array.from({ length: extraPages }, (_, i) => {
      const from = (i + 1) * PAGE_SIZE;
      return fetcher(from, from + PAGE_SIZE - 1);
    })
  );
  for (const r of rest) {
    if (r.error) throw r.error;
    rows.push(...(r.data ?? []));
  }
  return rows;
}

// ─── SVG Line Chart ───────────────────────────────────────────────────────────

function smoothSegment(points: [number, number][]): string {
  if (!points.length) return "";
  if (points.length === 1) {
    const [x, y] = points[0];
    return `M${x.toFixed(1)},${y.toFixed(1)}L${x.toFixed(1)},${y.toFixed(1)}`;
  }
  if (points.length === 2) {
    return points.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  }
  const d = [`M${points[0][0].toFixed(1)},${points[0][1].toFixed(1)}`];
  for (let i = 1; i < points.length - 1; i++) {
    const current = points[i];
    const next = points[i + 1];
    const midX = (current[0] + next[0]) / 2;
    const midY = (current[1] + next[1]) / 2;
    d.push(`Q${current[0].toFixed(1)},${current[1].toFixed(1)} ${midX.toFixed(1)},${midY.toFixed(1)}`);
  }
  const prev = points[points.length - 2];
  const last = points[points.length - 1];
  d.push(`Q${prev[0].toFixed(1)},${prev[1].toFixed(1)} ${last[0].toFixed(1)},${last[1].toFixed(1)}`);
  return d.join(" ");
}

function smoothSegments(pts: Array<[number, number] | null>): string[] {
  const segments: string[] = [];
  let current: [number, number][] = [];
  for (const pt of pts) {
    if (!pt) {
      if (current.length) segments.push(smoothSegment(current));
      current = [];
      continue;
    }
    current.push(pt);
  }
  if (current.length) segments.push(smoothSegment(current));
  return segments;
}

function LineChart({
  buckets,
  bucket,
  series,
  animationKey,
  formatYTick,
  tooltipTitle,
}: {
  buckets: string[];
  bucket: ChartBucket;
  series: LineChartSeries[];
  animationKey: string;
  formatYTick?: (value: number) => string;
  tooltipTitle?: string;
}) {
  const W = 960, H = 220, pL = 46, pR = 12, pT = 16, pB = 28;
  const iW = W - pL - pR;
  const iH = H - pT - pB;
  const chartRef = useRef<HTMLDivElement | null>(null);
  const tipRef = useRef<HTMLDivElement | null>(null);
  const [hover, setHover] = useState<{ i: number; x: number; y: number } | null>(null);
  const [tipPos, setTipPos] = useState<{ left: number; top: number }>({ left: 8, top: 8 });

  const allVals = series.flatMap((s) => s.values.map((point) => point.value)).filter((v): v is number => typeof v === "number" && Number.isFinite(v));
  const maxVal = allVals.length ? Math.max(...allVals, 1) : 1;
  const span = maxVal || 1;

  const xFor = (i: number) =>
    buckets.length <= 1 ? pL + iW / 2 : pL + (i * iW) / (buckets.length - 1);
  const yFor = (v: number) => pT + ((maxVal - v) * iH) / span;

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((t) => maxVal * t);
  const step = Math.max(1, Math.floor(buckets.length / 8));

  const hoverBreakdown = hover
    ? series
        .map((s) => ({
          label: s.label,
          color: s.color,
          emphasis: s.emphasis,
          point: s.values[hover.i],
        }))
        .filter((row) => row.point && typeof row.point.value === "number")
        .sort((a, b) => {
          if (a.emphasis === "total" && b.emphasis !== "total") return -1;
          if (a.emphasis !== "total" && b.emphasis === "total") return 1;
          return Number(b.point?.value ?? 0) - Number(a.point?.value ?? 0);
        })
    : [];

  useLayoutEffect(() => {
    if (!hover) return;
    const wrap = chartRef.current;
    const tip = tipRef.current;
    if (!wrap || !tip) return;
    const wRect = wrap.getBoundingClientRect();
    const tRect = tip.getBoundingClientRect();
    const pad = 8;
    const offset = 12;
    let left = hover.x + offset;
    let top = hover.y + offset;
    if (left + tRect.width > wRect.width - pad) left = hover.x - tRect.width - offset;
    if (top + tRect.height > wRect.height - pad) top = hover.y - tRect.height - offset;
    left = Math.max(pad, Math.min(left, wRect.width - tRect.width - pad));
    top = Math.max(pad, Math.min(top, wRect.height - tRect.height - pad));
    setTipPos({ left, top });
  }, [hover]);

  return (
    <div ref={chartRef} style={{ position: "relative" }}>
      <svg key={animationKey} viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: "block" }} onMouseLeave={() => setHover(null)}>
        {yTicks.map((t, i) => (
          <g key={i}>
            <line
              x1={pL} y1={yFor(t)} x2={W - pR} y2={yFor(t)}
              stroke="rgba(255,255,255,0.07)" strokeWidth={1}
            />
            <text x={pL - 6} y={yFor(t) + 4} textAnchor="end" fontSize={10} fill="rgba(255,255,255,0.4)">
              {formatYTick ? formatYTick(t) : t >= 1000 ? `${(t / 1000).toFixed(0)}k` : t.toFixed(0)}
            </text>
          </g>
        ))}

        {buckets.map((value, i) => {
          if (i % step !== 0 && i !== buckets.length - 1) return null;
          return (
            <text key={value} x={xFor(i)} y={H - 6} textAnchor="middle" fontSize={10} fill="rgba(255,255,255,0.38)">
              {formatBucketLabel(value, bucket)}
            </text>
          );
        })}

        {series.map((s, si) => {
          if (!s.values.length) return null;
          const pts = s.values.map((point, i) => (typeof point.value === "number" ? [xFor(i), yFor(point.value)] as [number, number] : null));
          const segments = smoothSegments(pts);
          const strokeWidth = s.emphasis === "total" ? 3.2 : 2.4;
          return (
            <g key={si}>
              {segments.map((d, i) => (
                <g key={i}>
                  <path className="chartLineGlow" d={d} pathLength={1} fill="none" stroke={s.color} strokeWidth={s.emphasis === "total" ? 9 : 7} opacity={s.emphasis === "total" ? 0.22 : 0.16} />
                  <path
                    className="chartLineStroke"
                    d={d}
                    pathLength={1}
                    fill="none"
                    stroke={s.color}
                    strokeWidth={strokeWidth}
                    opacity={s.emphasis === "total" ? 1 : 0.95}
                    style={{ animationDelay: `${si * 90 + i * 40}ms` }}
                  />
                </g>
              ))}
              {pts.map((pt, i) => (
                pt ? (
                  <circle
                    key={i}
                    className="chartPointReveal"
                    cx={pt[0]}
                    cy={pt[1]}
                    r={s.emphasis === "total" ? 3.8 : 3.2}
                    fill={s.color}
                    style={{ animationDelay: `${220 + si * 70 + i * 24}ms` }}
                  />
                ) : null
              ))}
            </g>
          );
        })}

        {buckets.map((_, i) => {
          const x = xFor(i);
          const colW = buckets.length > 1 ? (xFor(1) - xFor(0)) : (W - pL - pR);
          return (
            <rect
              key={`hover:${i}`}
              x={x - colW / 2}
              y={pT}
              width={colW}
              height={H - pT - pB}
              fill="transparent"
              onMouseMove={(e: any) => {
                const svg = e.currentTarget.ownerSVGElement as any;
                const rect = svg?.getBoundingClientRect?.();
                const relX = rect ? (e.clientX ?? 0) - rect.left : 0;
                const relY = rect ? (e.clientY ?? 0) - rect.top : 0;
                setHover({ i, x: relX, y: relY });
              }}
              onMouseEnter={() => setHover({ i, x, y: pT + 12 })}
            />
          );
        })}
      </svg>

      {hover && hoverBreakdown.length ? (
        <div
          className="card"
          ref={tipRef}
          style={{
            position: "absolute",
            left: tipPos.left,
            top: tipPos.top,
            padding: 10,
            width: 280,
            background: "rgba(10,12,18,0.94)",
            border: "1px solid rgba(255,255,255,0.10)",
            pointerEvents: "none",
            zIndex: 10,
          }}
        >
          <div className="mono" style={{ fontSize: 12, opacity: 0.9 }}>
            {tooltipTitle ? `${tooltipTitle} · ` : ""}{buckets[hover.i]}
          </div>
          <div style={{ marginTop: 8, display: "grid", gap: 6 }}>
            {hoverBreakdown.map((row) => (
              <div key={row.label} style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <span style={{ width: 10, height: 10, borderRadius: 2, background: row.color, display: "inline-block" }} />
                <span style={{ flex: 1, fontSize: 12, opacity: row.emphasis === "total" ? 1 : 0.9, fontWeight: row.emphasis === "total" ? 700 : 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {row.label}
                </span>
                <span className="mono" style={{ fontSize: 12, opacity: 0.92 }}>
                  {row.point?.tooltip ?? ""}
                </span>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <div style={{ display: "flex", gap: 14, flexWrap: "wrap", marginTop: 10 }}>
        {series.map((s, i) => (
          <span key={i} style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: "rgba(255,255,255,0.6)" }}>
            <span style={{ width: s.emphasis === "total" ? 16 : 12, height: s.emphasis === "total" ? 4 : 3, borderRadius: 2, background: s.color, display: "inline-block" }} />
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

export default function ExpandiPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(
    () => (supabaseUrl && supabaseAnonKey ? createClient(supabaseUrl, supabaseAnonKey) : null),
    [supabaseUrl, supabaseAnonKey]
  );

  const [status, setStatus] = useState("");
  const [loading, setLoading] = useState(false);
  const [alltimeRows, setAlltimeRows] = useState<AlltimeRow[]>([]);
  const [dayRows, setDayRows] = useState<DayRow[]>([]);
  const [manualMeetings, setManualMeetings] = useState<MeetingRow[]>([]);
  const [monthlyRows, setMonthlyRows] = useState<MonthlyRow[]>([]);

  const [period, setPeriod] = useState<Period>("all");
  const [dateRange, setDateRange] = useState<{ since: string; until: string }>({ since: "", until: "" });
  const [filterAccount, setFilterAccount] = useState<string>("all");

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
  const [tab, setTab] = useState<Tab>("campaign");
  const [activityMetric, setActivityMetric] = useState<Metric>("replies");
  const [conversionMetric, setConversionMetric] = useState<ConversionMetric>("cr_reply");
  const [sortKey, setSortKey] = useState<string>("replies");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [tablePage, setTablePage] = useState(0);
  const [tablePageSize, setTablePageSize] = useState<number | "all">(25);

  // ─── Initial load: alltime view + date bounds ──────────────────────────────

  async function loadInitial() {
    if (!supabase) return;
    setStatus("Loading…");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) {
      setStatus("Not signed in. Go back to / and sign in.");
      return;
    }
    try {
      const [atData, mData, aliases] = await Promise.all([
        fetchAllPaginated<Record<string, unknown>>((from, to) =>
          supabase!
            .from("linkedin_kpi_alltime_v2")
            .select("account_name,campaign_name,li_account_id,current_instances,campaign_missing_in_live_api,api_connection_req,api_accepted,api_replies,history_sent_messages,manual_connection_req,manual_accepted,manual_sent_messages,manual_replies,booked_meetings,held_meetings", { count: "exact" })
            .order("account_name", { ascending: true })
            .order("campaign_name", { ascending: true })
            .range(from, to)
        ),
        fetchAllPaginated<MonthlyRow>((from, to) =>
          supabase!
            .from("linkedin_kpi_monthly_v2")
            .select("month,li_account_id,account_name,campaign_name,connection_req,accepted,sent_messages,replies,booked_meetings,held_meetings,month_total_booked_meetings,month_total_held_meetings", { count: "exact" })
            .order("month", { ascending: false })
            .range(from, to)
        ),
        fetchLinkedinAliases(supabase),
      ]);
      const aliasMap = new Map(
        aliases.map((row) => [row.alias.trim().toLowerCase(), row.canonical.trim()])
      );
      setAlltimeRows(
        atData.map((raw) => mapLinkedinAlltimeV2Row(raw))
      );
      setMonthlyRows(mData);
      setStatus("");
    } catch (err) {
      setStatus(`Error: ${err instanceof Error ? err.message : "Failed to load"}`);
    }
  }

  // ─── Range load: daily view + manual meetings (server-side filtered) ───────

  useEffect(() => {
    if (!supabase || !dateRange.since || !dateRange.until) {
      setDayRows([]);
      setManualMeetings([]);
      return;
    }
    const { since, until } = dateRange;
    let cancelled = false;
    setLoading(true);

    Promise.all([
      fetchAllPaginated<DayRow>((from, to) =>
        supabase!
          .from("linkedin_kpi_daily_v2")
          .select("day,account_name,campaign_name,connection_req,accepted,sent_messages,replies", { count: "exact" })
          .gte("day", since)
          .lte("day", until)
          .order("day", { ascending: true })
          .range(from, to)
      ),
      supabase
        .from("manual_stats")
        .select("record_date,account_name,campaign_name,metric_name,value")
        .eq("channel", "linkedin")
        .in("metric_name", ["booked_meetings", "held_meetings"])
        .gte("record_date", since)
        .lte("record_date", until),
      fetchLinkedinAliases(supabase),
    ])
      .then(([days, meetingsRes, aliases]) => {
        if (cancelled) return;
        const aliasMap = new Map(
          aliases.map((row) => [row.alias.trim().toLowerCase(), row.canonical.trim()])
        );
        setDayRows(days);
        setManualMeetings(
          ((meetingsRes.data ?? []) as MeetingRow[]).map((row) => ({
            ...row,
            campaign_name: normalizeLinkedinMeetingCampaign(row.campaign_name, row.account_name, aliasMap),
          }))
        );
      })
      .catch((err) => {
        if (!cancelled) setStatus(`Error: ${err instanceof Error ? err.message : "Failed to load range"}`);
      })
      .finally(() => { if (!cancelled) setLoading(false); });

    return () => { cancelled = true; };
  }, [dateRange.since, dateRange.until, supabase]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => { loadInitial(); }, [supabase]); // eslint-disable-line react-hooks/exhaustive-deps

  // ─── Derived data ──────────────────────────────────────────────────────────

  const hasRange = !!(dateRange.since && dateRange.until);

  const accounts = useMemo(
    () =>
      Array.from(
        new Set(
          [...alltimeRows.map((r) => r.account_name), ...dayRows.map((r) => r.account_name)].filter(
            (name) => name && !LINKEDIN_UI_EXCLUDED_ACCOUNTS.has(name)
          )
        )
      ).sort(),
    [alltimeRows, dayRows]
  );

  // meetings by campaign key (coalesce campaign_name, account_name — mirrors SQL fix)
  const meetingsMap = useMemo(() => {
    const map = new Map<string, { booked: number; held: number }>();
    for (const row of manualMeetings) {
      const key = row.campaign_name?.trim() || row.account_name?.trim() || "Unspecified";
      if (!map.has(key)) map.set(key, { booked: 0, held: 0 });
      const val = n(row.value);
      if (row.metric_name === "booked_meetings") map.get(key)!.booked += val;
      else map.get(key)!.held += val;
    }
    return map;
  }, [manualMeetings]);

  // Campaign rows: alltime view when no range, daily aggregation when range active
  const campaignRows = useMemo((): CampaignStat[] => {
    if (!hasRange) {
      const rows = filterAccount !== "all"
        ? alltimeRows.filter((r) => r.account_name === filterAccount)
        : alltimeRows;
      const grouped = new Map<string, CampaignStat & { __accounts: Set<string> }>();
      for (const row of rows) {
        const campaign = row.campaign_name?.trim() || "Unspecified";
        if (!grouped.has(campaign)) {
          grouped.set(campaign, {
            account_name: row.account_name,
            campaign_name: campaign,
            connection_req: 0,
            accepted: 0,
            sent_messages: 0,
            replies: 0,
            booked_meetings: 0,
            held_meetings: 0,
            cr_to_accept_pct: null,
            cr_to_reply_pct: null,
            __accounts: new Set<string>(),
          });
        }
        const d = grouped.get(campaign)!;
        d.__accounts.add(row.account_name);
        d.connection_req += n(row.connection_req);
        d.accepted += n(row.accepted);
        d.sent_messages += n(row.sent_messages);
        d.replies += n(row.replies);
        d.booked_meetings += n(row.booked_meetings);
        d.held_meetings += n(row.held_meetings);
      }
      return Array.from(grouped.values()).map((row) => ({
        account_name: displayLinkedinAccount(row.__accounts),
        campaign_name: row.campaign_name,
        connection_req: row.connection_req,
        accepted: row.accepted,
        sent_messages: row.sent_messages,
        replies: row.replies,
        booked_meetings: row.booked_meetings,
        held_meetings: row.held_meetings,
        cr_to_accept_pct: row.connection_req > 0 ? round2(Math.min((row.accepted / row.connection_req) * 100, 100)) : null,
        cr_to_reply_pct: row.sent_messages > 0 ? round2((row.replies / row.sent_messages) * 100) : null,
      })).sort((a, b) => {
        const av = n(a[sortKey as keyof CampaignStat]);
        const bv = n(b[sortKey as keyof CampaignStat]);
        return sortDir === "desc" ? bv - av : av - bv;
      });
    }

    const filtered = filterAccount !== "all"
      ? dayRows.filter((r) => r.account_name === filterAccount)
      : dayRows;

    const map = new Map<string, { conn: number; acc: number; msg: number; rep: number; accounts: Set<string>; campaign: string }>();
    for (const r of filtered) {
      const campaign = r.campaign_name?.trim() || "Unspecified";
      if (!map.has(campaign)) map.set(campaign, { conn: 0, acc: 0, msg: 0, rep: 0, accounts: new Set<string>(), campaign });
      const d = map.get(campaign)!;
      d.accounts.add(r.account_name);
      d.conn += n(r.connection_req);
      d.acc += n(r.accepted);
      d.msg += n(r.sent_messages);
      d.rep += n(r.replies);
    }

    return Array.from(map.values())
      .map((d) => {
        const m = meetingsMap.get(d.campaign) ?? { booked: 0, held: 0 };
        return {
          account_name: displayLinkedinAccount(d.accounts),
          campaign_name: d.campaign,
          connection_req: d.conn,
          accepted: d.acc,
          sent_messages: d.msg,
          replies: d.rep,
          booked_meetings: m.booked,
          held_meetings: m.held,
          cr_to_accept_pct: d.conn > 0 ? round2(Math.min((d.acc / d.conn) * 100, 100)) : null,
          cr_to_reply_pct: d.msg > 0 ? round2((d.rep / d.msg) * 100) : null,
          cr_booked: d.rep > 0 ? round2((m.booked / d.rep) * 100) : null,
          cr_held: m.booked > 0 ? round2((m.held / m.booked) * 100) : null,
        };
      })
      .sort((a, b) => {
        const av = n(a[sortKey as keyof CampaignStat]);
        const bv = n(b[sortKey as keyof CampaignStat]);
        return sortDir === "desc" ? bv - av : av - bv;
      });
  }, [hasRange, alltimeRows, dayRows, meetingsMap, filterAccount, sortKey, sortDir]);

  // Account rows derived from campaignRows (single source of truth)
  const accountRows = useMemo(() => {
    const source = hasRange
      ? (filterAccount !== "all" ? dayRows.filter((r) => r.account_name === filterAccount) : dayRows)
      : (filterAccount !== "all" ? alltimeRows.filter((r) => r.account_name === filterAccount) : alltimeRows);
    const map = new Map<string, { conn: number; acc: number; msg: number; rep: number }>();
    for (const r of source) {
      if (LINKEDIN_UI_EXCLUDED_ACCOUNTS.has(r.account_name)) continue;
      if (!map.has(r.account_name)) map.set(r.account_name, { conn: 0, acc: 0, msg: 0, rep: 0 });
      const d = map.get(r.account_name)!;
      d.conn += n(r.connection_req);
      d.acc += n(r.accepted);
      d.msg += n(r.sent_messages);
      d.rep += n(r.replies);
    }
    return Array.from(map.entries())
      .map(([name, d]) => ({
        name: displayLinkedinAccount(new Set([name])),
        connection_req: d.conn,
        accepted: d.acc,
        sent_messages: d.msg,
        replies: d.rep,
        cr_accept: d.conn > 0 ? Math.min((d.acc / d.conn) * 100, 100) : null,
        cr_reply: d.msg > 0 ? (d.rep / d.msg) * 100 : null,
      }))
      .sort((a, b) => {
        const av = n(a[sortKey as keyof typeof a]);
        const bv = n(b[sortKey as keyof typeof b]);
        return sortDir === "desc" ? bv - av : av - bv;
      });
  }, [hasRange, filterAccount, dayRows, alltimeRows, sortKey, sortDir]);

  // Totals derived from campaignRows
  const totals = useMemo(() => {
    let conn = 0, acc = 0, msg = 0, rep = 0, booked = 0, held = 0;
    for (const r of campaignRows) {
      conn += n(r.connection_req);
      acc += n(r.accepted);
      msg += n(r.sent_messages);
      rep += n(r.replies);
      booked += n(r.booked_meetings);
      held += n(r.held_meetings);
    }
    return {
      connection_req: conn,
      accepted: acc,
      sent_messages: msg,
      replies: rep,
      booked_meetings: booked,
      held_meetings: held,
      cr_accept: conn > 0 ? Math.min((acc / conn) * 100, 100) : 0,
      cr_reply: msg > 0 ? (rep / msg) * 100 : 0,
      cr_booked: rep > 0 ? (booked / rep) * 100 : 0,
      cr_held: booked > 0 ? (held / booked) * 100 : 0,
    };
  }, [campaignRows]);

  const chartBucket = useMemo(() => bucketForPeriod(period), [period]);

  const chartBuckets = useMemo(() => {
    if (chartBucket === "month") {
      const source = filterAccount !== "all"
        ? monthlyRows.filter((r) => r.account_name === filterAccount)
        : monthlyRows;
      if (!source.length) return [];
      const months = source.map((r) => bucketStart(r.month, "month")).sort();
      return enumerateBuckets(months[0], months[months.length - 1], "month");
    }
    if (!hasRange || !dateRange.since || !dateRange.until) return [];
    return enumerateBuckets(dateRange.since, dateRange.until, chartBucket);
  }, [chartBucket, filterAccount, monthlyRows, hasRange, dateRange]);

  const chartCampaignBuckets = useMemo(() => {
    if (chartBucket === "month") {
      const filtered = filterAccount !== "all"
        ? monthlyRows.filter((r) => r.account_name === filterAccount)
        : monthlyRows;
      const byCampaign = new Map<string, Map<string, CampaignBucketStats>>();
      for (const r of filtered) {
        const key = bucketStart(r.month, "month");
        const campaign = r.campaign_name?.trim() || "Unspecified";
        if (!byCampaign.has(campaign)) byCampaign.set(campaign, new Map());
        const wm = byCampaign.get(campaign)!;
        if (!wm.has(key)) wm.set(key, emptyCampaignBucketStats());
        const stats = wm.get(key)!;
        stats.connection_req += n(r.connection_req);
        stats.accepted += n(r.accepted);
        stats.sent_messages += n(r.sent_messages);
        stats.replies += n(r.replies);
      }
      return byCampaign;
    }
    const byCampaign = new Map<string, Map<string, CampaignBucketStats>>();
    if (!hasRange || !dayRows.length) return byCampaign;
    const filtered = filterAccount !== "all" ? dayRows.filter((r) => r.account_name === filterAccount) : dayRows;
    for (const r of filtered) {
      const key = bucketStart(r.day, chartBucket);
      const campaign = r.campaign_name?.trim() || "Unspecified";
      if (!byCampaign.has(campaign)) byCampaign.set(campaign, new Map());
      const wm = byCampaign.get(campaign)!;
      if (!wm.has(key)) wm.set(key, emptyCampaignBucketStats());
      const stats = wm.get(key)!;
      stats.connection_req += n(r.connection_req);
      stats.accepted += n(r.accepted);
      stats.sent_messages += n(r.sent_messages);
      stats.replies += n(r.replies);
    }
    return byCampaign;
  }, [chartBucket, monthlyRows, hasRange, dayRows, filterAccount]);

  const totalChartBuckets = useMemo(() => {
    const totals = new Map<string, CampaignBucketStats>();
    for (const bucketMap of chartCampaignBuckets.values()) {
      for (const [bucketKey, stats] of bucketMap.entries()) {
        if (!totals.has(bucketKey)) totals.set(bucketKey, emptyCampaignBucketStats());
        const total = totals.get(bucketKey)!;
        total.connection_req += stats.connection_req;
        total.accepted += stats.accepted;
        total.sent_messages += stats.sent_messages;
        total.replies += stats.replies;
      }
    }
    return totals;
  }, [chartCampaignBuckets]);

  const activityChartSeries = useMemo(() => {
    const topSeries = Array.from(chartCampaignBuckets.entries())
      .map(([label, wm]) => ({
        label,
        total: Array.from(wm.values()).reduce((sum, bucketStats) => sum + n(bucketStats[activityMetric]), 0),
        wm,
      }))
      .sort((a, b) => b.total - a.total)
      .slice(0, TOP_CAMPAIGN_LINES)
      .map(({ label, wm }, i) => ({
        label,
        color: COLORS[i % COLORS.length],
        values: chartBuckets.map((key) => {
          const value = wm.has(key) ? n(wm.get(key)?.[activityMetric]) : null;
          return {
            value,
            tooltip: value == null ? undefined : formatMetricValue(value),
          };
        }),
      }));
    const totalSeries: LineChartSeries = {
      label: "Total",
      color: "#ffffff",
      emphasis: "total",
      values: chartBuckets.map((key) => {
        const value = totalChartBuckets.has(key) ? n(totalChartBuckets.get(key)?.[activityMetric]) : null;
        return {
          value,
          tooltip: value == null ? undefined : formatMetricValue(value),
        };
      }),
    };
    return [totalSeries, ...topSeries];
  }, [chartCampaignBuckets, totalChartBuckets, activityMetric, chartBuckets]);

  const conversionChartSeries = useMemo(() => {
    const topSeries = Array.from(chartCampaignBuckets.entries())
      .map(([label, wm]) => ({
        label,
        signal: Array.from(wm.values()).reduce((sum, bucketStats) => {
          const base = conversionMetric === "cr_accept" ? bucketStats.connection_req : bucketStats.sent_messages;
          return sum + n(base);
        }, 0),
        wm,
        }))
      .filter((row) => row.signal > 0)
      .sort((a, b) => b.signal - a.signal)
      .slice(0, TOP_CAMPAIGN_LINES)
      .map(({ label, wm }, i) => ({
        label,
        color: COLORS[i % COLORS.length],
        values: chartBuckets.map((key) => {
          const bucketStats = wm.get(key);
          if (!bucketStats) return { value: null };
          if (conversionMetric === "cr_accept") {
            if (bucketStats.connection_req < MIN_CONVERSION_DENOMINATOR) return { value: null };
            const value = round2(Math.min((bucketStats.accepted / bucketStats.connection_req) * 100, 100));
            return {
              value,
              tooltip: `${bucketStats.accepted}/${bucketStats.connection_req} = ${pctStr(value, 1)}`,
            };
          }
          if (bucketStats.sent_messages < MIN_CONVERSION_DENOMINATOR) return { value: null };
          const value = round2((bucketStats.replies / bucketStats.sent_messages) * 100);
          return {
            value,
            tooltip: `${bucketStats.replies}/${bucketStats.sent_messages} = ${pctStr(value, 1)}`,
          };
        }),
      }));
    const totalSeries: LineChartSeries = {
      label: "Total",
      color: "#ffffff",
      emphasis: "total",
      values: chartBuckets.map((key) => {
        const bucketStats = totalChartBuckets.get(key);
        if (!bucketStats) return { value: null };
        if (conversionMetric === "cr_accept") {
          if (bucketStats.connection_req < MIN_CONVERSION_DENOMINATOR) return { value: null };
          const value = round2(Math.min((bucketStats.accepted / bucketStats.connection_req) * 100, 100));
          return {
            value,
            tooltip: `${bucketStats.accepted}/${bucketStats.connection_req} = ${pctStr(value, 1)}`,
          };
        }
        if (bucketStats.sent_messages < MIN_CONVERSION_DENOMINATOR) return { value: null };
        const value = round2((bucketStats.replies / bucketStats.sent_messages) * 100);
        return {
          value,
          tooltip: `${bucketStats.replies}/${bucketStats.sent_messages} = ${pctStr(value, 1)}`,
        };
      }),
    };
    return [totalSeries, ...topSeries];
  }, [chartCampaignBuckets, totalChartBuckets, conversionMetric, chartBuckets]);

  const activityChartAnimationKey = useMemo(
    () => [period, filterAccount, activityMetric, chartBucket, chartBuckets.join("|"), activityChartSeries.map((s) => s.label).join("|")].join("::"),
    [period, filterAccount, activityMetric, chartBucket, chartBuckets, activityChartSeries]
  );

  const conversionChartAnimationKey = useMemo(
    () => [period, filterAccount, conversionMetric, chartBucket, chartBuckets.join("|"), conversionChartSeries.map((s) => s.label).join("|")].join("::"),
    [period, filterAccount, conversionMetric, chartBucket, chartBuckets, conversionChartSeries]
  );

  const monthSummaryRows = useMemo(() => {
    const map = new Map<string, { conn: number; acc: number; msg: number; rep: number; booked: number; held: number }>();

    if (!hasRange) {
      const filtered = filterAccount !== "all"
        ? monthlyRows.filter((r) => r.account_name === filterAccount)
        : monthlyRows;
      for (const r of filtered) {
        if (!map.has(r.month)) map.set(r.month, { conn: 0, acc: 0, msg: 0, rep: 0, booked: 0, held: 0 });
        const d = map.get(r.month)!;
        d.conn += n(r.connection_req);
        d.acc += n(r.accepted);
        d.msg += n(r.sent_messages);
        d.rep += n(r.replies);
        const monthBooked = n(r.month_total_booked_meetings);
        const monthHeld = n(r.month_total_held_meetings);
        d.booked = Math.max(d.booked, monthBooked > 0 ? monthBooked : n(r.booked_meetings));
        d.held = Math.max(d.held, monthHeld > 0 ? monthHeld : n(r.held_meetings));
      }
    } else {
      const filteredDays = filterAccount !== "all"
        ? dayRows.filter((r) => r.account_name === filterAccount)
        : dayRows;
      for (const r of filteredDays) {
        const month = toMonth(r.day);
        if (!map.has(month)) map.set(month, { conn: 0, acc: 0, msg: 0, rep: 0, booked: 0, held: 0 });
        const d = map.get(month)!;
        d.conn += n(r.connection_req);
        d.acc += n(r.accepted);
        d.msg += n(r.sent_messages);
        d.rep += n(r.replies);
      }

      const meetingByMonth = new Map<string, { nullBooked: number; nullHeld: number; namedBooked: number; namedHeld: number }>();
      for (const row of manualMeetings) {
        const month = toMonth(row.record_date);
        if (!meetingByMonth.has(month)) {
          meetingByMonth.set(month, { nullBooked: 0, nullHeld: 0, namedBooked: 0, namedHeld: 0 });
        }
        const stats = meetingByMonth.get(month)!;
        const val = n(row.value);
        const isNullBucket = !row.account_name && !row.campaign_name;
        if (row.metric_name === "booked_meetings") {
          if (isNullBucket) stats.nullBooked += val;
          else stats.namedBooked += val;
        } else {
          if (isNullBucket) stats.nullHeld += val;
          else stats.namedHeld += val;
        }
      }

      for (const [month, stats] of meetingByMonth.entries()) {
        if (!map.has(month)) map.set(month, { conn: 0, acc: 0, msg: 0, rep: 0, booked: 0, held: 0 });
        const d = map.get(month)!;
        d.booked = stats.nullBooked > 0 ? stats.nullBooked : stats.namedBooked;
        d.held = stats.nullHeld > 0 ? stats.nullHeld : stats.namedHeld;
      }
    }

    return Array.from(map.entries())
      .map(([month, d]) => ({
        month,
        connection_req: d.conn,
        accepted: d.acc,
        sent_messages: d.msg,
        replies: d.rep,
        cr_accept: d.conn > 0 ? Math.min((d.acc / d.conn) * 100, 100) : null,
        cr_reply: d.msg > 0 ? (d.rep / d.msg) * 100 : null,
        booked: d.booked,
        held: d.held,
      }))
      .sort((a, b) => b.month.localeCompare(a.month));
  }, [hasRange, filterAccount, monthlyRows, dayRows, manualMeetings]);

  function toggleSort(key: string) {
    setTablePage(0);
    if (sortKey === key) setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    else { setSortKey(key); setSortDir("desc"); }
  }

  const rangeLabel = period === "all" ? "All time · since 2025-08-31" : `Last ${period}`;

  // ─── Render ──────────────────────────────────────────────────────────────────

  return (
    <main>
      <AppTopbar title="LinkedIn Analytics" subtitle="Expandi outreach performance" />

      <div className="page grid">
        {(status || loading) && (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardBody"><div className="notice">{loading ? "Loading range…" : status}</div></div>
          </div>
        )}

        {/* ── Filters ─────────────────────────────────────────────────────── */}
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">LinkedIn Outreach Analytics</div>
              <div className="cardDesc">
                {rangeLabel} ·{" "}
                {filterAccount === "all" ? "All accounts" : filterAccount}
              </div>
            </div>
            <div className="btnRow">
              <select
                className="select"
                style={{ width: "auto" }}
                value={filterAccount}
                onChange={(e) => setFilterAccount(e.target.value)}
              >
                <option value="all">All accounts</option>
                {accounts.map((a) => (
                  <option key={a} value={a}>{a}</option>
                ))}
              </select>
              <button className="btn" onClick={loadInitial} title="Refresh data">↻</button>
            </div>
          </div>
          <div className="cardBody">
            <div className="btnRow">
              {(["7d", "30d", "90d", "all"] as Period[]).map((p) => (
                <button
                  key={p}
                  className={`btn${period === p ? " btnPrimary" : ""}`}
                  onClick={() => setPeriod(p)}
                >
                  {p === "all" ? "All" : p.toUpperCase()}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* ── KPI cards ───────────────────────────────────────────────────── */}
        <FadeIn delay={60} style={{ gridColumn: "span 12" }}>
          <div className="kpiRow" style={{ gridTemplateColumns: "repeat(5, minmax(0, 1fr))" }}>
            {[
              { label: "Connections sent", numVal: totals.connection_req, strVal: null, sub: null },
              { label: "Accepted", numVal: totals.accepted, strVal: null, sub: `${pctStr(totals.cr_accept)} of connections` },
              { label: "Messages sent", numVal: totals.sent_messages, strVal: null, sub: null },
              { label: "Replies received", numVal: totals.replies, strVal: null, sub: `${pctStr(totals.cr_reply)} of messages` },
              { label: "Booked meetings", numVal: totals.booked_meetings, strVal: null, sub: `${pctStr(totals.cr_booked)} of replies` },
              { label: "Held meetings", numVal: totals.held_meetings, strVal: null, sub: `${pctStr(totals.cr_held)} of booked` },
              { label: "CR → Accept", numVal: null, strVal: pctStr(totals.cr_accept), sub: "accept / connections" },
              { label: "CR → Reply", numVal: null, strVal: pctStr(totals.cr_reply), sub: "replies / messages" },
              { label: "CR → Booked", numVal: null, strVal: pctStr(totals.cr_booked), sub: "booked / replies" },
              { label: "CR → Held", numVal: null, strVal: pctStr(totals.cr_held), sub: "held / booked" },
            ].map((kpi) => (
              <SpotlightCard key={kpi.label} className="card kpiCard" style={{ gridColumn: "auto" }}>
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

        <FadeIn delay={120} style={{ gridColumn: "span 12" }}>
          <div className="card">
            <div className="cardHeader">
              <div>
                <div className="cardTitle">{bucketTitle(chartBucket).replace("trend", "activity")}</div>
                <div className="cardDesc">{chartBuckets.length} {bucketUnit(chartBucket)} · total + top campaigns</div>
              </div>
              <div className="btnRow">
                {(Object.keys(METRIC_LABELS) as Metric[]).map((m) => (
                  <button
                    key={m}
                    className={`btn${activityMetric === m ? " btnPrimary" : ""}`}
                    onClick={() => setActivityMetric(m)}
                  >
                    {METRIC_LABELS[m]}
                  </button>
                ))}
              </div>
            </div>
            <div className="cardBody">
              {!chartBuckets.length
                ? <div className="muted2">No data for selected period.</div>
                : <LineChart
                    buckets={chartBuckets}
                    bucket={chartBucket}
                    series={activityChartSeries}
                    animationKey={activityChartAnimationKey}
                    tooltipTitle={METRIC_LABELS[activityMetric]}
                  />
              }
            </div>
          </div>
        </FadeIn>

        <FadeIn delay={160} style={{ gridColumn: "span 12" }}>
          <div className="card">
            <div className="cardHeader">
              <div>
                <div className="cardTitle">{bucketTitle(chartBucket).replace("trend", "conversion")}</div>
                <div className="cardDesc">{chartBuckets.length} {bucketUnit(chartBucket)} · total + top campaigns · hidden below {MIN_CONVERSION_DENOMINATOR} denominator</div>
              </div>
              <div className="btnRow">
                {(Object.keys(CONVERSION_METRIC_LABELS) as ConversionMetric[]).map((m) => (
                  <button
                    key={m}
                    className={`btn${conversionMetric === m ? " btnPrimary" : ""}`}
                    onClick={() => setConversionMetric(m)}
                  >
                    {CONVERSION_METRIC_LABELS[m]}
                  </button>
                ))}
              </div>
            </div>
            <div className="cardBody">
              {!chartBuckets.length
                ? <div className="muted2">No data for selected period.</div>
                : !conversionChartSeries.length
                ? <div className="muted2">Not enough denominator data for conversion trend.</div>
                : <LineChart
                    buckets={chartBuckets}
                    bucket={chartBucket}
                    series={conversionChartSeries}
                    animationKey={conversionChartAnimationKey}
                    formatYTick={formatPercentTick}
                    tooltipTitle={CONVERSION_METRIC_LABELS[conversionMetric]}
                  />
              }
            </div>
          </div>
        </FadeIn>

        {/* ── Tables ──────────────────────────────────────────────────────── */}
        <FadeIn delay={220} style={{ gridColumn: "span 12" }}>
        <div className="card">
          <div className="cardHeader">
            <div className="btnRow">
              <button
                className={`btn${tab === "campaign" ? " btnPrimary" : ""}`}
                onClick={() => { setTab("campaign"); setSortKey("replies"); setSortDir("desc"); setTablePage(0); }}
              >
                By Campaign
              </button>
              <button
                className={`btn${tab === "account" ? " btnPrimary" : ""}`}
                onClick={() => { setTab("account"); setSortKey("replies"); setSortDir("desc"); setTablePage(0); }}
              >
                By Account
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
                ? `${campaignRows.length} campaigns · ${rangeLabel.toLowerCase()}`
                : tab === "account"
                ? `${accountRows.length} accounts · ${rangeLabel.toLowerCase()}`
                : `${monthSummaryRows.length} months`
              }
            </div>
          </div>

          <div className="cardBody" style={{ overflowX: "auto" }}>
            {tab === "month" ? (
              <table className="table">
                <thead>
                  <tr>
                    <th>Month</th>
                    <th>Connections</th>
                    <th>Accepted</th>
                    <th>Messages</th>
                    <th>Replies</th>
                    <th>CR Accept</th>
                    <th>CR Reply</th>
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
                        <td className="mono">{r.connection_req.toLocaleString()}</td>
                        <td className="mono">{r.accepted.toLocaleString()}</td>
                        <td className="mono">{r.sent_messages.toLocaleString()}</td>
                        <td className="mono">{r.replies.toLocaleString()}</td>
                        <td className="mono">{pctStr(r.cr_accept)}</td>
                        <td className="mono">{pctStr(r.cr_reply)}</td>
                        <td className="mono">{r.booked || "—"}</td>
                        <td className="mono">{r.held || "—"}</td>
                      </tr>
                    );
                  })}
                  {!monthSummaryRows.length && (
                    <tr><td colSpan={9} className="muted2">No data.</td></tr>
                  )}
                </tbody>
              </table>
            ) : tab === "campaign" ? (() => {
              const ps = tablePageSize === "all" ? campaignRows.length : tablePageSize;
              const totalPages = Math.ceil(campaignRows.length / ps);
              const visibleRows = tablePageSize === "all" ? campaignRows : campaignRows.slice(tablePage * ps, (tablePage + 1) * ps);
              const from = Math.min(tablePage * ps + 1, campaignRows.length);
              const to = Math.min((tablePage + 1) * ps, campaignRows.length);
              return (
              <>
              <table className="table">
                <thead>
                  <tr>
                    <th>Campaign</th>
                    <th>Account</th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("connection_req")}>
                      Connections <SortIcon col="connection_req" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("accepted")}>
                      Accepted <SortIcon col="accepted" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("sent_messages")}>
                      Messages <SortIcon col="sent_messages" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("replies")}>
                      Replies <SortIcon col="replies" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("booked_meetings")}>
                      Booked <SortIcon col="booked_meetings" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("held_meetings")}>
                      Held <SortIcon col="held_meetings" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th>CR Accept</th>
                    <th>CR Reply</th>
                    <th>CR Booked</th>
                    <th>CR Held</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleRows.map((r, i) => {
                    const crAccept = r.cr_to_accept_pct ?? (r.connection_req > 0 ? (r.accepted / r.connection_req) * 100 : null);
                    const crReply = r.cr_to_reply_pct ?? (r.sent_messages > 0 ? (r.replies / r.sent_messages) * 100 : null);
                    const crBooked = r.replies > 0 ? (r.booked_meetings / r.replies) * 100 : null;
                    const crHeld = r.booked_meetings > 0 ? (r.held_meetings / r.booked_meetings) * 100 : null;
                    return (
                      <tr key={`${r.account_name}:${r.campaign_name}:${i}`}>
                        <td><b>{r.campaign_name}</b></td>
                        <td className="muted">{r.account_name}</td>
                        <td className="mono">{r.connection_req}</td>
                        <td className="mono">{r.accepted}</td>
                        <td className="mono">{r.sent_messages}</td>
                        <td className="mono">{r.replies}</td>
                        <td className="mono">{r.booked_meetings || "—"}</td>
                        <td className="mono">{r.held_meetings || "—"}</td>
                        <td className="mono">{pctStr(crAccept)}</td>
                        <td className="mono">{pctStr(crReply)}</td>
                        <td className="mono">{crBooked != null ? pctStr(crBooked) : "—"}</td>
                        <td className="mono">{crHeld != null ? pctStr(crHeld) : "—"}</td>
                      </tr>
                    );
                  })}
                  {!campaignRows.length && (
                    <tr><td colSpan={12} className="muted2">No data.</td></tr>
                  )}
                </tbody>
              </table>
              {campaignRows.length > 10 && (
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "12px 4px 4px", borderTop: "1px solid rgba(255,255,255,0.06)", marginTop: 4, fontSize: 13 }}>
                  <span className="muted2">
                    {tablePageSize === "all" ? `All ${campaignRows.length} campaigns` : `${from}–${to} of ${campaignRows.length}`}
                  </span>
                  <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                    <span className="muted2">Rows per page:</span>
                    <select
                      value={tablePageSize}
                      onChange={(e) => { const v = e.target.value === "all" ? "all" : Number(e.target.value); setTablePageSize(v as number | "all"); setTablePage(0); }}
                      style={{ background: "transparent", color: "inherit", border: "1px solid rgba(255,255,255,0.15)", borderRadius: 4, padding: "3px 6px", fontSize: 13, cursor: "pointer" }}
                    >
                      <option value={10}>10</option>
                      <option value={25}>25</option>
                      <option value={50}>50</option>
                      <option value="all">All</option>
                    </select>
                    {tablePageSize !== "all" && totalPages > 1 && (
                      <div style={{ display: "flex", gap: 2 }}>
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
            })() : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Account</th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("connection_req")}>
                      Connections <SortIcon col="connection_req" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("accepted")}>
                      Accepted <SortIcon col="accepted" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("sent_messages")}>
                      Messages <SortIcon col="sent_messages" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("replies")}>
                      Replies <SortIcon col="replies" sortKey={sortKey} sortDir={sortDir} />
                    </th>
                    <th>CR Accept</th>
                    <th>CR Reply</th>
                  </tr>
                </thead>
                <tbody>
                  {accountRows.map((r) => (
                    <tr key={r.name}>
                      <td><b>{r.name}</b></td>
                      <td className="mono">{r.connection_req}</td>
                      <td className="mono">{r.accepted}</td>
                      <td className="mono">{r.sent_messages}</td>
                      <td className="mono">{r.replies}</td>
                      <td className="mono">{pctStr(r.cr_accept)}</td>
                      <td className="mono">{pctStr(r.cr_reply)}</td>
                    </tr>
                  ))}
                  {!accountRows.length && (
                    <tr><td colSpan={7} className="muted2">No data.</td></tr>
                  )}
                </tbody>
              </table>
            )}
          </div>
        </div>
        </FadeIn>
      </div>
    </main>
  );
}
