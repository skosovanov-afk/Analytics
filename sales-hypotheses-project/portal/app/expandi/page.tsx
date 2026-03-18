"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { AppTopbar } from "../components/AppTopbar";
import { CountUp } from "../components/CountUp";
import { FadeIn } from "../components/FadeIn";
// ─── Types ────────────────────────────────────────────────────────────────────

type Period = "7d" | "30d" | "90d" | "all";

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
};

const METRIC_LABELS: Record<Metric, string> = {
  connection_req: "Connections",
  accepted: "Accepted",
  sent_messages: "Messages",
  replies: "Replies",
};

const COLORS = ["#7dd3fc", "#a7f3d0", "#fca5a5", "#c4b5fd", "#fde68a", "#fdba74"];
const PAGE_SIZE = 1000;

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

function smoothPath(pts: [number, number][]): string {
  if (pts.length < 2)
    return pts.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const tension = 0.4;
  const d: string[] = [`M${pts[0][0].toFixed(1)},${pts[0][1].toFixed(1)}`];
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[Math.max(i - 1, 0)];
    const p1 = pts[i];
    const p2 = pts[i + 1];
    const p3 = pts[Math.min(i + 2, pts.length - 1)];
    const cp1x = p1[0] + (p2[0] - p0[0]) * tension;
    const cp1y = p1[1] + (p2[1] - p0[1]) * tension;
    const cp2x = p2[0] - (p3[0] - p1[0]) * tension;
    const cp2y = p2[1] - (p3[1] - p1[1]) * tension;
    d.push(`C${cp1x.toFixed(1)},${cp1y.toFixed(1)} ${cp2x.toFixed(1)},${cp2y.toFixed(1)} ${p2[0].toFixed(1)},${p2[1].toFixed(1)}`);
  }
  return d.join(" ");
}

function LineChart({
  weeks,
  series,
}: {
  weeks: string[];
  series: Array<{ label: string; color: string; values: number[] }>;
}) {
  const W = 960, H = 220, pL = 46, pR = 12, pT = 16, pB = 28;
  const iW = W - pL - pR;
  const iH = H - pT - pB;

  const allVals = series.flatMap((s) => s.values).filter(Number.isFinite);
  const maxVal = allVals.length ? Math.max(...allVals) : 1;
  const span = maxVal || 1;

  const xFor = (i: number) =>
    weeks.length <= 1 ? pL + iW / 2 : pL + (i * iW) / (weeks.length - 1);
  const yFor = (v: number) => pT + ((maxVal - v) * iH) / span;

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((t) => maxVal * t);
  const step = Math.max(1, Math.floor(weeks.length / 8));

  return (
    <div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: "block" }}>
        {yTicks.map((t, i) => (
          <g key={i}>
            <line
              x1={pL} y1={yFor(t)} x2={W - pR} y2={yFor(t)}
              stroke="rgba(255,255,255,0.07)" strokeWidth={1}
            />
            <text x={pL - 6} y={yFor(t) + 4} textAnchor="end" fontSize={10} fill="rgba(255,255,255,0.4)">
              {t >= 1000 ? `${(t / 1000).toFixed(0)}k` : t.toFixed(0)}
            </text>
          </g>
        ))}

        {weeks.map((wk, i) => {
          if (i % step !== 0 && i !== weeks.length - 1) return null;
          return (
            <text key={wk} x={xFor(i)} y={H - 6} textAnchor="middle" fontSize={10} fill="rgba(255,255,255,0.38)">
              {wk.slice(5)}
            </text>
          );
        })}

        {series.map((s, si) => {
          if (!s.values.length) return null;
          const pts = s.values.map((v, i) => [xFor(i), yFor(v)] as [number, number]);
          const d = smoothPath(pts);
          return (
            <path key={si} d={d} fill="none" stroke={s.color} strokeWidth={2.2} opacity={0.9} />
          );
        })}
      </svg>

      <div style={{ display: "flex", gap: 14, flexWrap: "wrap", marginTop: 10 }}>
        {series.map((s, i) => (
          <span key={i} style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: "rgba(255,255,255,0.6)" }}>
            <span style={{ width: 12, height: 3, borderRadius: 2, background: s.color, display: "inline-block" }} />
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
      since.setDate(since.getDate() - days);
      setDateRange({
        since: since.toISOString().slice(0, 10),
        until: until.toISOString().slice(0, 10),
      });
    }
  }, [period]);
  const [tab, setTab] = useState<Tab>("campaign");
  const [chartMetric, setChartMetric] = useState<Metric>("replies");
  const [sortKey, setSortKey] = useState<string>("replies");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

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
      const [atData, mData] = await Promise.all([
        fetchAllPaginated<AlltimeRow>((from, to) =>
          supabase!
            .from("expandi_kpi_alltime_v")
            .select("account_name,campaign_name,connection_req,accepted,sent_messages,replies,cr_to_accept_pct,cr_to_reply_pct,booked_meetings,held_meetings", { count: "exact" })
            .order("account_name", { ascending: true })
            .order("campaign_name", { ascending: true })
            .range(from, to)
        ),
        fetchAllPaginated<MonthlyRow>((from, to) =>
          supabase!
            .from("expandi_kpi_monthly_v")
            .select("month,li_account_id,account_name,campaign_name,connection_req,accepted,sent_messages,replies,booked_meetings,held_meetings", { count: "exact" })
            .order("month", { ascending: false })
            .range(from, to)
        ),
      ]);
      setAlltimeRows(atData);
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
          .from("expandi_kpi_daily_v")
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
    ])
      .then(([days, meetingsRes]) => {
        if (cancelled) return;
        setDayRows(days);
        setManualMeetings((meetingsRes.data ?? []) as MeetingRow[]);
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
    () => Array.from(new Set([...alltimeRows.map((r) => r.account_name), ...dayRows.map((r) => r.account_name)])).sort(),
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
    const sep = "\x01";

    if (!hasRange) {
      const rows = filterAccount !== "all"
        ? alltimeRows.filter((r) => r.account_name === filterAccount)
        : alltimeRows;
      return [...rows].sort((a, b) => {
        const av = n(a[sortKey as keyof CampaignStat]);
        const bv = n(b[sortKey as keyof CampaignStat]);
        return sortDir === "desc" ? bv - av : av - bv;
      });
    }

    const filtered = filterAccount !== "all"
      ? dayRows.filter((r) => r.account_name === filterAccount)
      : dayRows;

    const map = new Map<string, { conn: number; acc: number; msg: number; rep: number; account: string; campaign: string }>();
    for (const r of filtered) {
      const campaign = r.campaign_name?.trim() || "Unspecified";
      const key = [r.account_name, campaign].join(sep);
      if (!map.has(key)) map.set(key, { conn: 0, acc: 0, msg: 0, rep: 0, account: r.account_name, campaign });
      const d = map.get(key)!;
      d.conn += n(r.connection_req);
      d.acc += n(r.accepted);
      d.msg += n(r.sent_messages);
      d.rep += n(r.replies);
    }

    return Array.from(map.values())
      .map((d) => {
        const m = meetingsMap.get(d.campaign) ?? { booked: 0, held: 0 };
        return {
          account_name: d.account,
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
    const map = new Map<string, { conn: number; acc: number; msg: number; rep: number; booked: number; held: number }>();
    for (const r of campaignRows) {
      if (!map.has(r.account_name)) map.set(r.account_name, { conn: 0, acc: 0, msg: 0, rep: 0, booked: 0, held: 0 });
      const d = map.get(r.account_name)!;
      d.conn += n(r.connection_req);
      d.acc += n(r.accepted);
      d.msg += n(r.sent_messages);
      d.rep += n(r.replies);
      d.booked += n(r.booked_meetings);
      d.held += n(r.held_meetings);
    }
    return Array.from(map.entries())
      .map(([name, d]) => ({
        name,
        connection_req: d.conn,
        accepted: d.acc,
        sent_messages: d.msg,
        replies: d.rep,
        booked_meetings: d.booked,
        held_meetings: d.held,
        cr_accept: d.conn > 0 ? Math.min((d.acc / d.conn) * 100, 100) : null,
        cr_reply: d.msg > 0 ? (d.rep / d.msg) * 100 : null,
        cr_booked: d.rep > 0 ? (d.booked / d.rep) * 100 : null,
        cr_held: d.booked > 0 ? (d.held / d.booked) * 100 : null,
      }))
      .sort((a, b) => {
        const av = n(a[sortKey as keyof typeof a]);
        const bv = n(b[sortKey as keyof typeof b]);
        return sortDir === "desc" ? bv - av : av - bv;
      });
  }, [campaignRows, sortKey, sortDir]);

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

  // Chart: bucket daily rows into weeks per account
  const chartWeeks = useMemo(
    () => Array.from(new Set(dayRows.map((r) => weekStart(r.day)))).sort(),
    [dayRows]
  );

  const chartSeries = useMemo(() => {
    if (!hasRange || !dayRows.length) return [];
    const filtered = filterAccount !== "all" ? dayRows.filter((r) => r.account_name === filterAccount) : dayRows;
    const byAccount = new Map<string, Map<string, number>>();
    for (const r of filtered) {
      const wk = weekStart(r.day);
      if (!byAccount.has(r.account_name)) byAccount.set(r.account_name, new Map());
      const wm = byAccount.get(r.account_name)!;
      wm.set(wk, (wm.get(wk) ?? 0) + n(r[chartMetric as keyof DayRow] as number));
    }
    return Array.from(byAccount.entries()).map(([label, wm], i) => ({
      label,
      color: COLORS[i % COLORS.length],
      values: chartWeeks.map((wk) => wm.get(wk) ?? 0),
    }));
  }, [hasRange, dayRows, filterAccount, chartMetric, chartWeeks]);

  const monthSummaryRows = useMemo(() => {
    const map = new Map<string, { conn: number; acc: number; msg: number; rep: number; booked: number; held: number }>();
    for (const r of monthlyRows) {
      if (!map.has(r.month)) map.set(r.month, { conn: 0, acc: 0, msg: 0, rep: 0, booked: 0, held: 0 });
      const d = map.get(r.month)!;
      d.conn += n(r.connection_req);
      d.acc += n(r.accepted);
      d.msg += n(r.sent_messages);
      d.rep += n(r.replies);
      d.booked = Math.max(d.booked, n(r.booked_meetings));
      d.held   = Math.max(d.held,   n(r.held_meetings));
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
  }, [monthlyRows]);

  function toggleSort(key: string) {
    if (sortKey === key) setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    else { setSortKey(key); setSortDir("desc"); }
  }

  const rangeLabel = period === "all" ? "All time" : `Last ${period}`;

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
              <div key={kpi.label} className="card kpiCard" style={{ gridColumn: "auto" }}>
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
              </div>
            ))}
          </div>
        </FadeIn>

        {/* ── Weekly chart (only when range is selected) ───────────────────── */}
        {hasRange && (
          <FadeIn delay={120} style={{ gridColumn: "span 12" }}>
            <div className="card">
              <div className="cardHeader">
                <div>
                  <div className="cardTitle">Weekly trend</div>
                  <div className="cardDesc">{chartWeeks.length} weeks · per account</div>
                </div>
                <div className="btnRow">
                  {(Object.keys(METRIC_LABELS) as Metric[]).map((m) => (
                    <button
                      key={m}
                      className={`btn${chartMetric === m ? " btnPrimary" : ""}`}
                      onClick={() => setChartMetric(m)}
                    >
                      {METRIC_LABELS[m]}
                    </button>
                  ))}
                </div>
              </div>
              <div className="cardBody">
                {!chartWeeks.length
                  ? <div className="muted2">No data for selected period.</div>
                  : <LineChart weeks={chartWeeks} series={chartSeries} />
                }
              </div>
            </div>
          </FadeIn>
        )}

        {/* ── Tables ──────────────────────────────────────────────────────── */}
        <FadeIn delay={200} style={{ gridColumn: "span 12" }}>
        <div className="card">
          <div className="cardHeader">
            <div className="btnRow">
              <button
                className={`btn${tab === "campaign" ? " btnPrimary" : ""}`}
                onClick={() => { setTab("campaign"); setSortKey("replies"); setSortDir("desc"); }}
              >
                By Campaign
              </button>
              <button
                className={`btn${tab === "account" ? " btnPrimary" : ""}`}
                onClick={() => { setTab("account"); setSortKey("replies"); setSortDir("desc"); }}
              >
                By Account
              </button>
              <button
                className={`btn${tab === "month" ? " btnPrimary" : ""}`}
                onClick={() => { setTab("month"); }}
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
            ) : tab === "campaign" ? (
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
                  {campaignRows.map((r, i) => {
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
            ) : (
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
