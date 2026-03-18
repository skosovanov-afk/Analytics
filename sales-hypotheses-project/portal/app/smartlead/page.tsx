"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { AppTopbar } from "../components/AppTopbar";
import { CountUp } from "../components/CountUp";
import { FadeIn } from "../components/FadeIn";
// ─── Types ────────────────────────────────────────────────────────────────────

type Period = "7d" | "30d" | "90d" | "all";

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

type Tab = "campaign" | "touch" | "month";

type MonthlyRow = {
  month: string;
  campaign_name: string;
  sent_count: number;
  reply_count: number;
  reply_rate_pct: number;
  booked_meetings: number;
  held_meetings: number;
};

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

// Fetch all rows with pagination (bypasses 1000-row API limit)
async function fetchAllRows(
  supabase: ReturnType<typeof createClient> | any,
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

async function fetchMonthlyRows(
  supabase: ReturnType<typeof createClient> | any
): Promise<MonthlyRow[]> {
  const all: MonthlyRow[] = [];
  for (let offset = 0; ; offset += PAGE_SIZE) {
    const { data, error } = await supabase
      .from("smartlead_kpi_monthly_v")
      .select("month,campaign_name,sent_count,reply_count,reply_rate_pct,booked_meetings,held_meetings")
      .order("month", { ascending: false })
      .range(offset, offset + PAGE_SIZE - 1);
    if (error) throw new Error(error.message);
    all.push(...((data ?? []) as MonthlyRow[]));
    if ((data?.length ?? 0) < PAGE_SIZE) break;
  }
  return all;
}

async function fetchManualEmailRows(
  supabase: ReturnType<typeof createClient> | any
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
            <line x1={pL} y1={yFor(t)} x2={W - pR} y2={yFor(t)} stroke="rgba(255,255,255,0.07)" strokeWidth={1} />
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
            <g key={si}>
              <path d={d} fill="none" stroke={s.color} strokeWidth={2.2} opacity={0.9} />
              {pts.map(([x, y], i) => (
                <circle key={i} cx={x} cy={y} r={3} fill={s.color} />
              ))}
            </g>
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

export default function SmartleadPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(
    () => (supabaseUrl && supabaseAnonKey ? createClient(supabaseUrl, supabaseAnonKey) : null),
    [supabaseUrl, supabaseAnonKey]
  );

  const [status, setStatus] = useState("");
  const [rows, setRows] = useState<DailyRow[]>([]);
  const [manualRows, setManualRows] = useState<ManualStatRow[]>([]);
  const [monthlyRows, setMonthlyRows] = useState<MonthlyRow[]>([]);

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
      since.setDate(since.getDate() - days);
      setDateRange({
        since: since.toISOString().slice(0, 10),
        until: until.toISOString().slice(0, 10),
      });
    }
  }, [period]);
  const [chartMetric, setChartMetric] = useState<"sent_count" | "reply_count">("sent_count");
  const [sortKey, setSortKey] = useState<string>("sent");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  // ─── Load (once, all data) ──────────────────────────────────────────────────

  async function load() {
    if (!supabase) return;
    setStatus("Loading…");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) {
      setStatus("Not signed in. Go back to / and sign in.");
      return;
    }
    try {
      const [data, manual, monthly] = await Promise.all([
        fetchAllRows(supabase, "smartlead_stats_daily", "date,campaign_name,sent_count,reply_count,touch_number,unique_leads_count"),
        fetchManualEmailRows(supabase),
        fetchMonthlyRows(supabase),
      ]);
      setRows(data);
      setManualRows(manual);
      setMonthlyRows(monthly);
    } catch (e: unknown) {
      setStatus(`Error: ${(e as Error).message}`);
      return;
    }
    setStatus("");
  }

  useEffect(() => { load(); }, [supabase]); // eslint-disable-line react-hooks/exhaustive-deps

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

  // ─── Derived ───────────────────────────────────────────────────────────────

  const weeks = useMemo(() => {
    return Array.from(new Set(filteredRows.map((r) => toWeek(r.date)))).sort();
  }, [filteredRows]);

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

  // Weekly chart series by campaign (top 8 by sent)
  const chartSeries = useMemo(() => {
    const byCampaign = new Map<string, Map<string, number>>();
    for (const r of filteredRows) {
      const wk = toWeek(r.date);
      if (!byCampaign.has(r.campaign_name)) byCampaign.set(r.campaign_name, new Map());
      const wm = byCampaign.get(r.campaign_name)!;
      wm.set(wk, (wm.get(wk) ?? 0) + n(r[chartMetric]));
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
      values: weeks.map((wk) => wm.get(wk) ?? 0),
    }));
  }, [filteredRows, weeks, chartMetric]);

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

  const filteredMonthlyRows = useMemo(() => {
    if (!dateRange.since && !dateRange.until) return monthlyRows;
    return monthlyRows.filter((r) => {
      if (dateRange.since && r.month < dateRange.since) return false;
      if (dateRange.until && r.month > dateRange.until) return false;
      return true;
    });
  }, [monthlyRows, dateRange]);

  const monthSummaryRows = useMemo(() => {
    const map = new Map<string, { sent: number; reply: number; booked: number; held: number }>();
    for (const r of filteredMonthlyRows) {
      if (!map.has(r.month)) map.set(r.month, { sent: 0, reply: 0, booked: 0, held: 0 });
      const d = map.get(r.month)!;
      d.sent += n(r.sent_count);
      d.reply += n(r.reply_count);
      d.booked = Math.max(d.booked, n(r.booked_meetings));
      d.held = Math.max(d.held, n(r.held_meetings));
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
  }, [filteredMonthlyRows]);

  function toggleSort(key: string) {
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
              <div className="cardTitle">Email Outreach Analytics</div>
              <div className="cardDesc">
                {rangeLabel} ·{" "}
                {Math.max(campaignRows.length, manualCampaignCount)} кампаний
              </div>
            </div>
            <div className="btnRow">
              <button className="btn" onClick={load} title="Refresh data">↻</button>
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

        {/* ── KPI cards ────────────────────────────────────────────────────── */}
        <FadeIn delay={60} style={{ gridColumn: "span 12" }}>
          <div className="kpiRow" style={{ gridTemplateColumns: "repeat(4, minmax(0, 1fr))" }}>
            {[
              { label: "Emails sent", numVal: totals.sent, strVal: null, sub: null },
              { label: "Replies", numVal: totals.reply, strVal: null, sub: pctStr(totals.reply, totals.sent) + " of sent" },
              { label: "Booked meetings", numVal: totals.booked_meetings, strVal: null, sub: pctValueStr(totals.cr_booked) + " of replies" },
              { label: "Held meetings", numVal: totals.held_meetings, strVal: null, sub: pctValueStr(totals.cr_held) + " of booked" },
              { label: "Reply rate", numVal: null, strVal: pctStr(totals.reply, totals.sent), sub: "replies / sent" },
              { label: "CR → Booked", numVal: null, strVal: pctValueStr(totals.cr_booked), sub: "booked / replies" },
              { label: "CR → Held", numVal: null, strVal: pctValueStr(totals.cr_held), sub: "held / booked" },
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

        {/* ── Weekly chart ─────────────────────────────────────────────────── */}
        <FadeIn delay={120} style={{ gridColumn: "span 12" }}>
        <div className="card">
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Weekly trend</div>
              <div className="cardDesc">{weeks.length} weeks · top campaigns</div>
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
            {!weeks.length
              ? <div className="muted2">No data for selected period.</div>
              : <LineChart weeks={weeks} series={chartSeries} />
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
                onClick={() => { setTab("campaign"); setSortKey("sent"); setSortDir("desc"); }}
              >
                By Campaign
              </button>
              <button
                className={`btn${tab === "touch" ? " btnPrimary" : ""}`}
                onClick={() => { setTab("touch"); }}
              >
                By Touch
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
                ? `${campaignRows.length} campaigns`
                : tab === "touch"
                ? `${touchRows.length} touches in sequence`
                : `${monthSummaryRows.length} months`
              }
            </div>
          </div>

          <div className="cardBody" style={{ overflowX: "auto" }}>
            {tab === "campaign" ? (
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
                  {campaignRows.map((r) => (
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
            ) : tab === "touch" ? (
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
                        <td className="mono">{r.booked || "—"}</td>
                        <td className="mono">{r.held || "—"}</td>
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
    </main>
  );
}
