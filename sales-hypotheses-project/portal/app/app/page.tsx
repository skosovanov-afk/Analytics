"use client";

import { useEffect, useMemo, useState } from "react";
import { AppTopbar } from "../components/AppTopbar";
import { CountUp } from "../components/CountUp";
import { DonutChart, type DonutSlice } from "../components/DonutChart";
import { FadeIn } from "../components/FadeIn";
import { SpotlightCard } from "../components/SpotlightCard";
import { getSupabase } from "../lib/supabase";
// ─── Types ────────────────────────────────────────────────────────────────────

type Period = "7d" | "30d" | "90d" | "all";

type ManualStatRow = {
  record_date: string;
  account_name: string | null;
  campaign_name: string | null;
  metric_name: string;
  value: number;
};

type CampaignRow = {
  campaign_name: string;
  total_touches: number;
  replies: number;
  booked_meetings: number;
  held_meetings: number;
  cr_reply: number | null;
  cr_booked: number | null;
  cr_held: number | null;
};

type SortDir = "asc" | "desc";

const PAGE_SIZE = 1000;

const METRICS = ["total_touches", "replies", "booked_meetings", "held_meetings"] as const;

// ─── Helpers ──────────────────────────────────────────────────────────────────

function n(v: unknown): number {
  const x = Number(v);
  return Number.isFinite(x) ? x : 0;
}

function pctStr(v: number | null, decimals = 1): string {
  if (v == null) return "—";
  return `${v.toFixed(decimals)}%`;
}

function SortIcon({ col, sortKey, sortDir }: { col: string; sortKey: string; sortDir: SortDir }) {
  if (sortKey !== col) return <span style={{ opacity: 0.28, marginLeft: 3 }}>↕</span>;
  return <span style={{ marginLeft: 3 }}>{sortDir === "desc" ? "↓" : "↑"}</span>;
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function AppAnalyticsPage() {
  const supabase = useMemo(() => getSupabase(), []);

  const [status, setStatus] = useState("");
  const [loading, setLoading] = useState(false);
  const [rows, setRows] = useState<ManualStatRow[]>([]);
  const [period, setPeriod] = useState<Period>("all");
  const [dateRange, setDateRange] = useState<{ since: string; until: string }>({ since: "", until: "" });
  const [sortKey, setSortKey] = useState<string>("total_touches");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

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

  async function load() {
    if (!supabase) return;
    setLoading(true);
    setStatus("Loading…");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) {
      setStatus("Not signed in. Go back to / and sign in.");
      setLoading(false);
      return;
    }
    try {
      const out: ManualStatRow[] = [];
      for (let from = 0; from < 50000; from += PAGE_SIZE) {
        const { data, error } = await supabase
          .from("manual_stats")
          .select("record_date,account_name,campaign_name,metric_name,value")
          .eq("channel", "app")
          .in("metric_name", METRICS)
          .order("record_date", { ascending: true })
          .range(from, from + PAGE_SIZE - 1);
        if (error) throw error;
        const batch = (data ?? []) as ManualStatRow[];
        out.push(...batch);
        if (batch.length < PAGE_SIZE) break;
      }
      setRows(out);
      setStatus("");
    } catch (err) {
      setStatus(`Error: ${err instanceof Error ? err.message : "Failed to load"}`);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load(); }, [supabase]); // eslint-disable-line react-hooks/exhaustive-deps

  // ─── Client-side filtering ────────────────────────────────────────────────

  const filteredRows = useMemo(() => {
    if (!dateRange.since && !dateRange.until) return rows;
    return rows.filter((r) => {
      if (dateRange.since && r.record_date < dateRange.since) return false;
      if (dateRange.until && r.record_date > dateRange.until) return false;
      return true;
    });
  }, [rows, dateRange]);

  const campaignRows = useMemo((): CampaignRow[] => {
    const map = new Map<string, { touches: number; replies: number; booked: number; held: number }>();
    for (const row of filteredRows) {
      const campaign = row.campaign_name?.trim() || row.account_name?.trim() || "Unspecified";
      if (!map.has(campaign)) map.set(campaign, { touches: 0, replies: 0, booked: 0, held: 0 });
      const d = map.get(campaign)!;
      if (row.metric_name === "total_touches") d.touches += n(row.value);
      if (row.metric_name === "replies") d.replies += n(row.value);
      if (row.metric_name === "booked_meetings") d.booked += n(row.value);
      if (row.metric_name === "held_meetings") d.held += n(row.value);
    }
    return Array.from(map.entries())
      .map(([campaign_name, d]) => ({
        campaign_name,
        total_touches: d.touches,
        replies: d.replies,
        booked_meetings: d.booked,
        held_meetings: d.held,
        cr_reply: d.touches > 0 ? (d.replies / d.touches) * 100 : null,
        cr_booked: d.replies > 0 ? (d.booked / d.replies) * 100 : null,
        cr_held: d.booked > 0 ? (d.held / d.booked) * 100 : null,
      }))
      .sort((a, b) => {
        const av = n(a[sortKey as keyof CampaignRow]);
        const bv = n(b[sortKey as keyof CampaignRow]);
        return sortDir === "desc" ? bv - av : av - bv;
      });
  }, [filteredRows, sortKey, sortDir]);

  const totals = useMemo(() => {
    let touches = 0, replies = 0, booked = 0, held = 0;
    for (const r of campaignRows) {
      touches += r.total_touches;
      replies += r.replies;
      booked += r.booked_meetings;
      held += r.held_meetings;
    }
    return {
      total_touches: touches,
      replies,
      booked_meetings: booked,
      held_meetings: held,
      cr_reply: touches > 0 ? (replies / touches) * 100 : null,
      cr_booked: replies > 0 ? (booked / replies) * 100 : null,
      cr_held: booked > 0 ? (held / booked) * 100 : null,
    };
  }, [campaignRows]);

  const [donutMetric, setDonutMetric] = useState<"touches" | "replies">("touches");
  const DONUT_COLORS = ["#86efac", "#22c55e", "#a78bfa", "#7dd3fc", "#fca5a5", "#facc15", "#fb923c", "#f9a8d4"];

  const donutSlices = useMemo((): DonutSlice[] => {
    const sorted = [...campaignRows].sort((a, b) =>
      donutMetric === "touches" ? b.total_touches - a.total_touches : b.replies - a.replies
    );
    const top = sorted.slice(0, 8);
    const otherTotal = sorted.slice(8).reduce((s, r) => s + (donutMetric === "touches" ? r.total_touches : r.replies), 0);
    const slices: DonutSlice[] = top
      .filter(r => (donutMetric === "touches" ? r.total_touches : r.replies) > 0)
      .map((r, i) => ({
        label: r.campaign_name.length > 30 ? r.campaign_name.slice(0, 28) + "…" : r.campaign_name,
        value: donutMetric === "touches" ? r.total_touches : r.replies,
        color: DONUT_COLORS[i % DONUT_COLORS.length],
      }));
    if (otherTotal > 0) slices.push({ label: "Other", value: otherTotal, color: "#64748b" });
    return slices;
  }, [campaignRows, donutMetric]);

  function toggleSort(key: string) {
    if (sortKey === key) setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    else { setSortKey(key); setSortDir("desc"); }
  }

  const rangeLabel = period === "all" ? "All time" : `Last ${period}`;

  return (
    <main>
      <AppTopbar title="App Analytics" subtitle="App outreach & meetings performance" />

      <div className="page grid">
        {status && (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardBody"><div className="notice">{status}</div></div>
          </div>
        )}

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">App Outreach Analytics</div>
              <div className="cardDesc">{rangeLabel} · full funnel by campaign</div>
            </div>
            <div className="btnRow">
              <button className="btn" onClick={load} disabled={loading} title="Refresh data">↻</button>
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

        {/* ── KPI cards ─────────────────────────────────────────────────────── */}
        <FadeIn delay={60} style={{ gridColumn: "span 12" }}>
          <div className="kpiRow kpiRowSeven">
            {[
              { label: "Total touches", val: totals.total_touches, isNum: true, sub: null },
              { label: "Replies", val: totals.replies, isNum: true, sub: `${pctStr(totals.cr_reply)} of touches` },
              { label: "Booked meetings", val: totals.booked_meetings, isNum: true, sub: `${pctStr(totals.cr_booked)} of replies` },
              { label: "Held meetings", val: totals.held_meetings, isNum: true, sub: `${pctStr(totals.cr_held)} of booked` },
              { label: "CR → Reply", val: pctStr(totals.cr_reply), isNum: false, sub: "replies / touches" },
              { label: "CR → Booked", val: pctStr(totals.cr_booked), isNum: false, sub: "booked / replies" },
              { label: "CR → Held", val: pctStr(totals.cr_held), isNum: false, sub: "held / booked" },
            ].map((kpi) => (
              <SpotlightCard key={kpi.label} className="card kpiCard" style={{ gridColumn: "auto" }}>
                <div className="cardBody">
                  <div className="muted2" style={{ fontSize: 11, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                    {kpi.label}
                  </div>
                  <div style={{ fontSize: 26, fontWeight: 700, letterSpacing: "-0.02em", lineHeight: 1 }}>
                    {kpi.isNum
                      ? <CountUp to={kpi.val as number} duration={1.2} />
                      : kpi.val}
                  </div>
                  {kpi.sub && (
                    <div className="muted2" style={{ fontSize: 11, marginTop: 5 }}>{kpi.sub}</div>
                  )}
                </div>
              </SpotlightCard>
            ))}
          </div>
        </FadeIn>

        {/* ── Campaign donut ─────────────────────────────────────────────── */}
        <FadeIn delay={100} style={{ gridColumn: "span 12" }}>
          <div className="card">
            <div className="cardHeader">
              <div>
                <div className="cardTitle">By campaign</div>
                <div className="cardDesc">Top campaigns breakdown</div>
              </div>
              <div className="btnRow">
                <button className={`btn${donutMetric === "touches" ? " btnPrimary" : ""}`} onClick={() => setDonutMetric("touches")}>Touches</button>
                <button className={`btn${donutMetric === "replies" ? " btnPrimary" : ""}`} onClick={() => setDonutMetric("replies")}>Replies</button>
              </div>
            </div>
            <div className="cardBody" style={{ padding: "24px 32px" }}>
              <DonutChart slices={donutSlices} title={donutMetric} size={210} thickness={38} />
            </div>
          </div>
        </FadeIn>

        {/* ── Campaign table ────────────────────────────────────────────────── */}
        <FadeIn delay={120} style={{ gridColumn: "span 12" }}>
          <div className="card">
            <div className="cardHeader">
              <div className="cardTitle">By Campaign</div>
              <div className="muted2" style={{ fontSize: 12 }}>
                {campaignRows.length} campaigns · {rangeLabel.toLowerCase()}
              </div>
            </div>
            <div className="cardBody" style={{ overflowX: "auto" }}>
              <table className="table">
                <thead>
                  <tr>
                    <th>Campaign</th>
                    <th style={{ cursor: "pointer" }} onClick={() => toggleSort("total_touches")}>
                      Total touches <SortIcon col="total_touches" sortKey={sortKey} sortDir={sortDir} />
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
                    <th>CR Reply</th>
                    <th>CR Booked</th>
                    <th>CR Held</th>
                  </tr>
                </thead>
                <tbody>
                  {campaignRows.map((r) => (
                    <tr key={r.campaign_name}>
                      <td style={{ wordBreak: "break-word", maxWidth: "200px" }}><b>{r.campaign_name}</b></td>
                      <td className="mono">{r.total_touches}</td>
                      <td className="mono">{r.replies}</td>
                      <td className="mono">{r.booked_meetings}</td>
                      <td className="mono">{r.held_meetings}</td>
                      <td className="mono">{pctStr(r.cr_reply)}</td>
                      <td className="mono">{pctStr(r.cr_booked)}</td>
                      <td className="mono">{pctStr(r.cr_held)}</td>
                    </tr>
                  ))}
                  {!campaignRows.length && (
                    <tr><td colSpan={8} className="muted2">No data.</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </FadeIn>
      </div>
    </main>
  );
}
