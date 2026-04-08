"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { AppTopbar } from "../components/AppTopbar";
import { getSupabase } from "../lib/supabase";

/* ── Types ─────────────────────────────────────────────────────────────────── */

type TalRow = {
  id: string;
  name: string;
  description: string | null;
  criteria: string | null;
  email_sent: number;
  email_replies: number;
  email_reply_rate: number | null;
  email_meetings: number;
  email_held_meetings: number;
  email_qualified_leads: number;
  li_invited: number;
  li_accepted: number;
  li_replies: number;
  li_accept_rate: number | null;
  li_meetings: number;
  li_held_meetings: number;
  li_qualified_leads: number;
  app_invitations: number;
  app_touches: number;
  app_replies: number;
  app_reply_rate: number | null;
  app_meetings: number;
  app_held_meetings: number;
  app_qualified_leads: number;
  tg_touches: number;
  tg_replies: number;
  tg_reply_rate: number | null;
  tg_meetings: number;
  tg_held_meetings: number;
  tg_qualified_leads: number;
  total_meetings: number;
  total_held_meetings: number;
  total_qualified_leads: number;
};

type HypRow = {
  id: string;
  title: string | null;
  status: string | null;
  decision: string | null;
  tal_id: string | null;
};

type UnlinkedCampaign = {
  channel: string;
  name: string;
  source_campaign_key: string;
};

/* ── Helpers ───────────────────────────────────────────────────────────────── */

function num(v: number) { return v.toLocaleString(); }

function pct(numerator: number, denominator: number): string {
  if (!denominator) return "-";
  return `${((numerator / denominator) * 100).toFixed(1)}%`;
}

function formatLabel(s: string) {
  return s.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function isoDate(d: Date) { return d.toISOString().slice(0, 10); }

/* ── Preset ranges ─────────────────────────────────────────────────────────── */

type RangePreset = "7d" | "30d" | "90d" | "ytd" | "all" | "custom";

function presetDates(preset: RangePreset): { since: string; until: string } {
  const now = new Date();
  const until = isoDate(now);
  if (preset === "all") return { since: "", until: "" };
  if (preset === "ytd") return { since: `${now.getFullYear()}-01-01`, until };
  const days = preset === "7d" ? 7 : preset === "30d" ? 30 : 90;
  const d = new Date(now);
  d.setDate(d.getDate() - days);
  return { since: isoDate(d), until };
}

/* ── Chart colors ──────────────────────────────────────────────────────────── */

const CH_COLORS: Record<string, string> = {
  email: "#7dd3fc",
  linkedin: "#a78bfa",
  app: "#86efac",
  telegram: "#fca5a5",
};

const FUNNEL_COLORS = ["rgba(255,255,255,0.85)", "rgba(255,255,255,0.55)", "#22c55e", "#38bdf8"];

/* ── Bar Chart (SVG) ───────────────────────────────────────────────────────── */

function BarChart({
  data,
  labelKey,
  bars,
}: {
  data: Array<Record<string, any>>;
  labelKey: string;
  bars: Array<{ key: string; color: string; label: string }>;
}) {
  const w = 600, h = 220, padL = 10, padR = 10, padT = 10, padB = 28;
  const barAreaW = w - padL - padR;
  const barAreaH = h - padT - padB;

  const maxVal = Math.max(1, ...data.flatMap((d) => bars.map((b) => Number(d[b.key]) || 0)));
  const groupW = barAreaW / Math.max(1, data.length);
  const barW = Math.min(32, (groupW - 8) / bars.length);

  return (
    <div>
      <svg viewBox={`0 0 ${w} ${h}`} width="100%" height={h} style={{ display: "block" }}>
        {data.map((d, gi) => {
          const gx = padL + gi * groupW + (groupW - barW * bars.length - (bars.length - 1) * 2) / 2;
          return bars.map((b, bi) => {
            const val = Number(d[b.key]) || 0;
            const barH = (val / maxVal) * barAreaH;
            const x = gx + bi * (barW + 2);
            const y = padT + barAreaH - barH;
            return (
              <g key={`${gi}-${bi}`}>
                <rect x={x} y={y} width={barW} height={barH} rx={3} fill={b.color} opacity={0.85} />
                {val > 0 && (
                  <text x={x + barW / 2} y={y - 4} textAnchor="middle" fontSize="10" fill="var(--muted)" fontFamily="var(--mono)">
                    {val >= 1000 ? `${(val / 1000).toFixed(1)}k` : val}
                  </text>
                )}
              </g>
            );
          });
        })}
        {data.map((d, gi) => {
          const cx = padL + gi * groupW + groupW / 2;
          return (
            <text key={`lbl-${gi}`} x={cx} y={h - 8} textAnchor="middle" fontSize="11" fill="var(--muted2)">
              {d[labelKey]}
            </text>
          );
        })}
      </svg>
      <div style={{ display: "flex", gap: 16, marginTop: 6, justifyContent: "center" }}>
        {bars.map((b) => (
          <span key={b.key} style={{ display: "inline-flex", alignItems: "center", gap: 5, fontSize: 11, color: "var(--muted)" }}>
            <span style={{ width: 8, height: 8, borderRadius: 2, background: b.color, display: "inline-block" }} />
            {b.label}
          </span>
        ))}
      </div>
    </div>
  );
}

/* ── Funnel Chart (SVG) ────────────────────────────────────────────────────── */

function FunnelChart({ steps }: { steps: Array<{ label: string; value: number; color: string; rate?: string }> }) {
  const maxVal = Math.max(1, ...steps.map((s) => s.value));
  const w = 480, h = 180, padL = 0, padR = 0;
  const stepH = (h - 20) / steps.length;

  return (
    <svg viewBox={`0 0 ${w} ${h}`} width="100%" height={h} style={{ display: "block" }}>
      {steps.map((s, i) => {
        const ratio = Math.max(0.08, s.value / maxVal);
        const barW = (w - padL - padR - 160) * ratio;
        const y = 10 + i * stepH;
        const barH = stepH - 6;
        return (
          <g key={i}>
            <rect x={padL} y={y} width={barW} height={barH} rx={6} fill={s.color} opacity={0.8} />
            <text x={barW + 10} y={y + barH / 2 + 1} dominantBaseline="middle" fontSize="13" fill="var(--text)" fontWeight="600" fontFamily="var(--mono)">
              {num(s.value)}
            </text>
            <text x={w - padR} y={y + barH / 2 + 1} dominantBaseline="middle" textAnchor="end" fontSize="12" fill="var(--muted2)">
              {s.label}{s.rate ? ` (${s.rate})` : ""}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

/* ── Page ──────────────────────────────────────────────────────────────────── */

export default function DashboardPage() {
  const supabase = useMemo(() => getSupabase(), []);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tals, setTals] = useState<TalRow[]>([]);
  const [hyps, setHyps] = useState<HypRow[]>([]);
  const [unlinked, setUnlinked] = useState<UnlinkedCampaign[]>([]);

  // Filters
  const [rangePreset, setRangePreset] = useState<RangePreset>("all");
  const [customSince, setCustomSince] = useState("");
  const [customUntil, setCustomUntil] = useState("");
  const [activeSince, setActiveSince] = useState("");
  const [activeUntil, setActiveUntil] = useState("");
  const [selectedTalId, setSelectedTalId] = useState<string>(""); // "" = all TALs

  const authRef = useMemo(() => ({ token: "" }), []);

  const loadTals = useCallback(async (since: string, until: string) => {
    if (!supabase) return;
    setLoading(true);
    setError(null);
    try {
      // Refresh session to avoid expired JWT
      const { data: { session } } = await supabase.auth.getSession();
      if (!session) { setError("Not signed in. Go back to / and sign in."); setLoading(false); return; }
      const token = `Bearer ${session.access_token}`;
      authRef.token = token;

      const res = await fetch("/api/dashboard/stats", {
        method: "POST",
        headers: { Authorization: token, "Content-Type": "application/json" },
        body: JSON.stringify({ since: since || null, until: until || null }),
      });
      const json = await res.json();
      if (!json.ok) throw new Error(json.error);
      setTals(json.tals ?? []);
      setActiveSince(since);
      setActiveUntil(until);
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, [supabase, authRef]);

  useEffect(() => {
    if (!supabase) return;
    async function init() {
      setLoading(true);
      setError(null);
      try {
        const { data: { session } } = await supabase!.auth.getSession();
        if (!session) { setError("Not signed in. Go back to / and sign in."); setLoading(false); return; }
        authRef.token = `Bearer ${session.access_token}`;

        const [statsRes, unlinkedRes, hypsRes] = await Promise.all([
          fetch("/api/dashboard/stats", {
            method: "POST",
            headers: { Authorization: authRef.token, "Content-Type": "application/json" },
            body: JSON.stringify({ since: null, until: null }),
          }),
          fetch("/api/tals/unlinked", { headers: { Authorization: authRef.token } }),
          supabase!.from("sales_hypothesis_rows")
            .select("id,title,status,decision,tal_id")
            .eq("source", "hypothesis")
            .order("updated_at", { ascending: false })
            .limit(500),
        ]);

        const statsJson = await statsRes.json();
        if (!statsJson.ok) throw new Error(statsJson.error);
        setTals(statsJson.tals ?? []);

        const unlinkedJson = await unlinkedRes.json().catch(() => ({ ok: false }));
        if (unlinkedJson.ok) setUnlinked(unlinkedJson.unlinked ?? []);

        if (!hypsRes.error) setHyps((hypsRes.data ?? []) as HypRow[]);
      } catch (e: any) {
        setError(String(e?.message || e));
      } finally {
        setLoading(false);
      }
    }
    init();
  }, [supabase, authRef]);

  function applyPreset(preset: RangePreset) {
    setRangePreset(preset);
    if (preset === "custom") return;
    const { since, until } = presetDates(preset);
    setCustomSince(since);
    setCustomUntil(until);
    loadTals(since, until);
  }

  function applyCustomRange() {
    setRangePreset("custom");
    loadTals(customSince, customUntil);
  }

  /* ── Filtered data (by selected TAL) ───────────────────────────────────── */

  const filteredTals = useMemo(() => {
    if (!selectedTalId) return tals;
    return tals.filter((t) => t.id === selectedTalId);
  }, [tals, selectedTalId]);

  /* ── Aggregations ──────────────────────────────────────────────────────── */

  const channelTotals = useMemo(() => {
    const ch = {
      email: { sent: 0, replies: 0, booked: 0, held: 0, ql: 0 },
      linkedin: { sent: 0, replies: 0, booked: 0, held: 0, ql: 0 },
      app: { sent: 0, replies: 0, booked: 0, held: 0, ql: 0 },
      telegram: { sent: 0, replies: 0, booked: 0, held: 0, ql: 0 },
    };
    for (const t of filteredTals) {
      ch.email.sent += t.email_sent; ch.email.replies += t.email_replies; ch.email.booked += t.email_meetings; ch.email.held += t.email_held_meetings; ch.email.ql += t.email_qualified_leads || 0;
      ch.linkedin.sent += t.li_invited; ch.linkedin.replies += t.li_replies; ch.linkedin.booked += t.li_meetings; ch.linkedin.held += t.li_held_meetings; ch.linkedin.ql += t.li_qualified_leads || 0;
      ch.app.sent += t.app_touches; ch.app.replies += t.app_replies; ch.app.booked += t.app_meetings; ch.app.held += t.app_held_meetings; ch.app.ql += t.app_qualified_leads || 0;
      ch.telegram.sent += t.tg_touches; ch.telegram.replies += t.tg_replies; ch.telegram.booked += t.tg_meetings; ch.telegram.held += t.tg_held_meetings; ch.telegram.ql += t.tg_qualified_leads || 0;
    }
    return ch;
  }, [filteredTals]);

  const grandTotal = useMemo(() => {
    const c = channelTotals;
    return {
      sent: c.email.sent + c.linkedin.sent + c.app.sent + c.telegram.sent,
      replies: c.email.replies + c.linkedin.replies + c.app.replies + c.telegram.replies,
      booked: c.email.booked + c.linkedin.booked + c.app.booked + c.telegram.booked,
      held: c.email.held + c.linkedin.held + c.app.held + c.telegram.held,
      ql: c.email.ql + c.linkedin.ql + c.app.ql + c.telegram.ql,
    };
  }, [channelTotals]);

  const statusCounts = useMemo(() => {
    const map: Record<string, number> = {};
    for (const h of hyps) {
      const s = h.status || "no_status";
      map[s] = (map[s] || 0) + 1;
    }
    return Object.entries(map).sort((a, b) => b[1] - a[1]);
  }, [hyps]);

  const decisionCounts = useMemo(() => {
    const map: Record<string, number> = {};
    for (const h of hyps) {
      const d = h.decision || "no_decision";
      map[d] = (map[d] || 0) + 1;
    }
    return Object.entries(map).sort((a, b) => b[1] - a[1]);
  }, [hyps]);

  /* ── Chart data ────────────────────────────────────────────────────────── */

  const channelBarData = useMemo(() => [
    { channel: "Email", sent: channelTotals.email.sent, replies: channelTotals.email.replies, booked: channelTotals.email.booked, held: channelTotals.email.held, ql: channelTotals.email.ql },
    { channel: "LinkedIn", sent: channelTotals.linkedin.sent, replies: channelTotals.linkedin.replies, booked: channelTotals.linkedin.booked, held: channelTotals.linkedin.held, ql: channelTotals.linkedin.ql },
    { channel: "App", sent: channelTotals.app.sent, replies: channelTotals.app.replies, booked: channelTotals.app.booked, held: channelTotals.app.held, ql: channelTotals.app.ql },
    { channel: "Telegram", sent: channelTotals.telegram.sent, replies: channelTotals.telegram.replies, booked: channelTotals.telegram.booked, held: channelTotals.telegram.held, ql: channelTotals.telegram.ql },
  ], [channelTotals]);

  const funnelSteps = useMemo(() => [
    { label: "Sent / Invited / Touches", value: grandTotal.sent, color: FUNNEL_COLORS[0] },
    { label: "Replies", value: grandTotal.replies, color: FUNNEL_COLORS[1], rate: grandTotal.sent ? pct(grandTotal.replies, grandTotal.sent) : undefined },
    { label: "Booked Meetings", value: grandTotal.booked, color: FUNNEL_COLORS[2], rate: grandTotal.replies ? pct(grandTotal.booked, grandTotal.replies) : undefined },
    { label: "Held Meetings", value: grandTotal.held, color: FUNNEL_COLORS[3], rate: grandTotal.booked ? pct(grandTotal.held, grandTotal.booked) : undefined },
    { label: "Qualified Leads", value: grandTotal.ql, color: FUNNEL_COLORS[4] ?? "#a78bfa", rate: grandTotal.held ? pct(grandTotal.ql, grandTotal.held) : undefined },
  ], [grandTotal]);

  /* ── Status colors ─────────────────────────────────────────────────────── */

  const STATUS_COLORS: Record<string, string> = {
    in_progress: "rgba(14, 165, 233, 0.16)", paused: "rgba(234, 179, 8, 0.16)",
    done: "rgba(34, 197, 94, 0.16)", cancelled: "rgba(239, 68, 68, 0.12)", no_status: "rgba(255, 255, 255, 0.04)",
  };
  const STATUS_TEXT: Record<string, string> = {
    in_progress: "#38bdf8", paused: "#facc15", done: "#22c55e", cancelled: "#f87171", no_status: "var(--muted2)",
  };

  const rangeLabel = activeSince && activeUntil
    ? `${activeSince} - ${activeUntil}`
    : activeSince ? `from ${activeSince}` : activeUntil ? `until ${activeUntil}` : "All time";

  const PRESETS: Array<{ key: RangePreset; label: string }> = [
    { key: "7d", label: "7 days" }, { key: "30d", label: "30 days" }, { key: "90d", label: "90 days" },
    { key: "ytd", label: "YTD" }, { key: "all", label: "All time" },
  ];

  /* ── Render ────────────────────────────────────────────────────────────── */

  return (
    <main>
      <AppTopbar title="Main dashboard" subtitle="Overview across TALs and hypotheses" />

      <div className="page" style={{ display: "flex", flexDirection: "column", gap: 16 }}>

        {/* ── Filters ─────────────────────────────────────────────────── */}
        <div className="card">
          <div className="cardBody" style={{ padding: "14px 22px" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
              <span className="muted2" style={{ fontSize: 12, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                Period
              </span>
              {PRESETS.map((p) => (
                <button
                  key={p.key}
                  className={`btn${rangePreset === p.key ? " btnPrimary" : ""}`}
                  style={{ padding: "6px 12px", fontSize: 13 }}
                  onClick={() => applyPreset(p.key)}
                >
                  {p.label}
                </button>
              ))}
              <span style={{ width: 1, height: 24, background: "var(--border)", margin: "0 4px" }} />
              <input type="date" className="input" style={{ width: 150, padding: "6px 10px", fontSize: 13 }} value={customSince} onChange={(e) => setCustomSince(e.target.value)} />
              <span className="muted2" style={{ fontSize: 13 }}>-</span>
              <input type="date" className="input" style={{ width: 150, padding: "6px 10px", fontSize: 13 }} value={customUntil} onChange={(e) => setCustomUntil(e.target.value)} />
              <button className="btn" style={{ padding: "6px 12px", fontSize: 13 }} onClick={applyCustomRange}>Apply</button>

              <span style={{ width: 1, height: 24, background: "var(--border)", margin: "0 4px" }} />

              <span className="muted2" style={{ fontSize: 12, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                TAL
              </span>
              <select
                className="select"
                style={{ width: 220, padding: "6px 10px", fontSize: 13 }}
                value={selectedTalId}
                onChange={(e) => setSelectedTalId(e.target.value)}
              >
                <option value="">All TALs ({tals.length})</option>
                {tals.filter((t) => t.name).sort((a, b) => a.name.localeCompare(b.name)).map((t) => (
                  <option key={t.id} value={t.id}>{t.name}</option>
                ))}
              </select>

              {loading && <span className="muted2" style={{ fontSize: 13 }}>Loading...</span>}
            </div>
          </div>
        </div>

        {error && (
          <div className="card"><div className="cardBody"><div className="notice">{error}</div></div></div>
        )}

        {!error && (
          <>
            {/* ── Channel KPI Cards ───────────────────────────────────── */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 16, opacity: loading ? 0.5 : 1, transition: "opacity 150ms" }}>
              {([
                { key: "email", title: "Email", sentLabel: "sent", data: channelTotals.email, color: CH_COLORS.email },
                { key: "linkedin", title: "LinkedIn", sentLabel: "invited", data: channelTotals.linkedin, color: CH_COLORS.linkedin },
                { key: "app", title: "App", sentLabel: "touches", data: channelTotals.app, color: CH_COLORS.app },
                { key: "telegram", title: "Telegram", sentLabel: "touches", data: channelTotals.telegram, color: CH_COLORS.telegram },
              ] as const).map((ch) => (
                <div key={ch.key} className="card" style={{ gridColumn: "auto", borderTopColor: ch.color, borderTopWidth: 2 }}>
                  <div className="cardBody" style={{ padding: "18px 22px" }}>
                    <div style={{ fontSize: 13, fontWeight: 650, marginBottom: 12, color: ch.color }}>
                      {ch.title}
                    </div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px 16px" }}>
                      <div>
                        <div style={{ fontSize: 24, fontWeight: 700, letterSpacing: "-0.03em", lineHeight: 1 }}>{num(ch.data.sent)}</div>
                        <div className="muted2" style={{ fontSize: 11, marginTop: 4 }}>{ch.sentLabel}</div>
                      </div>
                      <div>
                        <div style={{ fontSize: 24, fontWeight: 700, letterSpacing: "-0.03em", lineHeight: 1 }}>{num(ch.data.replies)}</div>
                        <div className="muted2" style={{ fontSize: 11, marginTop: 4 }}>replies {ch.data.sent ? `(${pct(ch.data.replies, ch.data.sent)})` : ""}</div>
                      </div>
                      <div>
                        <div style={{ fontSize: 24, fontWeight: 700, letterSpacing: "-0.03em", lineHeight: 1, color: ch.data.booked ? "#22c55e" : "var(--text)" }}>{num(ch.data.booked)}</div>
                        <div className="muted2" style={{ fontSize: 11, marginTop: 4 }}>booked</div>
                      </div>
                      <div>
                        <div style={{ fontSize: 24, fontWeight: 700, letterSpacing: "-0.03em", lineHeight: 1, color: ch.data.held ? "#38bdf8" : "var(--text)" }}>{num(ch.data.held)}</div>
                        <div className="muted2" style={{ fontSize: 11, marginTop: 4 }}>held</div>
                      </div>
                      {ch.data.ql > 0 && (
                        <div>
                          <div style={{ fontSize: 24, fontWeight: 700, letterSpacing: "-0.03em", lineHeight: 1, color: "#a78bfa" }}>{num(ch.data.ql)}</div>
                          <div className="muted2" style={{ fontSize: 11, marginTop: 4 }}>QL</div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>

            {/* ── Charts row ──────────────────────────────────────────── */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, opacity: loading ? 0.5 : 1, transition: "opacity 150ms" }}>
              {/* Funnel */}
              <div className="card">
                <div className="cardHeader">
                  <div>
                    <div className="cardTitle">Funnel</div>
                    <div className="cardDesc">{rangeLabel}{selectedTalId ? ` · ${tals.find((t) => t.id === selectedTalId)?.name ?? ""}` : ""}</div>
                  </div>
                </div>
                <div className="cardBody">
                  <FunnelChart steps={funnelSteps} />
                </div>
              </div>

              {/* Channel comparison bar chart */}
              <div className="card">
                <div className="cardHeader">
                  <div>
                    <div className="cardTitle">Channel comparison</div>
                    <div className="cardDesc">Sent vs Replies vs Booked vs Held</div>
                  </div>
                </div>
                <div className="cardBody">
                  <BarChart
                    data={channelBarData}
                    labelKey="channel"
                    bars={[
                      { key: "sent", color: "rgba(255,255,255,0.6)", label: "Sent" },
                      { key: "replies", color: "#fde68a", label: "Replies" },
                      { key: "booked", color: "#22c55e", label: "Booked" },
                      { key: "held", color: "#38bdf8", label: "Held" },
                    ]}
                  />
                </div>
              </div>
            </div>

            {/* ── Grand Total row ─────────────────────────────────────── */}
            <div className="card">
              <div className="cardBody" style={{ padding: "14px 22px" }}>
                <div style={{ display: "flex", gap: 32, alignItems: "center", flexWrap: "wrap" }}>
                  <span className="muted2" style={{ fontSize: 12, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                    Total · {rangeLabel}
                  </span>
                  <span style={{ fontSize: 15, fontWeight: 650 }}>{num(grandTotal.sent)} sent</span>
                  <span style={{ fontSize: 15, fontWeight: 650 }}>{num(grandTotal.replies)} replies {grandTotal.sent ? <span className="muted2" style={{ fontWeight: 400, fontSize: 13 }}>({pct(grandTotal.replies, grandTotal.sent)})</span> : null}</span>
                  <span style={{ fontSize: 15, fontWeight: 650, color: "#22c55e" }}>{num(grandTotal.booked)} booked</span>
                  <span style={{ fontSize: 15, fontWeight: 650, color: "#38bdf8" }}>{num(grandTotal.held)} held</span>
                  <span style={{ fontSize: 15, fontWeight: 650, color: "#a78bfa" }}>{num(grandTotal.ql)} QL</span>
                </div>
              </div>
            </div>

            {/* ── Hypotheses Pipeline ──────────────────────────────────── */}
            <div className="card">
              <div className="cardHeader">
                <div>
                  <div className="cardTitle">Hypotheses pipeline</div>
                  <div className="cardDesc">{hyps.length} hypotheses total</div>
                </div>
                <Link href="/hypotheses" className="btn">View all</Link>
              </div>
              <div className="cardBody">
                <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
                  <div style={{ flex: "1 1 320px" }}>
                    <div className="muted2" style={{ fontSize: 12, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 10 }}>By status</div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                      {statusCounts.map(([status, count]) => (
                        <div key={status} style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 14px", borderRadius: 12, background: STATUS_COLORS[status] || "rgba(255,255,255,0.04)" }}>
                          <span style={{ color: STATUS_TEXT[status] || "var(--muted)", fontWeight: 650, fontSize: 14, minWidth: 28 }}>{count}</span>
                          <span style={{ fontSize: 13, color: "var(--text)" }}>{formatLabel(status)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div style={{ flex: "1 1 320px" }}>
                    <div className="muted2" style={{ fontSize: 12, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 10 }}>By decision</div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                      {decisionCounts.map(([decision, count]) => (
                        <div key={decision} style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 14px", borderRadius: 12, background: "rgba(255, 255, 255, 0.04)" }}>
                          <span style={{ fontWeight: 650, fontSize: 14, minWidth: 28, color: "var(--text)" }}>{count}</span>
                          <span style={{ fontSize: 13, color: "var(--muted)" }}>{formatLabel(decision)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* ── Unlinked Campaigns Warning ───────────────────────────── */}
            {unlinked.length > 0 && (
              <div className="card" style={{ borderColor: "rgba(234, 179, 8, 0.3)" }}>
                <div className="cardHeader" style={{ borderBottomColor: "rgba(234, 179, 8, 0.15)" }}>
                  <div>
                    <div className="cardTitle" style={{ color: "#facc15" }}>{unlinked.length} unlinked campaign{unlinked.length !== 1 ? "s" : ""}</div>
                    <div className="cardDesc">These campaigns have data but are not linked to any TAL.</div>
                  </div>
                  <Link href="/tals" className="btn">Manage TALs</Link>
                </div>
                <div className="cardBody">
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                    {unlinked.slice(0, 20).map((c) => (
                      <span key={c.source_campaign_key} className="pill" style={{ borderColor: "rgba(234, 179, 8, 0.3)", color: "#facc15", fontSize: 12 }}>
                        <span className="muted2" style={{ fontSize: 10, textTransform: "uppercase" }}>{c.channel}</span>
                        {c.name}
                      </span>
                    ))}
                    {unlinked.length > 20 && <span className="pill muted2">+{unlinked.length - 20} more</span>}
                  </div>
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </main>
  );
}
