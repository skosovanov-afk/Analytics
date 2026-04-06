"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { AppTopbar } from "../components/AppTopbar";
import { getSupabase } from "../lib/supabase";
import { ThreadView, type ThreadMessage } from "./ThreadView";

/* ── Types ────────────────────────────────────────────────────────────────── */

type Tab = "email" | "linkedin";
type Period = "7d" | "30d" | "90d" | "all" | "custom";

type EmailReply = {
  event_id: number;
  occurred_at: string;
  reply_date: string;
  campaign_id: number | null;
  campaign_name: string | null;
  email: string | null;
  lead_first_name: string | null;
  lead_last_name: string | null;
  lead_company: string | null;
  subject: string | null;
  sequence_number: number | null;
  sentiment: string;
  is_positive: boolean;
  tal_name: string | null;
};

type LinkedInReply = {
  message_id: number;
  occurred_at: string;
  reply_date: string;
  messenger_id: number | null;
  account_name: string | null;
  campaign_name: string | null;
  contact_name: string | null;
  contact_email: string | null;
  contact_company_name: string | null;
  contact_job_title: string | null;
  reply_body: string | null;
  tal_name: string | null;
};

/* ── Helpers ──────────────────────────────────────────────────────────────── */

const PAGE_SIZE = 1000;

function isoDate(d: Date) { return d.toISOString().slice(0, 10); }

function sinceForPeriod(period: Period): string | null {
  if (period === "all" || period === "custom") return null;
  const d = new Date();
  d.setDate(d.getDate() - (period === "7d" ? 7 : period === "30d" ? 30 : 90));
  return isoDate(d);
}

function formatDate(s: string | null) {
  if (!s) return "-";
  const d = new Date(s);
  return d.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit", year: "numeric" });
}

function formatTime(s: string | null) {
  if (!s) return "";
  const d = new Date(s);
  return d.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" });
}

/** Normalize Expandi message direction (same logic as expandi_campaign_daily_mv) */
function normalizeDirection(m: { is_inbound?: boolean; is_outbound?: boolean; direction?: string | null; send_datetime?: string | null; received_datetime?: string | null }): "in" | "out" {
  const dir = (m.direction ?? "").trim().toLowerCase();
  if (dir === "outbound") return "out";
  if (dir === "inbound") return "in";
  const isIn = m.is_inbound ?? false;
  const isOut = m.is_outbound ?? false;
  if (isOut && !isIn) return "out";
  if (isIn && !isOut) return "in";
  if (m.send_datetime && !m.received_datetime) return "out";
  if (m.received_datetime && !m.send_datetime) return "in";
  // Both true or both null - default to outbound (matches MV logic)
  if (isOut && isIn) return "out";
  return "out";
}

const SENTIMENT_COLORS: Record<string, string> = {
  "Interested": "#22c55e",
  "Meeting Request": "#38bdf8",
  "Information Request": "#a78bfa",
  "Ask for Referral": "#86efac",
  "Not Interested": "#f87171",
  "Do Not Contact": "#ef4444",
  "Out Of Office": "#facc15",
  "Wrong Person": "#fb923c",
  "Uncategorizable by AI": "#94a3b8",
  "Sender Originated Bounce": "#94a3b8",
  "Uncategorized": "#64748b",
};

/* ── Page ─────────────────────────────────────────────────────────────────── */

export default function RepliesPage() {
  const supabase = useMemo(() => getSupabase(), []);

  const [tab, setTab] = useState<Tab>("email");
  const [period, setPeriod] = useState<Period>("30d");
  const [customSince, setCustomSince] = useState("");
  const [customUntil, setCustomUntil] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [emailReplies, setEmailReplies] = useState<EmailReply[]>([]);
  const [liReplies, setLiReplies] = useState<LinkedInReply[]>([]);

  // Filters
  const [campaignFilter, setCampaignFilter] = useState("");
  const [sentimentFilter, setSentimentFilter] = useState("");
  const [talFilter, setTalFilter] = useState("");

  // Thread modal
  const [threadOpen, setThreadOpen] = useState(false);
  const [threadRow, setThreadRow] = useState<EmailReply | LinkedInReply | null>(null);
  const [threadMessages, setThreadMessages] = useState<ThreadMessage[]>([]);
  const [threadLoading, setThreadLoading] = useState(false);
  const [threadError, setThreadError] = useState<string | null>(null);

  // Fetch
  useEffect(() => {
    if (!supabase) return;
    loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase, tab, period, customSince, customUntil]);

  async function loadData() {
    if (!supabase) return;
    setLoading(true);
    setError(null);
    closeThread();
    try {
      const since = period === "custom" ? (customSince || null) : sinceForPeriod(period);
      const until = period === "custom" ? (customUntil || null) : null;

      // Use API route with service role key to bypass RLS statement timeout
      const { data: { session } } = await supabase.auth.getSession();
      if (!session) { setError("Not signed in. Go to / and sign in."); setLoading(false); return; }
      const res = await fetch("/api/replies/list", {
        method: "POST",
        headers: { Authorization: `Bearer ${session.access_token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ tab, since: since || null, until: until || null }),
      });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`API error ${res.status}: ${text.slice(0, 200)}`);
      }
      const json = await res.json();
      if (!json.ok) throw new Error(json.error || "Unknown error");

      if (tab === "email") {
        setEmailReplies(json.rows ?? []);
      } else {
        setLiReplies(json.rows ?? []);
      }
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  /* ── Thread modal ──────────────────────────────────────────────────── */

  const openThread = useCallback(async (row: EmailReply | LinkedInReply) => {
    setThreadRow(row);
    setThreadOpen(true);
    setThreadMessages([]);
    setThreadLoading(true);
    setThreadError(null);

    try {
      if (tab === "email") {
        const r = row as EmailReply;
        if (!r.campaign_id || !r.email) { setThreadError("Missing campaign or email"); return; }
        const { data: { session } } = await supabase!.auth.getSession();
        if (!session) { setThreadError("Not signed in"); return; }
        const res = await fetch("/api/replies/thread", {
          method: "POST",
          headers: { Authorization: `Bearer ${session.access_token}`, "Content-Type": "application/json" },
          body: JSON.stringify({ campaign_id: r.campaign_id, email: r.email }),
        });
        const json = await res.json();
        if (!json.ok) throw new Error(json.error);
        setThreadMessages(json.messages ?? []);
      } else {
        const r = row as LinkedInReply;
        if (!r.messenger_id || !supabase) { setThreadError("Missing messenger_id"); return; }
        const { data, error } = await supabase
          .from("expandi_messages")
          .select("id,body,is_inbound,is_outbound,direction,send_datetime,received_datetime")
          .eq("messenger_id", r.messenger_id)
          .order("send_datetime", { ascending: true });
        if (error) throw new Error(error.message);
        const msgs: ThreadMessage[] = (data ?? []).map((m: any) => ({
          id: String(m.id),
          direction: normalizeDirection(m),
          body: m.body,
          timestamp: m.send_datetime || m.received_datetime || "",
        }));
        setThreadMessages(msgs);
      }
    } catch (e: any) {
      setThreadError(String(e?.message || e));
    } finally {
      setThreadLoading(false);
    }
  }, [tab, supabase]);

  function closeThread() {
    setThreadOpen(false);
    setThreadRow(null);
  }

  /* ── Derived ──────────────────────────────────────────────────────────── */

  const currentReplies = tab === "email" ? emailReplies : liReplies;

  const campaigns = useMemo(() => {
    const set = new Set<string>();
    for (const r of currentReplies) {
      if ((r as any).campaign_name) set.add((r as any).campaign_name);
    }
    return Array.from(set).sort();
  }, [currentReplies]);

  const tals = useMemo(() => {
    const set = new Set<string>();
    for (const r of currentReplies) {
      if ((r as any).tal_name) set.add((r as any).tal_name);
    }
    return Array.from(set).sort();
  }, [currentReplies]);

  const sentiments = useMemo(() => {
    if (tab !== "email") return [];
    const set = new Set<string>();
    for (const r of emailReplies) {
      if (r.sentiment) set.add(r.sentiment);
    }
    return Array.from(set).sort();
  }, [emailReplies, tab]);

  const filtered = useMemo(() => {
    return currentReplies.filter((r: any) => {
      if (campaignFilter && r.campaign_name !== campaignFilter) return false;
      if (talFilter && r.tal_name !== talFilter) return false;
      if (sentimentFilter && tab === "email" && (r as EmailReply).sentiment !== sentimentFilter) return false;
      return true;
    });
  }, [currentReplies, campaignFilter, talFilter, sentimentFilter, tab]);

  const summary = useMemo(() => {
    const total = filtered.length;
    if (tab === "email") {
      const positive = (filtered as EmailReply[]).filter(r => r.is_positive).length;
      return { total, positive, positivePct: total ? ((positive / total) * 100).toFixed(1) : "0" };
    }
    const withBody = (filtered as LinkedInReply[]).filter(r => r.reply_body).length;
    return { total, withBody };
  }, [filtered, tab]);

  const PERIODS: { key: Period; label: string }[] = [
    { key: "7d", label: "7 days" },
    { key: "30d", label: "30 days" },
    { key: "90d", label: "90 days" },
    { key: "all", label: "All time" },
  ];

  useEffect(() => {
    setCampaignFilter("");
    setSentimentFilter("");
    setTalFilter("");
    closeThread();
  }, [tab]);

  return (
    <main>
      <AppTopbar title="Replies" subtitle="All incoming replies across channels" />

      <div className="page" style={{ display: "flex", flexDirection: "column", gap: 16 }}>

        {/* Tabs */}
        <div style={{ display: "flex", gap: 8 }}>
          {(["email", "linkedin"] as Tab[]).map(t => (
            <button
              key={t}
              className={`btn${tab === t ? " btnPrimary" : ""}`}
              style={{ padding: "8px 20px", fontSize: 14 }}
              onClick={() => setTab(t)}
            >
              {t === "email" ? "Email" : "LinkedIn"}
              {!loading && (
                <span style={{ marginLeft: 6, opacity: 0.6 }}>
                  ({t === "email" ? emailReplies.length : liReplies.length})
                </span>
              )}
            </button>
          ))}
        </div>

        {/* Filters */}
        <div className="card">
          <div className="cardBody" style={{ padding: "14px 22px" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
              <span className="muted2" style={{ fontSize: 12, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                Period
              </span>
              {PERIODS.map(p => (
                <button key={p.key} className={`btn${period === p.key ? " btnPrimary" : ""}`}
                  style={{ padding: "6px 12px", fontSize: 13 }} onClick={() => setPeriod(p.key)}>
                  {p.label}
                </button>
              ))}
              <span style={{ width: 1, height: 24, background: "var(--border)", margin: "0 4px" }} />
              <input type="date" className="input" style={{ width: 150, padding: "6px 10px", fontSize: 13 }}
                value={customSince} onChange={e => setCustomSince(e.target.value)} />
              <span className="muted2" style={{ fontSize: 13 }}>-</span>
              <input type="date" className="input" style={{ width: 150, padding: "6px 10px", fontSize: 13 }}
                value={customUntil} onChange={e => setCustomUntil(e.target.value)} />
              <button className="btn" style={{ padding: "6px 12px", fontSize: 13 }}
                onClick={() => setPeriod("custom")}>Apply</button>
              <span style={{ width: 1, height: 24, background: "var(--border)", margin: "0 4px" }} />
              <select className="select" style={{ width: 200, padding: "6px 10px", fontSize: 13 }}
                value={campaignFilter} onChange={e => setCampaignFilter(e.target.value)}>
                <option value="">All campaigns ({campaigns.length})</option>
                {campaigns.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
              {tab === "email" && (
                <select className="select" style={{ width: 170, padding: "6px 10px", fontSize: 13 }}
                  value={sentimentFilter} onChange={e => setSentimentFilter(e.target.value)}>
                  <option value="">All sentiments</option>
                  {sentiments.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
              )}
              <select className="select" style={{ width: 170, padding: "6px 10px", fontSize: 13 }}
                value={talFilter} onChange={e => setTalFilter(e.target.value)}>
                <option value="">All TALs ({tals.length})</option>
                {tals.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
              {loading && <span className="muted2" style={{ fontSize: 13 }}>Loading...</span>}
            </div>
          </div>
        </div>

        {error && (
          <div className="card"><div className="cardBody"><div className="notice">{error}</div></div></div>
        )}

        {/* Table */}
        {!error && (
          <div className="card">
            <div className="cardBody" style={{ padding: 0, overflowX: "auto", opacity: loading ? 0.5 : 1, transition: "opacity 150ms" }}>
              {tab === "email" ? (
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid var(--border)" }}>
                      <th style={thStyle}>Date</th>
                      <th style={thStyle}>Campaign</th>
                      <th style={thStyle}>Lead</th>
                      <th style={thStyle}>Company</th>
                      <th style={thStyle}>Subject</th>
                      <th style={thStyle}>Touch</th>
                      <th style={thStyle}>Sentiment</th>
                      <th style={thStyle}>TAL</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(filtered as EmailReply[]).slice(0, 200).map((r, i) => (
                      <tr key={r.event_id ?? i} style={{ borderBottom: "1px solid var(--border)", cursor: "pointer" }}
                        onClick={() => openThread(r)}>
                        <td style={tdStyle}>
                          <div>{formatDate(r.occurred_at)}</div>
                          <div className="muted2" style={{ fontSize: 11 }}>{formatTime(r.occurred_at)}</div>
                        </td>
                        <td style={tdStyle}>{r.campaign_name || "-"}</td>
                        <td style={tdStyle}>
                          <div>{[r.lead_first_name, r.lead_last_name].filter(Boolean).join(" ") || "-"}</div>
                          <div className="muted2" style={{ fontSize: 11 }}>{r.email || ""}</div>
                        </td>
                        <td style={tdStyle}>{r.lead_company || "-"}</td>
                        <td style={{ ...tdStyle, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {r.subject || "-"}
                        </td>
                        <td style={{ ...tdStyle, textAlign: "center" }}>{r.sequence_number ?? "-"}</td>
                        <td style={tdStyle}>
                          <span style={{
                            display: "inline-block", padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600,
                            background: `${SENTIMENT_COLORS[r.sentiment] || "#64748b"}22`,
                            color: SENTIMENT_COLORS[r.sentiment] || "#64748b",
                          }}>
                            {r.sentiment}
                          </span>
                        </td>
                        <td style={tdStyle} className="muted2">{r.tal_name || "-"}</td>
                      </tr>
                    ))}
                    {filtered.length === 0 && !loading && (
                      <tr><td colSpan={8} style={{ ...tdStyle, textAlign: "center" }} className="muted2">No replies found</td></tr>
                    )}
                  </tbody>
                </table>
              ) : (
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid var(--border)" }}>
                      <th style={thStyle}>Date</th>
                      <th style={thStyle}>Account</th>
                      <th style={thStyle}>Campaign</th>
                      <th style={thStyle}>Contact</th>
                      <th style={thStyle}>Company</th>
                      <th style={thStyle}>Message</th>
                      <th style={thStyle}>TAL</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(filtered as LinkedInReply[]).slice(0, 200).map((r, i) => (
                      <tr key={r.message_id ?? i} style={{ borderBottom: "1px solid var(--border)", cursor: "pointer" }}
                        onClick={() => openThread(r)}>
                        <td style={tdStyle}>
                          <div>{formatDate(r.occurred_at)}</div>
                          <div className="muted2" style={{ fontSize: 11 }}>{formatTime(r.occurred_at)}</div>
                        </td>
                        <td style={tdStyle}>{r.account_name || "-"}</td>
                        <td style={tdStyle}>{r.campaign_name || "-"}</td>
                        <td style={tdStyle}>
                          <div>{r.contact_name || "-"}</div>
                          <div className="muted2" style={{ fontSize: 11 }}>{r.contact_job_title || ""}</div>
                        </td>
                        <td style={tdStyle}>{r.contact_company_name || "-"}</td>
                        <td style={{ ...tdStyle, maxWidth: 300, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {r.reply_body || <span className="muted2">-</span>}
                        </td>
                        <td style={tdStyle} className="muted2">{r.tal_name || "-"}</td>
                      </tr>
                    ))}
                    {filtered.length === 0 && !loading && (
                      <tr><td colSpan={7} style={{ ...tdStyle, textAlign: "center" }} className="muted2">No replies found</td></tr>
                    )}
                  </tbody>
                </table>
              )}

              {filtered.length > 200 && (
                <div style={{ padding: "12px 16px", textAlign: "center", borderTop: "1px solid var(--border)" }} className="muted2">
                  Showing 200 of {filtered.length.toLocaleString()} replies
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* ── Thread Modal ──────────────────────────────────────────────── */}
      {threadOpen && threadRow && (
        <div className="dialogScrim" onClick={closeThread}>
          <div className="card dialogCard" onClick={(e) => e.stopPropagation()}>
            <div className="cardHeader">
              <div>
                <div className="cardTitle" style={{ fontFamily: "var(--sans)", fontWeight: 700 }}>
                  Conversation
                </div>
                <div className="cardDesc">
                  {tab === "email"
                    ? `${[(threadRow as EmailReply).lead_first_name, (threadRow as EmailReply).lead_last_name].filter(Boolean).join(" ") || (threadRow as EmailReply).email || "Unknown"} - ${(threadRow as EmailReply).campaign_name || ""}`
                    : `${(threadRow as LinkedInReply).contact_name || "Unknown"} - ${(threadRow as LinkedInReply).campaign_name || ""}`
                  }
                </div>
              </div>
              <button className="btn" onClick={closeThread}>Close</button>
            </div>
            <div className="cardBody" style={{ padding: 0 }}>
              <ThreadView messages={threadMessages} loading={threadLoading} error={threadError} />
            </div>
          </div>
        </div>
      )}
    </main>
  );
}

const thStyle: React.CSSProperties = {
  padding: "10px 14px",
  textAlign: "left",
  fontSize: 11,
  fontWeight: 600,
  textTransform: "uppercase",
  letterSpacing: "0.04em",
  color: "var(--muted2)",
  whiteSpace: "nowrap",
};

const tdStyle: React.CSSProperties = {
  padding: "10px 14px",
  verticalAlign: "top",
};
