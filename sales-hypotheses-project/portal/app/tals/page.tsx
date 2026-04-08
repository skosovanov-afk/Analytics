"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { AppTopbar } from "../components/AppTopbar";
import { getSupabase } from "../lib/supabase";

type TalRow = {
  id: string;
  name: string;
  description: string | null;
  criteria: string | null;
  created_at: string;
  email_sent: number;
  email_replies: number;
  email_reply_rate: number | null;
  email_meetings: number;
  email_held_meetings: number;
  li_invited: number;
  li_accepted: number;
  li_replies: number;
  li_accept_rate: number | null;
  li_meetings: number;
  li_held_meetings: number;
  app_invitations: number;
  app_touches: number;
  app_replies: number;
  app_reply_rate: number | null;
  app_meetings: number;
  app_held_meetings: number;
  tg_touches: number;
  tg_replies: number;
  tg_reply_rate: number | null;
  tg_meetings: number;
  tg_held_meetings: number;
  total_meetings: number;
  total_held_meetings: number;
};

type UnlinkedCampaign = {
  channel: "smartlead" | "expandi" | "app" | "telegram";
  name: string;
  source_campaign_key: string;
  campaign_id?: string | null;
};

function pct(v: number | null) {
  if (v == null) return "0%";
  return `${v}%`;
}

function num(v: number) {
  return v.toLocaleString();
}

function calcRate(numerator: number, denominator: number): string {
  if (denominator === 0) return "0%";
  return `${Math.round((numerator / denominator) * 100)}%`;
}

function MetricChannel({
  title,
  items,
}: {
  title: string;
  items: Array<{ value: string; label: string }>;
}) {
  return (
    <div className="surfaceCard metricCard">
      <div className="metricCardHeader">{title}</div>
      <div className="metricStats">
        {items.map((item) => (
          <div key={`${title}:${item.label}`} className="metricStat">
            <div className="metricStatValue">{item.value}</div>
            <div className="metricStatLabel">{item.label}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

const CHANNEL_LABELS: Record<string, string> = {
  smartlead: "Email / Smartlead",
  expandi: "LinkedIn / Expandi",
  app: "App",
  telegram: "Telegram",
};

export default function TalsPage() {
  const supabase = useMemo(() => getSupabase(), []);

  const [tals, setTals] = useState<TalRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const [rowsPerPage, setRowsPerPage] = useState(25);
  const ROWS_PER_PAGE_OPTIONS = [10, 25, 50];

  const [unlinked, setUnlinked] = useState<UnlinkedCampaign[]>([]);
  const [unlinkedOpen, setUnlinkedOpen] = useState(false);
  const [unlinkedLoading, setUnlinkedLoading] = useState(false);
  const [unlinkedError, setUnlinkedError] = useState<string | null>(null);

  useEffect(() => {
    if (!supabase) return;

    async function load() {
      setLoading(true);
      setError(null);
      try {
        const { data: { session } } = await supabase!.auth.getSession();
        const token = session?.access_token;
        if (!token) { setError("Not signed in"); return; }

        const auth = `Bearer ${token}`;

        const [talsRes, unlinkedRes] = await Promise.all([
          fetch("/api/tals", { headers: { Authorization: auth } }),
          fetch("/api/tals/unlinked", { headers: { Authorization: auth } }),
        ]);

        const talsJson = await talsRes.json();
        if (!talsJson.ok) throw new Error(talsJson.error);
        setTals(talsJson.tals);

        const unlinkedJson = await unlinkedRes.json().catch(() => ({ ok: false }));
        if (unlinkedJson.ok) {
          setUnlinked(unlinkedJson.unlinked ?? []);
        }
      } catch (e: any) {
        setError(String(e?.message || e));
      } finally {
        setLoading(false);
      }
    }

    load();
  }, [supabase]);

  const filteredTals = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return tals;
    return tals.filter((t) =>
      t.name.toLowerCase().includes(q) ||
      (t.criteria ?? "").toLowerCase().includes(q)
    );
  }, [tals, search]);

  const totalPages = Math.max(1, Math.ceil(filteredTals.length / rowsPerPage));
  const pagedTals = filteredTals.slice((page - 1) * rowsPerPage, page * rowsPerPage);
  const rangeStart = filteredTals.length === 0 ? 0 : (page - 1) * rowsPerPage + 1;
  const rangeEnd = Math.min(page * rowsPerPage, filteredTals.length);

  // Reset page when search changes
  useEffect(() => { setPage(1); }, [search]);

  return (
    <div className="page">
      <AppTopbar title="TAL" subtitle="Territory Account Lists" />

      <div className="content">
        <div className="pageHeader">
          <div>
            <h1 className="pageTitle">Territory Account Lists</h1>
            <p className="pageSubtitle">
              Группы кампаний по сегментам. Аналитика агрегируется по Email, LinkedIn, App и Telegram.
            </p>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <input
              className="input"
              placeholder="Search TAL..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              style={{ width: 220 }}
            />
            <Link href="/tals/new" className="btn btnPrimary">+ New TAL</Link>
          </div>
        </div>

        {loading && <p className="muted2">Loading...</p>}
        {error && <p style={{ color: "#ef4444" }}>{error}</p>}

        {!loading && !error && tals.length === 0 && (
          <div className="card">
            <div className="cardBody" style={{ textAlign: "center", paddingTop: 60, paddingBottom: 60 }}>
            <p style={{ fontSize: 16, marginBottom: 12 }}>Нет TAL</p>
            <Link href="/tals/new" className="btn btnPrimary">Создать первый TAL</Link>
          </div>
          </div>
        )}

        {tals.length > 0 && (
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {!loading && search && filteredTals.length === 0 && (
              <p className="muted2">No TALs match "{search}"</p>
            )}
            {pagedTals.map((tal) => (
              <Link
                key={tal.id}
                href={`/tals/${tal.id}`}
                style={{ textDecoration: "none", color: "inherit" }}
              >
                <div className="card" style={{ cursor: "pointer" }}>
                  <div className="cardBody">
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16, marginBottom: 18 }}>
                    <div>
                      <div style={{ fontWeight: 650, fontSize: 16, letterSpacing: "-0.02em" }}>{tal.name}</div>
                      {tal.criteria && (
                        <div className="muted2" style={{ fontSize: 13, marginTop: 4 }}>{tal.criteria}</div>
                      )}
                    </div>
                    {tal.total_meetings > 0 && (
                      <div className="badgeRow">
                        <div className="statusBadge statusBadgeSuccess">
                          {tal.total_meetings} booked meetings
                        </div>
                        {tal.total_held_meetings > 0 && (
                          <div className="statusBadge statusBadgeInfo">
                            {tal.total_held_meetings} held meetings
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  <div className="metricGrid">
                    {(tal.email_sent + tal.email_replies + tal.email_meetings + tal.email_held_meetings > 0) && (
                      <MetricChannel
                        title="Email (Smartlead)"
                        items={[
                          { value: num(tal.email_sent), label: "sent" },
                          { value: num(tal.email_replies), label: "replies" },
                          { value: num(tal.email_meetings), label: "booked" },
                          { value: num(tal.email_held_meetings), label: "held" },
                          { value: pct(tal.email_reply_rate), label: "reply rate" },
                          { value: calcRate(tal.email_meetings, tal.email_replies), label: "reply → booked" },
                          { value: calcRate(tal.email_held_meetings, tal.email_meetings), label: "booked → held" },
                        ]}
                      />
                    )}
                    {(tal.li_invited + tal.li_accepted + tal.li_replies + tal.li_meetings + tal.li_held_meetings > 0) && (
                      <MetricChannel
                        title="LinkedIn (Expandi)"
                        items={[
                          { value: num(tal.li_invited), label: "invited" },
                          { value: num(tal.li_accepted), label: "accepted" },
                          { value: num(tal.li_replies), label: "replies" },
                          { value: num(tal.li_meetings), label: "booked" },
                          { value: num(tal.li_held_meetings), label: "held" },
                          { value: pct(tal.li_accept_rate), label: "accept rate" },
                          { value: calcRate(tal.li_replies, tal.li_accepted), label: "reply rate" },
                          { value: calcRate(tal.li_meetings, tal.li_replies), label: "reply → booked" },
                          { value: calcRate(tal.li_held_meetings, tal.li_meetings), label: "booked → held" },
                        ]}
                      />
                    )}
                    {(tal.app_invitations + tal.app_touches + tal.app_replies + tal.app_meetings + tal.app_held_meetings > 0) && (
                      <MetricChannel
                        title="App"
                        items={[
                          { value: num(tal.app_invitations), label: "invitations" },
                          { value: num(tal.app_touches), label: "touches" },
                          { value: num(tal.app_replies), label: "replies" },
                          { value: num(tal.app_meetings), label: "booked" },
                          { value: num(tal.app_held_meetings), label: "held" },
                          { value: pct(tal.app_reply_rate), label: "reply rate" },
                          { value: calcRate(tal.app_meetings, tal.app_replies), label: "reply → booked" },
                          { value: calcRate(tal.app_held_meetings, tal.app_meetings), label: "booked → held" },
                        ]}
                      />
                    )}
                    {(tal.tg_touches + tal.tg_replies + tal.tg_meetings + tal.tg_held_meetings > 0) && (
                      <MetricChannel
                        title="Telegram"
                        items={[
                          { value: num(tal.tg_touches), label: "touches" },
                          { value: num(tal.tg_replies), label: "replies" },
                          { value: num(tal.tg_meetings), label: "booked" },
                          { value: num(tal.tg_held_meetings), label: "held" },
                          { value: pct(tal.tg_reply_rate), label: "reply rate" },
                          { value: calcRate(tal.tg_meetings, tal.tg_replies), label: "reply → booked" },
                          { value: calcRate(tal.tg_held_meetings, tal.tg_meetings), label: "booked → held" },
                        ]}
                      />
                    )}
                  </div>
                  </div>
                </div>
              </Link>
            ))}

            {/* Pagination */}
            {filteredTals.length > 0 && (
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 8, padding: "8px 0" }}>
                <span className="muted2" style={{ fontSize: 13 }}>
                  {rangeStart}–{rangeEnd} of {filteredTals.length}
                </span>
                <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <span className="muted2" style={{ fontSize: 13 }}>Rows per page:</span>
                    <select
                      className="input"
                      value={rowsPerPage}
                      onChange={(e) => { setRowsPerPage(Number(e.target.value)); setPage(1); }}
                      style={{ width: 64, padding: "4px 6px", fontSize: 13 }}
                    >
                      {ROWS_PER_PAGE_OPTIONS.map((n) => (
                        <option key={n} value={n}>{n}</option>
                      ))}
                    </select>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                    <button className="btn" style={{ padding: "4px 8px", fontSize: 13 }} disabled={page <= 1} onClick={() => setPage(page - 1)}>‹</button>
                    {Array.from({ length: totalPages }, (_, i) => i + 1).map((p) => (
                      <button
                        key={p}
                        className={`btn${p === page ? " btnPrimary" : ""}`}
                        style={{ padding: "4px 10px", fontSize: 13, minWidth: 32 }}
                        onClick={() => setPage(p)}
                      >
                        {p}
                      </button>
                    ))}
                    <button className="btn" style={{ padding: "4px 8px", fontSize: 13 }} disabled={page >= totalPages} onClick={() => setPage(page + 1)}>›</button>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Unlinked campaigns */}
        {!loading && unlinked.length > 0 && (
          <div style={{ marginTop: 32 }}>
            <button
              type="button"
              onClick={() => setUnlinkedOpen((v) => !v)}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                background: "none",
                border: "none",
                cursor: "pointer",
                padding: 0,
                fontSize: 14,
                fontWeight: 600,
                color: "#f59e0b",
              }}
            >
              <span style={{
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                width: 22,
                height: 22,
                borderRadius: 6,
                background: "#fef3c7",
                color: "#d97706",
                fontSize: 12,
                fontWeight: 700,
              }}>
                {unlinked.length}
              </span>
              Unlinked campaigns
              <span style={{ fontSize: 11, transition: "transform 0.15s", transform: unlinkedOpen ? "rotate(180deg)" : "rotate(0)" }}>
                ▼
              </span>
            </button>

            {unlinkedOpen && (
              <div className="card" style={{ marginTop: 10 }}>
                <div className="cardBody" style={{ padding: "16px 20px" }}>
                  <p className="muted2" style={{ fontSize: 13, marginTop: 0, marginBottom: 16 }}>
                    These campaigns exist in Supabase but are not linked to any TAL. Their metrics are not included in TAL analytics.
                  </p>
                  {(["smartlead", "expandi", "app", "telegram"] as const).map((ch) => {
                    const items = unlinked.filter((c) => c.channel === ch);
                    if (!items.length) return null;
                    return (
                      <div key={ch} style={{ marginBottom: 16 }}>
                        <div style={{ fontSize: 12, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 6 }} className="muted2">
                          {CHANNEL_LABELS[ch]} ({items.length})
                        </div>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                          {items.map((c) => (
                            <span
                              key={c.source_campaign_key}
                              style={{
                                display: "inline-block",
                                padding: "4px 10px",
                                borderRadius: 6,
                                border: "1px solid #fde68a",
                                background: "#fffbeb",
                                color: "#92400e",
                                fontSize: 13,
                              }}
                            >
                              {c.name}
                            </span>
                          ))}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
