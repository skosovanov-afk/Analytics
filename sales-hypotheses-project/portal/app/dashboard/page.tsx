"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { isoDate, startOfWeekISO } from "../lib/utils";
import { AppTopbar } from "../components/AppTopbar";

type Hyp = { id: string; title: string; status: string | null };
type Metric = { id: string; slug: string; name: string; input_type: string; unit: string | null };

type CheckinRow = {
  hypothesis_id: string;
  week_start: string;
  opps_in_progress_count: number | null;
  tal_companies_count: number | null;
  contacts_count: number | null;
  metrics_snapshot_json: any;
  channel_activity_json: any;
};

type MetricSource = "builtin" | "hypothesis" | "channel_sum";

const COLORS = ["#7dd3fc", "#a7f3d0", "#fca5a5", "#c4b5fd", "#fde68a", "#f9a8d4", "#93c5fd", "#86efac", "#fdba74"];

function toNum(v: any): number | null {
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : Number(String(v).trim());
  return Number.isFinite(n) ? n : null;
}

function uniq<T>(xs: T[]) {
  return Array.from(new Set(xs));
}

function LineChart({
  title,
  weeks,
  series,
  unit,
  right
}: {
  title: string;
  weeks: string[];
  series: Array<{ key: string; label: string; values: Array<number | null> }>;
  unit?: string | null;
  right?: React.ReactNode;
}) {
  const w = 980;
  const h = 260;
  const padL = 50;
  const padR = 10;
  const padT = 18;
  const padB = 26;

  const flat = series.flatMap((s) => s.values).filter((x): x is number => typeof x === "number" && Number.isFinite(x));
  const min = flat.length ? Math.min(...flat) : 0;
  const max = flat.length ? Math.max(...flat) : 1;
  const span = max - min || 1;

  const xFor = (i: number) => {
    if (weeks.length <= 1) return padL;
    return padL + (i * (w - padL - padR)) / (weeks.length - 1);
  };
  const yFor = (v: number) => padT + ((max - v) * (h - padT - padB)) / span;

  const ticks = 4;
  const yTicks = Array.from({ length: ticks + 1 }).map((_, i) => min + (span * i) / ticks);

  return (
    <div className="card" style={{ gridColumn: "span 12" }}>
      <div className="cardHeader">
        <div>
          <div className="cardTitle">{title}</div>
          <div className="cardDesc">{weeks.length ? `${weeks[0]} → ${weeks[weeks.length - 1]}` : "No data"}{unit ? ` · ${unit}` : ""}</div>
        </div>
        {right ? <div className="btnRow">{right}</div> : null}
      </div>
      <div className="cardBody">
        {!weeks.length ? (
          <div className="muted2">No data in selected range.</div>
        ) : (
          <>
            <svg viewBox={`0 0 ${w} ${h}`} width="100%" height={h} style={{ display: "block" }}>
              {/* grid */}
              {yTicks.map((t, i) => {
                const y = yFor(t);
                return (
                  <g key={i}>
                    <line x1={padL} y1={y} x2={w - padR} y2={y} stroke="rgba(255,255,255,0.08)" strokeWidth={1} />
                    <text x={padL - 8} y={y + 4} textAnchor="end" fontSize="11" fill="rgba(255,255,255,0.55)">
                      {t.toFixed(0)}
                    </text>
                  </g>
                );
              })}
              {/* x labels (every ~4 points) */}
              {weeks.map((wk, i) => {
                const step = Math.max(1, Math.floor(weeks.length / 6));
                if (i % step !== 0 && i !== weeks.length - 1) return null;
                const x = xFor(i);
                return (
                  <text key={wk} x={x} y={h - 8} textAnchor="middle" fontSize="11" fill="rgba(255,255,255,0.45)">
                    {wk.slice(5)}
                  </text>
                );
              })}

              {/* series */}
              {series.map((s, si) => {
                const color = COLORS[si % COLORS.length];
                const pts = s.values
                  .map((v, i) => (typeof v === "number" ? [xFor(i), yFor(v)] : null))
                  .filter(Boolean) as Array<[number, number]>;
                if (!pts.length) return null;
                const d = pts.map((p, i) => `${i === 0 ? "M" : "L"} ${p[0].toFixed(1)} ${p[1].toFixed(1)}`).join(" ");
                return <path key={s.key} d={d} fill="none" stroke={color} strokeWidth={2.2} opacity={0.95} />;
              })}
            </svg>

            <div className="btnRow" style={{ flexWrap: "wrap", gap: 10, justifyContent: "flex-start", marginTop: 10 }}>
              {series.map((s, i) => (
                <span key={s.key} className="tag" style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                  <span style={{ width: 10, height: 10, borderRadius: 2, background: COLORS[i % COLORS.length], display: "inline-block" }} />
                  <span>{s.label}</span>
                </span>
              ))}
              {!series.length ? <span className="muted2">No hypotheses selected.</span> : null}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// ─── Module-level: stable across renders ─────────────────────────────────────

function getValue(row: any, metricSlug: string, source: MetricSource): number | null {
  if (source === "builtin") {
    if (metricSlug === "opps_in_progress_count") return toNum(row.opps_in_progress_count);
    if (metricSlug === "tal_companies_count") return toNum(row.tal_companies_count);
    if (metricSlug === "contacts_count") return toNum(row.contacts_count);
    return null;
  }
  if (source === "hypothesis") {
    const v = row?.metrics_snapshot_json?.metrics?.[metricSlug];
    return toNum(v);
  }
  // channel_sum
  const per = row?.channel_activity_json?.per_channel ?? {};
  if (!per || typeof per !== "object") return null;
  let sum = 0;
  let seen = false;
  for (const ch of Object.keys(per)) {
    const v = per?.[ch]?.metrics?.[metricSlug];
    const n = toNum(v);
    if (n == null) continue;
    sum += n;
    seen = true;
  }
  return seen ? sum : null;
}

function TableCard({
  metricSlug,
  weeks,
  selectedHypIds,
  byHypWeekCheckin,
  source,
  unitByMetricSlug,
  hypById,
  onSwitchToChart,
}: {
  metricSlug: string;
  weeks: string[];
  selectedHypIds: string[];
  byHypWeekCheckin: Map<string, CheckinRow>;
  source: MetricSource;
  unitByMetricSlug: Map<string, string | null>;
  hypById: Map<string, Hyp>;
  onSwitchToChart: () => void;
}) {
  const rows = useMemo(() => weeks.map((wk) => {
    const vals: Record<string, number | null> = {};
    for (const hid of selectedHypIds) {
      const row = byHypWeekCheckin.get(`${hid}:${wk}`) ?? null;
      vals[hid] = row ? getValue(row, metricSlug, source) : null;
    }
    return { week_start: wk, vals };
  }), [metricSlug, weeks, selectedHypIds, byHypWeekCheckin, source]);

  const unit = unitByMetricSlug.get(metricSlug) ?? null;

  return (
    <div className="card" style={{ gridColumn: "span 12" }}>
      <div className="cardHeader">
        <div>
          <div className="cardTitle">Table</div>
          <div className="cardDesc">
            Metric: <span className="mono">{metricSlug}</span>
            {unit ? <span> · {unit}</span> : null} · Weeks: <span className="mono">{weeks.length}</span>
          </div>
        </div>
        <div className="btnRow">
          <button className="btn" onClick={onSwitchToChart}>Graph</button>
        </div>
      </div>
      <div className="cardBody">
        {!metricSlug ? (
          <div className="muted2">Select a metric.</div>
        ) : (
          <div style={{ overflow: "auto" }}>
            <table className="table">
              <thead>
                <tr>
                  <th style={{ width: 120 }}>Week</th>
                  {selectedHypIds.slice(0, 12).map((hid) => (
                    <th key={hid}>{hypById.get(hid)?.title ?? hid}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={`${metricSlug}:${r.week_start}`}>
                    <td className="mono">{r.week_start}</td>
                    {selectedHypIds.slice(0, 12).map((hid) => {
                      const v = r.vals[hid];
                      return (
                        <td key={hid} className="mono">
                          {v == null ? "—" : String(v)}
                        </td>
                      );
                    })}
                  </tr>
                ))}
                {!rows.length ? (
                  <tr>
                    <td colSpan={1 + Math.min(12, selectedHypIds.length)} className="muted2">
                      No data.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
            {selectedHypIds.length > 12 ? (
              <div className="muted2" style={{ fontSize: 12, marginTop: 8 }}>
                Showing first 12 hypotheses in the table. Use charts for many-series view.
              </div>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function DashboardPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [status, setStatus] = useState("");
  const [sessionEmail, setSessionEmail] = useState<string | null>(null);

  const [hyps, setHyps] = useState<Hyp[]>([]);
  const [metrics, setMetrics] = useState<Metric[]>([]);
  const [checkins, setCheckins] = useState<CheckinRow[]>([]);

  const [source, setSource] = useState<MetricSource>("channel_sum");
  const [metricSlugs, setMetricSlugs] = useState<string[]>([]); // multi
  const [hypQ, setHypQ] = useState("");
  const [selectedHypIds, setSelectedHypIds] = useState<string[]>([]);
  const [metricView, setMetricView] = useState<Record<string, "table" | "chart">>({});

  const today = useMemo(() => new Date(), []);
  const defaultSince = useMemo(() => {
    const d = new Date(today);
    d.setDate(d.getDate() - 7 * 12);
    return startOfWeekISO(d);
  }, [today]);
  const [since, setSince] = useState<string>(defaultSince);
  const [until, setUntil] = useState<string>(startOfWeekISO(today));

  const builtinOptions = useMemo(
    () => [
      { slug: "opps_in_progress_count", name: "Opps in progress", unit: "count" },
      { slug: "tal_companies_count", name: "TAL companies", unit: "count" },
      { slug: "contacts_count", name: "Contacts", unit: "count" }
    ],
    []
  );

  const metricOptions = useMemo(() => {
    const lib = metrics
      .slice()
      .sort((a, b) => String(a.name ?? a.slug).localeCompare(String(b.name ?? b.slug)))
      .map((m) => ({ slug: m.slug, name: m.name || m.slug, unit: m.unit }));
    if (source === "builtin") return builtinOptions;
    return lib;
  }, [metrics, source, builtinOptions]);

  useEffect(() => {
    if (metricSlugs.length) return;
    // default
    if (source === "builtin") setMetricSlugs(["opps_in_progress_count"]);
    else setMetricSlugs([]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [source]);

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) return setStatus("Not signed in. Go back to / and sign in.");
    setSessionEmail(sess.data.session.user.email ?? null);

    const [hRes, mRes, cRes] = await Promise.all([
      supabase.from("sales_hypotheses").select("id,title,status").order("updated_at", { ascending: false }).limit(500),
      supabase.from("sales_metrics").select("id,slug,name,input_type,unit").eq("is_active", true).limit(2000),
      supabase
        .from("sales_hypothesis_checkins")
        .select("hypothesis_id,week_start,opps_in_progress_count,tal_companies_count,contacts_count,metrics_snapshot_json,channel_activity_json")
        .gte("week_start", since)
        .lte("week_start", until)
        .order("week_start", { ascending: true })
        .limit(5000)
    ]);
    if (hRes.error) return setStatus(`hypotheses error: ${hRes.error.message}`);
    if (mRes.error) return setStatus(`metrics error: ${mRes.error.message}`);
    if (cRes.error) return setStatus(`checkins error: ${cRes.error.message}`);

    setHyps((hRes.data ?? []) as any);
    setMetrics((mRes.data ?? []) as any);
    setCheckins((cRes.data ?? []) as any);

    // default hypothesis selection: all (first load)
    if (!selectedHypIds.length) setSelectedHypIds(((hRes.data ?? []) as any[]).map((x) => String(x.id)));
    setStatus("");
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase]);

  const filteredHyps = useMemo(() => {
    const needle = hypQ.trim().toLowerCase();
    if (!needle) return hyps;
    return hyps.filter((h) => String(h.title ?? "").toLowerCase().includes(needle) || String(h.id).toLowerCase().includes(needle));
  }, [hyps, hypQ]);

  const hypById = useMemo(() => {
    const m = new Map<string, Hyp>();
    for (const h of hyps) m.set(String(h.id), h);
    return m;
  }, [hyps]);

  const selectedHypIdsSet = useMemo(() => new Set(selectedHypIds), [selectedHypIds]);
  const metricSlugsSet = useMemo(() => new Set(metricSlugs), [metricSlugs]);

  const unitByMetricSlug = useMemo(() => {
    const m = new Map<string, string | null>();
    for (const b of builtinOptions) m.set(b.slug, b.unit);
    for (const x of metrics) m.set(String(x.slug), x.unit ?? null);
    return m;
  }, [builtinOptions, metrics]);

  const weeks = useMemo(() => {
    return uniq(checkins.map((c) => String(c.week_start))).sort();
  }, [checkins]);

  const byHypWeekCheckin = useMemo(() => {
    const m = new Map<string, CheckinRow>();
    for (const r of checkins) m.set(`${String(r.hypothesis_id)}:${String(r.week_start)}`, r);
    return m;
  }, [checkins]);

  const charts = useMemo(() => {
    const hypIds = selectedHypIds;
    const seriesFor = (metricSlug: string) =>
      hypIds.map((hid, i) => {
        const label = hypById.get(hid)?.title ?? hid;
        const values = weeks.map((wk) => {
          const row = byHypWeekCheckin.get(`${hid}:${wk}`) ?? null;
          return row ? getValue(row, metricSlug, source) : null;
        });
        return { key: hid, label, values, color: COLORS[i % COLORS.length] };
      });

    return (metricSlugs || []).filter(Boolean).slice(0, 6).map((slug) => ({
      slug,
      unit: unitByMetricSlug.get(slug) ?? null,
      series: seriesFor(slug)
    }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [metricSlugs, selectedHypIds, byHypWeekCheckin, weeks, hypById, source]);

  function viewFor(slug: string) {
    return metricView?.[slug] ?? "table";
  }

  function toggleMetric(slug: string, on: boolean) {
    setMetricSlugs((prev) => {
      const s = new Set(prev);
      if (on) s.add(slug);
      else s.delete(slug);
      return Array.from(s);
    });
  }

  return (
    <main>
      <AppTopbar title="Advanced dashboard" subtitle="Retrospective weekly analytics" />

      <div className="page grid">
        {status ? (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardBody"><div className="notice">{status}</div></div>
          </div>
        ) : null}

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Filters</div>
              <div className="cardDesc">Choose date range, metric source, metrics, and hypotheses.</div>
            </div>
            <div className="btnRow">
              <button className="btn" onClick={() => setSelectedHypIds(hyps.map((h) => String(h.id)))}>Select all</button>
              <button className="btn" onClick={() => setSelectedHypIds([])}>Clear</button>
            </div>
          </div>
          <div className="cardBody">
            <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10 }}>
              <div style={{ gridColumn: "span 3" }}>
                <label className="muted2" style={{ fontSize: 12 }}>Since (week_start)</label>
                <input className="input" value={since} onChange={(e) => setSince(e.target.value)} placeholder="YYYY-MM-DD" />
              </div>
              <div style={{ gridColumn: "span 3" }}>
                <label className="muted2" style={{ fontSize: 12 }}>Until (week_start)</label>
                <input className="input" value={until} onChange={(e) => setUntil(e.target.value)} placeholder="YYYY-MM-DD" />
              </div>
              <div style={{ gridColumn: "span 3" }}>
                <label className="muted2" style={{ fontSize: 12 }}>Metric source</label>
                <select className="select" value={source} onChange={(e) => setSource(e.target.value as any)}>
                  <option value="channel_sum">Channel metrics (sum)</option>
                  <option value="hypothesis">Hypothesis metrics</option>
                  <option value="builtin">Built-in (counts)</option>
                </select>
              </div>
              <div style={{ gridColumn: "span 3" }}>
                <label className="muted2" style={{ fontSize: 12 }}>Reload data</label>
                <button className="btn btnPrimary" onClick={load} style={{ width: "100%" }}>Apply</button>
              </div>

              <div style={{ gridColumn: "span 6" }}>
                <label className="muted2" style={{ fontSize: 12 }}>Metrics (select up to 6)</label>
                <div className="card" style={{ marginTop: 6 }}>
                  <div className="cardBody">
                    <div className="btnRow" style={{ flexWrap: "wrap", gap: 8, justifyContent: "flex-start" }}>
                      {metricOptions.slice(0, 80).map((m) => {
                        const checked = metricSlugsSet.has(m.slug);
                        return (
                          <label key={m.slug} className="tag" style={{ display: "inline-flex", gap: 8, alignItems: "center", cursor: "pointer" }}>
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={(e) => toggleMetric(m.slug, e.target.checked)}
                            />
                            <span>{m.name}</span>
                            <span className="muted2 mono" style={{ fontSize: 11 }}>{m.slug}</span>
                          </label>
                        );
                      })}
                    </div>
                    <div className="muted2" style={{ fontSize: 12, marginTop: 10 }}>
                      Tip: below you'll get one block per selected metric. Default view is Table; click “Graph” to toggle.
                    </div>
                  </div>
                </div>
              </div>

              <div style={{ gridColumn: "span 6" }}>
                <label className="muted2" style={{ fontSize: 12 }}>Hypotheses</label>
                <input className="input" style={{ marginTop: 6 }} placeholder="Search hypotheses…" value={hypQ} onChange={(e) => setHypQ(e.target.value)} />
                <div className="card" style={{ marginTop: 8 }}>
                  <div className="cardBody" style={{ maxHeight: 240, overflow: "auto" }}>
                    <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 6 }}>
                      {filteredHyps.map((h) => {
                        const id = String(h.id);
                        const checked = selectedHypIdsSet.has(id);
                        return (
                          <label key={id} style={{ gridColumn: "span 12", display: "flex", gap: 10, alignItems: "center" }}>
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={(e) => {
                                const on = !!e.target.checked;
                                setSelectedHypIds((prev) => {
                                  const s = new Set(prev);
                                  if (on) s.add(id);
                                  else s.delete(id);
                                  return Array.from(s);
                                });
                              }}
                            />
                            <div style={{ flex: 1 }}>
                              <div><b>{h.title}</b> {h.status ? <span className="tag" style={{ marginLeft: 8 }}>{h.status}</span> : null}</div>
                              <div className="muted2 mono" style={{ fontSize: 12 }}>{id}</div>
                            </div>
                          </label>
                        );
                      })}
                      {!filteredHyps.length ? <div className="muted2">No hypotheses.</div> : null}
                    </div>
                  </div>
                </div>
              </div>

            </div>
          </div>
        </div>

        {(charts || []).map((c) => {
          const titlePrefix = source === "builtin" ? "Built-in" : source === "hypothesis" ? "Hypothesis metrics" : "Channel metrics";
          const v = viewFor(c.slug);
          if (v === "chart") {
            return (
              <LineChart
                key={`metric:${c.slug}`}
                title={`${titlePrefix}: ${c.slug}`}
                weeks={weeks}
                series={c.series.map((s) => ({ key: s.key, label: s.label, values: s.values }))}
                unit={c.unit}
                right={
                  <button className="btn" onClick={() => setMetricView((p) => ({ ...p, [c.slug]: "table" }))}>
                    Table
                  </button>
                }
              />
            );
          }
          return (
            <TableCard
              key={`metric:${c.slug}`}
              metricSlug={c.slug}
              weeks={weeks}
              selectedHypIds={selectedHypIds}
              byHypWeekCheckin={byHypWeekCheckin}
              source={source}
              unitByMetricSlug={unitByMetricSlug}
              hypById={hypById}
              onSwitchToChart={() => setMetricView((p) => ({ ...p, [c.slug]: "chart" }))}
            />
          );
        })}
      </div>
    </main>
  );
}


