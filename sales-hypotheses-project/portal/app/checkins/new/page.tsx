"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { isoDate, startOfWeekISO } from "../../lib/utils";
import { AppTopbar } from "../../components/AppTopbar";

type OwnedRow = { hypothesis_id: string; channel_id: string };
type OwnedMetricRow = { hypothesis_id: string; channel_id: string; metric_id: string };
type ChannelRow = { id: string; slug: string; name: string };
type MetricRow = { id: string; slug: string; name: string; input_type: string; unit: string | null };
type HypRow = { id: string; title: string; status: string | null; updated_at: string | null; cjm_json: any };

type RecentCall = { id: string; title: string | null; occurred_at: string | null; owner_email: string | null };

export default function SubmitWeeklyReportPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [status, setStatus] = useState("");
  const [sessionEmail, setSessionEmail] = useState<string | null>(null);

  const [step, setStep] = useState<0 | 1 | 2>(0); // 0 pick hyps, 1 metrics, 2 calls
  const [weekStart, setWeekStart] = useState<string>(startOfWeekISO(new Date()));

  // Data
  const [owned, setOwned] = useState<OwnedRow[]>([]);
  const [ownedMetrics, setOwnedMetrics] = useState<OwnedMetricRow[]>([]);
  const [hyps, setHyps] = useState<HypRow[]>([]);
  const [channels, setChannels] = useState<ChannelRow[]>([]);
  const [metrics, setMetrics] = useState<MetricRow[]>([]);
  const [channelMetrics, setChannelMetrics] = useState<Array<{ hypothesis_id: string; channel_id: string; metric_id: string }>>([]);

  // Selection
  const [selectedHypIds, setSelectedHypIds] = useState<string[]>([]);
  const [q, setQ] = useState("");

  // Inputs: per hypothesis -> per channel slug -> metrics by slug
  const [perHypInputs, setPerHypInputs] = useState<Record<string, Record<string, { activity: string; results: string; metrics: Record<string, string> }>>>({});

  // Calls linking
  const [linkCalls, setLinkCalls] = useState<boolean>(false);
  const [recentCalls, setRecentCalls] = useState<RecentCall[]>([]);
  const [callMap, setCallMap] = useState<Record<string, { hypothesis_id: string; tag: string; notes: string }>>({});

  const channelById = useMemo(() => {
    const m = new Map<string, ChannelRow>();
    for (const c of channels) m.set(String(c.id), c);
    return m;
  }, [channels]);

  const metricById = useMemo(() => {
    const m = new Map<string, MetricRow>();
    for (const x of metrics) m.set(String(x.id), x);
    return m;
  }, [metrics]);

  const ownedByHyp = useMemo(() => {
    const m = new Map<string, string[]>(); // hyp -> channel_ids
    for (const r of owned) {
      const hid = String(r.hypothesis_id);
      const cid = String(r.channel_id);
      if (!hid || !cid) continue;
      const arr = m.get(hid) ?? [];
      if (!arr.includes(cid)) arr.push(cid);
      m.set(hid, arr);
    }
    return m;
  }, [owned]);

  const ownedMetricIdsByHypChannel = useMemo(() => {
    const m = new Map<string, string[]>(); // `${hypId}:${channelId}` -> metric_ids (owned by current user)
    for (const r of ownedMetrics) {
      const key = `${String(r.hypothesis_id)}:${String(r.channel_id)}`;
      const arr = m.get(key) ?? [];
      const mid = String(r.metric_id);
      if (mid && !arr.includes(mid)) arr.push(mid);
      m.set(key, arr);
    }
    return m;
  }, [ownedMetrics]);

  const channelMetricsByHypChannel = useMemo(() => {
    const m = new Map<string, string[]>(); // `${hypId}:${channelId}` -> metric_ids
    for (const r of channelMetrics) {
      const key = `${String(r.hypothesis_id)}:${String(r.channel_id)}`;
      const arr = m.get(key) ?? [];
      const mid = String(r.metric_id);
      if (mid && !arr.includes(mid)) arr.push(mid);
      m.set(key, arr);
    }
    return m;
  }, [channelMetrics]);

  const filteredHyps = useMemo(() => {
    const needle = q.trim().toLowerCase();
    const ids = new Set<string>([
      ...Array.from(ownedByHyp.keys()),
      ...Array.from(ownedMetricIdsByHypChannel.keys()).map((k) => String(k).split(":")[0]).filter(Boolean)
    ]);
    const xs = hyps.filter((h) => ids.has(String(h.id)));
    if (!needle) return xs;
    return xs.filter((h) => String(h.title ?? "").toLowerCase().includes(needle) || String(h.id).toLowerCase().includes(needle));
  }, [hyps, q, ownedByHyp, ownedMetricIdsByHypChannel]);

  function ensureInputsForHyp(hypId: string) {
    setPerHypInputs((prev) => {
      if (prev[hypId]) return prev;
      return { ...prev, [hypId]: {} };
    });
  }

  function hypLabel(id: string) {
    const h = hyps.find((x) => String(x.id) === String(id)) ?? null;
    return h?.title ?? id;
  }

  async function loadRecentCalls(email: string) {
    if (!supabase) return;
    const since = new Date();
    since.setDate(since.getDate() - 7);
    const sinceIso = since.toISOString();

    const partsRes = await supabase
      .from("call_participants")
      .select("call_id,calls:call_id(id,title,occurred_at,owner_email)")
      .eq("email", email)
      .gte("calls.occurred_at", sinceIso)
      .limit(200);

    const ownedRes = await supabase
      .from("calls")
      .select("id,title,occurred_at,owner_email")
      .eq("owner_email", email)
      .gte("occurred_at", sinceIso)
      .order("occurred_at", { ascending: false })
      .limit(200);

    const byId = new Map<string, RecentCall>();
    for (const r of (partsRes.data ?? []) as any[]) {
      const c = r?.calls ?? null;
      if (!c?.id) continue;
      byId.set(String(c.id), { id: String(c.id), title: c.title ?? null, occurred_at: c.occurred_at ?? null, owner_email: c.owner_email ?? null });
    }
    for (const c of (ownedRes.data ?? []) as any[]) {
      if (!c?.id) continue;
      byId.set(String(c.id), { id: String(c.id), title: c.title ?? null, occurred_at: c.occurred_at ?? null, owner_email: c.owner_email ?? null });
    }
    setRecentCalls(Array.from(byId.values()).sort((a, b) => String(b.occurred_at ?? "").localeCompare(String(a.occurred_at ?? ""))));
  }

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) return setStatus("Not signed in. Go back to / and sign in.");
    const email = sess.data.session.user.email ?? null;
    setSessionEmail(email);
    if (email) loadRecentCalls(email);

    const ownedRes = await supabase
      .from("sales_hypothesis_channel_owners")
      .select("hypothesis_id,channel_id,owner_email")
      .eq("owner_email", String(email ?? "").toLowerCase())
      .limit(5000);
    if (ownedRes.error) return setStatus(`owners error: ${ownedRes.error.message}`);

    const ownedMetricRes = await supabase
      .from("sales_hypothesis_channel_metric_owners")
      .select("hypothesis_id,channel_id,metric_id,owner_email")
      .eq("owner_email", String(email ?? "").toLowerCase())
      .limit(5000);
    if (ownedMetricRes.error) return setStatus(`metric owners error: ${ownedMetricRes.error.message}`);

    const hypIds = Array.from(
      new Set([
        ...((ownedRes.data ?? []) as any[]).map((x) => String(x.hypothesis_id)).filter(Boolean),
        ...((ownedMetricRes.data ?? []) as any[]).map((x) => String(x.hypothesis_id)).filter(Boolean)
      ])
    );
    if (!hypIds.length) {
      setOwned([]);
      setOwnedMetrics([]);
      setHyps([]);
      setStatus("You are not assigned as a channel owner or metric owner on any hypothesis yet.");
      return;
    }

    const [hRes, chRes, hcmRes, mRes] = await Promise.all([
      supabase
        .from("sales_hypotheses")
        .select("id,title,status,updated_at,cjm_json")
        .in("id", hypIds)
        .order("updated_at", { ascending: false })
        .limit(500),
      supabase
        .from("sales_channels")
        .select("id,slug,name")
        .eq("is_active", true)
        .limit(500),
      supabase
        .from("sales_hypothesis_channel_metrics")
        .select("hypothesis_id,channel_id,metric_id")
        .in("hypothesis_id", hypIds)
        .limit(5000),
      supabase
        .from("sales_metrics")
        .select("id,slug,name,input_type,unit")
        .eq("is_active", true)
        .limit(1000)
    ]);
    if (hRes.error) return setStatus(`hypotheses error: ${hRes.error.message}`);
    if (chRes.error) return setStatus(`channels error: ${chRes.error.message}`);
    if (hcmRes.error) return setStatus(`channel metrics error: ${hcmRes.error.message}`);
    if (mRes.error) return setStatus(`metrics error: ${mRes.error.message}`);

    setOwned(((ownedRes.data ?? []) as any[]).map((x) => ({ hypothesis_id: String(x.hypothesis_id), channel_id: String(x.channel_id) })));
    setOwnedMetrics(
      ((ownedMetricRes.data ?? []) as any[]).map((x) => ({
        hypothesis_id: String(x.hypothesis_id),
        channel_id: String(x.channel_id),
        metric_id: String(x.metric_id)
      }))
    );
    setHyps((hRes.data ?? []) as any);
    setChannels((chRes.data ?? []) as any);
    setChannelMetrics((hcmRes.data ?? []) as any);
    setMetrics((mRes.data ?? []) as any);
    setStatus("");
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase]);

  function toggleHyp(id: string, on: boolean) {
    setSelectedHypIds((prev) => {
      const s = new Set(prev);
      if (on) s.add(id);
      else s.delete(id);
      return Array.from(s);
    });
    ensureInputsForHyp(id);
  }

  function selectAll() {
    const ids = filteredHyps.map((h) => String(h.id));
    setSelectedHypIds(ids);
    for (const id of ids) ensureInputsForHyp(id);
  }

  function clearAll() {
    setSelectedHypIds([]);
  }

  function setChannelMetricValue(hypId: string, channelSlug: string, metricSlug: string, value: string) {
    setPerHypInputs((prev) => {
      const hyp = prev[hypId] ?? {};
      const ch = hyp[channelSlug] ?? { activity: "", results: "", metrics: {} };
      return {
        ...prev,
        [hypId]: {
          ...hyp,
          [channelSlug]: {
            ...ch,
            metrics: { ...(ch.metrics ?? {}), [metricSlug]: value }
          }
        }
      };
    });
  }

  async function submit() {
    if (!supabase) return;
    if (!selectedHypIds.length) return setStatus("Pick at least one hypothesis.");
    if (!weekStart) return setStatus("week_start is required");

    setStatus("Submitting...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) return setStatus("Not signed in.");

    // 1) Upsert check-ins per selected hypothesis, merging existing per_channel to avoid overwriting other owners.
    for (const hypId of selectedHypIds) {
      const ownedChannelIds = new Set<string>(ownedByHyp.get(hypId) ?? []);
      const metricOwnedChannelIds = new Set<string>(
        ownedMetrics
          .filter((x) => String(x.hypothesis_id) === String(hypId))
          .map((x) => String(x.channel_id))
          .filter(Boolean)
      );
      const channelIdsToTouch = new Set<string>([...Array.from(ownedChannelIds), ...Array.from(metricOwnedChannelIds)]);

      const channelsToTouch = Array.from(channelIdsToTouch)
        .map((id) => channelById.get(String(id)))
        .filter(Boolean) as ChannelRow[];
      const slugsToTouch = channelsToTouch.map((c) => c.slug).filter(Boolean);

      const existing = await supabase
        .from("sales_hypothesis_checkins")
        .select("*")
        .match({ hypothesis_id: hypId, week_start: weekStart })
        .maybeSingle();
      if (existing.error) return setStatus(`Load existing check-in error: ${existing.error.message}`);

      const prevPer = (existing.data?.channel_activity_json?.per_channel ?? {}) as any;
      const prevChannels = Array.isArray(existing.data?.channel_activity_json?.channels) ? existing.data?.channel_activity_json?.channels : [];

      const nextPer: any = { ...(prevPer || {}) };
      for (const c of channelsToTouch) {
        const slug = String(c.slug);
        const channelId = String(c.id);
        const isChannelOwner = ownedChannelIds.has(channelId);
        const key = `${hypId}:${channelId}`;
        const allMetricIds = channelMetricsByHypChannel.get(key) ?? [];
        const ownedMetricIds = ownedMetricIdsByHypChannel.get(key) ?? [];
        const metricIdsToWrite = isChannelOwner ? allMetricIds : ownedMetricIds;

        const metricSlugs = metricIdsToWrite
          .map((mid) => metricById.get(String(mid))?.slug)
          .filter(Boolean) as string[];

        const cur = perHypInputs[hypId]?.[slug] ?? { activity: "", results: "", metrics: {} };
        const prev = nextPer[slug] ?? {};
        const prevMetrics = (prev?.metrics && typeof prev.metrics === "object") ? prev.metrics : {};

        const nextMetrics: any = { ...(prevMetrics || {}) };
        for (const ms of metricSlugs) {
          const raw = String(cur.metrics?.[ms] ?? "").trim();
          nextMetrics[ms] = raw ? raw : null;
        }

        nextPer[slug] = {
          activity: isChannelOwner ? String(cur.activity ?? "") : String(prev?.activity ?? ""),
          results: isChannelOwner ? String(cur.results ?? "") : String(prev?.results ?? ""),
          metrics: nextMetrics
        };
      }

      const mergedChannels = Array.from(new Set([...(prevChannels || []), ...slugsToTouch])).filter(Boolean);

      const payload: any = {
        hypothesis_id: hypId,
        week_start: weekStart,
        channel_activity_json: { channels: mergedChannels, per_channel: nextPer },
        metrics_snapshot_json: existing.data?.metrics_snapshot_json ?? { metrics: {} }
      };
      const up = await supabase.from("sales_hypothesis_checkins").upsert(payload, { onConflict: "hypothesis_id,week_start" });
      if (up.error) return setStatus(`Check-in upsert error: ${up.error.message}`);
    }

    // 2) Optional call linking
    if (linkCalls) {
      const entries = Object.entries(callMap).filter(([_, v]) => v?.hypothesis_id);
      for (const [callId, v] of entries) {
        const hid = String(v.hypothesis_id);
        if (!selectedHypIds.includes(hid)) continue;
        const res = await supabase.from("sales_hypothesis_calls").upsert(
          {
            hypothesis_id: hid,
            call_id: callId,
            tag: String(v.tag ?? "").trim() || null,
            notes: String(v.notes ?? "").trim() || null
          },
          { onConflict: "hypothesis_id,call_id" }
        );
        if (res.error) return setStatus(`Call link error: ${res.error.message}`);
      }
    }

    setStatus("Report submitted. Redirecting...");
    window.location.href = "/hypotheses";
  }

  return (
    <main>
      <AppTopbar title="Submit weekly report" subtitle={`Week start (Mon): ${weekStart}`} />

      <div className="page" style={{ marginTop: 12 }}>
        <div className="btnRow" style={{ justifyContent: "flex-end" }}>
          <a className="btn" href="/hypotheses">Back to hypotheses</a>
          <button className="btn" onClick={() => setStep((s) => (s === 0 ? 0 : ((s - 1) as any)))} disabled={step === 0}>
            Back
          </button>
          <button
            className="btn btnPrimary"
            onClick={() => {
              if (step === 0) return setStep(1);
              if (step === 1) return setStep(2);
              return submit();
            }}
          >
            {step === 0 ? "Next: metrics" : step === 1 ? "Next: calls" : "Submit"}
          </button>
        </div>
      </div>

      <div className="page grid">
        {status ? (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardBody"><div className="notice">{status}</div></div>
          </div>
        ) : null}

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Week</div>
              <div className="cardDesc">We upsert check-ins per selected hypothesis for this week.</div>
            </div>
          </div>
          <div className="cardBody">
            <div className="grid">
              <div style={{ gridColumn: "span 4" }}>
                <label className="muted" style={{ fontSize: 13 }}>Week start (Mon) *</label>
                <input className="input" value={weekStart} onChange={(e) => setWeekStart(e.target.value)} placeholder="YYYY-MM-DD" />
              </div>
              <div style={{ gridColumn: "span 8" }}>
                <div className="muted2" style={{ fontSize: 12, marginTop: 22 }}>
                  Tip: keep using Monday start for consistent rollups.
                </div>
              </div>
            </div>
          </div>
        </div>

        {step === 0 ? (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardHeader">
              <div>
                <div className="cardTitle">1) Pick hypotheses</div>
                <div className="cardDesc">Only hypotheses where you are a channel owner appear here.</div>
              </div>
              <div className="btnRow">
                <input className="input" style={{ width: 280 }} placeholder="Search…" value={q} onChange={(e) => setQ(e.target.value)} />
                <button className="btn" onClick={selectAll}>Select all</button>
                <button className="btn" onClick={clearAll}>Clear</button>
              </div>
            </div>
            <div className="cardBody">
              <table className="table">
                <thead>
                  <tr>
                    <th style={{ width: 70 }}></th>
                    <th>Hypothesis</th>
                    <th style={{ width: 360 }}>Your channels</th>
                    <th style={{ width: 120 }}>Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredHyps.map((h) => {
                    const hid = String(h.id);
                    const checked = selectedHypIds.includes(hid);
                    const chIds = ownedByHyp.get(hid) ?? [];
                    const chLabels = chIds
                      .map((cid) => channelById.get(String(cid))?.name ?? channelById.get(String(cid))?.slug)
                      .filter(Boolean)
                      .slice(0, 6);
                    return (
                      <tr key={hid}>
                        <td>
                          <input type="checkbox" checked={checked} onChange={(e) => toggleHyp(hid, e.target.checked)} />
                        </td>
                        <td>
                          <b>{h.title}</b>
                          <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{hid}</div>
                        </td>
                        <td>
                          {chLabels.length ? (
                            <div className="btnRow" style={{ flexWrap: "wrap", gap: 8 }}>
                              {chLabels.map((x) => <span key={x} className="tag">{x}</span>)}
                            </div>
                          ) : <span className="muted2">—</span>}
                        </td>
                        <td className="mono">{h.updated_at ? isoDate(h.updated_at) : "—"}</td>
                      </tr>
                    );
                  })}
                  {!filteredHyps.length ? (
                    <tr><td colSpan={4} className="muted2">No hypotheses found for you.</td></tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        ) : null}

        {step === 1 ? (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardHeader">
              <div>
                <div className="cardTitle">2) Fill your channel metrics</div>
                <div className="cardDesc">We only show channels you own and metrics linked to those channels.</div>
              </div>
            </div>
            <div className="cardBody">
              {!selectedHypIds.length ? (
                <div className="notice">Pick hypotheses first.</div>
              ) : (
                <div className="grid">
                  {selectedHypIds.map((hid) => {
                    const ownedChIds = new Set<string>(ownedByHyp.get(hid) ?? []);
                    const metricChIds = new Set<string>(
                      ownedMetrics
                        .filter((x) => String(x.hypothesis_id) === String(hid))
                        .map((x) => String(x.channel_id))
                        .filter(Boolean)
                    );
                    const chIds = Array.from(new Set<string>([...Array.from(ownedChIds), ...Array.from(metricChIds)]));
                    const chList = chIds.map((cid) => channelById.get(String(cid))).filter(Boolean) as ChannelRow[];
                    return (
                      <div key={hid} className="card" style={{ gridColumn: "span 12" }}>
                        <div className="cardHeader">
                          <div>
                            <div className="cardTitle" style={{ fontSize: 14 }}>{hypLabel(hid)}</div>
                            <div className="cardDesc">Your scope: {chList.length ? chList.map((c) => c.name || c.slug).join(", ") : "—"}</div>
                          </div>
                        </div>
                        <div className="cardBody">
                          <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10 }}>
                            {chList.map((c) => {
                              const key = `${hid}:${String(c.id)}`;
                              const isChannelOwner = ownedChIds.has(String(c.id));
                              const mids = isChannelOwner
                                ? (channelMetricsByHypChannel.get(key) ?? [])
                                : (ownedMetricIdsByHypChannel.get(key) ?? []);
                              const ms = mids.map((mid) => metricById.get(String(mid))).filter(Boolean) as MetricRow[];
                              const cur = perHypInputs[hid]?.[c.slug] ?? { activity: "", results: "", metrics: {} };
                              return (
                                <div key={c.id} className="card" style={{ gridColumn: "span 6" }}>
                                  <div className="cardBody">
                                    <div className="tag" style={{ marginBottom: 10 }}>{c.name || c.slug}</div>
                                    {!ms.length ? (
                                      <div className="muted2" style={{ fontSize: 12 }}>No metrics linked to this channel in this hypothesis.</div>
                                    ) : (
                                      <table className="table">
                                        <thead>
                                          <tr><th>Metric</th><th style={{ width: 220 }}>Value</th></tr>
                                        </thead>
                                        <tbody>
                                          {ms.map((m) => {
                                            const slug = String(m.slug ?? "");
                                            const v = perHypInputs[hid]?.[c.slug]?.metrics?.[slug] ?? "";
                                            const t = String(m.input_type ?? "number");
                                            return (
                                              <tr key={m.id}>
                                                <td>
                                                  <b>{m.name || slug}</b>
                                                  <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>
                                                    {slug}{m.unit ? ` · ${m.unit}` : ""}
                                                  </div>
                                                </td>
                                                <td>
                                                  <input
                                                    className="input"
                                                    type={t === "number" ? "number" : "text"}
                                                    value={v}
                                                    onChange={(e) => setChannelMetricValue(hid, c.slug, slug, e.target.value)}
                                                  />
                                                </td>
                                              </tr>
                                            );
                                          })}
                                        </tbody>
                                      </table>
                                    )}

                                    {isChannelOwner ? (
                                      <div style={{ marginTop: 10 }}>
                                        <label className="muted2" style={{ fontSize: 12 }}>What did we do? (optional)</label>
                                        <textarea
                                          className="textarea"
                                          value={String(cur.activity ?? "")}
                                          onChange={(e) =>
                                            setPerHypInputs((prev) => ({
                                              ...prev,
                                              [hid]: {
                                                ...(prev[hid] ?? {}),
                                                [c.slug]: { ...(prev[hid]?.[c.slug] ?? { activity: "", results: "", metrics: {} }), activity: e.target.value }
                                              }
                                            }))
                                          }
                                          placeholder="Activity this week (messages, spend, experiments, etc.)"
                                        />
                                        <label className="muted2" style={{ fontSize: 12, marginTop: 8, display: "block" }}>What happened? (optional)</label>
                                        <textarea
                                          className="textarea"
                                          value={String(cur.results ?? "")}
                                          onChange={(e) =>
                                            setPerHypInputs((prev) => ({
                                              ...prev,
                                              [hid]: {
                                                ...(prev[hid] ?? {}),
                                                [c.slug]: { ...(prev[hid]?.[c.slug] ?? { activity: "", results: "", metrics: {} }), results: e.target.value }
                                              }
                                            }))
                                          }
                                          placeholder="Results (replies, meetings, learnings, negatives)"
                                        />
                                      </div>
                                    ) : null}
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        ) : null}

        {step === 2 ? (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardHeader">
              <div>
                <div className="cardTitle">3) Link calls (optional)</div>
                <div className="cardDesc">If you want, map your recent calls to selected hypotheses.</div>
              </div>
              <div className="btnRow">
                <label className="muted2" style={{ fontSize: 12, display: "inline-flex", gap: 8, alignItems: "center" }}>
                  <input type="checkbox" checked={linkCalls} onChange={(e) => setLinkCalls(!!e.target.checked)} />
                  Link my calls for this week
                </label>
              </div>
            </div>
            <div className="cardBody">
              {!linkCalls ? (
                <div className="muted2">Disabled. Toggle it on to link calls.</div>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Call</th>
                      <th style={{ width: 320 }}>Link to hypothesis</th>
                      <th style={{ width: 180 }}>Tag</th>
                      <th style={{ width: 360 }}>Notes</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recentCalls.slice(0, 30).map((c) => {
                      const cur = callMap[c.id] ?? { hypothesis_id: "", tag: "", notes: "" };
                      return (
                        <tr key={c.id}>
                          <td>
                            <b>{c.title || "Untitled"}</b>
                            <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>
                              {c.occurred_at ? isoDate(c.occurred_at) : "—"} · {c.id}
                            </div>
                          </td>
                          <td>
                            <select
                              className="select"
                              value={cur.hypothesis_id}
                              onChange={(e) => setCallMap({ ...callMap, [c.id]: { ...cur, hypothesis_id: e.target.value } })}
                            >
                              <option value="">—</option>
                              {selectedHypIds.map((hid) => (
                                <option key={hid} value={hid}>{hypLabel(hid)}</option>
                              ))}
                            </select>
                          </td>
                          <td>
                            <input
                              className="input"
                              value={cur.tag}
                              onChange={(e) => setCallMap({ ...callMap, [c.id]: { ...cur, tag: e.target.value } })}
                              placeholder="demo/discovery/..."
                            />
                          </td>
                          <td>
                            <input
                              className="input"
                              value={cur.notes}
                              onChange={(e) => setCallMap({ ...callMap, [c.id]: { ...cur, notes: e.target.value } })}
                              placeholder="why this call matters"
                            />
                          </td>
                        </tr>
                      );
                    })}
                    {!recentCalls.length ? (
                      <tr><td colSpan={4} className="muted2">No recent calls found for you.</td></tr>
                    ) : null}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        ) : null}
      </div>
    </main>
  );
}


