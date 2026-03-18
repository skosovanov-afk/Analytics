"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { AppTopbar } from "../components/AppTopbar";

// ─── Types ────────────────────────────────────────────────────────────────────

type Channel = "linkedin" | "email" | "telegram" | "app";

type MetricDef = { key: string; label: string; divider?: true };

type StatRow = {
  id: string;
  record_date: string;
  channel: string;
  account_name: string | null;
  campaign_name: string | null;
  metric_name: string;
  value: number;
  note: string | null;
};

// ─── Metric definitions ───────────────────────────────────────────────────────

const FUNNEL_METRICS: MetricDef[] = [
  { key: "booked_meetings", label: "Booked Meetings", divider: true },
  { key: "held_meetings", label: "Held Meetings" },
  { key: "sql", label: "SQL" },
  { key: "ft_sql", label: "FT SQL" },
  { key: "clients", label: "Clients" },
];

// Telegram / App fixed metrics (channel-specific + funnel)
const TELEGRAM_METRICS: MetricDef[] = [
  { key: "total_touches", label: "Total touches" },
  { key: "replies", label: "Replies" },
  ...FUNNEL_METRICS,
];

// Channel-specific metrics + funnel appended (all saved under the selected channel)
const FIXED_METRICS: Record<"linkedin" | "email", MetricDef[]> = {
  linkedin: [
    { key: "connection_req", label: "Connection requests sent" },
    { key: "accepted", label: "Connections accepted" },
    { key: "sent_messages", label: "Messages sent" },
    { key: "replies", label: "Replies" },
    ...FUNNEL_METRICS,
  ],
  email: [
    { key: "sent_count", label: "Emails sent" },
    { key: "reply_count", label: "Replies" },
    { key: "open_count", label: "Opens" },
    ...FUNNEL_METRICS,
  ],
};

const CHANNEL_LABELS: Record<Channel, string> = {
  linkedin: "LinkedIn",
  email: "Email",
  telegram: "Telegram",
  app: "App",
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

function today(): string {
  return new Date().toISOString().slice(0, 10);
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function ManualStatsPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(
    () => (supabaseUrl && supabaseAnonKey ? createClient(supabaseUrl, supabaseAnonKey) : null),
    [supabaseUrl, supabaseAnonKey]
  );

  // ─── Form state ───────────────────────────────────────────────────────────

  const [recordDate, setRecordDate] = useState(today());
  const [channel, setChannel] = useState<Channel>("linkedin");
  const [accountName, setAccountName] = useState("");
  const [campaignName, setCampaignName] = useState("");
  const [note, setNote] = useState("");

  // Values for fixed-metric channels (linkedin / email) — includes funnel
  const [fixedValues, setFixedValues] = useState<Record<string, string>>({});

  // Custom rows for telegram / app
  const [dynamicMetrics, setDynamicMetrics] = useState<{ name: string; value: string }[]>([
    { name: "", value: "" },
  ]);

  // Funnel values for telegram / app (shown as fixed block below dynamic)
  const [funnelValues, setFunnelValues] = useState<Record<string, string>>({});

  // ─── Entries state ────────────────────────────────────────────────────────

  const [rows, setRows] = useState<StatRow[]>([]);
  const [status, setStatus] = useState("");
  const [saving, setSaving] = useState(false);

  // ─── Load entries ─────────────────────────────────────────────────────────

  async function load() {
    if (!supabase) return;
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) {
      setStatus("Not signed in. Go back to / and sign in.");
      return;
    }
    const { data, error } = await supabase
      .from("manual_stats")
      .select("*")
      .order("record_date", { ascending: false })
      .order("created_at", { ascending: false });
    if (error) { setStatus(`Error: ${error.message}`); return; }
    setRows((data ?? []) as StatRow[]);
    setStatus("");
  }

  useEffect(() => { load(); }, [supabase]); // eslint-disable-line react-hooks/exhaustive-deps

  // Reset all metric values when channel changes
  useEffect(() => {
    setFixedValues({});
    setDynamicMetrics([{ name: "", value: "" }]);
    setFunnelValues({});
  }, [channel]);

  // ─── Submit ───────────────────────────────────────────────────────────────

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!supabase) return;
    setSaving(true);
    setStatus("");

    const base = {
      record_date: recordDate,
      channel,
      account_name: accountName.trim() || null,
      campaign_name: campaignName.trim() || null,
      note: note.trim() || null,
    };

    let inserts: object[] = [];

    if (channel === "linkedin" || channel === "email") {
      // All metrics (channel-specific + funnel) are in fixedValues
      inserts = FIXED_METRICS[channel]
        .filter((m) => fixedValues[m.key] !== "" && fixedValues[m.key] !== undefined)
        .map((m) => ({ ...base, metric_name: m.key, value: parseInt(fixedValues[m.key], 10) }))
        .filter((r) => !isNaN((r as { value: number }).value));
    } else {
      // telegram / app: dynamic custom rows
      const dynRows = dynamicMetrics
        .filter((m) => m.name.trim() && m.value !== "")
        .map((m) => ({ ...base, metric_name: m.name.trim(), value: parseInt(m.value, 10) }))
        .filter((r) => !isNaN((r as { value: number }).value));
      // + telegram/app fixed metrics (total_touches, replies + funnel)
      const fnlRows = TELEGRAM_METRICS
        .filter((m) => funnelValues[m.key] !== "" && funnelValues[m.key] !== undefined)
        .map((m) => ({ ...base, metric_name: m.key, value: parseInt(funnelValues[m.key], 10) }))
        .filter((r) => !isNaN((r as { value: number }).value));
      inserts = [...dynRows, ...fnlRows];
    }

    if (!inserts.length) {
      setStatus("Заполни хотя бы одну метрику.");
      setSaving(false);
      return;
    }

    const { error } = await supabase.from("manual_stats").insert(inserts);
    if (error) {
      setStatus(`Error: ${error.message}`);
    } else {
      setStatus(`Сохранено ${inserts.length} метрик.`);
      setFixedValues({});
      setDynamicMetrics([{ name: "", value: "" }]);
      setFunnelValues({});
      setNote("");
      await load();
    }
    setSaving(false);
  }

  // ─── Delete ───────────────────────────────────────────────────────────────

  async function deleteRow(id: string) {
    if (!supabase) return;
    const { error } = await supabase.from("manual_stats").delete().eq("id", id);
    if (error) { setStatus(`Error: ${error.message}`); return; }
    setRows((prev) => prev.filter((r) => r.id !== id));
  }

  // ─── Dynamic metric helpers (telegram / app) ──────────────────────────────

  function updateDynamic(i: number, field: "name" | "value", val: string) {
    setDynamicMetrics((prev) => prev.map((m, idx) => idx === i ? { ...m, [field]: val } : m));
  }

  function addDynamic() {
    setDynamicMetrics((prev) => [...prev, { name: "", value: "" }]);
  }

  function removeDynamic(i: number) {
    setDynamicMetrics((prev) => prev.filter((_, idx) => idx !== i));
  }

  // ─── Metric row renderer ──────────────────────────────────────────────────

  function MetricRow({ m, values, setValues }: {
    m: MetricDef;
    values: Record<string, string>;
    setValues: React.Dispatch<React.SetStateAction<Record<string, string>>>;
  }) {
    return (
      <>
        {m.divider && (
          <div style={{ borderTop: "1px solid rgba(255,255,255,0.08)", margin: "10px 0 10px" }} />
        )}
        <div style={{ display: "grid", gridTemplateColumns: "220px 1fr", alignItems: "center", gap: 12 }}>
          <div style={{ fontSize: 14, color: "rgba(255,255,255,0.8)" }}>{m.label}</div>
          <input
            type="number"
            className="input"
            min={0}
            placeholder="—"
            value={values[m.key] ?? ""}
            onChange={(e) => setValues((prev) => ({ ...prev, [m.key]: e.target.value }))}
            style={{ maxWidth: 180 }}
          />
        </div>
      </>
    );
  }

  // ─── Render ───────────────────────────────────────────────────────────────

  const isFixed = channel === "linkedin" || channel === "email";

  return (
    <main>
      <AppTopbar title="Manual Stats" subtitle="Ручной ввод данных по каналам" />

      <div className="page grid">
        {status && (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardBody"><div className="notice">{status}</div></div>
          </div>
        )}

        {/* ── Form ─────────────────────────────────────────────────────────── */}
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Добавить запись</div>
              <div className="cardDesc">Данные сохраняются в Supabase → таблица manual_stats</div>
            </div>
          </div>
          <div className="cardBody">
            <form onSubmit={handleSubmit}>

              {/* Header fields */}
              <div className="grid" style={{ gap: 10, marginBottom: 20 }}>
                <div style={{ gridColumn: "span 2" }}>
                  <div className="muted2" style={{ fontSize: 12, marginBottom: 5 }}>Дата</div>
                  <input
                    type="date"
                    className="input"
                    value={recordDate}
                    onChange={(e) => setRecordDate(e.target.value)}
                    required
                  />
                </div>
                <div style={{ gridColumn: "span 2" }}>
                  <div className="muted2" style={{ fontSize: 12, marginBottom: 5 }}>Канал</div>
                  <select
                    className="select"
                    value={channel}
                    onChange={(e) => setChannel(e.target.value as Channel)}
                  >
                    {(Object.entries(CHANNEL_LABELS) as [Channel, string][]).map(([k, v]) => (
                      <option key={k} value={k}>{v}</option>
                    ))}
                  </select>
                </div>
                <div style={{ gridColumn: "span 4" }}>
                  <div className="muted2" style={{ fontSize: 12, marginBottom: 5 }}>Аккаунт</div>
                  <input
                    type="text"
                    className="input"
                    placeholder="напр. Ilya Petrov"
                    value={accountName}
                    onChange={(e) => setAccountName(e.target.value)}
                  />
                </div>
                <div style={{ gridColumn: "span 4" }}>
                  <div className="muted2" style={{ fontSize: 12, marginBottom: 5 }}>Кампания</div>
                  <input
                    type="text"
                    className="input"
                    placeholder="напр. Slush 2025"
                    value={campaignName}
                    onChange={(e) => setCampaignName(e.target.value)}
                  />
                </div>
              </div>

              {/* Metrics section label */}
              <div className="muted2" style={{ fontSize: 12, marginBottom: 10, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                Метрики — {CHANNEL_LABELS[channel]}
              </div>

              {isFixed ? (
                /* LinkedIn / Email: all metrics in one list (channel + funnel), divider between them */
                <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 20 }}>
                  {FIXED_METRICS[channel as "linkedin" | "email"].map((m) => (
                    <MetricRow key={m.key} m={m} values={fixedValues} setValues={setFixedValues} />
                  ))}
                </div>
              ) : (
                /* Telegram / App: custom rows + funnel below */
                <div style={{ marginBottom: 20 }}>
                  <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 8 }}>
                    {dynamicMetrics.map((m, i) => (
                      <div key={i} style={{ display: "grid", gridTemplateColumns: "1fr 180px 32px", alignItems: "center", gap: 10 }}>
                        <input
                          type="text"
                          className="input"
                          placeholder="Название метрики"
                          value={m.name}
                          onChange={(e) => updateDynamic(i, "name", e.target.value)}
                        />
                        <input
                          type="number"
                          className="input"
                          min={0}
                          placeholder="Значение"
                          value={m.value}
                          onChange={(e) => updateDynamic(i, "value", e.target.value)}
                        />
                        {dynamicMetrics.length > 1 && (
                          <button
                            type="button"
                            onClick={() => removeDynamic(i)}
                            style={{ background: "none", border: "none", color: "rgba(255,255,255,0.4)", cursor: "pointer", fontSize: 18, lineHeight: 1 }}
                          >×</button>
                        )}
                      </div>
                    ))}
                    <button type="button" className="btn" style={{ alignSelf: "flex-start", marginTop: 4 }} onClick={addDynamic}>
                      + Add metric
                    </button>
                  </div>
                  {/* Telegram/app fixed metrics: total_touches, replies + funnel */}
                  <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 8 }}>
                    {TELEGRAM_METRICS.map((m, i) => (
                      <MetricRow
                        key={m.key}
                        m={i === 0 ? m : m}
                        values={funnelValues}
                        setValues={setFunnelValues}
                      />
                    ))}
                  </div>
                </div>
              )}

              {/* Note */}
              <div style={{ marginBottom: 16 }}>
                <div className="muted2" style={{ fontSize: 12, marginBottom: 5 }}>Комментарий (опционально)</div>
                <textarea
                  className="textarea"
                  placeholder="Источник данных, период, уточнения…"
                  value={note}
                  onChange={(e) => setNote(e.target.value)}
                  style={{ minHeight: 60 }}
                />
              </div>

              <button type="submit" className="btn btnPrimary" disabled={saving}>
                {saving ? "Сохраняю…" : "Сохранить"}
              </button>
            </form>
          </div>
        </div>

        {/* ── Entries table ─────────────────────────────────────────────────── */}
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Все записи</div>
              <div className="cardDesc">{rows.length} записей</div>
            </div>
            <button className="btn" onClick={load}>↻</button>
          </div>
          <div className="cardBody" style={{ overflowX: "auto" }}>
            <table className="table">
              <thead>
                <tr>
                  <th>Дата</th>
                  <th>Канал</th>
                  <th>Аккаунт</th>
                  <th>Кампания</th>
                  <th>Метрика</th>
                  <th>Значение</th>
                  <th>Комментарий</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.id}>
                    <td className="mono">{r.record_date}</td>
                    <td>{CHANNEL_LABELS[r.channel as Channel] ?? r.channel}</td>
                    <td>{r.account_name ?? "—"}</td>
                    <td>{r.campaign_name ?? "—"}</td>
                    <td className="mono">{r.metric_name}</td>
                    <td className="mono"><b>{r.value.toLocaleString()}</b></td>
                    <td className="muted2" style={{ fontSize: 12 }}>{r.note ?? ""}</td>
                    <td>
                      <button
                        onClick={() => deleteRow(r.id)}
                        style={{ background: "none", border: "none", color: "rgba(255,255,255,0.35)", cursor: "pointer", fontSize: 16 }}
                        title="Удалить"
                      >×</button>
                    </td>
                  </tr>
                ))}
                {!rows.length && (
                  <tr><td colSpan={8} className="muted2">Нет записей.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </main>
  );
}
