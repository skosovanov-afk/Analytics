"use client";

import { useEffect, useMemo, useState } from "react";
import { AppTopbar } from "../components/AppTopbar";
import { getSupabase } from "../lib/supabase";

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

type ManualStatInsert = {
  record_date: string;
  channel: Channel;
  account_name: string | null;
  campaign_name: string | null;
  note: string | null;
  metric_name: string;
  value: number;
};

// ─── Metric definitions ───────────────────────────────────────────────────────

const FUNNEL_METRICS: MetricDef[] = [
  { key: "booked_meetings", label: "Booked Meetings", divider: true },
  { key: "held_meetings", label: "Held Meetings" },
  { key: "qualified_leads", label: "Qualified Leads" },
];

// Telegram fixed metrics (channel-specific + funnel)
const TELEGRAM_METRICS: MetricDef[] = [
  { key: "total_touches", label: "Total touches" },
  { key: "replies", label: "Replies" },
  ...FUNNEL_METRICS,
];

// App fixed metrics (invitations + channel-specific + funnel)
const APP_METRICS: MetricDef[] = [
  { key: "invitations", label: "Invitations" },
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

function factKey(row: ManualStatInsert): string {
  return [
    row.record_date,
    row.channel,
    row.account_name?.trim() || "",
    row.campaign_name?.trim() || "",
    row.metric_name,
  ].join("::");
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function ManualStatsPage() {
  const supabase = useMemo(() => getSupabase(), []);

  // ─── Form state ───────────────────────────────────────────────────────────

  const [recordDate, setRecordDate] = useState(today());
  const [channel, setChannel] = useState<Channel>("linkedin");
  const [accountName, setAccountName] = useState("");
  const [campaignName, setCampaignName] = useState("");
  const [note, setNote] = useState("");

  // Values for fixed-metric channels (linkedin / email) — includes funnel
  const [fixedValues, setFixedValues] = useState<Record<string, string>>({});

  // Funnel values for telegram / app (shown as fixed block below dynamic)
  const [funnelValues, setFunnelValues] = useState<Record<string, string>>({});

  // Campaign picker state
  const [campaignOptions, setCampaignOptions] = useState<string[]>([]);
  const [campaignsLoading, setCampaignsLoading] = useState(false);
  const [isNewCampaign, setIsNewCampaign] = useState(false);
  const [newCampaignName, setNewCampaignName] = useState("");

  // LinkedIn account → campaigns mapping
  const [linkedinAccountOptions, setLinkedinAccountOptions] = useState<string[]>([]);
  const [linkedinAccountCampaigns, setLinkedinAccountCampaigns] = useState<Map<string, string[]>>(new Map());

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

  // Reset all metric values and load campaigns when channel changes
  useEffect(() => {
    setFixedValues({});
    setFunnelValues({});
    setCampaignName("");
    setAccountName("");
    setIsNewCampaign(false);
    setNewCampaignName("");
    setLinkedinAccountOptions([]);
    setLinkedinAccountCampaigns(new Map());

    if (!supabase) return;
    let cancelled = false;
    async function loadCampaigns() {
      setCampaignsLoading(true);
      setCampaignOptions([]);
      try {
        let names: string[] = [];
        if (channel === "linkedin") {
          const { data } = await supabase!
            .from("expandi_campaign_instances")
            .select("name,li_account_id")
            .eq("active", true)
            .or("archived.is.false,archived.is.null");
          // Also load account names from the alltime view
          const { data: accountData } = await supabase!
            .from("linkedin_kpi_alltime_v2")
            .select("account_name,campaign_name,li_account_id");
          // Build account_id → account_name mapping
          const EXCLUDED_ACCOUNTS = new Set(["Legacy / manual supplement"]);
          const idToName = new Map<number, string>();
          const accountToCampaigns = new Map<string, Set<string>>();
          for (const r of accountData ?? []) {
            const accName = String(r.account_name ?? "").trim();
            const campName = String(r.campaign_name ?? "").trim();
            const accId = Number(r.li_account_id);
            if (accName && !EXCLUDED_ACCOUNTS.has(accName) && Number.isFinite(accId)) idToName.set(accId, accName);
            if (accName && !EXCLUDED_ACCOUNTS.has(accName) && campName) {
              if (!accountToCampaigns.has(accName)) accountToCampaigns.set(accName, new Set());
              accountToCampaigns.get(accName)!.add(campName);
            }
          }
          // Add active campaigns to their accounts
          const seen = new Set<string>();
          for (const r of data ?? []) {
            const n = String(r.name ?? "").trim();
            const accId = Number(r.li_account_id);
            const accName = Number.isFinite(accId) ? idToName.get(accId) : undefined;
            if (n && !seen.has(n.toLowerCase())) { seen.add(n.toLowerCase()); names.push(n); }
            if (accName && n) {
              if (!accountToCampaigns.has(accName)) accountToCampaigns.set(accName, new Set());
              accountToCampaigns.get(accName)!.add(n);
            }
          }
          // Convert to sorted arrays
          const accountMap = new Map<string, string[]>();
          for (const [acc, camps] of accountToCampaigns) {
            accountMap.set(acc, Array.from(camps).sort((a, b) => a.localeCompare(b)));
          }
          if (!cancelled) {
            setLinkedinAccountOptions(Array.from(accountMap.keys()).sort((a, b) => a.localeCompare(b)));
            setLinkedinAccountCampaigns(accountMap);
          }
        } else if (channel === "email") {
          const { data } = await supabase!
            .from("smartlead_stats_daily")
            .select("campaign_id,campaign_name");
          const seen = new Map<string, string>();
          for (const r of data ?? []) {
            const n = String(r.campaign_name ?? "").trim();
            if (n && !seen.has(n.toLowerCase())) { seen.set(n.toLowerCase(), n); }
          }
          names = Array.from(seen.values());
        } else if (channel === "telegram" || channel === "app") {
          const { data } = await supabase!
            .from("manual_stats")
            .select("campaign_name")
            .eq("channel", channel);
          const seen = new Set<string>();
          for (const r of data ?? []) {
            const n = String(r.campaign_name ?? "").trim();
            if (n && !seen.has(n.toLowerCase())) { seen.add(n.toLowerCase()); names.push(n); }
          }
        }
        names.sort((a, b) => a.localeCompare(b));
        if (!cancelled) setCampaignOptions(names);
      } catch (e) {
        console.error("Failed to load campaigns:", e);
      } finally {
        if (!cancelled) setCampaignsLoading(false);
      }
    }
    loadCampaigns();
    return () => { cancelled = true; };
  }, [channel, supabase]);

  // ─── Submit ───────────────────────────────────────────────────────────────

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!supabase) return;
    setSaving(true);
    setStatus("");

    const effectiveCampaignName = isNewCampaign ? newCampaignName.trim() : campaignName.trim();
    const base = {
      record_date: recordDate,
      channel,
      account_name: accountName.trim() || null,
      campaign_name: effectiveCampaignName || null,
      note: note.trim() || null,
    };

    let inserts: ManualStatInsert[] = [];

    if (channel === "linkedin" || channel === "email") {
      inserts = FIXED_METRICS[channel]
        .filter((m) => fixedValues[m.key] !== "" && fixedValues[m.key] !== undefined)
        .map((m) => ({ ...base, metric_name: m.key, value: parseInt(fixedValues[m.key], 10) }))
        .filter((r) => !isNaN((r as { value: number }).value));
    } else {
      // telegram / app: fixed metrics only
      const channelMetrics = channel === "app" ? APP_METRICS : TELEGRAM_METRICS;
      inserts = channelMetrics
        .filter((m) => funnelValues[m.key] !== "" && funnelValues[m.key] !== undefined)
        .map((m) => ({ ...base, metric_name: m.key, value: parseInt(funnelValues[m.key], 10) }))
        .filter((r) => !isNaN((r as { value: number }).value));
    }

    if (!inserts.length) {
      setStatus("Заполни хотя бы одну метрику.");
      setSaving(false);
      return;
    }

    // Keep only the last value for the same logical fact inside a single submit batch.
    const deduped = Array.from(
      inserts.reduce((map, row) => map.set(factKey(row), row), new Map<string, ManualStatInsert>()).values()
    );

    const { error } = await supabase
      .from("manual_stats")
      .upsert(deduped, {
        onConflict: "record_date,channel,account_name_key,campaign_name_key,metric_name",
      });

    if (error) {
      setStatus(`Error: ${error.message}`);
    } else {
      setStatus(`Сохранено ${deduped.length} метрик.`);
      setFixedValues({});
      setFunnelValues({});
      setNote("");
      setIsNewCampaign(false);
      setNewCampaignName("");
      // Reload campaigns list (new campaign_name may have appeared)
      setCampaignOptions((prev) => {
        if (effectiveCampaignName && !prev.some((n) => n.toLowerCase() === effectiveCampaignName.toLowerCase())) {
          return [...prev, effectiveCampaignName].sort((a, b) => a.localeCompare(b));
        }
        return prev;
      });
      setCampaignName(effectiveCampaignName);
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

  // For LinkedIn: filter campaigns by selected account
  const filteredCampaignOptions = useMemo(() => {
    if (channel !== "linkedin" || !accountName.trim()) return campaignOptions;
    const forAccount = linkedinAccountCampaigns.get(accountName.trim());
    return forAccount ?? campaignOptions;
  }, [channel, accountName, campaignOptions, linkedinAccountCampaigns]);

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
                  {channel === "linkedin" ? (
                    <select
                      className="select"
                      value={accountName}
                      onChange={(e) => {
                        setAccountName(e.target.value);
                        setCampaignName("");
                      }}
                    >
                      <option value="">Все аккаунты</option>
                      {linkedinAccountOptions.map((name) => (
                        <option key={name} value={name}>{name}</option>
                      ))}
                    </select>
                  ) : (
                    <input
                      type="text"
                      className="input"
                      placeholder="напр. Ilya Petrov"
                      value={accountName}
                      onChange={(e) => setAccountName(e.target.value)}
                    />
                  )}
                </div>
                <div style={{ gridColumn: "span 4" }}>
                  <div className="muted2" style={{ fontSize: 12, marginBottom: 5 }}>Кампания</div>
                  {isNewCampaign ? (
                    <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                      <input
                        type="text"
                        className="input"
                        placeholder="Название новой кампании"
                        value={newCampaignName}
                        onChange={(e) => setNewCampaignName(e.target.value)}
                        style={{ flex: 1 }}
                        autoFocus
                      />
                      <button
                        type="button"
                        className="btn"
                        style={{ padding: "6px 10px", fontSize: 13 }}
                        onClick={() => { setIsNewCampaign(false); setNewCampaignName(""); }}
                      >
                        Отмена
                      </button>
                    </div>
                  ) : (
                    <select
                      className="select"
                      value={campaignName}
                      onChange={(e) => {
                        if (e.target.value === "__new__") {
                          setIsNewCampaign(true);
                          setCampaignName("");
                        } else {
                          setCampaignName(e.target.value);
                        }
                      }}
                      disabled={campaignsLoading}
                    >
                      <option value="">{campaignsLoading ? "Загрузка..." : "Выбери кампанию"}</option>
                      {filteredCampaignOptions.map((name) => (
                        <option key={name} value={name}>{name}</option>
                      ))}
                      {(channel === "telegram" || channel === "app") && (
                        <option value="__new__">+ Новая кампания...</option>
                      )}
                    </select>
                  )}
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
                /* Telegram / App: fixed metrics only */
                <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 20 }}>
                  {(channel === "app" ? APP_METRICS : TELEGRAM_METRICS).map((m) => (
                    <MetricRow
                      key={m.key}
                      m={m}
                      values={funnelValues}
                      setValues={setFunnelValues}
                    />
                  ))}
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
