"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { AppTopbar } from "../../components/AppTopbar";
import { getSupabase } from "../../lib/supabase";

type CampaignOption = {
  id: string;
  name: string;
  channel: "smartlead" | "expandi" | "app" | "telegram";
  campaign_id?: string;
  source_campaign_key?: string;
};

type SelectedCampaign = CampaignOption & { selected: boolean; linkedTalName?: string | null };

export default function NewTalPage() {
  const router = useRouter();
  const supabase = useMemo(() => getSupabase(), []);

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [criteria, setCriteria] = useState("");

  const [campaigns, setCampaigns] = useState<SelectedCampaign[]>([]);
  const [campaignsLoading, setCampaignsLoading] = useState(true);
  const [filter, setFilter] = useState("");

  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!supabase) return;

    async function loadCampaigns() {
      setCampaignsLoading(true);
      try {
        const { data: { session } } = await supabase!.auth.getSession();
        const token = session?.access_token ?? "";
        const auth = `Bearer ${token}`;

        const [slRes, exRes] = await Promise.all([
          fetch("/api/smartlead/campaigns", { headers: { Authorization: auth } }),
          fetch("/api/expandi/campaigns?include_archived=true", { headers: { Authorization: auth } }),
        ]);

        const slJson = await slRes.json().catch(() => ({ campaigns: [] }));
        const exJson = await exRes.json().catch(() => ({ campaigns: [] }));

        // Load existing TAL campaign links to mark already-linked campaigns
        const linkedMap = new Map<string, string>(); // source_key -> TAL name
        const { data: talCampaignRows } = await supabase!
          .from("tal_campaigns")
          .select("source_campaign_key,campaign_name,channel,tal_id");
        const { data: talRows } = await supabase!
          .from("tals")
          .select("id,name");
        const talNameById = new Map<string, string>();
        for (const t of talRows ?? []) talNameById.set(t.id, t.name);
        for (const tc of talCampaignRows ?? []) {
          const talName = talNameById.get(tc.tal_id) ?? "TAL";
          if (tc.source_campaign_key) linkedMap.set(tc.source_campaign_key, talName);
          linkedMap.set(`${tc.channel}:fallback:${normalizeCampaignKey(tc.campaign_name)}`, talName);
        }

        function getLinkedTalName(sourceKey: string | undefined, channel: string, name: string): string | null {
          if (sourceKey && linkedMap.has(sourceKey)) return linkedMap.get(sourceKey)!;
          const fallback = `${channel}:fallback:${normalizeCampaignKey(name)}`;
          if (linkedMap.has(fallback)) return linkedMap.get(fallback)!;
          return null;
        }

        const slCampaigns: SelectedCampaign[] = (slJson.campaigns ?? []).map((c: any) => {
          const cName = c.name ?? c.campaign_name ?? String(c.id);
          const sourceKey = c.id != null ? `smartlead:id:${String(c.id)}` : undefined;
          return {
            id: `sl-${c.id ?? c.name}`,
            name: cName,
            channel: "smartlead" as const,
            campaign_id: String(c.id ?? ""),
            source_campaign_key: sourceKey,
            selected: false,
            linkedTalName: getLinkedTalName(sourceKey, "smartlead", cName),
          };
        });

        const exCampaignMap = new Map<string, SelectedCampaign>();
        for (const c of exJson.campaigns ?? []) {
          const name = String(c?.name ?? "").trim();
          if (!name) continue;
          const normalizedName = normalizeCampaignKey(name);
          if (exCampaignMap.has(normalizedName)) continue;
          const sourceKey = makeLinkedinTalSourceKey(name);
          exCampaignMap.set(normalizedName, {
            id: `ex-${normalizedName}`,
            name,
            channel: "expandi" as const,
            source_campaign_key: sourceKey,
            selected: false,
            linkedTalName: getLinkedTalName(sourceKey, "expandi", name),
          });
        }
        const exCampaigns = Array.from(exCampaignMap.values()).sort((a, b) => a.name.localeCompare(b.name));

        const [appRows, tgRows] = await Promise.all([
          supabase!
            .from("manual_stats")
            .select("account_name,campaign_name")
            .eq("channel", "app")
            .in("metric_name", ["total_touches", "replies", "booked_meetings", "held_meetings"]),
          supabase!
            .from("manual_stats")
            .select("account_name,campaign_name")
            .eq("channel", "telegram")
            .in("metric_name", ["total_touches", "replies", "booked_meetings", "held_meetings"]),
        ]);

        if (appRows.error) throw new Error(appRows.error.message);
        if (tgRows.error) throw new Error(tgRows.error.message);

        function normalizeManualCampaign(row: { account_name?: string | null; campaign_name?: string | null }) {
          const campaign = row.campaign_name?.trim();
          if (campaign) return campaign;
          const account = row.account_name?.trim();
          return account || null;
        }

        function uniqueManualCampaigns(
          rows: Array<{ account_name?: string | null; campaign_name?: string | null }> | null | undefined,
          channel: "app" | "telegram"
        ): SelectedCampaign[] {
          const names = Array.from(
            new Set(
              (rows ?? [])
                .map(normalizeManualCampaign)
                .filter((name): name is string => Boolean(name))
            )
          ).sort((a, b) => a.localeCompare(b));

          return names.map((name) => {
            const sourceKey = `${channel}:name:${normalizeCampaignKey(name)}`;
            return {
              id: `${channel}-${name}`,
              name,
              channel,
              campaign_id: name,
              source_campaign_key: sourceKey,
              selected: false,
              linkedTalName: getLinkedTalName(sourceKey, channel, name),
            };
          });
        }

        const appCampaigns = uniqueManualCampaigns(appRows.data, "app");
        const tgCampaigns = uniqueManualCampaigns(tgRows.data, "telegram");

        setCampaigns([...slCampaigns, ...exCampaigns, ...appCampaigns, ...tgCampaigns]);
      } catch (e: any) {
        setError(String(e?.message || e));
      } finally {
        setCampaignsLoading(false);
      }
    }

    loadCampaigns();
  }, [supabase]);

  function toggleCampaign(id: string) {
    setCampaigns((prev) => prev.map((c) => {
      if (c.id !== id || c.linkedTalName) return c;
      return { ...c, selected: !c.selected };
    }));
  }

  const selected = campaigns.filter((c) => c.selected);

  const filtered = campaigns.filter((c) => {
    if (!filter.trim()) return true;
    return c.name.toLowerCase().includes(filter.toLowerCase());
  });

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) { setError("Введи название TAL"); return; }
    if (!supabase) { setError("Supabase client is not configured"); return; }

    setSaving(true);
    setError(null);

    try {
      const talRes = await supabase
        .from("tals")
        .insert({
          name: name.trim(),
          description: description.trim() || null,
          criteria: criteria.trim() || null,
        })
        .select("id")
        .single();

      if (talRes.error || !talRes.data?.id) {
        throw new Error(talRes.error?.message || "Failed to create TAL");
      }

      const rows = selected.map((c) => ({
        tal_id: talRes.data.id,
        channel: c.channel,
        campaign_id: c.campaign_id || null,
        campaign_name: c.name,
        source_campaign_key: c.source_campaign_key || null,
      }));

      if (rows.length) {
        const relRes = await supabase
          .from("tal_campaigns")
          .upsert(rows, { onConflict: "tal_id,channel,campaign_name" });
        if (relRes.error) throw new Error(relRes.error.message);
      }

      window.location.assign(`/tals/${talRes.data.id}`);
    } catch (e: any) {
      setError(String(e?.message || e));
      setSaving(false);
    }
  }

  const slFiltered = filtered.filter((c) => c.channel === "smartlead");
  const exFiltered = filtered.filter((c) => c.channel === "expandi");
  const appFiltered = filtered.filter((c) => c.channel === "app");
  const tgFiltered = filtered.filter((c) => c.channel === "telegram");

  return (
    <div className="page">
      <AppTopbar title="New TAL" subtitle="Territory Account List" />

      <div className="content" style={{ maxWidth: 800 }}>
        <h1 style={{ fontSize: 22, fontWeight: 600, marginBottom: 24 }}>Новый TAL</h1>

        <form onSubmit={handleSubmit} style={{ display: "flex", flexDirection: "column", gap: 20 }}>
          {/* Name */}
          <div>
            <label style={{ display: "block", fontSize: 13, fontWeight: 500, marginBottom: 6 }}>
              Название *
            </label>
            <input
              className="input"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Например: Financial Sector - Banks EU"
              style={{ width: "100%" }}
            />
          </div>

          {/* Criteria */}
          <div>
            <label style={{ display: "block", fontSize: 13, fontWeight: 500, marginBottom: 6 }}>
              Критерий (что описывает этот список)
            </label>
            <input
              className="input"
              value={criteria}
              onChange={(e) => setCriteria(e.target.value)}
              placeholder="Например: Банки, 1000+ сотрудников, Европа"
              style={{ width: "100%" }}
            />
          </div>

          {/* Description */}
          <div>
            <label style={{ display: "block", fontSize: 13, fontWeight: 500, marginBottom: 6 }}>
              Заметки
            </label>
            <input
              className="input"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Доп. контекст, гипотеза которую тестируешь..."
              style={{ width: "100%" }}
            />
          </div>

          {/* Campaign picker */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
              <label style={{ fontSize: 13, fontWeight: 500 }}>
                Кампании ({selected.length} выбрано)
              </label>
              <input
                className="input"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                placeholder="Поиск кампании..."
                style={{ width: 220, fontSize: 13 }}
              />
            </div>

            {campaignsLoading && <p className="muted2" style={{ fontSize: 13 }}>Загружаю кампании...</p>}

            {!campaignsLoading && (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 16 }}>
                {([
                  { title: "Email / Smartlead", items: slFiltered },
                  { title: "LinkedIn / Expandi", items: exFiltered },
                  { title: "App", items: appFiltered },
                  { title: "Telegram", items: tgFiltered },
                ] as const).map(({ title, items }) => (
                  <div key={title}>
                    <div className="muted2" style={{ fontSize: 12, fontWeight: 600, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.05em" }}>
                      {title} ({items.length})
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 4, maxHeight: 320, overflowY: "auto" }}>
                      {items.length === 0 && <p className="muted2" style={{ fontSize: 13 }}>Нет кампаний</p>}
                      {items.map((c) => {
                        const isLinked = Boolean(c.linkedTalName);
                        return (
                          <label
                            key={c.id}
                            title={isLinked ? `Already linked to "${c.linkedTalName}"` : undefined}
                            style={{
                              display: "flex",
                              alignItems: "center",
                              gap: 8,
                              padding: "8px 10px",
                              borderRadius: 6,
                              cursor: isLinked ? "not-allowed" : "pointer",
                              opacity: isLinked ? 0.55 : 1,
                              background: isLinked ? "#f5f5f4" : c.selected ? "#eff6ff" : "#ffffff",
                              border: `1px solid ${isLinked ? "#e7e5e4" : c.selected ? "#1d4ed8" : "#d4d4d8"}`,
                              color: "#111827",
                              fontSize: 13,
                            }}
                          >
                            <input
                              type="checkbox"
                              checked={c.selected}
                              disabled={isLinked}
                              onChange={() => toggleCampaign(c.id)}
                              style={{ margin: 0 }}
                            />
                            <span style={{ flex: 1 }}>{c.name}</span>
                            {isLinked && (
                              <span style={{ fontSize: 11, color: "#a8a29e", whiteSpace: "nowrap" }}>
                                {c.linkedTalName}
                              </span>
                            )}
                          </label>
                        );
                      })}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {error && <p style={{ color: "#ef4444", margin: 0 }}>{error}</p>}

          <div style={{ display: "flex", gap: 10 }}>
            <button type="submit" className="btn btnPrimary" disabled={saving}>
              {saving ? "Сохраняю..." : "Создать TAL"}
            </button>
            <button type="button" className="btn" onClick={() => router.back()}>
              Отмена
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function normalizeCampaignKey(value: string) {
  return value.trim().toLowerCase().replace(/\s+/g, " ");
}

function makeLinkedinTalSourceKey(value: string) {
  return `expandi:canonical:${normalizeCampaignKey(value)}`;
}
