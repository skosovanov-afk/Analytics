"use client";

import { useEffect, useState, useMemo } from "react";
import { useParams, useRouter } from "next/navigation";
import Link from "next/link";
import { AppTopbar } from "../../components/AppTopbar";
import { getSupabase } from "../../lib/supabase";

type TalDetail = {
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

type TalCampaign = {
  id: string;
  channel: "smartlead" | "expandi" | "app" | "telegram";
  campaign_id: string | null;
  campaign_name: string;
  source_campaign_key?: string | null;
};

type AvailableCampaign = {
  channel: "smartlead" | "expandi" | "app" | "telegram";
  name: string;
  source_campaign_key: string;
  campaign_id?: string | null;
};

function StatBox({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="surfaceCard metricCard" style={{ minWidth: 132 }}>
      <div className="metricCardHeader" style={{ marginBottom: 10 }}>{label}</div>
      <div className="metricStatValue" style={{ fontSize: 28 }}>{value}</div>
      {sub && <div className="metricStatLabel" style={{ marginTop: 8 }}>{sub}</div>}
    </div>
  );
}

function pct(v: number | null) {
  if (v == null) return "0%";
  return `${v}%`;
}

function calcRate(numerator: number, denominator: number): string {
  if (denominator === 0) return "0%";
  return `${Math.round((numerator / denominator) * 100)}%`;
}


function channelLabel(channel: TalCampaign["channel"]) {
  if (channel === "smartlead") return "Email";
  if (channel === "expandi") return "LinkedIn";
  if (channel === "app") return "App";
  return "Telegram";
}

export default function TalDetailPage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();

  const supabase = useMemo(() => getSupabase(), []);

  const [tal, setTal] = useState<TalDetail | null>(null);
  const [campaigns, setCampaigns] = useState<TalCampaign[]>([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [unlinked, setUnlinked] = useState<AvailableCampaign[]>([]);
  const [addingKey, setAddingKey] = useState<string | null>(null);
  const [removingId, setRemovingId] = useState<string | null>(null);
  const [showAdd, setShowAdd] = useState(false);
  const [addFilter, setAddFilter] = useState("");

  async function getToken() {
    const { data: { session } } = await supabase!.auth.getSession();
    return session?.access_token ?? "";
  }

  async function reload() {
    const token = await getToken();
    const [talRes, unlinkedRes] = await Promise.all([
      fetch(`/api/tals/${id}`, { headers: { Authorization: `Bearer ${token}` } }),
      fetch("/api/tals/unlinked", { headers: { Authorization: `Bearer ${token}` } }),
    ]);
    const talJson = await talRes.json();
    if (!talJson.ok) throw new Error(talJson.error);
    setTal(talJson.tal);
    setCampaigns(talJson.campaigns);
    const unlinkedJson = await unlinkedRes.json().catch(() => ({ ok: false }));
    if (unlinkedJson.ok) setUnlinked(unlinkedJson.unlinked ?? []);
  }

  useEffect(() => {
    if (!supabase || !id) return;

    async function load() {
      setLoading(true);
      setError(null);
      try {
        await reload();
      } catch (e: any) {
        setError(String(e?.message || e));
      } finally {
        setLoading(false);
      }
    }

    load();
  }, [supabase, id]);

  async function handleRemoveCampaign(campaignRowId: string, campaignName: string) {
    if (!confirm(`Отвязать "${campaignName}" от этого TAL?`)) return;
    setRemovingId(campaignRowId);
    try {
      const { error: err } = await supabase!
        .from("tal_campaigns")
        .delete()
        .eq("id", campaignRowId);
      if (err) throw new Error(err.message);
      await reload();
    } catch (e: any) {
      alert(String(e?.message || e));
    } finally {
      setRemovingId(null);
    }
  }

  async function handleAddCampaign(c: AvailableCampaign) {
    setAddingKey(c.source_campaign_key);
    try {
      const { error: err } = await supabase!
        .from("tal_campaigns")
        .insert({
          tal_id: id,
          channel: c.channel,
          campaign_id: c.campaign_id || null,
          campaign_name: c.name,
          source_campaign_key: c.source_campaign_key,
        });
      if (err) throw new Error(err.message);
      await reload();
      // If no more unlinked, close panel
      setShowAdd(false);
    } catch (e: any) {
      alert(String(e?.message || e));
    } finally {
      setAddingKey(null);
    }
  }




  async function handleDelete() {
    if (!confirm(`Удалить TAL "${tal?.name}"? Это действие нельзя отменить.`)) return;

    try {
      const { data: { session } } = await supabase!.auth.getSession();
      const token = session?.access_token ?? "";

      const res = await fetch(`/api/tals/${id}`, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${token}` },
      });
      const json = await res.json();
      if (!json.ok) throw new Error(json.error);

      router.push("/tals");
    } catch (e: any) {
      alert(String(e?.message || e));
    }
  }

  if (loading) return (
    <div className="page">
      <AppTopbar title="TAL" />
      <div className="content"><p className="muted2">Loading...</p></div>
    </div>
  );

  if (error || !tal) return (
    <div className="page">
      <AppTopbar title="TAL" />
      <div className="content">
        <p style={{ color: "#ef4444" }}>{error || "TAL not found"}</p>
        <Link href="/tals" className="btn">← Back</Link>
      </div>
    </div>
  );

  return (
    <div className="page">
      <AppTopbar title={tal.name} subtitle={tal.criteria ?? undefined} />

      <div className="content">
        <div className="pageHeader">
          <div>
            <Link href="/tals" className="muted2" style={{ fontSize: 13, textDecoration: "none" }}>
              ← TAL
            </Link>
            <h1 className="pageTitle" style={{ fontSize: 24, marginTop: 8 }}>{tal.name}</h1>
            {tal.criteria && <p className="muted2" style={{ margin: 0 }}>{tal.criteria}</p>}
            {tal.description && <p style={{ marginTop: 8, fontSize: 14, color: "#6b7280" }}>{tal.description}</p>}
            <div className="badgeRow" style={{ marginTop: 12 }}>
              <span className="statusBadge statusBadgeSuccess">
                {tal.total_meetings} booked meetings
              </span>
              {tal.total_held_meetings > 0 && (
                <span className="statusBadge statusBadgeInfo">
                  {tal.total_held_meetings} held meetings
                </span>
              )}
            </div>
          </div>
          <button className="btn btnDanger" onClick={handleDelete}>
            Удалить
          </button>
        </div>

        {(tal.email_sent + tal.email_replies + tal.email_meetings + tal.email_held_meetings > 0) && (
          <div>
            <div className="sectionLabel" style={{ marginBottom: 12 }}>
              Email / Smartlead
            </div>
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
              <StatBox label="Sent" value={tal.email_sent.toLocaleString()} />
              <StatBox label="Replies" value={tal.email_replies.toLocaleString()} />
              <StatBox label="Booked" value={tal.email_meetings} />
              <StatBox label="Held" value={tal.email_held_meetings} />
              <StatBox label="Reply rate" value={pct(tal.email_reply_rate)} />
              <StatBox label="Reply → Booked" value={calcRate(tal.email_meetings, tal.email_replies)} />
              <StatBox label="Booked → Held" value={calcRate(tal.email_held_meetings, tal.email_meetings)} />
            </div>
          </div>
        )}

        {(tal.li_invited + tal.li_accepted + tal.li_replies + tal.li_meetings + tal.li_held_meetings > 0) && (
          <div>
            <div className="sectionLabel" style={{ marginBottom: 12 }}>
              LinkedIn / Expandi
            </div>
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
              <StatBox label="Invited" value={tal.li_invited.toLocaleString()} />
              <StatBox label="Accepted" value={tal.li_accepted.toLocaleString()} />
              <StatBox label="Replies" value={tal.li_replies.toLocaleString()} />
              <StatBox label="Booked" value={tal.li_meetings} />
              <StatBox label="Held" value={tal.li_held_meetings} />
              <StatBox label="Accept rate" value={pct(tal.li_accept_rate)} />
              <StatBox label="Reply rate" value={calcRate(tal.li_replies, tal.li_accepted)} />
              <StatBox label="Reply → Booked" value={calcRate(tal.li_meetings, tal.li_replies)} />
              <StatBox label="Booked → Held" value={calcRate(tal.li_held_meetings, tal.li_meetings)} />
            </div>
          </div>
        )}

        {(tal.app_invitations + tal.app_touches + tal.app_replies + tal.app_meetings + tal.app_held_meetings > 0) && (
          <div>
            <div className="sectionLabel" style={{ marginBottom: 12 }}>
              App
            </div>
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
              <StatBox label="Invitations" value={tal.app_invitations.toLocaleString()} />
              <StatBox label="Touches" value={tal.app_touches.toLocaleString()} />
              <StatBox label="Replies" value={tal.app_replies.toLocaleString()} />
              <StatBox label="Booked" value={tal.app_meetings} />
              <StatBox label="Held" value={tal.app_held_meetings} />
              <StatBox label="Reply rate" value={pct(tal.app_reply_rate)} />
              <StatBox label="Reply → Booked" value={calcRate(tal.app_meetings, tal.app_replies)} />
              <StatBox label="Booked → Held" value={calcRate(tal.app_held_meetings, tal.app_meetings)} />
            </div>
          </div>
        )}

        {(tal.tg_touches + tal.tg_replies + tal.tg_meetings + tal.tg_held_meetings > 0) && (
          <div>
            <div className="sectionLabel" style={{ marginBottom: 12 }}>
              Telegram
            </div>
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
              <StatBox label="Touches" value={tal.tg_touches.toLocaleString()} />
              <StatBox label="Replies" value={tal.tg_replies.toLocaleString()} />
              <StatBox label="Booked" value={tal.tg_meetings} />
              <StatBox label="Held" value={tal.tg_held_meetings} />
              <StatBox label="Reply rate" value={pct(tal.tg_reply_rate)} />
              <StatBox label="Reply → Booked" value={calcRate(tal.tg_meetings, tal.tg_replies)} />
              <StatBox label="Booked → Held" value={calcRate(tal.tg_held_meetings, tal.tg_meetings)} />
            </div>
          </div>
        )}

        {/* Campaigns in this TAL */}
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
            <div className="sectionLabel" style={{ margin: 0 }}>
              Campaigns in this TAL ({campaigns.length})
            </div>
            {unlinked.length > 0 && (
              <button
                type="button"
                className="btn"
                style={{ fontSize: 13, padding: "6px 12px" }}
                onClick={() => setShowAdd((v) => !v)}
              >
                {showAdd ? "Close" : "+ Add campaign"}
              </button>
            )}
          </div>

          {campaigns.length === 0 && (
            <p className="muted2" style={{ fontSize: 13 }}>No campaigns linked yet.</p>
          )}

          {campaigns.length > 0 && (
            <div className="surfaceList">
              {campaigns
                .sort((a, b) => channelLabel(a.channel).localeCompare(channelLabel(b.channel)) || a.campaign_name.localeCompare(b.campaign_name))
                .map((campaign) => (
                  <div key={campaign.id} className="matchCampaignRow" style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <span className="matchChannelBadge">{channelLabel(campaign.channel)}</span>
                    <div className="matchCampaignName" style={{ flex: 1 }}>{campaign.campaign_name}</div>
                    <button
                      type="button"
                      disabled={removingId === campaign.id}
                      onClick={() => handleRemoveCampaign(campaign.id, campaign.campaign_name)}
                      style={{
                        background: "none",
                        border: "none",
                        cursor: removingId === campaign.id ? "wait" : "pointer",
                        color: "#a8a29e",
                        fontSize: 13,
                        padding: "4px 8px",
                        borderRadius: 4,
                      }}
                      onMouseEnter={(e) => { (e.target as HTMLElement).style.color = "#ef4444"; }}
                      onMouseLeave={(e) => { (e.target as HTMLElement).style.color = "#a8a29e"; }}
                    >
                      {removingId === campaign.id ? "..." : "Remove"}
                    </button>
                  </div>
                ))}
            </div>
          )}
        </div>

        {/* Add unlinked campaigns */}
        {showAdd && unlinked.length > 0 && (
          <div className="card" style={{ marginTop: 8 }}>
            <div className="cardBody" style={{ padding: "16px 20px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                <div style={{ fontSize: 14, fontWeight: 600 }}>
                  Available campaigns ({unlinked.length})
                </div>
                <input
                  className="input"
                  value={addFilter}
                  onChange={(e) => setAddFilter(e.target.value)}
                  placeholder="Search..."
                  style={{ width: 200, fontSize: 13 }}
                />
              </div>
              <p className="muted2" style={{ fontSize: 12, marginTop: 0, marginBottom: 12 }}>
                Campaigns not linked to any TAL. Click + to add to this TAL.
              </p>
              <div style={{ maxHeight: 320, overflowY: "auto", display: "flex", flexDirection: "column", gap: 4 }}>
                {unlinked
                  .filter((c) => !addFilter.trim() || c.name.toLowerCase().includes(addFilter.toLowerCase()))
                  .sort((a, b) => a.channel.localeCompare(b.channel) || a.name.localeCompare(b.name))
                  .map((c) => (
                    <div
                      key={c.source_campaign_key}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 8,
                        padding: "6px 10px",
                        borderRadius: 6,
                        border: "1px solid #e7e5e4",
                        fontSize: 13,
                      }}
                    >
                      <span className="matchChannelBadge">{channelLabel(c.channel as TalCampaign["channel"])}</span>
                      <span style={{ flex: 1 }}>{c.name}</span>
                      <button
                        type="button"
                        disabled={addingKey === c.source_campaign_key}
                        onClick={() => handleAddCampaign(c)}
                        className="btn btnPrimary"
                        style={{ fontSize: 12, padding: "3px 10px", minWidth: 0 }}
                      >
                        {addingKey === c.source_campaign_key ? "..." : "+"}
                      </button>
                    </div>
                  ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
