"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { isoDate } from "../../lib/utils";
import { AppTopbar } from "../../components/AppTopbar";

type CallRow = {
  id: string;
  title: string | null;
  occurred_at: string | null;
  owner_email: string | null;
  transcript_url: string | null;
};

type HypRow = { hypothesis_id: string; title: string; status: string; vertical_name: string | null };

export default function CallPage({ params }: { params: { call_id: string } }) {
  const callId = String(params?.call_id ?? "");
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [status, setStatus] = useState("");
  const [call, setCall] = useState<CallRow | null>(null);
  const [hyps, setHyps] = useState<HypRow[]>([]);
  const [linkedHypIds, setLinkedHypIds] = useState<string[]>([]);
  const [selectedHypId, setSelectedHypId] = useState<string>("");

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) return setStatus("Not signed in. Go back to /.");

    const [callRes, hypsRes, linkedRes] = await Promise.all([
      supabase.from("calls").select("id,title,occurred_at,owner_email,transcript_url").eq("id", callId).single(),
      supabase
        .from("sales_hypotheses")
        .select("id,title,status,vertical_name,updated_at")
        .order("updated_at", { ascending: false })
        .limit(200),
      supabase.from("sales_hypothesis_calls").select("hypothesis_id").eq("call_id", callId)
    ]);
    if (callRes.error) return setStatus(`call error: ${callRes.error.message}`);
    if (hypsRes.error) return setStatus(`hypotheses error: ${hypsRes.error.message}`);
    if (linkedRes.error) return setStatus(`links error: ${linkedRes.error.message}`);

    setCall(callRes.data as any);
    setHyps(
      ((hypsRes.data ?? []) as any[]).map((h) => ({
        hypothesis_id: String(h.id),
        title: String(h.title ?? ""),
        status: String(h.status ?? ""),
        vertical_name: h.vertical_name ?? null
      })) as any
    );
    const ids = (linkedRes.data ?? []).map((x: any) => String(x.hypothesis_id));
    setLinkedHypIds(ids);
    setStatus("");
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase, callId]);

  async function link() {
    if (!supabase) return;
    const hid = selectedHypId.trim();
    if (!hid) return setStatus("Select a hypothesis");
    setStatus("Linking...");
    const res = await supabase.from("sales_hypothesis_calls").upsert({ hypothesis_id: hid, call_id: callId }, { onConflict: "hypothesis_id,call_id" });
    if (res.error) return setStatus(`Link error: ${res.error.message}`);
    await load();
    setSelectedHypId("");
    setStatus("Linked.");
  }

  async function unlink(hid: string) {
    if (!supabase) return;
    setStatus("Unlinking...");
    const res = await supabase.from("sales_hypothesis_calls").delete().match({ hypothesis_id: hid, call_id: callId });
    if (res.error) return setStatus(`Unlink error: ${res.error.message}`);
    await load();
    setStatus("Unlinked.");
  }

  return (
    <main>
      <AppTopbar
        title={call?.title || "Call"}
        subtitle={`${call?.occurred_at ? isoDate(call.occurred_at) : ""}${call?.occurred_at ? " · " : ""}${callId}`}
      />

      {call?.transcript_url ? (
        <div className="page" style={{ marginTop: 12 }}>
          <div className="btnRow" style={{ justifyContent: "flex-end" }}>
            <a className="btn" href={call.transcript_url} target="_blank" rel="noreferrer">
              Transcript
            </a>
          </div>
        </div>
      ) : null}

      <div className="page grid">
        {status ? (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardBody">
              <div className="notice">{status}</div>
            </div>
          </div>
        ) : null}

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Link to hypothesis</div>
              <div className="cardDesc">Assign/unassign this call to hypotheses (two-way linkage).</div>
            </div>
          </div>
          <div className="cardBody">
            <div className="grid" style={{ marginBottom: 12 }}>
              <div style={{ gridColumn: "span 8" }}>
                <label className="muted" style={{ fontSize: 13 }}>Select hypothesis</label>
                <select className="select" value={selectedHypId} onChange={(e) => setSelectedHypId(e.target.value)}>
                  <option value="">—</option>
                  {hyps.map((h) => (
                    <option key={h.hypothesis_id} value={h.hypothesis_id}>
                      {h.title} [{h.status}]
                    </option>
                  ))}
                </select>
              </div>
              <div style={{ gridColumn: "span 4", display: "flex", alignItems: "end" }}>
                <button className="btn btnPrimary" onClick={link} style={{ width: "100%" }}>
                  Link
                </button>
              </div>
            </div>

            <div className="muted" style={{ marginBottom: 8 }}>Linked hypotheses</div>
            <table className="table">
              <thead>
                <tr>
                  <th>Hypothesis</th>
                  <th>Status</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {linkedHypIds.map((hid) => {
                  const h = hyps.find((x) => x.hypothesis_id === hid);
                  return (
                    <tr key={hid}>
                      <td>
                        <a href={`/hypotheses/${hid}`} style={{ textDecoration: "none" }}>
                          <b>{h?.title || hid}</b>
                        </a>
                        {h?.vertical_name ? <div className="muted2" style={{ fontSize: 12 }}>{h.vertical_name}</div> : null}
                      </td>
                      <td>{h?.status ? <span className="tag">{h.status}</span> : <span className="muted2">—</span>}</td>
                      <td style={{ width: 120 }}>
                        <button className="btn" onClick={() => unlink(hid)}>Unlink</button>
                      </td>
                    </tr>
                  );
                })}
                {!linkedHypIds.length ? (
                  <tr>
                    <td colSpan={3} className="muted2">Not linked to any hypothesis.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </main>
  );
}


