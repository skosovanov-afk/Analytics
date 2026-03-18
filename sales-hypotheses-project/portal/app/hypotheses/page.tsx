"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { isoDate } from "../lib/utils";
import { AppTopbar } from "../components/AppTopbar";

type HypRow = {
  hypothesis_id: string;
  parent_hypothesis_id: string | null;
  version: number;
  title: string;
  status: string;
  priority: number;
  owner_user_id: string;
  vertical_name: string | null;
  opps_in_progress_count: number;
  updated_at: string;
  created_at: string;
};

export default function HypothesesListPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [status, setStatus] = useState("");
  const [sessionEmail, setSessionEmail] = useState<string | null>(null);
  const [rows, setRows] = useState<HypRow[]>([]);
  const [q, setQ] = useState("");
  const [filterStatus, setFilterStatus] = useState<string>("");

  async function deleteHypothesis(hypothesisId: string, title: string) {
    if (!supabase) return;
    if (!confirm(`Delete hypothesis "${title}"?\n\nThis will delete the hypothesis and all related rows.`)) return;
    setStatus("Deleting...");
    const res = await supabase.from("sales_hypotheses").delete().eq("id", hypothesisId);
    if (res.error) return setStatus(`Delete error: ${res.error.message}`);
    await load();
    setStatus("Deleted.");
  }

  useEffect(() => {
    if (!supabase) return;
    supabase.auth.getSession().then(({ data }) => {
      setSessionEmail(data.session?.user?.email ?? null);
    });
    const { data: sub } = supabase.auth.onAuthStateChange((_event, s) => setSessionEmail(s?.user?.email ?? null));
    return () => sub.subscription.unsubscribe();
  }, [supabase]);

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) {
      setStatus("Not signed in. Go back to / and sign in.");
      return;
    }
    const res = await supabase
      .from("sales_hypotheses")
      .select("id,parent_hypothesis_id,version,title,status,priority,owner_user_id,vertical_name,opps_in_progress_count,updated_at,created_at")
      .order("updated_at", { ascending: false })
      .limit(200);
    if (res.error) {
      setStatus(`sales_hypotheses select error: ${res.error.message}`);
      return;
    }
    setRows(
      ((res.data ?? []) as any[]).map((r) => ({
        hypothesis_id: r.id,
        parent_hypothesis_id: r.parent_hypothesis_id,
        version: r.version,
        title: r.title,
        status: r.status,
        priority: r.priority,
        owner_user_id: r.owner_user_id,
        vertical_name: r.vertical_name,
        opps_in_progress_count: r.opps_in_progress_count,
        updated_at: r.updated_at,
        created_at: r.created_at
      })) as HypRow[]
    );
    setStatus("");
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase]);

  const filtered = rows.filter((r) => {
    const needle = q.trim().toLowerCase();
    if (filterStatus && r.status !== filterStatus) return false;
    if (!needle) return true;
    return (
      (r.title || "").toLowerCase().includes(needle) ||
      String(r.vertical_name || "").toLowerCase().includes(needle) ||
      String(r.status || "").toLowerCase().includes(needle)
    );
  });

  return (
    <main>
      <AppTopbar title="Hypotheses" subtitle="All hypotheses" />

      <div className="page grid">
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">List</div>
              <div className="cardDesc">All hypotheses (Supabase source of truth).</div>
            </div>
            <div className="btnRow">
              <input className="input" style={{ width: 280 }} placeholder="Search…" value={q} onChange={(e) => setQ(e.target.value)} />
              <select className="select" style={{ width: 160 }} value={filterStatus} onChange={(e) => setFilterStatus(e.target.value)}>
                <option value="">All statuses</option>
                <option value="draft">draft</option>
                <option value="active">active</option>
                <option value="paused">paused</option>
                <option value="won">won</option>
                <option value="lost">lost</option>
              </select>
              <button className="btn" onClick={load}>Reload</button>
            </div>
          </div>
          <div className="cardBody">
            {status ? <div className="notice">{status}</div> : null}
            <table className="table" style={{ marginTop: 10 }}>
              <thead>
                <tr>
                  <th>Title</th>
                  <th>Status</th>
                  <th>Vertical</th>
                  <th>Opps</th>
                  <th>Updated</th>
                  <th style={{ width: 120 }}></th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((r) => (
                  <tr key={r.hypothesis_id}>
                    <td>
                      <a href={`/hypotheses/${r.hypothesis_id}`} style={{ textDecoration: "none" }}>
                        <b>{r.title}</b>
                      </a>
                      <div className="muted2" style={{ fontSize: 12, marginTop: 2 }}>
                        v{r.version}{r.parent_hypothesis_id ? "" : " (root)"} · priority {r.priority}
                      </div>
                    </td>
                    <td><span className="tag">{r.status}</span></td>
                    <td>{r.vertical_name || <span className="muted2">—</span>}</td>
                    <td className="mono">{Number(r.opps_in_progress_count ?? 0)}</td>
                    <td className="mono">{isoDate(r.updated_at || r.created_at)}</td>
                    <td>
                      <div className="btnRow" style={{ justifyContent: "flex-end" }}>
                        <button
                          className="btn"
                          style={{ borderColor: "rgba(255,80,80,0.6)", color: "rgba(255,160,160,0.95)" }}
                          onClick={() => deleteHypothesis(r.hypothesis_id, r.title)}
                        >
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
                {!filtered.length ? (
                  <tr>
                    <td colSpan={6} className="muted2">No hypotheses yet.</td>
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


