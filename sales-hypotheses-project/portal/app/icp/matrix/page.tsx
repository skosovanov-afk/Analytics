"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { AppTopbar } from "../../components/AppTopbar";
import { parseVp } from "../../hypotheses/_lib/hypothesisJson";

type RoleRow = { id: string; name: string | null };
type CompanyRow = { id: string; vertical_name: string | null; sub_vertical: string | null; region: string | null; size_bucket: string | null };

type HypothesisRow = { id: string; title: string | null };

type HypVpRow = {
  hypothesis_id: string;
  role_id: string;
  company_profile_id: string;
  vp_json: any;
  updated_at: string | null;
  hypothesis?: HypothesisRow | null;
};

function companyLabel(c: CompanyRow) {
  const v = c.vertical_name ?? "—";
  const sv = c.sub_vertical ? ` / ${c.sub_vertical}` : "";
  const reg = c.region ? ` · ${c.region}` : "";
  const size = c.size_bucket ? ` · ${c.size_bucket}` : "";
  return `${v}${sv}${reg}${size}`;
}

function vpCellPreview(v: any) {
  const stmt = String(v?.value_proposition ?? v?.statement ?? "").trim();
  if (!stmt) return <span className="muted2">No VP</span>;
  return (
    <div style={{ fontSize: 12, lineHeight: 1.25 }}>
      <div className="muted2" style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {stmt}
      </div>
    </div>
  );
}

export default function IcpMatrixPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [status, setStatus] = useState("");
  const [roles, setRoles] = useState<RoleRow[]>([]);
  const [companies, setCompanies] = useState<CompanyRow[]>([]);
  const [vps, setVps] = useState<HypVpRow[]>([]);
  const [activeKey, setActiveKey] = useState<string | null>(null);

  // Matrix filters (MVP)
  const [roleQ, setRoleQ] = useState("");
  const [companyQ, setCompanyQ] = useState("");

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) return setStatus("Not signed in. Go back to /.");

    const [rRes, cRes, vRes] = await Promise.all([
      supabase.from("sales_icp_roles").select("id,name").order("name", { ascending: true }).limit(500),
      supabase
        .from("sales_icp_company_profiles")
        .select("id,vertical_name,sub_vertical,region,size_bucket")
        .order("updated_at", { ascending: false })
        .limit(500),
      supabase
        .from("sales_hypothesis_vps")
        .select(
          "hypothesis_id,role_id,company_profile_id,vp_json,updated_at,hypothesis:sales_hypotheses(id,title)"
        )
        .order("updated_at", { ascending: false })
        .limit(2000)
    ]);
    if (rRes.error) return setStatus(`roles error: ${rRes.error.message}`);
    if (cRes.error) return setStatus(`companies error: ${cRes.error.message}`);
    if (vRes.error) return setStatus(`hypothesis vps error: ${vRes.error.message}`);
    setRoles((rRes.data ?? []) as any);
    setCompanies((cRes.data ?? []) as any);
    setVps((vRes.data ?? []) as any);
    setStatus("");
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase]);

  const roleFiltered = useMemo(() => {
    const q = roleQ.trim().toLowerCase();
    if (!q) return roles;
    return roles.filter((r) => String(r.name ?? "").toLowerCase().includes(q));
  }, [roles, roleQ]);

  const companyFiltered = useMemo(() => {
    const q = companyQ.trim().toLowerCase();
    if (!q) return companies;
    return companies.filter((c) => companyLabel(c).toLowerCase().includes(q));
  }, [companies, companyQ]);

  const aggByKey = useMemo(() => {
    const latest = new Map<string, { vp_json: any; updated_at: string | null; hypothesis_title: string | null; count: number }>();
    for (const row of vps) {
      const k = `${row.role_id}::${row.company_profile_id}`;
      const prev = latest.get(k);
      if (!prev) {
        latest.set(k, {
          vp_json: parseVp(row.vp_json ?? {}),
          updated_at: row.updated_at ?? null,
          hypothesis_title: row.hypothesis?.title ?? null,
          count: 1
        });
      } else {
        // vps are ordered by updated_at desc in load(); first one wins as "latest"
        latest.set(k, { ...prev, count: prev.count + 1 });
      }
    }
    return latest;
  }, [vps]);

  const activeRows = useMemo(() => {
    if (!activeKey) return [];
    const [roleId, companyId] = activeKey.split("::");
    if (!roleId || !companyId) return [];
    return vps
      .filter((x) => String(x.role_id) === roleId && String(x.company_profile_id) === companyId)
      .slice(0, 20);
  }, [activeKey, vps]);

  return (
    <main>
      <AppTopbar title="VP Matrix" subtitle="Read-only aggregation. Edit VP inside a hypothesis." />

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
              <div className="cardTitle">Matrix</div>
              <div className="cardDesc">Rows = company profiles, columns = roles. Shows latest VP written in any hypothesis for that intersection.</div>
            </div>
          </div>
          <div className="cardBody">
            <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10, marginBottom: 10 }}>
              <div style={{ gridColumn: "span 6" }}>
                <label className="muted" style={{ fontSize: 13 }}>Filter roles</label>
                <input className="input" value={roleQ} onChange={(e) => setRoleQ(e.target.value)} placeholder="e.g. CISO" />
              </div>
              <div style={{ gridColumn: "span 6" }}>
                <label className="muted" style={{ fontSize: 13 }}>Filter company profiles</label>
                <input className="input" value={companyQ} onChange={(e) => setCompanyQ(e.target.value)} placeholder="e.g. Finance / Payments" />
              </div>
            </div>

            <div style={{ overflow: "auto", borderRadius: 12, border: "1px solid rgba(255,255,255,0.08)" }}>
              <table className="table" style={{ minWidth: 920 }}>
                <thead>
                  <tr>
                    <th
                      style={{
                        position: "sticky",
                        left: 0,
                        zIndex: 3,
                        background: "rgba(10,10,14,0.95)",
                        minWidth: 280
                      }}
                    >
                      Company profile
                    </th>
                    {roleFiltered.map((r) => (
                      <th key={r.id} style={{ minWidth: 220 }}>
                        {r.name ?? "—"}
                      </th>
                    ))}
                    {!roleFiltered.length ? <th className="muted2">No roles match filter</th> : null}
                  </tr>
                </thead>
                <tbody>
                  {companyFiltered.map((c) => (
                    <tr key={c.id}>
                      <td
                        style={{
                          position: "sticky",
                          left: 0,
                          zIndex: 2,
                          background: "rgba(10,10,14,0.92)",
                          minWidth: 280
                        }}
                      >
                        <b>{companyLabel(c)}</b>
                        <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>
                          {c.id}
                        </div>
                      </td>
                      {roleFiltered.map((r) => {
                        const k = `${r.id}::${c.id}`;
                        const agg = aggByKey.get(k) ?? null;
                        return (
                          <td key={`${c.id}::${r.id}`}>
                            <div className="btnRow" style={{ justifyContent: "space-between", gap: 10, alignItems: "flex-start" }}>
                              <div style={{ minWidth: 0 }}>
                                {agg ? (
                                  <>
                                    <span className="tag" style={{ marginRight: 8 }}>VP</span>
                                    <div style={{ marginTop: 6 }}>{vpCellPreview(agg.vp_json ?? {})}</div>
                                    <div className="muted2" style={{ fontSize: 12, marginTop: 8 }}>
                                      {agg.hypothesis_title ? `Latest: ${agg.hypothesis_title}` : "Latest: —"}
                                      {agg.count > 1 ? ` · from ${agg.count} hypotheses` : ""}
                                    </div>
                                  </>
                                ) : (
                                  <span className="muted2">—</span>
                                )}
                              </div>
                              <button className="btn" onClick={() => setActiveKey(k)}>
                                Details
                              </button>
                            </div>
                          </td>
                        );
                      })}
                      {!roleFiltered.length ? <td className="muted2">—</td> : null}
                    </tr>
                  ))}
                  {!companyFiltered.length ? (
                    <tr>
                      <td className="muted2">No company profiles match filter</td>
                      <td colSpan={Math.max(1, roleFiltered.length)} className="muted2"></td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        </div>
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Cell details</div>
              <div className="cardDesc">
                {activeKey ? (
                  <>
                    Intersection: <span className="mono">{activeKey}</span>
                  </>
                ) : (
                  "Pick any cell → Details"
                )}
              </div>
            </div>
            <div className="btnRow">
              <button className="btn" onClick={() => setActiveKey(null)} disabled={!activeKey}>Clear</button>
            </div>
          </div>
          <div className="cardBody">
            {!activeKey ? (
              <div className="muted2">No cell selected.</div>
            ) : !activeRows.length ? (
              <div className="muted2">No hypothesis VP found for this intersection yet.</div>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Hypothesis</th>
                    <th style={{ width: 220 }}>Updated</th>
                    <th>VP preview</th>
                  </tr>
                </thead>
                <tbody>
                  {activeRows.map((x, idx) => (
                    <tr key={`${x.hypothesis_id}:${idx}`}>
                      <td>
                        <b>{x.hypothesis?.title ?? x.hypothesis_id}</b>
                        <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{x.hypothesis_id}</div>
                      </td>
                      <td className="muted2 mono">{x.updated_at ?? "—"}</td>
                      <td>{vpCellPreview(parseVp(x.vp_json ?? {}))}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </main>
  );
}


