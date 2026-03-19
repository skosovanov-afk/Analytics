"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { CHANNEL_OPTIONS, parseCjm, type CjmJson } from "../_lib/hypothesisJson";
import { AppTopbar } from "../../components/AppTopbar";

export default function NewHypothesisPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [status, setStatus] = useState("");
  const [sessionUserId, setSessionUserId] = useState<string | null>(null);
  const [sessionEmail, setSessionEmail] = useState<string | null>(null);

  // VP per hypothesis (Roles + Company profiles + VP is filled later in the hypothesis page)
  const [allRoles, setAllRoles] = useState<any[]>([]);
  const [allCompanies, setAllCompanies] = useState<any[]>([]);
  const [selectedRoleIds, setSelectedRoleIds] = useState<string[]>([]);
  const [selectedCompanyIds, setSelectedCompanyIds] = useState<string[]>([]);
  const [roleToAdd, setRoleToAdd] = useState<string>("");
  const [companyToAdd, setCompanyToAdd] = useState<string>("");

  // Channels library
  const [channelOptions, setChannelOptions] = useState<Array<{ slug: string; name: string }>>(() =>
    ([...CHANNEL_OPTIONS] as unknown as string[]).map((slug) => ({ slug, name: slug }))
  );
  const [channelToAdd, setChannelToAdd] = useState<string>("");

  // Metrics library
  const [allMetrics, setAllMetrics] = useState<any[]>([]);
  const [selectedMetricIds, setSelectedMetricIds] = useState<string[]>([]);

  // Required / core fields
  const [title, setTitle] = useState("");
  const [hypStatus, setHypStatus] = useState("draft");
  const [priority, setPriority] = useState<number>(0);
  const [timeboxDays, setTimeboxDays] = useState<number>(28);
  const [winCriteria, setWinCriteria] = useState("");
  const [killCriteria, setKillCriteria] = useState("");

  // Counts + vertical
  const [verticalName, setVerticalName] = useState("");
  const [oppsInProgress, setOppsInProgress] = useState<number>(0);
  const [talBaseline, setTalBaseline] = useState<number | "">("");
  const [contactsBaseline, setContactsBaseline] = useState<number | "">("");

  // Messaging
  const [oneSentencePitch, setOneSentencePitch] = useState("");
  const [productDescription, setProductDescription] = useState("");
  const [companyProfileText, setCompanyProfileText] = useState("");
  const [pricingModel, setPricingModel] = useState("");

  // CJM (we only use channels + notes in v1 UI)
  const [cjm, setCjm] = useState<CjmJson>(() => parseCjm({ channels: [] }));

  // Creation flow: Wizard (questionnaire) vs manual form
  const [createMode, setCreateMode] = useState<"wizard" | "form">("wizard");
  const [wizardStep, setWizardStep] = useState<number>(0);
  // NOTE: auto-sync is intentionally disabled for large TALs (can be thousands of companies).

  function autoGrowTextarea(el: HTMLTextAreaElement) {
    // Reset then set to scroll height; cap to keep layout stable.
    el.style.height = "auto";
    el.style.height = `${el.scrollHeight}px`;
  }

  function canGoNext(step: number): string | null {
    if (step === 0) {
      if (!title.trim()) return "Title is required";
      return null;
    }
    if (step === 1) {
      if (!winCriteria.trim()) return "Win criteria is required";
      if (!killCriteria.trim()) return "Kill criteria is required";
      if (!Number.isFinite(timeboxDays) || timeboxDays <= 0) return "Timebox must be > 0";
      return null;
    }
    return null;
  }

  function nextStep() {
    const err = canGoNext(wizardStep);
    if (err) {
      setStatus(`Wizard: ${err}`);
      return;
    }
    setStatus("");
    setWizardStep((s) => Math.min(5, s + 1));
  }

  function prevStep() {
    setStatus("");
    setWizardStep((s) => Math.max(0, s - 1));
  }

  const channelNameBySlug = useMemo(() => {
    const m = new Map<string, string>();
    for (const c of channelOptions) m.set(c.slug, c.name || c.slug);
    return m;
  }, [channelOptions]);

  useEffect(() => {
    if (!supabase) return;
    supabase.auth.getSession().then(({ data }) => {
      setSessionUserId(data.session?.user?.id ?? null);
      setSessionEmail(data.session?.user?.email ?? null);
    });
  }, [supabase]);

  useEffect(() => {
    if (!supabase) return;
    supabase.auth.getSession().then(async ({ data }) => {
      if (!data.session) return;
      const [rRes, cRes] = await Promise.all([
        supabase.from("sales_icp_roles").select("id,name").order("name", { ascending: true }).limit(500),
        supabase
          .from("sales_icp_company_profiles")
          .select("id,vertical_name,sub_vertical,region,size_bucket")
          .order("updated_at", { ascending: false })
          .limit(500)
      ]);
      if (rRes.error) return setStatus(`roles error: ${rRes.error.message}`);
      if (cRes.error) return setStatus(`companies error: ${cRes.error.message}`);
      setAllRoles((rRes.data ?? []) as any[]);
      setAllCompanies((cRes.data ?? []) as any[]);

      const chRes = await supabase
        .from("sales_channels")
        .select("slug,is_active,sort_order,name")
        .eq("is_active", true)
        .order("sort_order", { ascending: true })
        .order("name", { ascending: true })
        .limit(200);
      if (!chRes.error) {
        const opts = (chRes.data ?? [])
          .map((x: any) => ({ slug: String(x.slug ?? "").trim(), name: String(x.name ?? "").trim() }))
          .filter((x: any) => x.slug);
        if (opts.length) setChannelOptions(opts.map((o: any) => ({ slug: o.slug, name: o.name || o.slug })));
      }

      const mRes = await supabase
        .from("sales_metrics")
        .select("id,slug,name,input_type,unit,sort_order,is_active")
        .eq("is_active", true)
        .order("sort_order", { ascending: true })
        .order("name", { ascending: true })
        .limit(200);
      if (!mRes.error) setAllMetrics((mRes.data ?? []) as any[]);
    });
  }, [supabase]);

  function roleLabel(r: any) {
    return String(r?.name ?? "—");
  }

  function companyLabel(c: any) {
    const v = c?.vertical_name ?? "—";
    const sv = c?.sub_vertical ? ` / ${c.sub_vertical}` : "";
    const reg = c?.region ? ` · ${c.region}` : "";
    const size = c?.size_bucket ? ` · ${c.size_bucket}` : "";
    return `${v}${sv}${reg}${size}`;
  }

  function normalizeUrl(v: string) {
    const t = String(v ?? "").trim();
    if (!t) return null;
    if (t === "-") return null;
    return t;
  }

  function normalizeEmail(v: string) {
    const t = String(v ?? "").trim().toLowerCase();
    return t || null;
  }

  async function readJsonResponse(res: Response, label: string) {
    const contentType = String(res.headers.get("content-type") ?? "");
    const txt = await res.text();
    try {
      return JSON.parse(txt || "null");
    } catch {
      const snippet = String(txt || "")
        .slice(0, 220)
        .replace(/\s+/g, " ")
        .trim();
      throw new Error(`${label} returned non-JSON (status ${res.status}). content-type=${contentType || "?"}. body="${snippet}"`);
    }
  }

  function validate(): string | null {
    if (!title.trim()) return "title is required";
    if (!winCriteria.trim()) return "win criteria is required";
    if (!killCriteria.trim()) return "kill criteria is required";
    if (!Number.isFinite(timeboxDays) || timeboxDays <= 0) return "timeboxDays must be > 0";
    return null;
  }

  async function create() {
    if (!supabase) return;
    const sess = await supabase.auth.getSession();
    const s = sess.data.session;
    if (!s) {
      setStatus("Not signed in. Go back to / and sign in.");
      return;
    }
    const err = validate();
    if (err) return setStatus(`Validation: ${err}`);

    setStatus("Creating...");
    const payload: any = {
      title: title.trim(),
      status: hypStatus,
      priority: Number(priority) || 0,
      owner_user_id: s.user.id,
      owner_email: s.user.email ?? null,
      vertical_name: verticalName.trim() || null,
      pricing_model: pricingModel.trim() || null,
      opps_in_progress_count: Number(oppsInProgress) || 0,
      timebox_days: Number(timeboxDays) || 28,
      win_criteria: winCriteria.trim(),
      kill_criteria: killCriteria.trim(),
      tal_companies_count_baseline: talBaseline === "" ? null : Number(talBaseline),
      contacts_count_baseline: contactsBaseline === "" ? null : Number(contactsBaseline),
      one_sentence_pitch: oneSentencePitch.trim() || null,
      product_description: productDescription.trim() || null,
      company_profile_text: companyProfileText.trim() || null,
      cjm_json: cjm
    };

    const res = await supabase.from("sales_hypotheses").insert(payload).select("id").single();
    if (res.error) return setStatus(`Insert error: ${res.error.message}`);
    const id = String(res.data?.id ?? "");

    if (selectedRoleIds.length) {
      const rows = selectedRoleIds.map((rid) => ({ hypothesis_id: id, role_id: rid }));
      const rIns = await supabase.from("sales_hypothesis_roles").insert(rows);
      if (rIns.error) return setStatus(`Roles insert error: ${rIns.error.message}`);
    }

    if (selectedCompanyIds.length) {
      const rows = selectedCompanyIds.map((cid) => ({ hypothesis_id: id, company_profile_id: cid }));
      const cIns = await supabase.from("sales_hypothesis_company_profiles").insert(rows);
      if (cIns.error) return setStatus(`Companies insert error: ${cIns.error.message}`);
    }

    if (selectedMetricIds.length) {
      const mRows = selectedMetricIds.map((mid) => ({ hypothesis_id: id, metric_id: mid }));
      const mRes = await supabase.from("sales_hypothesis_metrics").insert(mRows);
      if (mRes.error) return setStatus(`Metrics insert error: ${mRes.error.message}`);
    }
    setStatus("Created. Redirecting...");
    window.location.href = `/hypotheses/${id}`;
  }


  return (
    <main>
      <AppTopbar title="New hypothesis" subtitle="Create a hypothesis" />

      <div className="page" style={{ marginTop: 12 }}>
        <div className="btnRow" style={{ justifyContent: "flex-end" }}>
          <a className="btn" href="/hypotheses">Back to hypotheses</a>
          {createMode === "form" ? <button className="btn btnPrimary" onClick={create}>Create</button> : null}
        </div>
      </div>

      <div className="page grid">
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Create hypothesis</div>
              <div className="cardDesc">Two modes: guided questionnaire (recommended) or manual form.</div>
            </div>
            <div className="btnRow">
              <button
                className={`btn ${createMode === "wizard" ? "btnPrimary" : ""}`}
                onClick={() => {
                  setCreateMode("wizard");
                  setStatus("");
                }}
              >
                Questionnaire
              </button>
              <button
                className={`btn ${createMode === "form" ? "btnPrimary" : ""}`}
                onClick={() => {
                  setCreateMode("form");
                  setStatus("");
                }}
              >
                Manual form
              </button>
            </div>
          </div>
          <div className="cardBody">
            {status ? <div className="notice" style={{ marginBottom: 12 }}>{status}</div> : null}

            {createMode === "wizard" ? (
              <div className="grid">
                <div style={{ gridColumn: "span 12" }}>
                  <div className="muted2" style={{ fontSize: 12, marginBottom: 10 }}>
                    Step <span className="mono">{wizardStep + 1}/6</span>
                  </div>
                </div>

                {wizardStep === 0 ? (
                  <div style={{ gridColumn: "span 12" }} className="card">
                    <div className="cardBody">
                      <div className="cardTitle" style={{ fontSize: 14, marginBottom: 6 }}>1) What are we testing?</div>
                      <div className="grid formGridTight">
                        <div style={{ gridColumn: "span 8" }}>
                      <label className="muted" style={{ fontSize: 13 }}>Hypothesis title *</label>
                      <input
                        className="input"
                        value={title}
                        onChange={(e) => setTitle(e.target.value)}
                        placeholder="Role + company profile + what we believe (short)"
                      />
                        </div>
                        <div style={{ gridColumn: "span 2" }}>
                          <label className="muted" style={{ fontSize: 13 }}>Status</label>
                          <select className="select" value={hypStatus} onChange={(e) => setHypStatus(e.target.value)}>
                            <option value="draft">draft</option>
                            <option value="active">active</option>
                            <option value="paused">paused</option>
                            <option value="won">won</option>
                            <option value="lost">lost</option>
                          </select>
                        </div>
                        <div style={{ gridColumn: "span 2" }}>
                          <label className="muted" style={{ fontSize: 13 }}>Priority</label>
                          <input className="input" type="number" value={priority} onChange={(e) => setPriority(Number(e.target.value || 0))} />
                        </div>

                        <div style={{ gridColumn: "span 12" }}>
                        <label className="muted" style={{ fontSize: 13 }}>Pricing model (what we test)</label>
                          <textarea
                            className="textarea textareaAutoGrow"
                          value={pricingModel}
                          onChange={(e) => setPricingModel(e.target.value)}
                            onInput={(e) => autoGrowTextarea(e.currentTarget)}
                          placeholder="e.g. per-app subscription / usage-based / per-scan"
                            style={{ minHeight: 44 }}
                        />
                      <div className="muted2" style={{ fontSize: 12, marginTop: 8 }}>
                        Tip: keep it scannable. You’ll fill VP later in the hypothesis matrix.
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                ) : null}

                {wizardStep === 1 ? (
                  <div style={{ gridColumn: "span 12" }} className="card">
                    <div className="cardBody">
                      <div className="cardTitle" style={{ fontSize: 14, marginBottom: 6 }}>2) Timebox + win/kill (mandatory)</div>
                      <div className="grid">
                        <div style={{ gridColumn: "span 3" }}>
                          <label className="muted" style={{ fontSize: 13 }}>Timebox (days) *</label>
                          <input className="input" type="number" value={timeboxDays} onChange={(e) => setTimeboxDays(Number(e.target.value || 0))} />
                        </div>
                        <div style={{ gridColumn: "span 5" }}>
                          <label className="muted" style={{ fontSize: 13 }}>Win criteria *</label>
                          <textarea
                            className="textarea textareaAutoGrow"
                            value={winCriteria}
                            onChange={(e) => setWinCriteria(e.target.value)}
                            onInput={(e) => autoGrowTextarea(e.currentTarget)}
                            placeholder="e.g. ≥ 5 opp created"
                            style={{ minHeight: 44 }}
                          />
                        </div>
                        <div style={{ gridColumn: "span 4" }}>
                          <label className="muted" style={{ fontSize: 13 }}>Kill criteria *</label>
                          <textarea
                            className="textarea textareaAutoGrow"
                            value={killCriteria}
                            onChange={(e) => setKillCriteria(e.target.value)}
                            onInput={(e) => autoGrowTextarea(e.currentTarget)}
                            placeholder="e.g. < 2 opp created"
                            style={{ minHeight: 44 }}
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                ) : null}


                {wizardStep === 3 ? (
                  <div style={{ gridColumn: "span 12" }} className="card">
                    <div className="cardBody">
                      <div className="cardTitle" style={{ fontSize: 14, marginBottom: 6 }}>4) Who are we targeting?</div>
                      <div className="muted2" style={{ fontSize: 12, marginBottom: 10 }}>
                        Select Roles + Company Profiles (VP is filled later per intersection).
                      </div>
                      <div className="btnRow" style={{ justifyContent: "flex-start", marginBottom: 10 }}>
                        <a className="btn" href="/icp" target="_blank" rel="noreferrer">Open Library</a>
                      </div>

                      <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10, alignItems: "end" }}>
                        <div style={{ gridColumn: "span 5" }}>
                          <label className="muted2" style={{ fontSize: 12 }}>Add role</label>
                          <select className="select" value={roleToAdd} onChange={(e) => setRoleToAdd(e.target.value)}>
                            <option value="">—</option>
                            {allRoles
                              .filter((r: any) => !selectedRoleIds.includes(String(r.id)))
                              .map((r: any) => (
                                <option key={String(r.id)} value={String(r.id)}>
                                  {roleLabel(r)}
                                </option>
                              ))}
                          </select>
                        </div>
                        <div style={{ gridColumn: "span 1" }}>
                          <button
                            className="btn btnPrimary"
                            onClick={() => {
                              if (!roleToAdd) return;
                              const next = new Set(selectedRoleIds);
                              next.add(roleToAdd);
                              setSelectedRoleIds(Array.from(next));
                              setRoleToAdd("");
                            }}
                          >
                            Add
                          </button>
                      </div>

                        <div style={{ gridColumn: "span 5" }}>
                          <label className="muted2" style={{ fontSize: 12 }}>Add company profile</label>
                          <select className="select" value={companyToAdd} onChange={(e) => setCompanyToAdd(e.target.value)}>
                            <option value="">—</option>
                            {allCompanies
                              .filter((c: any) => !selectedCompanyIds.includes(String(c.id)))
                              .map((c: any) => (
                                <option key={String(c.id)} value={String(c.id)}>
                                  {companyLabel(c)}
                                </option>
                              ))}
                          </select>
                        </div>
                        <div style={{ gridColumn: "span 1" }}>
                          <button
                            className="btn btnPrimary"
                            onClick={() => {
                              if (!companyToAdd) return;
                              const next = new Set(selectedCompanyIds);
                              next.add(companyToAdd);
                              setSelectedCompanyIds(Array.from(next));
                              setCompanyToAdd("");
                            }}
                          >
                            Add
                          </button>
                        </div>
                      </div>

                      <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10, marginTop: 12 }}>
                        <div style={{ gridColumn: "span 6" }}>
                          <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Selected roles</div>
                          <table className="table">
                            <thead>
                              <tr><th>Role</th><th style={{ width: 120 }}></th></tr>
                            </thead>
                            <tbody>
                              {selectedRoleIds.map((rid) => {
                                const r = allRoles.find((x: any) => String(x.id) === String(rid));
                                return (
                                  <tr key={rid}>
                                    <td><b>{roleLabel(r)}</b><div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{rid}</div></td>
                                    <td><button className="btn" onClick={() => setSelectedRoleIds(selectedRoleIds.filter((x) => x !== rid))}>Remove</button></td>
                                  </tr>
                                );
                              })}
                              {!selectedRoleIds.length ? <tr><td colSpan={2} className="muted2">No roles selected yet.</td></tr> : null}
                            </tbody>
                          </table>
                        </div>
                        <div style={{ gridColumn: "span 6" }}>
                          <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Selected company profiles</div>
                          <table className="table">
                            <thead>
                              <tr><th>Company profile</th><th style={{ width: 120 }}></th></tr>
                            </thead>
                            <tbody>
                              {selectedCompanyIds.map((cid) => {
                                const c = allCompanies.find((x: any) => String(x.id) === String(cid));
                                return (
                                  <tr key={cid}>
                                    <td><b>{companyLabel(c)}</b><div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{cid}</div></td>
                                    <td><button className="btn" onClick={() => setSelectedCompanyIds(selectedCompanyIds.filter((x) => x !== cid))}>Remove</button></td>
                                  </tr>
                                );
                              })}
                              {!selectedCompanyIds.length ? <tr><td colSpan={2} className="muted2">No company profiles selected yet.</td></tr> : null}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    </div>
                  </div>
                ) : null}

                {wizardStep === 4 ? (
                  <div style={{ gridColumn: "span 12" }} className="card">
                    <div className="cardBody">
                      <div className="cardTitle" style={{ fontSize: 14, marginBottom: 6 }}>5) Channels + metrics</div>
                      <div className="muted2" style={{ fontSize: 12, marginBottom: 10 }}>
                        Choose channels you will run, and which metrics you want tracked on the hypothesis.
                      </div>
                      <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10, alignItems: "end" }}>
                        <div style={{ gridColumn: "span 10" }}>
                          <label className="muted2" style={{ fontSize: 12 }}>Add channel</label>
                          <select className="select" value={channelToAdd} onChange={(e) => setChannelToAdd(e.target.value)}>
                            <option value="">—</option>
                            {channelOptions
                              .filter((ch) => !(cjm.channels ?? []).includes(ch.slug))
                              .map((ch) => (
                                <option key={ch.slug} value={ch.slug}>
                                  {ch.name || ch.slug} ({ch.slug})
                                </option>
                              ))}
                          </select>
                      </div>
                        <div style={{ gridColumn: "span 2" }}>
                          <button
                            className="btn btnPrimary"
                            onClick={() => {
                              if (!channelToAdd) return;
                              const next = new Set<string>(cjm.channels ?? []);
                              next.add(channelToAdd);
                              setCjm({ ...cjm, channels: Array.from(next) });
                              setChannelToAdd("");
                            }}
                          >
                            Add
                          </button>
                        </div>
                      </div>

                      <table className="table" style={{ marginTop: 10 }}>
                        <thead><tr><th>Selected channels</th><th style={{ width: 120 }}></th></tr></thead>
                        <tbody>
                          {(cjm.channels ?? []).map((slug) => (
                            <tr key={slug}>
                              <td>
                                <b>{channelNameBySlug.get(slug) ?? slug}</b>
                                <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{slug}</div>
                              </td>
                              <td>
                                <button
                                  className="btn"
                                  onClick={() => {
                                    const next = new Set<string>(cjm.channels ?? []);
                                    next.delete(slug);
                                    setCjm({ ...cjm, channels: Array.from(next) });
                                  }}
                                >
                                  Remove
                                </button>
                              </td>
                            </tr>
                          ))}
                          {!(cjm.channels ?? []).length ? <tr><td colSpan={2} className="muted2">No channels selected yet.</td></tr> : null}
                        </tbody>
                      </table>

                      <div className="muted" style={{ fontSize: 13, marginTop: 14, marginBottom: 6 }}>Metrics</div>
                      <table className="table">
                        <thead>
                          <tr>
                            <th style={{ width: 70 }}>Use</th>
                            <th>Metric</th>
                            <th style={{ width: 160 }}>Type</th>
                            <th style={{ width: 140 }}>Unit</th>
                          </tr>
                        </thead>
                        <tbody>
                          {allMetrics.map((m: any) => {
                            const mid = String(m.id);
                            const checked = selectedMetricIds.includes(mid);
                            return (
                              <tr key={mid}>
                                <td>
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    onChange={(e) => {
                                      const next = new Set(selectedMetricIds);
                                      if (e.target.checked) next.add(mid);
                                      else next.delete(mid);
                                      setSelectedMetricIds(Array.from(next));
                                    }}
                                  />
                                </td>
                                <td>
                                  <b>{String(m.name ?? m.slug ?? "Metric")}</b>
                                  <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{String(m.slug ?? "")}</div>
                                </td>
                                <td className="mono">{String(m.input_type ?? "number")}</td>
                                <td className="mono">{String(m.unit ?? "—")}</td>
                              </tr>
                            );
                          })}
                          {!allMetrics.length ? <tr><td colSpan={4} className="muted2">No metrics in Library yet.</td></tr> : null}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : null}

                {wizardStep === 5 ? (
                  <div style={{ gridColumn: "span 12" }} className="card">
                    <div className="cardBody">
                      <div className="cardTitle" style={{ fontSize: 14, marginBottom: 6 }}>6) Review</div>
                      <table className="table">
                        <tbody>
                          <tr><td><b>Title</b></td><td>{title || "—"}</td></tr>
                          <tr><td><b>Status</b></td><td className="mono">{hypStatus}</td></tr>
                          <tr><td><b>Priority</b></td><td className="mono">{priority}</td></tr>
                          <tr><td><b>Timebox</b></td><td className="mono">{timeboxDays}d</td></tr>
                          <tr><td><b>Win</b></td><td>{winCriteria || "—"}</td></tr>
                          <tr><td><b>Kill</b></td><td>{killCriteria || "—"}</td></tr>
                          <tr><td><b>Channels</b></td><td>{(cjm.channels ?? []).length ? (cjm.channels ?? []).join(", ") : "—"}</td></tr>
                          <tr><td><b>Roles selected</b></td><td className="mono">{selectedRoleIds.length}</td></tr>
                          <tr><td><b>Companies selected</b></td><td className="mono">{selectedCompanyIds.length}</td></tr>
                          <tr><td><b>Metrics selected</b></td><td className="mono">{selectedMetricIds.length}</td></tr>
                        </tbody>
                      </table>

                      <div className="grid" style={{ marginTop: 12 }}>
                        <div style={{ gridColumn: "span 12" }}>
                          <label className="muted" style={{ fontSize: 13 }}>One sentence pitch (optional)</label>
                          <textarea
                            className="textarea textareaAutoGrow"
                            value={oneSentencePitch}
                            onChange={(e) => setOneSentencePitch(e.target.value)}
                            onInput={(e) => autoGrowTextarea(e.currentTarget)}
                            style={{ minHeight: 44 }}
                          />
                        </div>
                        <div style={{ gridColumn: "span 12" }}>
                          <label className="muted" style={{ fontSize: 13 }}>Product description (optional)</label>
                          <textarea
                            className="textarea textareaAutoGrow"
                            value={productDescription}
                            onChange={(e) => setProductDescription(e.target.value)}
                            onInput={(e) => autoGrowTextarea(e.currentTarget)}
                            style={{ minHeight: 92 }}
                          />
                        </div>
                        <div style={{ gridColumn: "span 12" }}>
                          <label className="muted" style={{ fontSize: 13 }}>Client profile (optional)</label>
                          <textarea
                            className="textarea textareaAutoGrow"
                            value={companyProfileText}
                            onChange={(e) => setCompanyProfileText(e.target.value)}
                            onInput={(e) => autoGrowTextarea(e.currentTarget)}
                            style={{ minHeight: 92 }}
                          />
                        </div>
                      </div>

                      <div className="btnRow" style={{ marginTop: 12 }}>
                        <button className="btn btnPrimary" onClick={create}>Create hypothesis</button>
                      </div>
                    </div>
                  </div>
                ) : null}

                <div style={{ gridColumn: "span 12" }}>
                  <div className="btnRow" style={{ justifyContent: "space-between" }}>
                    <button className="btn" onClick={prevStep} disabled={wizardStep === 0}>Back</button>
                    <button className="btn btnPrimary" onClick={nextStep} disabled={wizardStep >= 5}>Next</button>
                  </div>
                </div>
              </div>
            ) : (
              <div className="muted2" style={{ fontSize: 13 }}>
                Manual mode enabled. Use the form cards below and click “Create”.
              </div>
            )}
          </div>
        </div>

        {createMode === "form" ? (
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Core</div>
              <div className="cardDesc">Required fields for systematic testing (timebox + win/kill are mandatory).</div>
            </div>
          </div>
          <div className="cardBody">
            {createMode === "form" && status ? <div className="notice" style={{ marginBottom: 12 }}>{status}</div> : null}

            <div className="grid">
              <div style={{ gridColumn: "span 8" }}>
                <label className="muted" style={{ fontSize: 13 }}>Title *</label>
                <input className="input" value={title} onChange={(e) => setTitle(e.target.value)} placeholder="e.g. FinTech AppSec Lead — VP: mobile pentest automation" />
              </div>
              <div style={{ gridColumn: "span 2" }}>
                <label className="muted" style={{ fontSize: 13 }}>Status</label>
                <select className="select" value={hypStatus} onChange={(e) => setHypStatus(e.target.value)}>
                  <option value="draft">draft</option>
                  <option value="active">active</option>
                  <option value="paused">paused</option>
                  <option value="won">won</option>
                  <option value="lost">lost</option>
                </select>
              </div>
              <div style={{ gridColumn: "span 2" }}>
                <label className="muted" style={{ fontSize: 13 }}>Priority</label>
                <input className="input" type="number" value={priority} onChange={(e) => setPriority(Number(e.target.value || 0))} />
              </div>

              <div style={{ gridColumn: "span 12" }}>
                <label className="muted" style={{ fontSize: 13 }}>Pricing model (what we test)</label>
                <textarea
                  className="textarea textareaAutoGrow"
                  value={pricingModel}
                  onChange={(e) => setPricingModel(e.target.value)}
                  onInput={(e) => autoGrowTextarea(e.currentTarget)}
                  placeholder="e.g. per-app subscription / usage-based / per-scan"
                  style={{ minHeight: 44 }}
                />
              </div>

              <div style={{ gridColumn: "span 2" }}>
                <label className="muted" style={{ fontSize: 13 }}>Timebox (days) *</label>
                <input className="input" type="number" value={timeboxDays} onChange={(e) => setTimeboxDays(Number(e.target.value || 0))} />
              </div>
              <div style={{ gridColumn: "span 5" }}>
                <label className="muted" style={{ fontSize: 13 }}>Win criteria *</label>
                <textarea
                  className="textarea textareaAutoGrow"
                  value={winCriteria}
                  onChange={(e) => setWinCriteria(e.target.value)}
                  onInput={(e) => autoGrowTextarea(e.currentTarget)}
                  placeholder="e.g. ≥ 5 opp created in 4 weeks"
                  style={{ minHeight: 44 }}
                />
              </div>
              <div style={{ gridColumn: "span 5" }}>
                <label className="muted" style={{ fontSize: 13 }}>Kill criteria *</label>
                <textarea
                  className="textarea textareaAutoGrow"
                  value={killCriteria}
                  onChange={(e) => setKillCriteria(e.target.value)}
                  onInput={(e) => autoGrowTextarea(e.currentTarget)}
                  placeholder="e.g. < 2 opp created in 4 weeks"
                  style={{ minHeight: 44 }}
                />
              </div>
            </div>
          </div>
        </div>
        ) : null}


        {createMode === "form" ? (
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Messaging</div>
              <div className="cardDesc">One-liner and quick context.</div>
            </div>
          </div>
          <div className="cardBody">
            <div className="grid">
              <div style={{ gridColumn: "span 12" }}>
                <label className="muted" style={{ fontSize: 13 }}>One sentence pitch</label>
                <input className="input" value={oneSentencePitch} onChange={(e) => setOneSentencePitch(e.target.value)} />
              </div>
              <div style={{ gridColumn: "span 12" }}>
                <label className="muted" style={{ fontSize: 13 }}>Product description</label>
                <textarea className="textarea" value={productDescription} onChange={(e) => setProductDescription(e.target.value)} />
              </div>
              <div style={{ gridColumn: "span 12" }}>
                <label className="muted" style={{ fontSize: 13 }}>Client profile (TAL / segment description)</label>
                <textarea className="textarea" value={companyProfileText} onChange={(e) => setCompanyProfileText(e.target.value)} />
                <div className="muted2" style={{ fontSize: 12, marginTop: 6 }}>
                  Paste the detailed segment definition here (TAL description, company/app profile, constraints, buying context, etc.).
                </div>
              </div>
            </div>
          </div>
        </div>
        ) : null}

        {createMode === "form" ? (
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">VP scope + Channels</div>
              <div className="cardDesc">Pick Roles + Company Profiles for this hypothesis (VP is filled per intersection inside the hypothesis).</div>
            </div>
            <div className="btnRow">
              <a className="btn" href="/icp">Library</a>
              <a className="btn" href="/icp/matrix">VP matrix</a>
            </div>
          </div>
          <div className="cardBody">
            <div className="grid">
              <div style={{ gridColumn: "span 12" }}>
                <div className="muted" style={{ fontSize: 13, marginBottom: 6 }}>Roles + Company Profiles</div>
                <div className="muted2" style={{ fontSize: 12, marginBottom: 10 }}>
                  VP will be edited later on the hypothesis page as a matrix (rows=companies, cols=roles).
                </div>

                <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10, alignItems: "end" }}>
                  <div style={{ gridColumn: "span 5" }}>
                    <label className="muted2" style={{ fontSize: 12 }}>Add role</label>
                    <select className="select" value={roleToAdd} onChange={(e) => setRoleToAdd(e.target.value)}>
                      <option value="">—</option>
                      {allRoles
                        .filter((r: any) => !selectedRoleIds.includes(String(r.id)))
                        .map((r: any) => (
                          <option key={String(r.id)} value={String(r.id)}>
                            {roleLabel(r)}
                          </option>
                        ))}
                    </select>
                  </div>
                  <div style={{ gridColumn: "span 1" }}>
                    <button
                      className="btn btnPrimary"
                      onClick={() => {
                        if (!roleToAdd) return;
                        const next = new Set(selectedRoleIds);
                        next.add(roleToAdd);
                        setSelectedRoleIds(Array.from(next));
                        setRoleToAdd("");
                      }}
                    >
                      Add
                    </button>
                  </div>

                  <div style={{ gridColumn: "span 5" }}>
                    <label className="muted2" style={{ fontSize: 12 }}>Add company profile</label>
                    <select className="select" value={companyToAdd} onChange={(e) => setCompanyToAdd(e.target.value)}>
                      <option value="">—</option>
                      {allCompanies
                        .filter((c: any) => !selectedCompanyIds.includes(String(c.id)))
                        .map((c: any) => (
                          <option key={String(c.id)} value={String(c.id)}>
                            {companyLabel(c)}
                          </option>
                        ))}
                    </select>
                  </div>
                  <div style={{ gridColumn: "span 1" }}>
                    <button
                      className="btn btnPrimary"
                      onClick={() => {
                        if (!companyToAdd) return;
                        const next = new Set(selectedCompanyIds);
                        next.add(companyToAdd);
                        setSelectedCompanyIds(Array.from(next));
                        setCompanyToAdd("");
                      }}
                    >
                      Add
                    </button>
                  </div>
                </div>

                <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10, marginTop: 12 }}>
                  <div style={{ gridColumn: "span 6" }}>
                    <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Selected roles</div>
                    <table className="table">
                      <thead>
                        <tr>
                          <th>Role</th>
                          <th style={{ width: 120 }}></th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedRoleIds.map((rid) => {
                          const r = allRoles.find((x: any) => String(x.id) === String(rid));
                          return (
                            <tr key={rid}>
                              <td><b>{roleLabel(r)}</b><div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{rid}</div></td>
                              <td>
                                <button className="btn" onClick={() => setSelectedRoleIds(selectedRoleIds.filter((x) => x !== rid))}>Remove</button>
                              </td>
                            </tr>
                          );
                        })}
                        {!selectedRoleIds.length ? (
                          <tr><td colSpan={2} className="muted2">No roles selected yet.</td></tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                  <div style={{ gridColumn: "span 6" }}>
                    <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Selected company profiles</div>
                    <table className="table">
                      <thead>
                        <tr>
                          <th>Company profile</th>
                          <th style={{ width: 120 }}></th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedCompanyIds.map((cid) => {
                          const c = allCompanies.find((x: any) => String(x.id) === String(cid));
                          return (
                            <tr key={cid}>
                              <td><b>{companyLabel(c)}</b><div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{cid}</div></td>
                              <td>
                                <button className="btn" onClick={() => setSelectedCompanyIds(selectedCompanyIds.filter((x) => x !== cid))}>Remove</button>
                              </td>
                            </tr>
                          );
                        })}
                        {!selectedCompanyIds.length ? (
                          <tr><td colSpan={2} className="muted2">No company profiles selected yet.</td></tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>

              <div style={{ gridColumn: "span 12" }}>
                <div className="muted" style={{ fontSize: 13, marginBottom: 6 }}>Channels</div>
                <div className="muted2" style={{ fontSize: 12, marginBottom: 8 }}>
                  Managed in <a href="/icp/channels">Library → Channels</a>.
                </div>
                <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10, alignItems: "end" }}>
                  <div style={{ gridColumn: "span 10" }}>
                    <label className="muted2" style={{ fontSize: 12 }}>Add channel</label>
                    <select className="select" value={channelToAdd} onChange={(e) => setChannelToAdd(e.target.value)}>
                      <option value="">—</option>
                      {channelOptions
                        .filter((ch) => !(cjm.channels ?? []).includes(ch.slug))
                        .map((ch) => (
                          <option key={ch.slug} value={ch.slug}>
                            {ch.name || ch.slug} ({ch.slug})
                          </option>
                        ))}
                    </select>
                  </div>
                  <div style={{ gridColumn: "span 2" }}>
                    <button
                      className="btn btnPrimary"
                      onClick={() => {
                        if (!channelToAdd) return;
                        const next = new Set<string>(cjm.channels ?? []);
                        next.add(channelToAdd);
                        setCjm({ ...cjm, channels: Array.from(next) });
                        setChannelToAdd("");
                      }}
                    >
                      Add
                    </button>
                  </div>
                </div>

                <table className="table" style={{ marginTop: 10 }}>
                  <thead>
                    <tr>
                      <th>Selected channels</th>
                      <th style={{ width: 120 }}></th>
                    </tr>
                  </thead>
                  <tbody>
                    {(cjm.channels ?? []).map((slug) => (
                      <tr key={slug}>
                        <td>
                          <b>{channelNameBySlug.get(slug) ?? slug}</b>
                          <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{slug}</div>
                        </td>
                        <td>
                          <button
                            className="btn"
                            onClick={() => {
                              const next = new Set<string>(cjm.channels ?? []);
                              next.delete(slug);
                              setCjm({ ...cjm, channels: Array.from(next) });
                            }}
                          >
                            Remove
                          </button>
                        </td>
                      </tr>
                    ))}
                    {!(cjm.channels ?? []).length ? (
                      <tr>
                        <td colSpan={2} className="muted2">No channels selected yet.</td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>

              <div style={{ gridColumn: "span 12" }}>
                <div className="muted" style={{ fontSize: 13, marginBottom: 6 }}>Metrics</div>
                <div className="muted2" style={{ fontSize: 12, marginBottom: 10 }}>
                  Select metrics from <a href="/icp/metrics">Library → Metrics</a>. Weekly check-ins will include inputs for selected metrics.
                </div>
                <table className="table">
                  <thead>
                    <tr>
                      <th style={{ width: 70 }}>Use</th>
                      <th>Metric</th>
                      <th style={{ width: 160 }}>Type</th>
                      <th style={{ width: 140 }}>Unit</th>
                    </tr>
                  </thead>
                  <tbody>
                    {allMetrics.map((m: any) => {
                      const mid = String(m.id);
                      const checked = selectedMetricIds.includes(mid);
                      return (
                        <tr key={mid}>
                          <td>
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={(e) => {
                                const next = new Set(selectedMetricIds);
                                if (e.target.checked) next.add(mid);
                                else next.delete(mid);
                                setSelectedMetricIds(Array.from(next));
                              }}
                            />
                          </td>
                          <td>
                            <b>{String(m.name ?? m.slug ?? "Metric")}</b>
                            <div className="muted2 mono" style={{ fontSize: 12, marginTop: 2 }}>{String(m.slug ?? "")}</div>
                          </td>
                          <td className="mono">{String(m.input_type ?? "number")}</td>
                          <td className="mono">{String(m.unit ?? "—")}</td>
                        </tr>
                      );
                    })}
                    {!allMetrics.length ? (
                      <tr>
                        <td colSpan={4} className="muted2">No metrics in Library yet.</td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
        ) : null}
      </div>
    </main>
  );
}


