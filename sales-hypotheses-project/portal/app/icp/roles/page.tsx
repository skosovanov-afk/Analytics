"use client";

import { useEffect, useMemo, useState } from "react";
import { AppTopbar } from "../../components/AppTopbar";
import { getSupabase } from "../../lib/supabase";

type RoleRow = {
  id: string;
  name: string;
  decision_role?: string | null; // legacy; may be absent in standalone/prod schema
  decision_roles?: string[] | null; // preferred (multi-select)
  seniority: string | null;
  titles: string[] | null;
  notes: string | null;
  updated_at: string;
};

const DECISION_ROLE_OPTIONS = ["DecisionMaker", "Influencer", "User"] as const;

export default function IcpRolesPage() {
  const supabase = useMemo(() => getSupabase(), []);

  const [status, setStatus] = useState("");
  const [rows, setRows] = useState<RoleRow[]>([]);
  const [newName, setNewName] = useState("");
  const [newDecisionRoles, setNewDecisionRoles] = useState<string[]>([]);
  const [newSeniority, setNewSeniority] = useState("");
  const [newTitles, setNewTitles] = useState("");
  const [newNotes, setNewNotes] = useState("");
  const [draftById, setDraftById] = useState<Record<string, Partial<RoleRow> & { titles_text?: string }>>({});

  function normalizeDecisionRoles(r: RoleRow): string[] {
    const fromNew = Array.isArray(r.decision_roles) ? r.decision_roles.filter(Boolean) : [];
    if (fromNew.length) return fromNew;
    const legacy = String(r.decision_role ?? "").trim();
    return legacy ? [legacy] : [];
  }

  function toggleInArray(xs: string[], v: string): string[] {
    const set = new Set(xs);
    if (set.has(v)) set.delete(v);
    else set.add(v);
    return Array.from(set);
  }

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    try {
      const sess = await supabase.auth.getSession();
      if (!sess.data.session) return setStatus("Not signed in. Go back to /.");
      const res = await supabase.from("sales_icp_roles").select("*").order("updated_at", { ascending: false }).limit(200);
      if (res.error) return setStatus(`roles error: ${res.error.message}`);
      setRows((res.data ?? []) as any);
      setStatus("");
    } catch (err) {
      setStatus(`Error: ${err instanceof Error ? err.message : "Failed to load"}`);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase]);

  async function add() {
    if (!supabase) return;
    if (!newName.trim()) return setStatus("Role name is required");
    setStatus("Saving...");
    const roles = newDecisionRoles.filter((x) => DECISION_ROLE_OPTIONS.includes(x as any));
    const payload: any = {
      name: newName.trim(),
      decision_roles: roles,
      seniority: newSeniority.trim() || null,
      titles: newTitles.split(",").map((x) => x.trim()).filter(Boolean),
      notes: newNotes.trim() || null
    };
    const res = await supabase.from("sales_icp_roles").insert(payload);
    if (res.error) return setStatus(`insert error: ${res.error.message}`);
    setNewName("");
    setNewDecisionRoles([]);
    setNewSeniority("");
    setNewTitles("");
    setNewNotes("");
    await load();
    setStatus("Saved.");
  }

  async function remove(id: string) {
    if (!supabase) return;
    if (!confirm("Delete role? (segments referencing it will be deleted too)")) return;
    setStatus("Deleting...");
    const res = await supabase.from("sales_icp_roles").delete().eq("id", id);
    if (res.error) return setStatus(`delete error: ${res.error.message}`);
    await load();
    setStatus("Deleted.");
  }

  async function updateField(id: string, patch: Partial<RoleRow>) {
    if (!supabase) return;
    setStatus("Saving...");
    const res = await supabase.from("sales_icp_roles").update(patch).eq("id", id);
    if (res.error) return setStatus(`update error: ${res.error.message}`);
    // Update UI optimistically to avoid typing jitter (we don't reload on every edit).
    setRows((prev) => prev.map((r) => (r.id === id ? ({ ...r, ...patch } as any) : r)));
    setStatus("Saved.");
  }

  /**
   * Commit a single draft field on blur.
   * This prevents "save on every keystroke" which makes the table jumpy.
   */
  async function commitDraftField(id: string, field: "name" | "seniority" | "titles") {
    const d = draftById[id] ?? null;
    if (!d) return;
    const row = rows.find((r) => r.id === id) ?? null;
    if (!row) return;

    if (field === "name") {
      const v = String(d.name ?? "").trim();
      if (v === String(row.name ?? "").trim()) return;
      await updateField(id, { name: v });
      setDraftById((prev) => {
        const next = { ...(prev ?? {}) };
        const cur = { ...(next[id] ?? {}) };
        delete (cur as any).name;
        next[id] = cur;
        if (!Object.keys(next[id] ?? {}).length) delete next[id];
        return next;
      });
      return;
    }

    if (field === "seniority") {
      const v = String(d.seniority ?? "").trim();
      const nextVal = v ? v : null;
      if (String(nextVal ?? "") === String(row.seniority ?? "")) return;
      await updateField(id, { seniority: nextVal } as any);
      setDraftById((prev) => {
        const next = { ...(prev ?? {}) };
        const cur = { ...(next[id] ?? {}) };
        delete (cur as any).seniority;
        next[id] = cur;
        if (!Object.keys(next[id] ?? {}).length) delete next[id];
        return next;
      });
      return;
    }

    const text = String((d as any).titles_text ?? "").trim();
    const parsed = text
      ? text.split(",").map((x) => x.trim()).filter(Boolean)
      : [];
    const prevText = (row.titles ?? []).join(", ");
    if (text === prevText) return;
    await updateField(id, { titles: parsed as any } as any);
    setDraftById((prev) => {
      const next = { ...(prev ?? {}) };
      const cur = { ...(next[id] ?? {}) };
      delete (cur as any).titles_text;
      next[id] = cur;
      if (!Object.keys(next[id] ?? {}).length) delete next[id];
      return next;
    });
  }

  return (
    <main>
      <AppTopbar title="ICP Roles" subtitle="Library: personas (titles, decision role). Used by VP matrix." />

      <div className="page grid">
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Create role</div>
              <div className="cardDesc">Keep role names stable to keep matrix readable.</div>
            </div>
          </div>
          <div className="cardBody">
            {status ? <div className="notice" style={{ marginBottom: 12 }}>{status}</div> : null}
            <div className="grid">
              <div style={{ gridColumn: "span 4" }}>
                <label className="muted" style={{ fontSize: 13 }}>Name *</label>
                <input className="input" value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="e.g. AppSec Lead" />
              </div>
              <div style={{ gridColumn: "span 2" }}>
                <label className="muted" style={{ fontSize: 13 }}>Decision roles</label>
                <div className="btnRow" style={{ justifyContent: "flex-start", gap: 10, flexWrap: "wrap" }}>
                  {DECISION_ROLE_OPTIONS.map((opt) => (
                    <label key={opt} className="muted2" style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                      <input
                        type="checkbox"
                        checked={newDecisionRoles.includes(opt)}
                        onChange={() => setNewDecisionRoles((prev) => toggleInArray(prev, opt))}
                      />
                      <span>{opt}</span>
                    </label>
                  ))}
                </div>
              </div>
              <div style={{ gridColumn: "span 3" }}>
                <label className="muted" style={{ fontSize: 13 }}>Seniority</label>
                <input className="input" value={newSeniority} onChange={(e) => setNewSeniority(e.target.value)} placeholder="Director/Head/VP" />
              </div>
              <div style={{ gridColumn: "span 3" }}>
                <label className="muted" style={{ fontSize: 13 }}>Titles (comma)</label>
                <input className="input" value={newTitles} onChange={(e) => setNewTitles(e.target.value)} placeholder="Title1, Title2" />
              </div>
              <div style={{ gridColumn: "span 12" }}>
                <label className="muted" style={{ fontSize: 13 }}>Notes</label>
                <textarea className="textarea" value={newNotes} onChange={(e) => setNewNotes(e.target.value)} />
              </div>
            </div>
            <div className="btnRow" style={{ marginTop: 10 }}>
              <button className="btn btnPrimary" onClick={add}>Add role</button>
            </div>
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Roles</div>
              <div className="cardDesc">Inline editable.</div>
            </div>
          </div>
          <div className="cardBody" style={{ overflowX: "auto" }}>
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Decision role</th>
                  <th>Seniority</th>
                  <th>Titles</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.id}>
                    <td>
                      <input
                        className="input"
                        value={draftById[r.id]?.name ?? r.name ?? ""}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), name: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "name")}
                      />
                      {r.notes ? <div className="muted2" style={{ fontSize: 12, marginTop: 4 }}>{r.notes}</div> : null}
                    </td>
                    <td>
                      <div className="btnRow" style={{ justifyContent: "flex-start", gap: 10, flexWrap: "wrap" }}>
                        {DECISION_ROLE_OPTIONS.map((opt) => {
                          const selected = normalizeDecisionRoles(r);
                          const checked = selected.includes(opt);
                          return (
                            <label key={`${r.id}:${opt}`} className="muted2" style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                              <input
                                type="checkbox"
                                checked={checked}
                                onChange={async () => {
                                  const next = toggleInArray(selected, opt);
                                  await updateField(r.id, { decision_roles: next as any } as any);
                                }}
                              />
                              <span>{opt}</span>
                            </label>
                          );
                        })}
                      </div>
                    </td>
                    <td>
                      <input
                        className="input"
                        value={draftById[r.id]?.seniority ?? r.seniority ?? ""}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), seniority: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "seniority")}
                      />
                    </td>
                    <td>
                      <input
                        className="input"
                        value={draftById[r.id]?.titles_text ?? (r.titles ?? []).join(", ")}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), titles_text: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "titles")}
                      />
                    </td>
                    <td style={{ width: 120 }}>
                      <button className="btn" onClick={() => remove(r.id)}>Delete</button>
                    </td>
                  </tr>
                ))}
                {!rows.length ? (
                  <tr>
                    <td colSpan={5} className="muted2">No roles yet.</td>
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

