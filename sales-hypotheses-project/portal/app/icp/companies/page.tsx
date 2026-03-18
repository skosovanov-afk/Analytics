"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { AppTopbar } from "../../components/AppTopbar";

type CompanyRow = {
  id: string;
  vertical_name: string | null;
  sub_vertical: string | null;
  region: string | null;
  size_bucket: string | null;
  tech_stack: string[] | null;
  notes: string | null;
  updated_at: string;
};

export default function IcpCompaniesPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [status, setStatus] = useState("");
  const [rows, setRows] = useState<CompanyRow[]>([]);
  const [newVertical, setNewVertical] = useState("");
  const [newSubVertical, setNewSubVertical] = useState("");
  const [newRegion, setNewRegion] = useState("");
  const [newSize, setNewSize] = useState("");
  const [newTech, setNewTech] = useState("");
  const [newNotes, setNewNotes] = useState("");
  const [draftById, setDraftById] = useState<Record<string, Partial<CompanyRow> & { tech_text?: string }>>({});

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) return setStatus("Not signed in. Go back to /.");
    const res = await supabase.from("sales_icp_company_profiles").select("*").order("updated_at", { ascending: false }).limit(200);
    if (res.error) return setStatus(`companies error: ${res.error.message}`);
    setRows((res.data ?? []) as any);
    setStatus("");
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase]);

  async function add() {
    if (!supabase) return;
    if (!newVertical.trim()) return setStatus("vertical_name is required");
    setStatus("Saving...");
    const payload: any = {
      vertical_name: newVertical.trim(),
      sub_vertical: newSubVertical.trim() || null,
      region: newRegion.trim() || null,
      size_bucket: newSize.trim() || null,
      tech_stack: newTech.split(",").map((x) => x.trim()).filter(Boolean),
      notes: newNotes.trim() || null
    };
    const res = await supabase.from("sales_icp_company_profiles").insert(payload);
    if (res.error) return setStatus(`insert error: ${res.error.message}`);
    setNewVertical("");
    setNewSubVertical("");
    setNewRegion("");
    setNewSize("");
    setNewTech("");
    setNewNotes("");
    await load();
    setStatus("Saved.");
  }

  async function remove(id: string) {
    if (!supabase) return;
    if (!confirm("Delete company profile? (segments referencing it will be deleted too)")) return;
    setStatus("Deleting...");
    const res = await supabase.from("sales_icp_company_profiles").delete().eq("id", id);
    if (res.error) return setStatus(`delete error: ${res.error.message}`);
    await load();
    setStatus("Deleted.");
  }

  async function updateField(id: string, patch: Partial<CompanyRow>) {
    if (!supabase) return;
    setStatus("Saving...");
    const res = await supabase.from("sales_icp_company_profiles").update(patch).eq("id", id);
    if (res.error) return setStatus(`update error: ${res.error.message}`);
    // Optimistic UI update to avoid jitter while editing.
    setRows((prev) => prev.map((r) => (r.id === id ? ({ ...r, ...patch } as any) : r)));
    setStatus("Saved.");
  }

  /**
   * Commit a single draft field on blur.
   * Avoids saving on every keystroke which makes the table jump.
   */
  async function commitDraftField(
    id: string,
    field: "vertical_name" | "sub_vertical" | "region" | "size_bucket" | "tech_stack"
  ) {
    const d = draftById[id] ?? null;
    if (!d) return;
    const row = rows.find((r) => r.id === id) ?? null;
    if (!row) return;

    if (field === "tech_stack") {
      const text = String((d as any).tech_text ?? "").trim();
      const parsed = text ? text.split(",").map((x) => x.trim()).filter(Boolean) : [];
      const prevText = (row.tech_stack ?? []).join(", ");
      if (text === prevText) return;
      await updateField(id, { tech_stack: parsed as any } as any);
      setDraftById((prev) => {
        const next = { ...(prev ?? {}) };
        const cur = { ...(next[id] ?? {}) };
        delete (cur as any).tech_text;
        next[id] = cur;
        if (!Object.keys(next[id] ?? {}).length) delete next[id];
        return next;
      });
      return;
    }

    const raw = String((d as any)[field] ?? "");
    const nextVal = raw.trim() ? raw : null;
    const prevVal = (row as any)[field] ?? null;
    if (String(nextVal ?? "") === String(prevVal ?? "")) return;
    await updateField(id, { [field]: nextVal } as any);
    setDraftById((prev) => {
      const next = { ...(prev ?? {}) };
      const cur = { ...(next[id] ?? {}) };
      delete (cur as any)[field];
      next[id] = cur;
      if (!Object.keys(next[id] ?? {}).length) delete next[id];
      return next;
    });
  }

  return (
    <main>
      <AppTopbar title="ICP Company Profiles" subtitle="Library: vertical + sub-vertical. Used by VP matrix." />

      <div className="page grid">
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Create company profile</div>
              <div className="cardDesc">Use sub-vertical to differentiate messaging (VP) per niche.</div>
            </div>
          </div>
          <div className="cardBody">
            {status ? <div className="notice" style={{ marginBottom: 12 }}>{status}</div> : null}
            <div className="grid">
              <div style={{ gridColumn: "span 4" }}>
                <label className="muted" style={{ fontSize: 13 }}>Vertical *</label>
                <input className="input" value={newVertical} onChange={(e) => setNewVertical(e.target.value)} placeholder="e.g. FinTech" />
              </div>
              <div style={{ gridColumn: "span 4" }}>
                <label className="muted" style={{ fontSize: 13 }}>Sub-vertical</label>
                <input className="input" value={newSubVertical} onChange={(e) => setNewSubVertical(e.target.value)} placeholder="e.g. Payments" />
              </div>
              <div style={{ gridColumn: "span 4" }}>
                <label className="muted" style={{ fontSize: 13 }}>Region</label>
                <input className="input" value={newRegion} onChange={(e) => setNewRegion(e.target.value)} placeholder="US/EU/MENA" />
              </div>
              <div style={{ gridColumn: "span 4" }}>
                <label className="muted" style={{ fontSize: 13 }}>Size bucket</label>
                <input className="input" value={newSize} onChange={(e) => setNewSize(e.target.value)} placeholder="200-1000" />
              </div>
              <div style={{ gridColumn: "span 8" }}>
                <label className="muted" style={{ fontSize: 13 }}>Tech stack (comma)</label>
                <input className="input" value={newTech} onChange={(e) => setNewTech(e.target.value)} placeholder="Android, iOS, Kotlin" />
              </div>
              <div style={{ gridColumn: "span 12" }}>
                <label className="muted" style={{ fontSize: 13 }}>Notes</label>
                <textarea className="textarea" value={newNotes} onChange={(e) => setNewNotes(e.target.value)} />
              </div>
            </div>
            <div className="btnRow" style={{ marginTop: 10 }}>
              <button className="btn btnPrimary" onClick={add}>Add company profile</button>
            </div>
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Company profiles</div>
              <div className="cardDesc">Inline editable.</div>
            </div>
          </div>
          <div className="cardBody">
            <table className="table">
              <thead>
                <tr>
                  <th>Vertical</th>
                  <th>Sub-vertical</th>
                  <th>Region</th>
                  <th>Size</th>
                  <th>Tech stack</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.id}>
                    <td>
                      <input
                        className="input"
                        value={draftById[r.id]?.vertical_name ?? r.vertical_name ?? ""}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), vertical_name: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "vertical_name")}
                      />
                    </td>
                    <td>
                      <input
                        className="input"
                        value={draftById[r.id]?.sub_vertical ?? r.sub_vertical ?? ""}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), sub_vertical: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "sub_vertical")}
                      />
                    </td>
                    <td>
                      <input
                        className="input"
                        value={draftById[r.id]?.region ?? r.region ?? ""}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), region: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "region")}
                      />
                    </td>
                    <td>
                      <input
                        className="input"
                        value={draftById[r.id]?.size_bucket ?? r.size_bucket ?? ""}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), size_bucket: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "size_bucket")}
                      />
                    </td>
                    <td>
                      <input
                        className="input"
                        value={draftById[r.id]?.tech_text ?? (r.tech_stack ?? []).join(", ")}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), tech_text: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "tech_stack")}
                      />
                    </td>
                    <td style={{ width: 120 }}>
                      <button className="btn" onClick={() => remove(r.id)}>Delete</button>
                    </td>
                  </tr>
                ))}
                {!rows.length ? (
                  <tr>
                    <td colSpan={6} className="muted2">No company profiles yet.</td>
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


