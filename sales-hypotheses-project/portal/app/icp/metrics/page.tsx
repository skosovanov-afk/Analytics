"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { AppTopbar } from "../../components/AppTopbar";

type MetricRow = {
  id: string;
  slug: string;
  name: string;
  input_type: "number" | "text";
  unit: string | null;
  sort_order: number;
  is_active: boolean;
  updated_at: string;
};

function slugify(x: string) {
  return String(x ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48);
}

export default function MetricsPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [status, setStatus] = useState("");
  const [rows, setRows] = useState<MetricRow[]>([]);
  const [draftById, setDraftById] = useState<Record<string, Partial<MetricRow>>>({});

  const [newName, setNewName] = useState("");
  const [newSlug, setNewSlug] = useState("");
  const [newUnit, setNewUnit] = useState("");
  const [newType, setNewType] = useState<"number" | "text">("number");
  const [newOrder, setNewOrder] = useState<number>(0);

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) return setStatus("Not signed in. Go back to /.");
    const res = await supabase
      .from("sales_metrics")
      .select("*")
      .order("is_active", { ascending: false })
      .order("sort_order", { ascending: true })
      .order("name", { ascending: true });
    if (res.error) return setStatus(`metrics error: ${res.error.message}`);
    setRows((res.data ?? []) as any);
    setStatus("");
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase]);

  useEffect(() => {
    if (!newSlug && newName) setNewSlug(slugify(newName));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [newName]);

  async function add() {
    if (!supabase) return;
    const name = newName.trim();
    const slug = (newSlug || slugify(name)).trim();
    if (!name) return setStatus("Name is required");
    if (!slug) return setStatus("Slug is required");
    setStatus("Saving...");
    const res = await supabase.from("sales_metrics").insert({
      name,
      slug,
      input_type: newType,
      unit: newUnit.trim() || null,
      sort_order: Number(newOrder) || 0,
      is_active: true
    });
    if (res.error) return setStatus(`insert error: ${res.error.message}`);
    setNewName("");
    setNewSlug("");
    setNewUnit("");
    setNewType("number");
    setNewOrder(0);
    await load();
    setStatus("Saved.");
  }

  async function updateField(id: string, patch: Partial<MetricRow>) {
    if (!supabase) return;
    setStatus("Saving...");
    const res = await supabase.from("sales_metrics").update(patch).eq("id", id);
    if (res.error) return setStatus(`update error: ${res.error.message}`);
    // Optimistic UI update to avoid jitter while editing.
    setRows((prev) => prev.map((r) => (r.id === id ? ({ ...r, ...patch } as any) : r)));
    setStatus("Saved.");
  }

  /**
   * Commit a single draft field on blur.
   * Avoids saving on every keystroke which makes the table jump.
   */
  async function commitDraftField(id: string, field: "name" | "slug" | "unit" | "sort_order") {
    const d = draftById[id] ?? null;
    if (!d) return;
    const row = rows.find((r) => r.id === id) ?? null;
    if (!row) return;

    if (field === "sort_order") {
      const n = Number((d as any).sort_order ?? row.sort_order ?? 0);
      const v = Number.isFinite(n) ? n : 0;
      if (v === Number(row.sort_order ?? 0)) return;
      await updateField(id, { sort_order: v } as any);
      setDraftById((prev) => {
        const next = { ...(prev ?? {}) };
        const cur = { ...(next[id] ?? {}) };
        delete (cur as any).sort_order;
        next[id] = cur;
        if (!Object.keys(next[id] ?? {}).length) delete next[id];
        return next;
      });
      return;
    }

    const raw = String((d as any)[field] ?? "");
    const nextVal = field === "unit" ? (raw.trim() ? raw : null) : raw;
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

  async function remove(id: string) {
    if (!supabase) return;
    if (!confirm("Delete metric? Hypotheses referencing it will lose it from the picker, but old check-ins keep the saved values.")) return;
    setStatus("Deleting...");
    const res = await supabase.from("sales_metrics").delete().eq("id", id);
    if (res.error) return setStatus(`delete error: ${res.error.message}`);
    await load();
    setStatus("Deleted.");
  }

  return (
    <main>
      <AppTopbar title="Metrics" subtitle="Library: metrics for hypotheses and weekly check-ins." />

      <div className="page grid">
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Create metric</div>
              <div className="cardDesc">Slug is used as the key in saved weekly check-ins.</div>
            </div>
          </div>
          <div className="cardBody">
            {status ? <div className="notice" style={{ marginBottom: 12 }}>{status}</div> : null}
            <div className="grid">
              <div style={{ gridColumn: "span 4" }}>
                <label className="muted" style={{ fontSize: 13 }}>Name *</label>
                <input className="input" value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="e.g. Meetings booked" />
              </div>
              <div style={{ gridColumn: "span 3" }}>
                <label className="muted" style={{ fontSize: 13 }}>Slug *</label>
                <input className="input" value={newSlug} onChange={(e) => setNewSlug(e.target.value)} placeholder="meetings-booked" />
              </div>
              <div style={{ gridColumn: "span 2" }}>
                <label className="muted" style={{ fontSize: 13 }}>Type</label>
                <select className="select" value={newType} onChange={(e) => setNewType(e.target.value as any)}>
                  <option value="number">number</option>
                  <option value="text">text</option>
                </select>
              </div>
              <div style={{ gridColumn: "span 2" }}>
                <label className="muted" style={{ fontSize: 13 }}>Unit</label>
                <input className="input" value={newUnit} onChange={(e) => setNewUnit(e.target.value)} placeholder="%, $, count" />
              </div>
              <div style={{ gridColumn: "span 1" }}>
                <label className="muted" style={{ fontSize: 13 }}>Sort</label>
                <input className="input" type="number" value={newOrder} onChange={(e) => setNewOrder(Number(e.target.value || 0))} />
              </div>
            </div>
            <div className="btnRow" style={{ marginTop: 10 }}>
              <button className="btn btnPrimary" onClick={add}>Add metric</button>
            </div>
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Metrics</div>
              <div className="cardDesc">Editable. Disable instead of delete if unsure.</div>
            </div>
          </div>
          <div className="cardBody">
            <table className="table">
              <thead>
                <tr>
                  <th style={{ width: 90 }}>Active</th>
                  <th>Name</th>
                  <th>Slug</th>
                  <th style={{ width: 130 }}>Type</th>
                  <th style={{ width: 140 }}>Unit</th>
                  <th style={{ width: 120 }}>Sort</th>
                  <th style={{ width: 120 }}></th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.id}>
                    <td>
                      <input type="checkbox" checked={!!r.is_active} onChange={(e) => updateField(r.id, { is_active: e.target.checked } as any)} />
                    </td>
                    <td>
                      <input
                        className="input"
                        value={draftById[r.id]?.name ?? r.name ?? ""}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), name: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "name")}
                      />
                    </td>
                    <td>
                      <input
                        className="input mono"
                        value={draftById[r.id]?.slug ?? r.slug ?? ""}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), slug: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "slug")}
                      />
                    </td>
                    <td>
                      <select className="select" value={r.input_type ?? "number"} onChange={(e) => updateField(r.id, { input_type: e.target.value as any } as any)}>
                        <option value="number">number</option>
                        <option value="text">text</option>
                      </select>
                    </td>
                    <td>
                      <input
                        className="input"
                        value={draftById[r.id]?.unit ?? r.unit ?? ""}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), unit: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "unit")}
                      />
                    </td>
                    <td>
                      <input
                        className="input"
                        type="number"
                        value={Number(draftById[r.id]?.sort_order ?? r.sort_order ?? 0)}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), sort_order: Number(e.target.value || 0) } }))}
                        onBlur={() => commitDraftField(r.id, "sort_order")}
                      />
                    </td>
                    <td><button className="btn" onClick={() => remove(r.id)}>Delete</button></td>
                  </tr>
                ))}
                {!rows.length ? (
                  <tr>
                    <td colSpan={7} className="muted2">No metrics yet.</td>
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


