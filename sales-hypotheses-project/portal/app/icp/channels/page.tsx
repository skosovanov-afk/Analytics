"use client";

import { useEffect, useMemo, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { AppTopbar } from "../../components/AppTopbar";

type ChannelRow = {
  id: string;
  slug: string;
  name: string;
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

export default function ChannelsPage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [status, setStatus] = useState("");
  const [rows, setRows] = useState<ChannelRow[]>([]);
  const [draftById, setDraftById] = useState<Record<string, Partial<ChannelRow>>>({});

  const [newName, setNewName] = useState("");
  const [newSlug, setNewSlug] = useState("");
  const [newOrder, setNewOrder] = useState<number>(0);

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) return setStatus("Not signed in. Go back to /.");
    const res = await supabase.from("sales_channels").select("*").order("is_active", { ascending: false }).order("sort_order", { ascending: true }).order("name", { ascending: true });
    if (res.error) return setStatus(`channels error: ${res.error.message}`);
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
    const res = await supabase.from("sales_channels").insert({ name, slug, sort_order: Number(newOrder) || 0, is_active: true });
    if (res.error) return setStatus(`insert error: ${res.error.message}`);
    setNewName("");
    setNewSlug("");
    setNewOrder(0);
    await load();
    setStatus("Saved.");
  }

  async function updateField(id: string, patch: Partial<ChannelRow>) {
    if (!supabase) return;
    setStatus("Saving...");
    const res = await supabase.from("sales_channels").update(patch).eq("id", id);
    if (res.error) return setStatus(`update error: ${res.error.message}`);
    // Optimistic UI update to avoid jitter while editing.
    setRows((prev) => prev.map((r) => (r.id === id ? ({ ...r, ...patch } as any) : r)));
    setStatus("Saved.");
  }

  /**
   * Commit a single draft field on blur.
   * Avoids saving on every keystroke which makes the table jump.
   */
  async function commitDraftField(id: string, field: "name" | "slug" | "sort_order") {
    const d = draftById[id] ?? null;
    if (!d) return;
    const row = rows.find((r) => r.id === id) ?? null;
    if (!row) return;

    if (field === "name") {
      const v = String(d.name ?? "");
      if (v === String(row.name ?? "")) return;
      await updateField(id, { name: v } as any);
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

    if (field === "slug") {
      const v = String(d.slug ?? "");
      if (v === String(row.slug ?? "")) return;
      await updateField(id, { slug: v } as any);
      setDraftById((prev) => {
        const next = { ...(prev ?? {}) };
        const cur = { ...(next[id] ?? {}) };
        delete (cur as any).slug;
        next[id] = cur;
        if (!Object.keys(next[id] ?? {}).length) delete next[id];
        return next;
      });
      return;
    }

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
  }

  async function remove(id: string) {
    if (!supabase) return;
    if (!confirm("Delete channel? Hypotheses referencing this slug will keep the string, but it won't appear in pickers.")) return;
    setStatus("Deleting...");
    const res = await supabase.from("sales_channels").delete().eq("id", id);
    if (res.error) return setStatus(`delete error: ${res.error.message}`);
    await load();
    setStatus("Deleted.");
  }

  return (
    <main>
      <AppTopbar
        title="Channels"
        subtitle="Library: channels for hypotheses + weekly check-ins."
      />

      <div className="page grid">
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Create channel</div>
              <div className="cardDesc">Slug is stored in hypotheses (e.g. OutboundEmail, LinkedIn).</div>
            </div>
          </div>
          <div className="cardBody">
            {status ? <div className="notice" style={{ marginBottom: 12 }}>{status}</div> : null}
            <div className="grid">
              <div style={{ gridColumn: "span 5" }}>
                <label className="muted" style={{ fontSize: 13 }}>Name *</label>
                <input className="input" value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="e.g. Outbound email" />
              </div>
              <div style={{ gridColumn: "span 4" }}>
                <label className="muted" style={{ fontSize: 13 }}>Slug *</label>
                <input className="input" value={newSlug} onChange={(e) => setNewSlug(e.target.value)} placeholder="OutboundEmail" />
              </div>
              <div style={{ gridColumn: "span 3" }}>
                <label className="muted" style={{ fontSize: 13 }}>Sort order</label>
                <input className="input" type="number" value={newOrder} onChange={(e) => setNewOrder(Number(e.target.value || 0))} />
              </div>
            </div>
            <div className="btnRow" style={{ marginTop: 10 }}>
              <button className="btn btnPrimary" onClick={add}>Add channel</button>
            </div>
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Channels</div>
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
                    <td className="mono">
                      <input
                        className="input"
                        value={draftById[r.id]?.slug ?? r.slug ?? ""}
                        onChange={(e) => setDraftById((prev) => ({ ...(prev ?? {}), [r.id]: { ...(prev?.[r.id] ?? {}), slug: e.target.value } }))}
                        onBlur={() => commitDraftField(r.id, "slug")}
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
                    <td>
                      <button className="btn" onClick={() => remove(r.id)}>Delete</button>
                    </td>
                  </tr>
                ))}
                {!rows.length ? (
                  <tr>
                    <td colSpan={5} className="muted2">No channels yet.</td>
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


