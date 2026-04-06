"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { AppTopbar } from "../../components/AppTopbar";
import { getSupabase } from "../../lib/supabase";

type CompanyRow = {
  id: string;
  vertical_name: string | null;
  sub_vertical: string | null;
  region: string | null;
  size_bucket: string | null;
  notes: string | null;
  updated_at: string;
};

type VerticalOption = {
  id: string;
  name: string;
};

type SubverticalOption = {
  id: string;
  vertical_id: string;
  name: string;
};

type CompanyScaleOption = {
  id: string;
  name: string;
};

type SelectOption = {
  value: string;
  label: string;
  hint?: string;
};

type SelectPopoverProps = {
  value?: string;
  displayValue?: string;
  options: SelectOption[];
  placeholder: string;
  onChange?: (value: string) => void;
  searchPlaceholder?: string;
  emptyMessage?: string;
  searchable?: boolean;
  width?: number | string;
};

function SelectPopover({
  value,
  displayValue,
  options,
  placeholder,
  onChange,
  searchPlaceholder = "Search...",
  emptyMessage = "No options found.",
  searchable = true,
  width = "100%"
}: SelectPopoverProps) {
  const detailsRef = useRef<HTMLDetailsElement | null>(null);
  const searchInputRef = useRef<HTMLInputElement | null>(null);
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");

  const selectedOption = options.find((item) => item.value === value) ?? null;
  const visibleOptions = useMemo(() => {
    if (!searchable) return options;
    const needle = search.trim().toLowerCase();
    if (!needle) return options;
    return options.filter((item) => `${item.label} ${item.hint ?? ""}`.toLowerCase().includes(needle));
  }, [options, search, searchable]);

  useEffect(() => {
    if (!open) {
      setSearch("");
      return;
    }
    if (searchable) {
      const timer = window.setTimeout(() => searchInputRef.current?.focus(), 0);
      return () => window.clearTimeout(timer);
    }
  }, [open, searchable]);

  function close() {
    setOpen(false);
  }

  function handleSelect(nextValue: string) {
    onChange?.(nextValue);
    close();
  }

  return (
    <details
      ref={detailsRef}
      className="popover selectPopover"
      style={{ width }}
      onToggle={(event) => setOpen((event.currentTarget as HTMLDetailsElement).open)}
    >
      <summary className="selectTrigger" data-open={open ? "1" : "0"}>
        <span className="selectTriggerMain">
          <span className="selectTriggerValue">{displayValue || selectedOption?.label || placeholder}</span>
        </span>
        <span className="selectCaret" />
      </summary>
      <div className="card popoverPanel selectPopoverPanel">
        <div className="cardBody" style={{ padding: 12 }}>
          {searchable ? (
            <input
              ref={searchInputRef}
              className="input"
              placeholder={searchPlaceholder}
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
          ) : null}
          <div className="selectOptionList" style={{ marginTop: searchable ? 10 : 0 }}>
            {visibleOptions.map((option) => (
              <button
                key={option.value || `empty-${option.label}`}
                type="button"
                className={`selectOptionButton${value === option.value ? " isActive" : ""}`}
                onClick={() => handleSelect(option.value)}
              >
                <span>{option.label}</span>
                {option.hint ? <span className="selectOptionHint">{option.hint}</span> : null}
              </button>
            ))}
            {!visibleOptions.length ? (
              <div className="muted2" style={{ padding: "8px 10px" }}>{emptyMessage}</div>
            ) : null}
          </div>
        </div>
      </div>
    </details>
  );
}

export default function IcpCompaniesPage() {
  const supabase = useMemo(() => getSupabase(), []);

  const [status, setStatus] = useState("");
  const [rows, setRows] = useState<CompanyRow[]>([]);
  const [verticals, setVerticals] = useState<VerticalOption[]>([]);
  const [subverticals, setSubverticals] = useState<SubverticalOption[]>([]);
  const [companyScales, setCompanyScales] = useState<CompanyScaleOption[]>([]);
  const [newVertical, setNewVertical] = useState("");
  const [newSubVertical, setNewSubVertical] = useState("");
  const [newRegion, setNewRegion] = useState("");
  const [newSize, setNewSize] = useState("");
  const [newNotes, setNewNotes] = useState("");
  const [draftById, setDraftById] = useState<Record<string, Partial<CompanyRow>>>({});
  const verticalOptions = useMemo<SelectOption[]>(
    () => verticals.map((item) => ({ value: item.name, label: item.name })),
    [verticals]
  );
  const companyScaleOptions = useMemo<SelectOption[]>(
    () => companyScales.map((item) => ({ value: item.name, label: item.name })),
    [companyScales]
  );

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

  async function loadTaxonomy() {
    if (!supabase) return;
    try {
      const [verticalRes, subverticalRes, scaleRes] = await Promise.all([
        supabase.from("sales_verticals").select("id,name").eq("is_active", true).order("sort_order", { ascending: true }).order("name", { ascending: true }).limit(500),
        supabase.from("sales_subverticals").select("id,vertical_id,name").eq("is_active", true).order("sort_order", { ascending: true }).order("name", { ascending: true }).limit(1000),
        supabase.from("sales_company_scales").select("id,name").eq("is_active", true).order("sort_order", { ascending: true }).order("name", { ascending: true }).limit(500)
      ]);
      if (!verticalRes.error) setVerticals((verticalRes.data ?? []) as VerticalOption[]);
      if (!subverticalRes.error) setSubverticals((subverticalRes.data ?? []) as SubverticalOption[]);
      if (!scaleRes.error) setCompanyScales((scaleRes.data ?? []) as CompanyScaleOption[]);
    } catch (err) {
      setStatus(`Taxonomy load error: ${err instanceof Error ? err.message : "Failed to load"}`);
    }
  }

  useEffect(() => {
    load();
    loadTaxonomy();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase]);

  function subverticalOptionsForVerticalName(verticalName: string) {
    const match = verticals.find((item) => item.name.toLowerCase() === String(verticalName ?? "").trim().toLowerCase());
    if (!match) return [] as SubverticalOption[];
    return subverticals.filter((item) => item.vertical_id === match.id);
  }

  function subverticalSelectOptions(verticalName: string) {
    return subverticalOptionsForVerticalName(verticalName).map((item) => ({ value: item.name, label: item.name }));
  }

  async function add() {
    if (!supabase) return;
    if (!newVertical.trim()) return setStatus("vertical_name is required");
    setStatus("Saving...");
    const payload: any = {
      vertical_name: newVertical.trim(),
      sub_vertical: newSubVertical.trim() || null,
      region: newRegion.trim() || null,
      size_bucket: newSize.trim() || null,
      notes: newNotes.trim() || null
    };
    const res = await supabase.from("sales_icp_company_profiles").insert(payload);
    if (res.error) return setStatus(`insert error: ${res.error.message}`);
    setNewVertical("");
    setNewSubVertical("");
    setNewRegion("");
    setNewSize("");
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

  async function updateSelectField(id: string, patch: Partial<CompanyRow>) {
    await updateField(id, patch);
    setDraftById((prev) => {
      const next = { ...(prev ?? {}) };
      const cur = { ...(next[id] ?? {}) };
      for (const key of Object.keys(patch)) delete (cur as any)[key];
      next[id] = cur;
      if (!Object.keys(next[id] ?? {}).length) delete next[id];
      return next;
    });
  }

  /**
   * Commit a single draft field on blur.
   * Avoids saving on every keystroke which makes the table jump.
   */
  async function commitDraftField(
    id: string,
    field: "vertical_name" | "sub_vertical" | "region" | "size_bucket"
  ) {
    const d = draftById[id] ?? null;
    if (!d) return;
    const row = rows.find((r) => r.id === id) ?? null;
    if (!row) return;

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
                <SelectPopover
                  value={newVertical}
                  displayValue={newVertical || undefined}
                  options={verticalOptions}
                  placeholder="Choose vertical"
                  searchPlaceholder="Search vertical..."
                  emptyMessage="No verticals found."
                  onChange={(nextVertical) => {
                    setNewVertical(nextVertical);
                    const nextOptions = subverticalOptionsForVerticalName(nextVertical);
                    if (!nextOptions.some((item) => item.name === newSubVertical)) setNewSubVertical("");
                  }}
                />
              </div>
              <div style={{ gridColumn: "span 4" }}>
                <label className="muted" style={{ fontSize: 13 }}>Sub-vertical</label>
                <SelectPopover
                  value={newSubVertical}
                  displayValue={newSubVertical || undefined}
                  options={subverticalSelectOptions(newVertical)}
                  placeholder={newVertical ? "Choose sub-vertical" : "Pick vertical first"}
                  searchPlaceholder="Search sub-vertical..."
                  emptyMessage={newVertical ? "No sub-verticals found." : "Pick vertical first."}
                  onChange={setNewSubVertical}
                />
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

        <div className="card" style={{ gridColumn: "span 12", overflow: "visible" }}>
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
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.id}>
                    <td>
                      <SelectPopover
                        value={draftById[r.id]?.vertical_name ?? r.vertical_name ?? ""}
                        displayValue={draftById[r.id]?.vertical_name ?? r.vertical_name ?? undefined}
                        options={verticalOptions}
                        placeholder="Choose vertical"
                        searchPlaceholder="Search vertical..."
                        emptyMessage="No verticals found."
                        onChange={(nextVertical) => {
                          const nextSubverticalOptions = subverticalOptionsForVerticalName(nextVertical);
                          const currentSubvertical = draftById[r.id]?.sub_vertical ?? r.sub_vertical ?? "";
                          const nextSubvertical = nextSubverticalOptions.some((item) => item.name === currentSubvertical) ? currentSubvertical : null;
                          void updateSelectField(r.id, {
                            vertical_name: nextVertical || null,
                            sub_vertical: nextSubvertical
                          });
                        }}
                      />
                    </td>
                    <td>
                      <SelectPopover
                        value={draftById[r.id]?.sub_vertical ?? r.sub_vertical ?? ""}
                        displayValue={draftById[r.id]?.sub_vertical ?? r.sub_vertical ?? undefined}
                        options={subverticalSelectOptions(String(draftById[r.id]?.vertical_name ?? r.vertical_name ?? ""))}
                        placeholder={String(draftById[r.id]?.vertical_name ?? r.vertical_name ?? "").trim() ? "Choose sub-vertical" : "Pick vertical first"}
                        searchPlaceholder="Search sub-vertical..."
                        emptyMessage={String(draftById[r.id]?.vertical_name ?? r.vertical_name ?? "").trim() ? "No sub-verticals found." : "Pick vertical first."}
                        onChange={(nextSubVertical) => void updateSelectField(r.id, { sub_vertical: nextSubVertical || null })}
                      />
                    </td>
                    <td style={{ width: 120 }}>
                      <button className="btn" onClick={() => remove(r.id)}>Delete</button>
                    </td>
                  </tr>
                ))}
                {!rows.length ? (
                  <tr>
                    <td colSpan={5} className="muted2">No company profiles yet.</td>
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
