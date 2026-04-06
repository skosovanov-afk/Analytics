"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { isoDate } from "../../lib/utils";
import { AppTopbar } from "../../components/AppTopbar";
import { getSupabase } from "../../lib/supabase";

type RoleRow = {
  id: string;
  name: string | null;
};

type CompanyProfileRow = {
  id: string;
  vertical_name: string | null;
  sub_vertical: string | null;
  region: string | null;
  size_bucket: string | null;
  notes: string | null;
};

type MatrixRow = {
  id: string;
  title: string | null;
  role_id: string | null;
  role_label: string | null;
  company_profile_id: string | null;
  vertical_name: string | null;
  sub_vertical: string | null;
  company_scale: string | null;
  vp_point: string | null;
  decision_context: string | null;
  pain: string | null;
  notes: string | null;
  updated_at: string | null;
  created_at: string | null;
};

type MatrixMeta = {
  job_to_be_done: string;
  outcome_metric: string;
};

type MatrixDraft = {
  value_proposition: string;
  decision_context: string;
  job_to_be_done: string;
  outcome_metric: string;
  pain_points: string;
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
    detailsRef.current?.removeAttribute("open");
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

function companyLabel(profile: CompanyProfileRow) {
  const vertical = String(profile.vertical_name ?? "").trim() || "—";
  const subVertical = String(profile.sub_vertical ?? "").trim();
  return `${vertical}${subVertical ? ` / ${subVertical}` : ""}`;
}

function rowMatchesCompany(row: MatrixRow, profile: CompanyProfileRow) {
  if (String(row.company_profile_id ?? "").trim()) {
    return row.company_profile_id === profile.id;
  }
  return (
    String(row.vertical_name ?? "").trim().toLowerCase() === String(profile.vertical_name ?? "").trim().toLowerCase()
    && String(row.sub_vertical ?? "").trim().toLowerCase() === String(profile.sub_vertical ?? "").trim().toLowerCase()
    && String(row.company_scale ?? "").trim().toLowerCase() === String(profile.size_bucket ?? "").trim().toLowerCase()
  );
}

function parseMatrixMeta(value: string | null | undefined): MatrixMeta {
  const text = String(value ?? "").trim();
  if (!text) return { job_to_be_done: "", outcome_metric: "" };
  try {
    const parsed = JSON.parse(text);
    return {
      job_to_be_done: String(parsed?.job_to_be_done ?? "").trim(),
      outcome_metric: String(parsed?.outcome_metric ?? "").trim()
    };
  } catch {
    return { job_to_be_done: "", outcome_metric: "" };
  }
}

function serializeMatrixMeta(draft: MatrixDraft) {
  const payload = {
    job_to_be_done: draft.job_to_be_done.trim(),
    outcome_metric: draft.outcome_metric.trim()
  };
  if (!payload.job_to_be_done && !payload.outcome_metric) return null;
  return JSON.stringify(payload);
}

function emptyDraft(): MatrixDraft {
  return {
    value_proposition: "",
    decision_context: "",
    job_to_be_done: "",
    outcome_metric: "",
    pain_points: ""
  };
}

function toDraft(row: MatrixRow | null): MatrixDraft {
  const meta = parseMatrixMeta(row?.notes);
  return {
    value_proposition: String(row?.vp_point ?? "").trim(),
    decision_context: String(row?.decision_context ?? "").trim(),
    job_to_be_done: meta.job_to_be_done,
    outcome_metric: meta.outcome_metric,
    pain_points: String(row?.pain ?? "").trim()
  };
}

function preview(value: string | null | undefined, empty = "—") {
  const text = String(value ?? "").trim();
  return text || empty;
}

function normalizeVpPoint(value: string | null | undefined) {
  return String(value ?? "").trim();
}

function vpGroupKey(value: string | null | undefined) {
  const normalized = normalizeVpPoint(value);
  return normalized ? normalized.toLowerCase() : "__untitled_vp__";
}

function resolveCompanyProfileId(row: MatrixRow, companyProfiles: CompanyProfileRow[]) {
  const explicitId = String(row.company_profile_id ?? "").trim();
  if (explicitId) return explicitId;
  return companyProfiles.find((profile) => rowMatchesCompany(row, profile))?.id ?? "";
}

export default function IcpMatrixPage() {
  const supabase = useMemo(() => getSupabase(), []);

  const [status, setStatus] = useState("");
  const [matrixWorkspaceId, setMatrixWorkspaceId] = useState("");
  const [roles, setRoles] = useState<RoleRow[]>([]);
  const [companyProfiles, setCompanyProfiles] = useState<CompanyProfileRow[]>([]);
  const [rows, setRows] = useState<MatrixRow[]>([]);
  const [search, setSearch] = useState("");
  const [openVpKey, setOpenVpKey] = useState<string | null>(null);
  const [createVpPoint, setCreateVpPoint] = useState("");
  const [createCompanyId, setCreateCompanyId] = useState("");
  const [createRoleId, setCreateRoleId] = useState("");
  const [editingRowId, setEditingRowId] = useState<string | null>(null);
  const [draftCompanyId, setDraftCompanyId] = useState<string>("");
  const [draftRoleId, setDraftRoleId] = useState<string | null>(null);
  const [draftRoleIds, setDraftRoleIds] = useState<string[]>([]);
  const [draftVpPoint, setDraftVpPoint] = useState("");
  const [draft, setDraft] = useState<MatrixDraft>(emptyDraft);
  const [renamingGroupKey, setRenamingGroupKey] = useState<string | null>(null);
  const [renameInputValue, setRenameInputValue] = useState("");
  const [movingPosition, setMovingPosition] = useState(false);
  const [moveTarget, setMoveTarget] = useState("");
  const [roleSearch, setRoleSearch] = useState("");
  const [roleDropdownOpen, setRoleDropdownOpen] = useState(false);

  async function ensureMatrixWorkspace() {
    if (!supabase) return null;
    const existing = await supabase
      .from("sales_hypotheses")
      .select("id,title")
      .eq("title", "VP Matrix Library")
      .order("created_at", { ascending: true })
      .limit(1)
      .maybeSingle();
    if (existing.error) throw new Error(existing.error.message);
    if (existing.data?.id) return String(existing.data.id);

    const inserted = await supabase
      .from("sales_hypotheses")
      .insert({
        title: "VP Matrix Library",
        status: "active",
        timebox_days: 0
      })
      .select("id")
      .single();
    if (inserted.error) throw new Error(inserted.error.message);
    return String(inserted.data?.id ?? "");
  }

  async function load() {
    if (!supabase) return;
    setStatus("Loading...");
    try {
      const sess = await supabase.auth.getSession();
      if (!sess.data.session) {
        setStatus("Not signed in. Go back to /.");
        return;
      }

      const workspaceId = await ensureMatrixWorkspace();
      if (!workspaceId) return setStatus("Could not initialize VP Library.");
      setMatrixWorkspaceId(workspaceId);

      const [rolesRes, companiesRes, rowsRes] = await Promise.all([
        supabase.from("sales_icp_roles").select("id,name").order("name", { ascending: true }).limit(500),
        supabase
          .from("sales_icp_company_profiles")
          .select("id,vertical_name,sub_vertical,region,size_bucket,notes")
          .order("vertical_name", { ascending: true })
          .order("sub_vertical", { ascending: true })
          .limit(500),
        supabase
          .from("sales_hypothesis_rows")
          .select("id,role_id,role_label,company_profile_id,vertical_name,sub_vertical,company_scale,vp_point,decision_context,pain,notes,updated_at,created_at")
          .eq("workspace_id", workspaceId)
          .order("updated_at", { ascending: false })
          .limit(5000)
      ]);

      if (rolesRes.error) throw new Error(rolesRes.error.message);
      if (companiesRes.error) throw new Error(companiesRes.error.message);
      if (rowsRes.error) throw new Error(rowsRes.error.message);

      setRoles((rolesRes.data ?? []) as RoleRow[]);
      setCompanyProfiles((companiesRes.data ?? []) as CompanyProfileRow[]);
      setRows((rowsRes.data ?? []) as MatrixRow[]);
      setStatus("");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Failed to load VP Library.");
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase]);

  const roleById = useMemo(() => new Map(roles.map((role) => [role.id, role])), [roles]);
  const companyProfileById = useMemo(() => new Map(companyProfiles.map((profile) => [profile.id, profile])), [companyProfiles]);
  const roleOptions = useMemo<SelectOption[]>(
    () => roles.map((role) => ({ value: role.id, label: role.name ?? "Unnamed role" })),
    [roles]
  );

  const companyProfileOptions = useMemo<SelectOption[]>(
    () =>
      companyProfiles.map((profile) => ({
        value: profile.id,
        label: companyLabel(profile),
        hint: [profile.region, profile.notes].filter(Boolean).join(" · ") || undefined
      })),
    [companyProfiles]
  );

  const filteredVpGroups = useMemo(() => {
    const groups = new Map<string, {
      key: string;
      label: string;
      rows: MatrixRow[];
      companyIds: Set<string>;
      orphanRows: MatrixRow[];
    }>();

    for (const row of rows) {
      const key = vpGroupKey(row.vp_point);
      const label = normalizeVpPoint(row.vp_point) || "Untitled VP Point";
      if (!groups.has(key)) {
        groups.set(key, { key, label, rows: [], companyIds: new Set<string>(), orphanRows: [] });
      }
      const group = groups.get(key)!;
      group.rows.push(row);
      const companyId = resolveCompanyProfileId(row, companyProfiles);
      if (companyId) group.companyIds.add(companyId);
      else group.orphanRows.push(row);
    }

    if (editingRowId?.startsWith("draft:") && draftCompanyId) {
      const key = vpGroupKey(draftVpPoint);
      const label = normalizeVpPoint(draftVpPoint) || "Untitled VP Point";
      if (!groups.has(key)) {
        groups.set(key, { key, label, rows: [], companyIds: new Set<string>(), orphanRows: [] });
      }
      groups.get(key)!.companyIds.add(draftCompanyId);
    }

    const needle = search.trim().toLowerCase();
    return Array.from(groups.values())
      .filter((group) => {
        if (!needle) return true;
        const haystack = [
          group.label,
          ...group.rows.flatMap((row) => {
            const company = companyProfileById.get(resolveCompanyProfileId(row, companyProfiles));
            const roleName = roleById.get(String(row.role_id ?? ""))?.name ?? row.role_label ?? "";
            return [
              company ? companyLabel(company) : "",
              company?.region,
              company?.notes,
              roleName,
              row.vp_point,
              row.decision_context,
              row.pain,
              row.notes
            ];
          })
        ]
          .map((value) => String(value ?? "").toLowerCase())
          .join(" ");
        return haystack.includes(needle);
      })
      .map((group) => ({
        ...group,
        companyIds: Array.from(group.companyIds).sort((a, b) => {
          const companyA = companyProfileById.get(a);
          const companyB = companyProfileById.get(b);
          const labelA = companyA ? companyLabel(companyA) : a;
          const labelB = companyB ? companyLabel(companyB) : b;
          return labelA.localeCompare(labelB);
        }),
        orphanRows: group.orphanRows.sort((a, b) =>
          String(roleById.get(String(a.role_id ?? ""))?.name ?? a.role_label ?? "")
            .localeCompare(String(roleById.get(String(b.role_id ?? ""))?.name ?? b.role_label ?? ""))
        )
      }))
      .sort((a, b) => a.label.localeCompare(b.label));
  }, [rows, editingRowId, draftCompanyId, draftVpPoint, search, companyProfiles, companyProfileById, roleById]);

  useEffect(() => {
    if (!filteredVpGroups.length) {
      setOpenVpKey(null);
      return;
    }
    if (!openVpKey || !filteredVpGroups.some((group) => group.key === openVpKey)) {
      setOpenVpKey(filteredVpGroups[0].key);
    }
  }, [filteredVpGroups, openVpKey]);

  function rowsForVpCompany(vpKey: string, companyId: string) {
    const companyRows = rows
      .filter((row) => vpGroupKey(row.vp_point) === vpKey && resolveCompanyProfileId(row, companyProfiles) === companyId)
      .sort((a, b) => String(roleById.get(String(a.role_id ?? ""))?.name ?? a.role_label ?? "").localeCompare(String(roleById.get(String(b.role_id ?? ""))?.name ?? b.role_label ?? "")));

    if (
      editingRowId?.startsWith("draft:")
      && draftCompanyId === companyId
      && vpGroupKey(draftVpPoint) === vpKey
      && draftRoleId
      && !companyRows.some((row) => String(row.role_id ?? "") === draftRoleId)
    ) {
      const profile = companyProfileById.get(companyId);
      companyRows.push({
        id: editingRowId,
        title: null,
        role_id: draftRoleId,
        role_label: roleById.get(draftRoleId)?.name ?? null,
        company_profile_id: companyId,
        vertical_name: profile?.vertical_name ?? null,
        sub_vertical: profile?.sub_vertical ?? null,
        company_scale: profile?.size_bucket ?? null,
        vp_point: draftVpPoint,
        decision_context: draft.decision_context,
        pain: draft.pain_points,
        notes: serializeMatrixMeta(draft),
        updated_at: null,
        created_at: null
      });
    }

    return companyRows;
  }

  function clearEditor() {
    setEditingRowId(null);
    setDraftCompanyId("");
    setDraftRoleId(null);
    setDraftRoleIds([]);
    setDraftVpPoint("");
    setDraft(emptyDraft());
    setRoleSearch("");
  }

  function startEdit(row: MatrixRow) {
    const companyId = resolveCompanyProfileId(row, companyProfiles);
    const roleId = String(row.role_id ?? "").trim();
    if (!roleId) {
      setStatus("This library row is missing a role.");
      return;
    }
    setEditingRowId(row.id);
    setDraftCompanyId(companyId);
    setDraftRoleId(roleId);
    // Collect all role IDs already in this VP Point + Company Profile
    const existingRoleIds = rows
      .filter((r) => vpGroupKey(r.vp_point) === vpGroupKey(row.vp_point) && resolveCompanyProfileId(r, companyProfiles) === companyId)
      .map((r) => String(r.role_id ?? "").trim())
      .filter(Boolean);
    setDraftRoleIds(Array.from(new Set(existingRoleIds)));
    setDraftVpPoint(normalizeVpPoint(row.vp_point));
    setDraft(toDraft(row.id.startsWith("draft:") ? null : row));
    setOpenVpKey(vpGroupKey(row.vp_point));
    setRoleSearch("");
    setRoleDropdownOpen(false);
  }

  function startCreateSegment() {
    const vpPoint = normalizeVpPoint(createVpPoint);
    if (!vpPoint) {
      setStatus("VP Point is required.");
      return;
    }
    if (!createCompanyId) {
      setStatus("Company profile is required.");
      return;
    }
    if (!createRoleId) {
      setStatus("Role is required.");
      return;
    }

    const existing = rows.find((row) => {
      return (
        vpGroupKey(row.vp_point) === vpGroupKey(vpPoint)
        && resolveCompanyProfileId(row, companyProfiles) === createCompanyId
        && String(row.role_id ?? "") === createRoleId
      );
    });

    if (existing) {
      startEdit(existing);
      setStatus("Opened existing library entry for this VP point.");
      return;
    }

    setEditingRowId(`draft:${vpGroupKey(vpPoint)}:${createCompanyId}:${createRoleId}`);
    setDraftCompanyId(createCompanyId);
    setDraftRoleId(createRoleId);
    setDraftVpPoint(vpPoint);
    setDraft({
      ...emptyDraft(),
      value_proposition: vpPoint
    });
    setOpenVpKey(vpGroupKey(vpPoint));
    setStatus("New library entry draft.");
  }

  async function renameVpPoint(groupKey: string, nextValue: string) {
    if (!supabase) return;
    const trimmed = nextValue.trim();
    if (!trimmed) return;
    if (vpGroupKey(trimmed) === groupKey) { setRenamingGroupKey(null); return; }
    const nextValueFinal = trimmed;
    const affectedRows = rows.filter((row) => vpGroupKey(row.vp_point) === groupKey);
    if (!affectedRows.length) return;
    setStatus("Renaming VP Point...");
    const res = await supabase
      .from("sales_hypothesis_rows")
      .update({ vp_point: nextValueFinal })
      .in("id", affectedRows.map((row) => row.id));
    if (res.error) {
      setStatus(`rename VP Point error: ${res.error.message}`);
      return;
    }
    if (vpGroupKey(draftVpPoint) === groupKey) {
      setDraftVpPoint(nextValueFinal);
      setDraft((prev) => ({ ...prev, value_proposition: nextValueFinal }));
    }
    setRenamingGroupKey(null);
    await load();
    setOpenVpKey(vpGroupKey(nextValueFinal));
    setStatus("VP Point renamed.");
  }

  function moveEditingPosition() {
    if (!editingRowId) return;
    const nextValue = moveTarget.trim();
    if (!nextValue) return;
    if (vpGroupKey(nextValue) === vpGroupKey(draftVpPoint)) { setMovingPosition(false); return; }
    const duplicate = rows.find((row) => {
      return (
        row.id !== editingRowId
        && vpGroupKey(row.vp_point) === vpGroupKey(nextValue)
        && resolveCompanyProfileId(row, companyProfiles) === draftCompanyId
        && String(row.role_id ?? "") === String(draftRoleId ?? "")
      );
    });
    if (duplicate) {
      setStatus("This VP Point already has the same company profile and role.");
      return;
    }
    setDraftVpPoint(nextValue);
    setDraft((prev) => ({ ...prev, value_proposition: nextValue }));
    setOpenVpKey(vpGroupKey(nextValue));
    setMovingPosition(false);
    setMoveTarget("");
    setStatus("Position moved in draft. Save to apply.");
  }

  function toggleDraftRole(roleId: string) {
    setDraftRoleIds((prev) => prev.includes(roleId) ? prev.filter((id) => id !== roleId) : [...prev, roleId]);
  }

  async function saveSegment() {
    if (!supabase) { setStatus("No database connection."); return; }
    if (!matrixWorkspaceId) { setStatus("No workspace loaded."); return; }
    if (!editingRowId) { setStatus("No row selected."); return; }
    const isNewDraft = editingRowId.startsWith("draft:");
    const profile = draftCompanyId ? companyProfiles.find((item) => item.id === draftCompanyId) ?? null : null;
    if (isNewDraft && !profile) {
      setStatus("Company profile is required for new entries.");
      return;
    }
    const primaryRoleId = draftRoleId || (isNewDraft
      ? ""
      : String(rows.find((row) => row.id === editingRowId)?.role_id ?? ""));
    // All selected roles (must include at least one)
    const allRoleIds = Array.from(new Set([...(primaryRoleId ? [primaryRoleId] : []), ...draftRoleIds])).filter(Boolean);
    if (!allRoleIds.length) {
      setStatus("At least one role is required.");
      return;
    }
    const vpPoint = normalizeVpPoint(draftVpPoint || draft.value_proposition);
    if (!vpPoint) {
      setStatus("VP Point is required.");
      return;
    }
    setStatus("Saving...");

    // Build base payload (without role fields)
    const existingRow = rows.find((row) => row.id === editingRowId);
    const basePayload = {
      workspace_id: matrixWorkspaceId,
      company_profile_id: profile?.id ?? existingRow?.company_profile_id ?? null,
      vertical_name: profile?.vertical_name ?? existingRow?.vertical_name ?? null,
      sub_vertical: profile?.sub_vertical ?? existingRow?.sub_vertical ?? null,
      company_scale: profile?.size_bucket ?? existingRow?.company_scale ?? null,
      vp_point: vpPoint,
      decision_context: draft.decision_context.trim() || null,
      pain: draft.pain_points.trim() || null,
      expected_signal: null,
      disqualifiers: null,
      notes: serializeMatrixMeta(draft),
      status: "in_test",
      priority: 2,
      source: "vp_matrix"
    };

    // Update the primary row (the one being edited)
    const primaryPayload = {
      ...basePayload,
      title: profile
        ? `${roleById.get(primaryRoleId)?.name ?? "Role"} × ${companyLabel(profile)}`
        : existingRow?.title ?? `${roleById.get(primaryRoleId)?.name ?? "Role"}`,
      role_id: primaryRoleId,
      role_label: roleById.get(primaryRoleId)?.name ?? null,
    };

    const res = isNewDraft
      ? await supabase.from("sales_hypothesis_rows").insert(primaryPayload)
      : await supabase.from("sales_hypothesis_rows").update(primaryPayload).eq("id", editingRowId);
    if (res.error) return setStatus(`save error: ${res.error.message}`);

    // Create rows for newly added roles (skip roles that already have a row)
    const additionalRoleIds = allRoleIds.filter((rid) => rid !== primaryRoleId);
    const companyId = profile?.id ?? existingRow?.company_profile_id ?? null;
    const newRoleIds = additionalRoleIds.filter((rid) => {
      return !rows.some((r) =>
        vpGroupKey(r.vp_point) === vpGroupKey(vpPoint)
        && resolveCompanyProfileId(r, companyProfiles) === companyId
        && String(r.role_id ?? "") === rid
      );
    });

    if (newRoleIds.length > 0) {
      const newRows = newRoleIds.map((rid) => ({
        ...basePayload,
        title: profile
          ? `${roleById.get(rid)?.name ?? "Role"} × ${companyLabel(profile)}`
          : `${roleById.get(rid)?.name ?? "Role"}`,
        role_id: rid,
        role_label: roleById.get(rid)?.name ?? null,
      }));
      const insertRes = await supabase.from("sales_hypothesis_rows").insert(newRows);
      if (insertRes.error) return setStatus(`save additional roles error: ${insertRes.error.message}`);
    }

    // Delete rows for roles that were unchecked
    if (!isNewDraft && companyId) {
      const removedRows = rows.filter((r) =>
        r.id !== editingRowId
        && vpGroupKey(r.vp_point) === vpGroupKey(vpPoint)
        && resolveCompanyProfileId(r, companyProfiles) === companyId
        && !allRoleIds.includes(String(r.role_id ?? ""))
      );
      for (const r of removedRows) {
        await supabase.from("sales_hypothesis_rows").delete().eq("id", r.id);
      }
    }

    await load();
    setOpenVpKey(vpGroupKey(vpPoint));
    clearEditor();
    setStatus("Library entry saved.");
  }

  async function deleteSegment(rowId: string) {
    if (!supabase) return;
    if (rowId.startsWith("draft:")) {
      if (editingRowId === rowId) clearEditor();
      return;
    }
    if (!confirm("Delete this VP point × company profile × role entry from Library?")) return;
    setStatus("Deleting segment...");
    const res = await supabase.from("sales_hypothesis_rows").delete().eq("id", rowId);
    if (res.error) return setStatus(`delete segment error: ${res.error.message}`);
    if (editingRowId === rowId) clearEditor();
    await load();
    setStatus("Library entry deleted.");
  }

  return (
    <main>
      <AppTopbar title="VP Library" subtitle="Start from a VP Point, then map where it is tested across company profiles and roles." />

      <div className="page grid">
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">VP Points</div>
              <div className="cardDesc">1. Start from a VP Point. 2. Attach the company profile where it is tested. 3. Add the role-specific messaging for that position.</div>
            </div>
            <div className="btnRow">
              <input
                className="input"
                style={{ width: 280 }}
                placeholder="Search VP points, company profiles or roles..."
                value={search}
                onChange={(event) => setSearch(event.target.value)}
              />
              <Link className="btn" href="/icp/companies">Manage company profiles</Link>
              <button className="btn" onClick={() => void load()}>Reload</button>
            </div>
          </div>
          <div className="cardBody">
            {status ? <div className="notice" style={{ marginBottom: 12 }}>{status}</div> : null}

            {!companyProfiles.length ? (
              <div className="subcard">
                <div style={{ fontWeight: 600, marginBottom: 6 }}>No company profiles yet</div>
                <div className="muted2" style={{ marginBottom: 12 }}>
                  Сначала создай хотя бы один `Company profile` в Library. После этого можно будет привязывать VP Points к сегментам и писать messaging под конкретную роль.
                </div>
                <Link className="btn btnPrimary" href="/icp/companies">Open company profiles</Link>
              </div>
            ) : (
              <div style={{ display: "grid", gap: 12 }}>
                <div className="subcard" style={{ margin: 0 }}>
                  <div className="subcardTitle">Create library entry</div>
                  <div className="helpInline" style={{ marginTop: -4, marginBottom: 10 }}>
                    Create a concrete position inside a VP Point: one company profile plus one role.
                  </div>
                  <div className="grid formGridTight">
                    <div style={{ gridColumn: "span 12" }}>
                      <label className="muted" style={{ fontSize: 12 }}>VP Point *</label>
                      <textarea className="textarea" value={createVpPoint} onChange={(event) => setCreateVpPoint(event.target.value)} placeholder="Example: Reduce failed payments before payout day" />
                    </div>
                    <div style={{ gridColumn: "span 6" }}>
                      <label className="muted" style={{ fontSize: 12 }}>Company profile *</label>
                      <SelectPopover
                        value={createCompanyId}
                        displayValue={companyProfileById.get(createCompanyId) ? companyLabel(companyProfileById.get(createCompanyId)!) : undefined}
                        options={companyProfileOptions}
                        placeholder="Choose company profile"
                        searchPlaceholder="Search company profile..."
                        emptyMessage="No company profiles found."
                        onChange={setCreateCompanyId}
                      />
                    </div>
                    <div style={{ gridColumn: "span 6" }}>
                      <label className="muted" style={{ fontSize: 12 }}>Role *</label>
                      <SelectPopover
                        value={createRoleId}
                        displayValue={roleById.get(createRoleId)?.name ?? undefined}
                        options={roleOptions}
                        placeholder="Choose role"
                        searchPlaceholder="Search role..."
                        emptyMessage="No roles found."
                        onChange={setCreateRoleId}
                      />
                    </div>
                    <div style={{ gridColumn: "span 12" }} className="btnRow">
                      <div className="helpInline" style={{ marginRight: "auto" }}>
                        If this exact VP Point + company profile + role already exists, the editor will open that entry instead of creating a duplicate.
                      </div>
                      <button className="btn btnPrimary" onClick={startCreateSegment}>Start segment</button>
                    </div>
                  </div>
                </div>

                {!filteredVpGroups.length ? (
                  <div className="subcard" style={{ margin: 0 }}>
                    <div style={{ fontWeight: 600, marginBottom: 6 }}>No VP Points yet</div>
                    <div className="muted2">
                      Create the first library entry above. The page will then group positions by VP Point, and inside each VP you will see company profiles and roles.
                    </div>
                  </div>
                ) : filteredVpGroups.map((group) => {
                  const isOpen = openVpKey === group.key;
                  return (
                    <div
                      key={group.key}
                      style={{ borderRadius: 12, border: "1px solid rgba(255,255,255,0.08)", overflow: "hidden" }}
                    >
                      <button
                        type="button"
                        onClick={() => setOpenVpKey(isOpen ? null : group.key)}
                        style={{
                          width: "100%",
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                          gap: 12,
                          padding: "16px 18px",
                          background: "rgba(255,255,255,0.02)",
                          border: 0,
                          color: "rgba(255,255,255,0.92)",
                          cursor: "pointer",
                          textAlign: "left"
                        }}
                      >
                        <div>
                          <div className="muted2" style={{ fontSize: 11, letterSpacing: "0.16em", textTransform: "uppercase", marginBottom: 6 }}>
                            VP Point
                          </div>
                          <div style={{ fontSize: 22, fontWeight: 700 }}>{group.label}</div>
                          <div className="muted2" style={{ fontSize: 13, marginTop: 4 }}>
                            {group.companyIds.length} company profile{group.companyIds.length === 1 ? "" : "s"}
                            {` · ${group.rows.length} position${group.rows.length === 1 ? "" : "s"}`}
                          </div>
                        </div>
                        <div className="muted2" style={{ fontSize: 14 }}>{isOpen ? "Hide positions" : "Show positions"}</div>
                      </button>

                      {isOpen ? (
                        <div style={{ borderTop: "1px solid rgba(255,255,255,0.08)" }}>
                          <div className="cardBody" style={{ paddingBottom: 0 }} />
                          <div className="cardBody" style={{ display: "grid", gap: 12 }}>
                            {group.companyIds.map((companyId) => {
                              const company = companyProfileById.get(companyId);
                              if (!company) return null;
                              const companyRows = rowsForVpCompany(group.key, companyId);
                              return (
                                <div key={`${group.key}:${companyId}`} className="subcard" style={{ margin: 0 }}>
                                  <div className="subcardTitle" style={{ marginBottom: 4 }}>{companyLabel(company)}</div>
                                  <div className="muted2" style={{ fontSize: 13, marginBottom: 12 }}>
                                    {[company.region, company.notes].filter(Boolean).join(" · ") || "Role-specific positions inside this VP Point."}
                                  </div>

                                  <div style={{ overflowX: "auto" }}>
                                    <table className="table" style={{ minWidth: 1240 }}>
                                      <thead>
                                        <tr>
                                          <th style={{ width: 220 }}>Role</th>
                                          <th style={{ minWidth: 260 }}>Decision context</th>
                                          <th style={{ minWidth: 260 }}>Pain/Friction</th>
                                          <th style={{ width: 120 }}>Updated</th>
                                          <th style={{ width: 170 }}></th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {companyRows.map((row) => (
                                          <tr key={row.id}>
                                            <td><b>{roleById.get(String(row.role_id ?? ""))?.name ?? row.role_label ?? "Unknown role"}</b></td>
                                            <td>{preview(row.decision_context)}</td>
                                            <td>{preview(row.pain)}</td>
                                            <td className="mono">{row.updated_at || row.created_at ? isoDate(row.updated_at || row.created_at || "") : "—"}</td>
                                            <td>
                                              <div className="btnRow" style={{ justifyContent: "flex-start" }}>
                                                <button className="btn" aria-label={`Edit entry`} onClick={() => startEdit(row)}>Edit</button>
                                                <button className="btn" aria-label={`Delete entry`} onClick={() => void deleteSegment(row.id)}>Delete</button>
                                              </div>
                                            </td>
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>

                                </div>
                              );
                            })}
                            {group.orphanRows.length > 0 && (
                              <div className="subcard" style={{ margin: 0 }}>
                                <div className="subcardTitle" style={{ marginBottom: 4 }}>No company profile</div>
                                <div className="muted2" style={{ fontSize: 13, marginBottom: 12 }}>
                                  These positions have no matching company profile. Edit them to assign one.
                                </div>
                                <div style={{ overflowX: "auto" }}>
                                  <table className="table" style={{ minWidth: 1240 }}>
                                    <thead>
                                      <tr>
                                        <th style={{ width: 220 }}>Role</th>
                                        <th style={{ minWidth: 260 }}>Decision context</th>
                                        <th style={{ minWidth: 260 }}>Pain/Friction</th>
                                        <th style={{ width: 120 }}>Updated</th>
                                        <th style={{ width: 170 }}></th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {group.orphanRows.map((row) => (
                                        <tr key={row.id}>
                                          <td><b>{roleById.get(String(row.role_id ?? ""))?.name ?? row.role_label ?? "Unknown role"}</b></td>
                                          <td>{preview(row.decision_context)}</td>
                                          <td>{preview(row.pain)}</td>
                                          <td className="mono">{row.updated_at || row.created_at ? isoDate(row.updated_at || row.created_at || "") : "—"}</td>
                                          <td>
                                            <div className="btnRow" style={{ justifyContent: "flex-start" }}>
                                              <button className="btn" aria-label={`Edit entry`} onClick={() => startEdit(row)}>Edit</button>
                                              <button className="btn" aria-label={`Delete entry`} onClick={() => void deleteSegment(row.id)}>Delete</button>
                                            </div>
                                          </td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                </div>
                              </div>
                            )}
                          </div>
                        </div>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      </div>
      {editingRowId && !editingRowId.startsWith("draft:") ? (
        <div className="dialogScrim" onClick={clearEditor}>
          <div className="card dialogCard" onClick={(e) => e.stopPropagation()}>
            <div className="cardBody">
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
                <div>
                  <div style={{ fontSize: 20, fontWeight: 700 }}>Edit Position</div>
                  <div className="muted2" style={{ fontSize: 13, marginTop: 4 }}>
                    {draftVpPoint || "—"} · {draftCompanyId && companyProfileById.get(draftCompanyId) ? companyLabel(companyProfileById.get(draftCompanyId)!) : "No company profile"}
                  </div>
                </div>
                <button className="btn" onClick={clearEditor}>Close</button>
              </div>
              {status ? <div className="notice" style={{ marginBottom: 12 }}>{status}</div> : null}
              <div className="grid formGridTight">
                {!draftCompanyId ? (
                  <div style={{ gridColumn: "span 12" }}>
                    <label className="muted" style={{ fontSize: 12 }}>Company profile *</label>
                    <select className="input" value={draftCompanyId} onChange={(e) => setDraftCompanyId(e.target.value)}>
                      <option value="">Select company profile...</option>
                      {companyProfiles.map((p) => (
                        <option key={p.id} value={p.id}>{companyLabel(p)}</option>
                      ))}
                    </select>
                  </div>
                ) : null}
                <div style={{ gridColumn: "span 12", position: "relative" }}>
                  <label className="muted" style={{ fontSize: 12 }}>Roles *</label>
                  <button
                    type="button"
                    className="selectTrigger"
                    data-open={roleDropdownOpen ? "1" : "0"}
                    onClick={() => { setRoleDropdownOpen((v) => !v); setRoleSearch(""); }}
                    style={{
                      width: "100%",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      textAlign: "left",
                      cursor: "pointer",
                    }}
                  >
                    <span className="selectTriggerValue" style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {draftRoleIds.length === 0
                        ? "Select roles..."
                        : draftRoleIds.map((id) => roleById.get(id)?.name ?? "?").join(", ")}
                    </span>
                    <span className="selectCaret" />
                  </button>
                  {roleDropdownOpen && (
                    <div
                      className="card popoverPanel selectPopoverPanel"
                      style={{ position: "absolute", top: "100%", left: 0, right: 0, zIndex: 50, marginTop: 4 }}
                    >
                      <div className="cardBody" style={{ padding: 12 }}>
                        <input
                          className="input"
                          placeholder="Search roles..."
                          value={roleSearch}
                          onChange={(e) => setRoleSearch(e.target.value)}
                          autoFocus
                        />
                        <div style={{ maxHeight: 220, overflowY: "auto", marginTop: 8, display: "flex", flexDirection: "column", gap: 2 }}>
                          {roles
                            .filter((r) => !roleSearch.trim() || (r.name ?? "").toLowerCase().includes(roleSearch.toLowerCase()))
                            .map((role) => {
                              const checked = draftRoleIds.includes(role.id);
                              return (
                                <label
                                  key={role.id}
                                  style={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: 8,
                                    padding: "6px 10px",
                                    borderRadius: 6,
                                    cursor: "pointer",
                                    background: checked ? "rgba(59,130,246,0.12)" : "transparent",
                                    fontSize: 13,
                                  }}
                                >
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    onChange={() => toggleDraftRole(role.id)}
                                    style={{ margin: 0 }}
                                  />
                                  {role.name ?? "Unnamed role"}
                                </label>
                              );
                            })}
                        </div>
                      </div>
                    </div>
                  )}
                </div>
                <div style={{ gridColumn: "span 12" }}>
                  <label className="muted" style={{ fontSize: 12 }}>VP Point *</label>
                  <textarea className="textarea" value={draftVpPoint} onChange={(e) => setDraftVpPoint(e.target.value)} placeholder="VP Point name" />
                </div>
                <div style={{ gridColumn: "span 12" }}>
                  <label className="muted" style={{ fontSize: 12 }}>Decision context</label>
                  <textarea className="textarea" value={draft.decision_context} onChange={(event) => setDraft((prev) => ({ ...prev, decision_context: event.target.value }))} />
                </div>
                <div style={{ gridColumn: "span 6" }}>
                  <label className="muted" style={{ fontSize: 12 }}>Job to be done</label>
                  <textarea className="textarea" value={draft.job_to_be_done} onChange={(event) => setDraft((prev) => ({ ...prev, job_to_be_done: event.target.value }))} />
                </div>
                <div style={{ gridColumn: "span 6" }}>
                  <label className="muted" style={{ fontSize: 12 }}>Outcome / metric</label>
                  <textarea className="textarea" value={draft.outcome_metric} onChange={(event) => setDraft((prev) => ({ ...prev, outcome_metric: event.target.value }))} />
                </div>
                <div style={{ gridColumn: "span 12" }}>
                  <label className="muted" style={{ fontSize: 12 }}>Pain / friction</label>
                  <textarea className="textarea" value={draft.pain_points} onChange={(event) => setDraft((prev) => ({ ...prev, pain_points: event.target.value }))} />
                </div>
                <div style={{ gridColumn: "span 12" }} className="btnRow">
                  <div style={{ flex: 1 }} />
                  <button className="btn" onClick={clearEditor}>Close</button>
                  <button className="btn btnPrimary" onClick={() => void saveSegment()}>Save segment</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </main>
  );
}
