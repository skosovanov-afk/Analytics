"use client";

import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { isoDate } from "../lib/utils";
import { AppTopbar } from "../components/AppTopbar";
import { getSupabase } from "../lib/supabase";

type RegistryRow = {
  id: string;
  title: string;
  status: string;
  vertical_name: string | null;
  opps_in_progress_count: number;
  updated_at: string;
  created_at: string;
};

type RoleOption = {
  id: string;
  name: string;
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

type CompanyProfileOption = {
  id: string;
  vertical_name: string | null;
  sub_vertical: string | null;
  region: string | null;
  size_bucket: string | null;
  tech_stack: string[] | null;
  notes: string | null;
};

type ChannelOption = {
  slug: string;
  name: string;
};

type OwnerOption = {
  id: string;
  name: string;
};

type SelectOption = {
  value: string;
  label: string;
  hint?: string;
};

type HypothesisRow = {
  id: string;
  workspace_id: string;
  title: string | null;
  tal_id: string | null;
  role_id: string | null;
  role_label: string | null;
  role: { name: string | null } | null;
  company_profile_id: string | null;
  vertical_id: string | null;
  subvertical_id: string | null;
  company_scale_id: string | null;
  vertical_name: string | null;
  sub_vertical: string | null;
  company_scale: string | null;
  channel: string | null;
  decision_context: string | null;
  vp_point: string;
  pain: string | null;
  expected_signal: string | null;
  disqualifiers: string | null;
  calls_count: number;
  pain_confirmed_rate: number | null;
  severity_rate: number | null;
  interest_rate: number | null;
  decision: string | null;
  status: string;
  priority: number;
  owner_id: string | null;
  notes: string | null;
  updated_at: string;
  created_at: string;
};

type RowDraft = {
  title: string;
  position_key: string;
  tal_id: string;
  company_profile_id: string;
  role_ids: string[];
  role_labels: string[];
  vertical_ids: string[];
  vertical_names: string[];
  subvertical_ids: string[];
  sub_verticals: string[];
  company_scale_ids: string[];
  company_scales: string[];
  regions: string[];
  channels: string[];
  decision_context: string;
  vp_point: string;
  pain: string;
  expected_signal: string;
  disqualifiers: string;
  job_to_be_done: string;
  outcome_metric: string;
  calls_count: string;
  pain_confirmed_rate: string;
  severity_rate: string;
  interest_rate: string;
  decision: string;
  status: string;
  priority: string;
  owner_id: string;
  notes: string;
};

type SegmentTemplate = {
  source_row_id: string;
  source_title: string | null;
  role_id: string;
  role_name: string;
  company_profile_id: string;
  company_profile_label: string;
  vp_point: string | null;
  job_to_be_done: string | null;
  decision_context: string | null;
  pain: string | null;
  outcome_metric: string | null;
  updated_at: string | null;
};

type TalOption = {
  id: string;
  name: string;
  criteria: string | null;
  description: string | null;
  email_sent: number;
  email_replies: number;
  email_reply_rate: number | null;
  email_meetings: number;
  email_held_meetings: number;
  li_invited: number;
  li_accepted: number;
  li_replies: number;
  li_accept_rate: number | null;
  li_meetings: number;
  li_held_meetings: number;
  app_touches: number;
  app_replies: number;
  app_reply_rate: number | null;
  app_meetings: number;
  app_held_meetings: number;
  tg_touches: number;
  tg_replies: number;
  tg_reply_rate: number | null;
  tg_meetings: number;
  tg_held_meetings: number;
  total_meetings: number;
  total_held_meetings: number;
};

type MatrixLibraryRow = {
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
  expected_signal: string | null;
  disqualifiers: string | null;
  notes: string | null;
  updated_at: string | null;
  created_at: string | null;
};

function deriveTalChannels(tal: TalOption | null | undefined): string[] {
  if (!tal) return [];
  const result: string[] = [];
  if (tal.email_sent > 0 || tal.email_replies > 0) result.push("Email");
  if (tal.li_invited > 0 || tal.li_accepted > 0) result.push("LinkedIn");
  if (tal.app_touches > 0 || tal.app_replies > 0) result.push("App");
  if (tal.tg_touches > 0 || tal.tg_replies > 0) result.push("Telegram");
  return result;
}

const ROW_STATUS_OPTIONS = ["draft", "in_progress", "validated", "paused", "archived"] as const;
const ROW_DECISION_OPTIONS = ["continue", "refine", "scale", "hold", "kill"] as const;
const PRIORITY_OPTIONS = [
  { value: "high", label: "High", score: 3 },
  { value: "medium", label: "Medium", score: 2 },
  { value: "low", label: "Low", score: 1 }
] as const;

function emptyDraft(): RowDraft {
  return {
    title: "",
    position_key: "",
    tal_id: "",
    company_profile_id: "",
    role_ids: [],
    role_labels: [],
    vertical_ids: [],
    vertical_names: [],
    subvertical_ids: [],
    sub_verticals: [],
    company_scale_ids: [],
    company_scales: [],
    regions: [],
    channels: [],
    decision_context: "",
    vp_point: "",
    pain: "",
    expected_signal: "",
    disqualifiers: "",
    job_to_be_done: "",
    outcome_metric: "",
    calls_count: "0",
    pain_confirmed_rate: "",
    severity_rate: "",
    interest_rate: "",
    decision: "",
    status: "draft",
    priority: "medium",
    owner_id: "",
    notes: ""
  };
}

function parseList(value: string | null | undefined) {
  return String(value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeVpPoint(value: string | null | undefined) {
  return String(value ?? "").trim();
}

function buildPositionMatchKey(roleId: string | null | undefined, companyProfileId: string | null | undefined, vpPoint: string | null | undefined) {
  return [
    String(roleId ?? "").trim(),
    String(companyProfileId ?? "").trim(),
    normalizeVpPoint(vpPoint).toLowerCase()
  ].join("::");
}

function companyProfileKey(verticalName: string | null | undefined, subVertical: string | null | undefined, sizeBucket: string | null | undefined) {
  return [
    String(verticalName ?? "").trim(),
    String(subVertical ?? "").trim(),
    String(sizeBucket ?? "").trim()
  ].join("::");
}

function companyProfileLabel(profile: CompanyProfileOption) {
  const vertical = String(profile.vertical_name ?? "").trim() || "—";
  const subVertical = String(profile.sub_vertical ?? "").trim();
  return `${vertical}${subVertical ? ` / ${subVertical}` : ""}`;
}

function findMatchingCompanyProfileId(
  companyProfiles: CompanyProfileOption[],
  verticalNames: string[],
  subVerticals: string[],
  companyScales: string[]
) {
  const key = companyProfileKey(verticalNames[0], subVerticals[0], companyScales[0]);
  if (!key.replace(/:/g, "").trim()) return "";
  return companyProfiles.find((profile) => companyProfileKey(profile.vertical_name, profile.sub_vertical, profile.size_bucket) === key)?.id ?? "";
}

function parseSegmentMeta(value: string | null | undefined) {
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

function parseHypothesisMeta(value: string | null | undefined) {
  const text = String(value ?? "").trim();
  if (!text) return { user_notes: "", job_to_be_done: "", outcome_metric: "", position_source_row_id: "", tal_id: "", regions: [] as string[] };
  try {
    const parsed = JSON.parse(text);
    return {
      user_notes: String(parsed?.user_notes ?? "").trim(),
      job_to_be_done: String(parsed?.job_to_be_done ?? "").trim(),
      outcome_metric: String(parsed?.outcome_metric ?? "").trim(),
      position_source_row_id: String(parsed?.position_source_row_id ?? "").trim(),
      tal_id: String(parsed?.tal_id ?? "").trim(),
      regions: Array.isArray(parsed?.regions) ? parsed.regions.map((r: any) => String(r).trim()).filter(Boolean) : [] as string[]
    };
  } catch {
    return { user_notes: text, job_to_be_done: "", outcome_metric: "", position_source_row_id: "", tal_id: "", regions: [] as string[] };
  }
}

function isMissingTalIdColumnError(message: string | null | undefined) {
  const text = String(message ?? "").toLowerCase();
  return text.includes("tal_id") && (
    text.includes("does not exist") ||
    text.includes("schema cache") ||
    text.includes("could not find the column")
  );
}

function serializeHypothesisMeta(draft: RowDraft) {
  const payload = {
    user_notes: draft.notes.trim(),
    job_to_be_done: draft.job_to_be_done.trim(),
    outcome_metric: draft.outcome_metric.trim(),
    position_source_row_id: draft.position_key.trim(),
    tal_id: draft.tal_id.trim(),
    regions: draft.regions.filter(Boolean)
  };
  if (!payload.user_notes && !payload.job_to_be_done && !payload.outcome_metric && !payload.position_source_row_id && !payload.tal_id && !payload.regions.length) return null;
  return JSON.stringify(payload);
}

function toDraft(
  row: HypothesisRow | null,
  roles: RoleOption[],
  verticals: VerticalOption[],
  subverticals: SubverticalOption[],
  companyScales: CompanyScaleOption[],
  companyProfiles: CompanyProfileOption[]
): RowDraft {
  if (!row) return emptyDraft();
  const meta = parseHypothesisMeta(row.notes);
  const roleLabels = parseList(row.role_label ?? row.role?.name ?? "");
  const verticalNames = parseList(row.vertical_name);
  const subVerticalNames = parseList(row.sub_vertical);
  const companyScaleNames = parseList(row.company_scale);
  const verticalIds = verticalNames
    .map((item) => verticals.find((vertical) => vertical.name.toLowerCase() === item.toLowerCase())?.id ?? "")
    .filter(Boolean);
  const subverticalIds = subVerticalNames
    .map((item) => {
      const match = subverticals.find((subvertical) => {
        if (subvertical.name.toLowerCase() !== item.toLowerCase()) return false;
        if (!verticalIds.length) return true;
        return verticalIds.includes(subvertical.vertical_id);
      });
      return match?.id ?? "";
    })
    .filter(Boolean);
  const companyProfileId = row.company_profile_id ?? findMatchingCompanyProfileId(companyProfiles, verticalNames, subVerticalNames, companyScaleNames);
  const roleIdsFromLabels = roleLabels
    .map((item) => roles.find((role) => role.name.toLowerCase() === item.toLowerCase())?.id ?? "")
    .filter(Boolean);
  const roleIds = roleIdsFromLabels.length > 0
    ? roleIdsFromLabels
    : row.role_id ? [row.role_id] : [];
  return {
    title: row.title ?? "",
    position_key: meta.position_source_row_id,
    tal_id: row.tal_id ?? meta.tal_id ?? "",
    company_profile_id: companyProfileId,
    role_ids: roleIds,
    role_labels: roleLabels,
    vertical_ids: verticalIds,
    vertical_names: verticalNames,
    subvertical_ids: subverticalIds,
    sub_verticals: subVerticalNames,
    company_scale_ids: row.company_scale_id ? [row.company_scale_id] : [],
    company_scales: companyScaleNames,
    regions: meta.regions,
    channels: parseList(row.channel),
    decision_context: row.decision_context ?? "",
    vp_point: row.vp_point ?? "",
    pain: row.pain ?? "",
    expected_signal: row.expected_signal ?? "",
    disqualifiers: row.disqualifiers ?? "",
    job_to_be_done: meta.job_to_be_done,
    outcome_metric: meta.outcome_metric,
    calls_count: String(row.calls_count ?? 0),
    pain_confirmed_rate: row.pain_confirmed_rate == null ? "" : String(row.pain_confirmed_rate),
    severity_rate: row.severity_rate == null ? "" : String(row.severity_rate),
    interest_rate: row.interest_rate == null ? "" : String(row.interest_rate),
    decision: row.decision ?? "",
    status: row.status === "new" ? "draft" : row.status === "in_test" ? "in_progress" : (row.status ?? "draft"),
    priority: priorityScoreToValue(row.priority),
    owner_id: row.owner_id ?? "",
    notes: meta.user_notes
  };
}

function cleanText(value: string) {
  const text = String(value ?? "").trim();
  return text || null;
}

function snapshotValuesFromDraft(draft: RowDraft, template: SegmentTemplate | null) {
  if (template) {
    return {
      vp_point: String(template.vp_point ?? draft.vp_point).trim(),
      decision_context: String(template.decision_context ?? draft.decision_context).trim(),
      pain: String(template.pain ?? draft.pain).trim(),
      job_to_be_done: String(template.job_to_be_done ?? draft.job_to_be_done).trim(),
      outcome_metric: String(template.outcome_metric ?? draft.outcome_metric).trim()
    };
  }
  return {
    vp_point: draft.vp_point.trim(),
    decision_context: draft.decision_context.trim(),
    pain: draft.pain.trim(),
    job_to_be_done: draft.job_to_be_done.trim(),
    outcome_metric: draft.outcome_metric.trim()
  };
}

function parseIntOrZero(value: string) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseNumberOrNull(value: string) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

function preview(value: string | null | undefined, max = 80) {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  if (!text) return "—";
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function roleDisplay(row: HypothesisRow, totalRolesCount?: number) {
  // Prefer role_label (comma-separated) over role.name (single join)
  const label = row.role_label || row.role?.name || "—";
  const parts = label.split(",").map((s) => s.trim()).filter(Boolean);
  if (parts.length <= 1) return label;
  if (totalRolesCount && parts.length >= totalRolesCount - 1) return "All";
  return `${parts[0]} + ${parts.length - 1} more`;
}

function priorityScoreToValue(score: number | null | undefined) {
  if (score === 3) return "high";
  if (score === 1) return "low";
  return "medium";
}

function priorityValueToScore(value: string) {
  return PRIORITY_OPTIONS.find((item) => item.value === value)?.score ?? 2;
}

function priorityValueToLabel(value: string) {
  return PRIORITY_OPTIONS.find((item) => item.value === value)?.label ?? "Medium";
}

type SelectPopoverProps = {
  value?: string;
  values?: string[];
  displayValue?: string;
  options: SelectOption[];
  placeholder: string;
  onChange?: (value: string) => void;
  onChangeMultiple?: (values: string[]) => void;
  searchPlaceholder?: string;
  emptyMessage?: string;
  searchable?: boolean;
  allowCustomValue?: boolean;
  customValueLabel?: string;
  width?: number | string;
  multiple?: boolean;
  disabled?: boolean;
};

function SelectPopover({
  value,
  values = [],
  displayValue,
  options,
  placeholder,
  onChange,
  onChangeMultiple,
  searchPlaceholder = "Search...",
  emptyMessage = "No options found.",
  searchable = true,
  allowCustomValue = false,
  customValueLabel = "Use",
  width = "100%",
  multiple = false,
  disabled = false
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
  const trimmedSearch = search.trim();
  const canUseCustomValue = allowCustomValue && trimmedSearch.length > 0 && !options.some((item) => item.label.toLowerCase() === trimmedSearch.toLowerCase());
  const selectedLabels = useMemo(
    () => options.filter((item) => values.includes(item.value)).map((item) => item.label),
    [options, values]
  );
  const triggerLabel = multiple
    ? (displayValue || (selectedLabels.length ? selectedLabels.join(", ") : placeholder))
    : (displayValue || selectedOption?.label || placeholder);

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

  useEffect(() => {
    if (!open) return;
    function handleOutsideClick(e: MouseEvent) {
      if (detailsRef.current && !detailsRef.current.contains(e.target as Node)) {
        close();
      }
    }
    document.addEventListener("mousedown", handleOutsideClick);
    return () => document.removeEventListener("mousedown", handleOutsideClick);
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  function close() {
    setOpen(false);
    detailsRef.current?.removeAttribute("open");
  }

  function handleSelect(nextValue: string) {
    if (!multiple) onChange?.(nextValue);
    close();
  }

  function handleToggle(nextValue: string) {
    if (!multiple) return;
    const nextValues = values.includes(nextValue)
      ? values.filter((item) => item !== nextValue)
      : [...values, nextValue];
    onChangeMultiple?.(nextValues);
  }

  return (
    <details
      ref={detailsRef}
      className="popover selectPopover"
      style={{ width }}
      onToggle={(event) => {
        if (disabled) {
          (event.currentTarget as HTMLDetailsElement).open = false;
          setOpen(false);
          return;
        }
        setOpen((event.currentTarget as HTMLDetailsElement).open);
      }}
    >
      <summary
        className="selectTrigger"
        data-open={open ? "1" : "0"}
        data-disabled={disabled ? "1" : "0"}
        aria-disabled={disabled}
        onClick={disabled ? (event) => event.preventDefault() : undefined}
      >
        <span className="selectTriggerMain">
          <span className="selectTriggerValue">{triggerLabel}</span>
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
              disabled={disabled}
              onChange={(event) => setSearch(event.target.value)}
            />
          ) : null}
          <div className="selectOptionList" style={{ marginTop: searchable ? 10 : 0 }}>
            {canUseCustomValue ? (
              <button
                type="button"
                className={`selectOptionButton${multiple ? (values.includes(trimmedSearch) ? " isActive" : "") : (value === trimmedSearch ? " isActive" : "")}`}
                disabled={disabled}
                onClick={() => (multiple ? handleToggle(trimmedSearch) : handleSelect(trimmedSearch))}
              >
                {customValueLabel}: {trimmedSearch}
              </button>
            ) : null}
            {visibleOptions.map((option) => (
              <button
                key={option.value || `empty-${option.label}`}
                type="button"
                className={`selectOptionButton${multiple ? (values.includes(option.value) ? " isActive" : "") : (value === option.value ? " isActive" : "")}`}
                disabled={disabled}
                onClick={() => (multiple ? handleToggle(option.value) : handleSelect(option.value))}
              >
                <span>{option.label}</span>
                {option.hint ? <span className="selectOptionHint">{option.hint}</span> : null}
              </button>
            ))}
            {!visibleOptions.length && !canUseCustomValue ? (
              <div className="muted2" style={{ padding: "8px 10px" }}>{emptyMessage}</div>
            ) : null}
          </div>
        </div>
      </div>
    </details>
  );
}

export default function HypothesesRegistryPage() {
  const supabase = useMemo(() => getSupabase(), []);

  const [status, setStatus] = useState("");
  const [talLinkAvailable, setTalLinkAvailable] = useState(true);
  const [talLinkNotice, setTalLinkNotice] = useState("");
  const [registry, setRegistry] = useState<RegistryRow | null>(null);
  const [roles, setRoles] = useState<RoleOption[]>([]);
  const [verticals, setVerticals] = useState<VerticalOption[]>([]);
  const [subverticals, setSubverticals] = useState<SubverticalOption[]>([]);
  const [companyScales, setCompanyScales] = useState<CompanyScaleOption[]>([]);
  const [companyProfiles, setCompanyProfiles] = useState<CompanyProfileOption[]>([]);
  const [channels, setChannels] = useState<ChannelOption[]>([]);
  const [tals, setTals] = useState<TalOption[]>([]);
  const [owners, setOwners] = useState<OwnerOption[]>([]);
  const [matrixHypothesisId, setMatrixHypothesisId] = useState("");
  const [matrixRows, setMatrixRows] = useState<MatrixLibraryRow[]>([]);
  const [rows, setRows] = useState<HypothesisRow[]>([]);
  const [selectedRowId, setSelectedRowId] = useState<string>("");
  const [isEditorOpen, setIsEditorOpen] = useState(false);
  const [rowDraft, setRowDraft] = useState<RowDraft>(emptyDraft);
  const [draftDirty, setDraftDirty] = useState(false);
  const [rowsLoading, setRowsLoading] = useState(false);
  const [savingRow, setSavingRow] = useState(false);
  const [deletingRow, setDeletingRow] = useState(false);
  const [reloadingRows, setReloadingRows] = useState(false);
  const [updatingChannelsRowId, setUpdatingChannelsRowId] = useState("");
  const [rowSearch, setRowSearch] = useState("");
  const [rowStatusFilter, setRowStatusFilter] = useState<string[]>([]);
  const [rowDecisionFilter, setRowDecisionFilter] = useState<string[]>([]);
  const [focusConsumed, setFocusConsumed] = useState(false);

  const [initialFocusId, setInitialFocusId] = useState("");

  const selectedRow = rows.find((row) => row.id === selectedRowId) ?? null;
  const selectedCompanyProfile = useMemo(
    () => companyProfiles.find((profile) => profile.id === rowDraft.company_profile_id) ?? null,
    [companyProfiles, rowDraft.company_profile_id]
  );
  const selectedTal = useMemo(
    () => tals.find((tal) => tal.id === rowDraft.tal_id) ?? null,
    [tals, rowDraft.tal_id]
  );
  const rowActionsBusy = savingRow || deletingRow || reloadingRows || rowsLoading;
  const editorBusy = savingRow || deletingRow;
  const verticalById = useMemo(() => new Map(verticals.map((item) => [item.id, item.name])), [verticals]);
  const filteredSubverticalOptions = useMemo(() => {
    if (!rowDraft.vertical_ids.length) return subverticals;
    return subverticals.filter((item) => rowDraft.vertical_ids.includes(item.vertical_id));
  }, [subverticals, rowDraft.vertical_ids]);
  const roleOptions = useMemo<SelectOption[]>(
    () => roles.map((item) => ({ value: item.id, label: item.name })),
    [roles]
  );
  const formatLabel = (s: string) => s.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  const statusOptions = useMemo<SelectOption[]>(
    () => ROW_STATUS_OPTIONS.map((item) => ({ value: item, label: formatLabel(item) })),
    []
  );
  const decisionOptions = useMemo<SelectOption[]>(
    () => [{ value: "", label: "—" }, ...ROW_DECISION_OPTIONS.map((item) => ({ value: item, label: formatLabel(item) }))],
    []
  );
  const filterStatusOptions = useMemo<SelectOption[]>(
    () => ROW_STATUS_OPTIONS.map((item) => ({ value: item, label: formatLabel(item) })),
    []
  );
  const filterDecisionOptions = useMemo<SelectOption[]>(
    () => ROW_DECISION_OPTIONS.map((item) => ({ value: item, label: formatLabel(item) })),
    []
  );
  const priorityOptions = useMemo<SelectOption[]>(
    () => PRIORITY_OPTIONS.map((item) => ({ value: item.value, label: item.label })),
    []
  );
  const talById = useMemo(
    () => new Map(tals.map((tal) => [tal.id, tal])),
    [tals]
  );
  const talNameById = useMemo(
    () => new Map(tals.map((tal) => [tal.id, tal.name || tal.id])),
    [tals]
  );
  const channelNameBySlug = useMemo(
    () => new Map(channels.map((item) => [item.slug, item.name || item.slug])),
    [channels]
  );
  const channelOptions = useMemo<SelectOption[]>(
    () => channels.map((item) => ({ value: item.slug, label: item.name || item.slug, hint: item.name && item.name !== item.slug ? item.slug : undefined })),
    [channels]
  );
  const talOptions = useMemo<SelectOption[]>(
    () => [
      { value: "", label: "No TAL", hint: "Hypothesis is not linked to a Territory Account List yet." },
      ...tals.map((tal) => ({
        value: tal.id,
        label: tal.name,
        hint: tal.criteria || tal.description || undefined
      }))
    ],
    [tals]
  );
  const verticalOptions = useMemo<SelectOption[]>(
    () => verticals.map((item) => ({ value: item.name, label: item.name })),
    [verticals]
  );
  const subverticalOptions = useMemo<SelectOption[]>(
    () => filteredSubverticalOptions.map((item) => ({ value: item.name, label: item.name })),
    [filteredSubverticalOptions]
  );
  const companyScaleOptions = useMemo<SelectOption[]>(
    () => companyScales.map((item) => ({ value: item.name, label: item.name })),
    [companyScales]
  );
  const companyProfileOptions = useMemo<SelectOption[]>(
    () =>
      companyProfiles.map((profile) => ({
        value: profile.id,
        label: companyProfileLabel(profile),
        hint: [profile.region, (profile.tech_stack ?? []).join(", ")].filter(Boolean).join(" · ") || undefined
      })),
    [companyProfiles]
  );
  const roleNameById = useMemo(() => new Map(roles.map((item) => [item.id, item.name])), [roles]);
  const verticalNameById = useMemo(() => new Map(verticals.map((item) => [item.id, item.name])), [verticals]);
  const subverticalNameById = useMemo(() => new Map(subverticals.map((item) => [item.id, item.name])), [subverticals]);
  const companyScaleNameById = useMemo(() => new Map(companyScales.map((item) => [item.id, item.name])), [companyScales]);
  const companyProfileById = useMemo(() => new Map(companyProfiles.map((item) => [item.id, item])), [companyProfiles]);
  const segmentTemplates = useMemo(() => {
    const latestBySourceRowId = new Map<string, SegmentTemplate>();
    for (const row of matrixRows) {
      const roleId = String(row.role_id ?? "").trim();
      if (!roleId) continue;
      const companyProfileId = String(row.company_profile_id ?? "").trim() || findMatchingCompanyProfileId(
        companyProfiles,
        parseList(row.vertical_name),
        parseList(row.sub_vertical),
        parseList(row.company_scale)
      );
      const companyProfile = companyProfileId ? companyProfileById.get(companyProfileId) ?? null : null;
      const roleName = roleNameById.get(roleId);
      if (!roleName) continue;
      const meta = parseSegmentMeta(row.notes);
      latestBySourceRowId.set(row.id, {
        source_row_id: row.id,
        source_title: row.title ?? "VP Library",
        role_id: roleId,
        role_name: roleName,
        company_profile_id: companyProfileId,
        company_profile_label: companyProfile ? companyProfileLabel(companyProfile) : "No company profile",
        vp_point: row.vp_point ?? null,
        job_to_be_done: meta.job_to_be_done || null,
        decision_context: row.decision_context ?? null,
        pain: row.pain ?? null,
        outcome_metric: meta.outcome_metric || null,
        updated_at: row.updated_at ?? row.created_at ?? null
      });
    }
    return Array.from(latestBySourceRowId.values());
  }, [companyProfileById, companyProfiles, matrixRows, roleNameById]);
  const segmentTemplateBySourceRowId = useMemo(
    () => new Map(segmentTemplates.map((template) => [template.source_row_id, template])),
    [segmentTemplates]
  );
  // Map row.id → live VP fields from library (for table display + search)
  const liveRowFields = useMemo(() => {
    const map = new Map<string, { vp_point: string; decision_context: string; pain: string; job_to_be_done: string; outcome_metric: string }>();
    for (const row of rows) {
      const meta = parseHypothesisMeta(row.notes);
      const posKey = meta.position_source_row_id;
      if (!posKey) continue;
      const tpl = segmentTemplateBySourceRowId.get(posKey);
      if (!tpl) continue;
      map.set(row.id, {
        vp_point: String(tpl.vp_point ?? row.vp_point ?? "").trim(),
        decision_context: String(tpl.decision_context ?? row.decision_context ?? "").trim(),
        pain: String(tpl.pain ?? row.pain ?? "").trim(),
        job_to_be_done: String(tpl.job_to_be_done ?? "").trim(),
        outcome_metric: String(tpl.outcome_metric ?? "").trim()
      });
    }
    return map;
  }, [rows, segmentTemplateBySourceRowId]);
  const segmentTemplateByMatchKey = useMemo(
    () =>
      new Map(
        segmentTemplates.map((template) => [
          buildPositionMatchKey(template.role_id, template.company_profile_id, template.vp_point),
          template
        ])
      ),
    [segmentTemplates]
  );
  const selectedSegmentTemplate = useMemo(() => {
    // Auto-resolve: VP Point + Company Profile + primary role
    if (!rowDraft.company_profile_id || !rowDraft.role_ids.length || !rowDraft.vp_point.trim()) return null;
    // Try primary role first
    const primaryMatch = segmentTemplateByMatchKey.get(buildPositionMatchKey(rowDraft.role_ids[0], rowDraft.company_profile_id, rowDraft.vp_point));
    if (primaryMatch) return primaryMatch;
    // Fallback: try position_key if set
    if (rowDraft.position_key) {
      return segmentTemplateBySourceRowId.get(rowDraft.position_key) ?? null;
    }
    return null;
  }, [rowDraft.company_profile_id, rowDraft.position_key, rowDraft.role_ids, rowDraft.vp_point, segmentTemplateByMatchKey, segmentTemplateBySourceRowId]);
  const liveDraftSnapshot = useMemo(
    () => snapshotValuesFromDraft(rowDraft, selectedSegmentTemplate),
    [rowDraft, selectedSegmentTemplate]
  );

  const [positionVpFilter, setPositionVpFilter] = useState("");
  const vpPointOptions = useMemo<SelectOption[]>(() => {
    const counts = new Map<string, number>();
    for (const template of segmentTemplates) {
      const vpPoint = normalizeVpPoint(template.vp_point);
      if (!vpPoint) continue;
      counts.set(vpPoint, (counts.get(vpPoint) ?? 0) + 1);
    }
    return Array.from(counts.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([vpPoint, count]) => ({
        value: vpPoint,
        label: vpPoint,
        hint: `${count} position${count === 1 ? "" : "s"}`
      }));
  }, [segmentTemplates]);
  const allPositionOptions = useMemo<SelectOption[]>(
    () =>
      segmentTemplates
        .sort((a, b) => {
          const byVp = normalizeVpPoint(a.vp_point).localeCompare(normalizeVpPoint(b.vp_point));
          if (byVp !== 0) return byVp;
          const byCompany = a.company_profile_label.localeCompare(b.company_profile_label);
          if (byCompany !== 0) return byCompany;
          return a.role_name.localeCompare(b.role_name);
        })
        .map((template) => ({
          value: template.source_row_id,
          label: `${template.role_name} × ${template.company_profile_label}`,
          hint: template.vp_point ?? undefined
        })),
    [segmentTemplates]
  );
  const positionOptions = useMemo<SelectOption[]>(
    () => {
      const normalizedFilter = normalizeVpPoint(positionVpFilter).toLowerCase();
      if (!normalizedFilter) return allPositionOptions;
      return allPositionOptions.filter((option) => normalizeVpPoint(option.hint).toLowerCase() === normalizedFilter);
    },
    [allPositionOptions, positionVpFilter]
  );

  function updateDraft(nextDraft: RowDraft | ((draft: RowDraft) => RowDraft)) {
    setDraftDirty(true);
    setRowDraft(nextDraft);
  }

  function hydrateDraft(nextRow: HypothesisRow | null) {
    setRowDraft(toDraft(nextRow, roles, verticals, subverticals, companyScales, companyProfiles));
    setDraftDirty(false);
  }

  function confirmDiscardDraft() {
    if (!draftDirty) return true;
    return window.confirm("You have unsaved changes in the hypothesis editor. Discard them?");
  }

  async function ensureRegistry() {
    if (!supabase) return null;
    setStatus("Loading registry...");
    const session = await supabase.auth.getSession();
    if (!session.data.session) {
      setStatus("Not signed in. Go back to / and sign in.");
      return null;
    }

    const existing = await supabase
      .from("sales_hypotheses")
      .select("id,title,status,vertical_name,opps_in_progress_count,updated_at,created_at")
      .eq("title", "Hypotheses Registry")
      .order("created_at", { ascending: true })
      .limit(1)
      .maybeSingle();
    if (existing.error) {
      setStatus(`Registry load error: ${existing.error.message}`);
      return null;
    }
    if (existing.data) {
      setRegistry(existing.data as RegistryRow);
      setStatus("");
      return existing.data as RegistryRow;
    }

    const created = await supabase
      .from("sales_hypotheses")
      .insert({
        title: "Hypotheses Registry",
        status: "active",
        timebox_days: 28
      })
      .select("id,title,status,vertical_name,opps_in_progress_count,updated_at,created_at")
      .single();
    if (created.error) {
      setStatus(`Registry create error: ${created.error.message}`);
      return null;
    }
    setRegistry(created.data as RegistryRow);
    setStatus("");
    return created.data as RegistryRow;
  }

  async function loadRoles() {
    if (!supabase) return;
    const { data, error } = await supabase
      .from("sales_icp_roles")
      .select("id,name")
      .order("name", { ascending: true })
      .limit(500);
    if (!error) setRoles((data ?? []) as RoleOption[]);
  }

  async function ensureMatrixLibrary() {
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

  async function loadMatrixLibrary() {
    if (!supabase) return;
    const hypothesisId = await ensureMatrixLibrary();
    if (!hypothesisId) return;
    setMatrixHypothesisId(hypothesisId);
    const rowsRes = await supabase
      .from("sales_hypothesis_rows")
      .select("id,title,role_id,role_label,company_profile_id,vertical_name,sub_vertical,company_scale,vp_point,decision_context,pain,expected_signal,disqualifiers,notes,updated_at,created_at")
      .eq("workspace_id", hypothesisId)
      .order("updated_at", { ascending: false })
      .limit(5000);
    if (!rowsRes.error) setMatrixRows((rowsRes.data ?? []) as MatrixLibraryRow[]);
  }

  async function loadTaxonomy() {
    if (!supabase) return;
    const [verticalRes, subverticalRes, scaleRes, companyProfileRes, channelRes, talRes, ownerRes] = await Promise.all([
      supabase.from("sales_verticals").select("id,name").eq("is_active", true).order("sort_order", { ascending: true }).order("name", { ascending: true }).limit(500),
      supabase.from("sales_subverticals").select("id,vertical_id,name").eq("is_active", true).order("sort_order", { ascending: true }).order("name", { ascending: true }).limit(1000),
      supabase.from("sales_company_scales").select("id,name").eq("is_active", true).order("sort_order", { ascending: true }).order("name", { ascending: true }).limit(500),
      supabase.from("sales_icp_company_profiles").select("id,vertical_name,sub_vertical,region,size_bucket,tech_stack,notes").order("vertical_name", { ascending: true }).order("sub_vertical", { ascending: true }).limit(500),
      supabase.from("sales_channels").select("slug,name").eq("is_active", true).order("sort_order", { ascending: true }).order("name", { ascending: true }).limit(200),
      supabase.from("tal_analytics_v").select("id,name,criteria,description,email_sent,email_replies,email_reply_rate,email_meetings,email_held_meetings,li_invited,li_accepted,li_replies,li_accept_rate,li_meetings,li_held_meetings,app_touches,app_replies,app_reply_rate,app_meetings,app_held_meetings,tg_touches,tg_replies,tg_reply_rate,tg_meetings,tg_held_meetings,total_meetings,total_held_meetings").order("name", { ascending: true }).limit(500),
      supabase.from("hypothesis_owners").select("id,name").order("name", { ascending: true }).limit(50)
    ]);
    if (!verticalRes.error) setVerticals((verticalRes.data ?? []) as VerticalOption[]);
    if (!subverticalRes.error) setSubverticals((subverticalRes.data ?? []) as SubverticalOption[]);
    if (!scaleRes.error) setCompanyScales((scaleRes.data ?? []) as CompanyScaleOption[]);
    if (!companyProfileRes.error) setCompanyProfiles((companyProfileRes.data ?? []) as CompanyProfileOption[]);
    if (!channelRes.error) setChannels((channelRes.data ?? []) as ChannelOption[]);
    if (!talRes.error) setTals((talRes.data ?? []) as TalOption[]);
    if (!ownerRes.error) setOwners((ownerRes.data ?? []) as OwnerOption[]);
  }

  async function loadRows(registryId: string) {
    if (!supabase || !registryId) return;
    setRowsLoading(true);
    const rowsTable = supabase.from("sales_hypothesis_rows") as any;
    const selectRows = async (includeTalId: boolean) => {
      const talField = includeTalId ? "\n        tal_id," : "";
      return rowsTable
        .select(`
          id,
          workspace_id,
          title,${talField}
          role_id,
          role_label,
          company_profile_id,
          vertical_id,
          subvertical_id,
          company_scale_id,
          vertical_name,
          sub_vertical,
          company_scale,
          signal_speed,
          decision_context,
          vp_point,
          pain,
          expected_signal,
          disqualifiers,
          calls_count,
          pain_confirmed_rate,
          severity_rate,
          interest_rate,
          decision,
          status,
          priority,
          owner_id,
          notes,
          updated_at,
          created_at,
          role:sales_icp_roles(name)
        `)
        .eq("workspace_id", registryId)
        .order("priority", { ascending: false })
        .order("updated_at", { ascending: false })
        .limit(1000);
    };
    let { data, error } = await selectRows(true);
    if (error && isMissingTalIdColumnError(error.message)) {
      setTalLinkAvailable(false);
      setTalLinkNotice("TAL column is not in Supabase yet. Selection still works and is stored in hypothesis metadata until migration 20260401101500_sales_hypothesis_rows_add_tal_id.sql is applied.");
      ({ data, error } = await selectRows(false));
    } else {
      setTalLinkAvailable(true);
      setTalLinkNotice("");
    }
    setRowsLoading(false);
    if (error) {
      setStatus(`Rows load error: ${error.message}`);
      return;
    }
    const nextRows = ((data ?? []) as any[]).map((row) => ({
      ...row,
      tal_id: row.tal_id ?? parseHypothesisMeta(row.notes).tal_id ?? null,
      channel: row.channel ?? row.signal_speed ?? null,
      role: Array.isArray(row.role) ? row.role[0] ?? null : row.role ?? null
    })) as HypothesisRow[];
    setRows(nextRows);
    setSelectedRowId((current) => {
      if (current && nextRows.some((row) => row.id === current)) return current;
      return nextRows[0]?.id ?? "";
    });
    setStatus("");
  }

  async function ensureVertical(name: string) {
    if (!supabase) return null;
    const normalized = cleanText(name);
    if (!normalized) return null;
    const existing = verticals.find((item) => item.name.toLowerCase() === normalized.toLowerCase());
    if (existing) return existing;
    const inserted = await supabase.from("sales_verticals").insert({ name: normalized }).select("id,name").single();
    if (inserted.error) throw new Error(inserted.error.message);
    return inserted.data as VerticalOption;
  }

  async function ensureSubvertical(verticalId: string, name: string) {
    if (!supabase) return null;
    const normalized = cleanText(name);
    if (!normalized || !verticalId) return null;
    const existing = subverticals.find((item) => item.vertical_id === verticalId && item.name.toLowerCase() === normalized.toLowerCase());
    if (existing) return existing;
    const inserted = await supabase.from("sales_subverticals").insert({ vertical_id: verticalId, name: normalized }).select("id,vertical_id,name").single();
    if (inserted.error) throw new Error(inserted.error.message);
    return inserted.data as SubverticalOption;
  }

  async function ensureCompanyScale(name: string) {
    if (!supabase) return null;
    const normalized = cleanText(name);
    if (!normalized) return null;
    const existing = companyScales.find((item) => item.name.toLowerCase() === normalized.toLowerCase());
    if (existing) return existing;
    const inserted = await supabase.from("sales_company_scales").insert({ name: normalized }).select("id,name").single();
    if (inserted.error) throw new Error(inserted.error.message);
    return inserted.data as CompanyScaleOption;
  }

  useEffect(() => {
    if (!supabase) return;
    let cancelled = false;
    (async () => {
      const currentRegistry = await ensureRegistry();
      if (!currentRegistry || cancelled) return;
      await Promise.all([loadRoles(), loadTaxonomy(), loadRows(currentRegistry.id), loadMatrixLibrary()]);
    })();
    return () => {
      cancelled = true;
    };
  }, [supabase]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (typeof window === "undefined") return;
    setInitialFocusId(String(new URLSearchParams(window.location.search).get("focus") ?? "").trim());
  }, []);

  useEffect(() => {
    if (draftDirty) return;
    hydrateDraft(selectedRow);
  }, [selectedRow, roles, verticals, subverticals, companyScales, companyProfiles, draftDirty]);

  useEffect(() => {
    if (!selectedRowId || draftDirty) return;
    setRowDraft((draft) => {
      let changed = false;
      const nextDraft = { ...draft };

      if (!nextDraft.role_ids.length && nextDraft.role_labels.length && roles.length) {
        nextDraft.role_ids = nextDraft.role_labels
          .map((item) => roles.find((role) => role.name.toLowerCase() === item.toLowerCase())?.id ?? "")
          .filter(Boolean);
        changed = true;
      }

      if (!nextDraft.vertical_ids.length && nextDraft.vertical_names.length && verticals.length) {
        nextDraft.vertical_ids = nextDraft.vertical_names
          .map((item) => verticals.find((vertical) => vertical.name.toLowerCase() === item.toLowerCase())?.id ?? "")
          .filter(Boolean);
        changed = true;
      }

      if (!nextDraft.subvertical_ids.length && nextDraft.sub_verticals.length && subverticals.length) {
        nextDraft.subvertical_ids = nextDraft.sub_verticals
          .map((item) => {
            const match = subverticals.find((subvertical) => {
              if (subvertical.name.toLowerCase() !== item.toLowerCase()) return false;
              if (!nextDraft.vertical_ids.length) return true;
              return nextDraft.vertical_ids.includes(subvertical.vertical_id);
            });
            return match?.id ?? "";
          })
          .filter(Boolean);
        changed = true;
      }

      if (!nextDraft.company_scale_ids.length && nextDraft.company_scales.length && companyScales.length) {
        nextDraft.company_scale_ids = nextDraft.company_scales
          .map((item) => companyScales.find((scale) => scale.name.toLowerCase() === item.toLowerCase())?.id ?? "")
          .filter(Boolean);
        changed = true;
      }

      if (!nextDraft.company_profile_id && companyProfiles.length) {
        const matchedCompanyProfileId = findMatchingCompanyProfileId(companyProfiles, nextDraft.vertical_names, nextDraft.sub_verticals, nextDraft.company_scales);
        if (matchedCompanyProfileId) {
          nextDraft.company_profile_id = matchedCompanyProfileId;
          changed = true;
        }
      }

      return changed ? nextDraft : draft;
    });
  }, [selectedRowId, roles, verticals, subverticals, companyScales, companyProfiles, draftDirty]);

  useEffect(() => {
    if (!initialFocusId || !rows.length || isEditorOpen || focusConsumed) return;
    const target = rows.find((row) => row.id === initialFocusId);
    if (!target) return;
    setSelectedRowId(target.id);
    setIsEditorOpen(true);
    setFocusConsumed(true);
  }, [focusConsumed, initialFocusId, isEditorOpen, rows]);

  useEffect(() => {
    document.body.style.overflow = isEditorOpen ? "hidden" : "";
    return () => { document.body.style.overflow = ""; };
  }, [isEditorOpen]);

  useEffect(() => {
    // Auto-set position_key from resolved template
    if (selectedSegmentTemplate && !rowDraft.position_key) {
      setRowDraft((draft) => ({ ...draft, position_key: selectedSegmentTemplate.source_row_id }));
    }
  }, [rowDraft.position_key, selectedSegmentTemplate]);

  function setRoleValues(nextValues: string[]) {
    const selectedOptions = nextValues
      .map((value) => roles.find((role) => role.id === value) ?? null)
      .filter(Boolean) as RoleOption[];
    updateDraft((draft) => ({
      ...draft,
      role_ids: selectedOptions.map((item) => item.id),
      role_labels: selectedOptions.map((item) => item.name),
      position_key: ""
    }));
  }

  function setCompanyProfileValue(nextValue: string) {
    const profile = companyProfiles.find((item) => item.id === nextValue) ?? null;
    if (!profile) {
      updateDraft((draft) => ({ ...draft, company_profile_id: "" }));
      return;
    }
    const verticalName = String(profile.vertical_name ?? "").trim();
    const subVertical = String(profile.sub_vertical ?? "").trim();
    const sizeBucket = String(profile.size_bucket ?? "").trim();
    const matchedVertical = verticals.find((item) => item.name.toLowerCase() === verticalName.toLowerCase()) ?? null;
    const matchedSubvertical = subVertical
      ? subverticals.find((item) => item.name.toLowerCase() === subVertical.toLowerCase() && (!matchedVertical || item.vertical_id === matchedVertical.id)) ?? null
      : null;
    const matchedScale = sizeBucket
      ? companyScales.find((item) => item.name.toLowerCase() === sizeBucket.toLowerCase()) ?? null
      : null;
    updateDraft((draft) => {
      // Auto-populate roles from library for this VP Point + Company Profile
      let autoRoleIds = draft.role_ids;
      let autoRoleLabels = draft.role_labels;
      if (draft.vp_point.trim()) {
        const vpKey = draft.vp_point.trim().toLowerCase();
        const matching = segmentTemplates.filter((t) =>
          (t.vp_point ?? "").trim().toLowerCase() === vpKey
          && t.company_profile_id === profile.id
        );
        if (matching.length > 0) {
          autoRoleIds = Array.from(new Set(matching.map((t) => t.role_id)));
          autoRoleLabels = autoRoleIds.map((id) => roles.find((r) => r.id === id)?.name ?? "").filter(Boolean);
        }
      }
      return {
        ...draft,
        company_profile_id: profile.id,
        position_key: "",
        role_ids: autoRoleIds,
        role_labels: autoRoleLabels,
        vertical_names: verticalName ? [verticalName] : [],
        vertical_ids: matchedVertical ? [matchedVertical.id] : [],
        sub_verticals: subVertical ? [subVertical] : [],
        subvertical_ids: matchedSubvertical ? [matchedSubvertical.id] : [],
        company_scales: sizeBucket ? [sizeBucket] : [],
        company_scale_ids: matchedScale ? [matchedScale.id] : []
      };
    });
  }

  function setVerticalValues(nextValues: string[]) {
    const normalizedValues = nextValues.map((item) => item.trim()).filter(Boolean);
    const matchedVerticals = normalizedValues
      .map((item) => verticals.find((vertical) => vertical.name.toLowerCase() === item.toLowerCase()) ?? null)
      .filter(Boolean) as VerticalOption[];
    const nextVerticalIds = matchedVerticals.map((item) => item.id);
    const nextSubVerticals = rowDraft.sub_verticals.filter((subName) => {
      const subMatch = subverticals.find((subvertical) => subvertical.name.toLowerCase() === subName.toLowerCase());
      return !subMatch || nextVerticalIds.includes(subMatch.vertical_id);
    });
    const nextSubverticalIds = rowDraft.subvertical_ids.filter((subId) => {
      const subMatch = subverticals.find((subvertical) => subvertical.id === subId);
      return !subMatch || nextVerticalIds.includes(subMatch.vertical_id);
    });
    updateDraft((draft) => ({
      ...draft,
      vertical_names: normalizedValues,
      vertical_ids: nextVerticalIds,
      sub_verticals: nextSubVerticals,
      subvertical_ids: nextSubverticalIds,
      company_profile_id: findMatchingCompanyProfileId(companyProfiles, normalizedValues, nextSubVerticals, draft.company_scales)
    }));
  }

  function setSubverticalValues(nextValues: string[]) {
    const normalizedValues = nextValues.map((item) => item.trim()).filter(Boolean);
    const matchedSubverticals = normalizedValues
      .map((item) => {
        return filteredSubverticalOptions.find((subvertical) => subvertical.name.toLowerCase() === item.toLowerCase())
          ?? subverticals.find((subvertical) => subvertical.name.toLowerCase() === item.toLowerCase())
          ?? null;
      })
      .filter(Boolean) as SubverticalOption[];
    const nextVerticalIds = Array.from(new Set([
      ...rowDraft.vertical_ids,
      ...matchedSubverticals.map((item) => item.vertical_id)
    ]));
    const nextVerticalNames = Array.from(new Set([
      ...rowDraft.vertical_names,
      ...matchedSubverticals.map((item) => verticalById.get(item.vertical_id) ?? "").filter(Boolean)
    ]));
    updateDraft((draft) => ({
      ...draft,
      sub_verticals: normalizedValues,
      subvertical_ids: matchedSubverticals.map((item) => item.id),
      vertical_ids: nextVerticalIds,
      vertical_names: nextVerticalNames,
      company_profile_id: findMatchingCompanyProfileId(companyProfiles, nextVerticalNames, normalizedValues, draft.company_scales)
    }));
  }

  function setCompanyScaleValues(nextValues: string[]) {
    const normalizedValues = nextValues
      .map((item) => item.trim())
      .filter(Boolean);
    const ids = normalizedValues
      .map((item) => companyScales.find((scale) => scale.name.toLowerCase() === item.toLowerCase())?.id ?? "")
      .filter(Boolean);
    updateDraft((draft) => ({
      ...draft,
      company_scales: normalizedValues,
      company_scale_ids: ids,
      company_profile_id: findMatchingCompanyProfileId(companyProfiles, draft.vertical_names, draft.sub_verticals, normalizedValues)
    }));
  }

  function setChannelValues(nextValues: string[]) {
    const normalizedValues = Array.from(
      new Set(nextValues.map((item) => item.trim()).filter(Boolean))
    );
    updateDraft((draft) => ({ ...draft, channels: normalizedValues }));
  }

  function applyPositionTemplate(template: SegmentTemplate) {
    const companyProfile = companyProfileById.get(template.company_profile_id) ?? null;
    // Find ALL roles for the same VP Point + Company Profile in the library
    const vpKey = (template.vp_point ?? "").trim().toLowerCase();
    const siblingTemplates = segmentTemplates.filter((t) =>
      (t.vp_point ?? "").trim().toLowerCase() === vpKey
      && t.company_profile_id === template.company_profile_id
    );
    const allRoleIds = Array.from(new Set(siblingTemplates.map((t) => t.role_id).filter(Boolean)));
    const allRoleLabels = allRoleIds.map((id) => roles.find((r) => r.id === id)?.name ?? "").filter(Boolean);
    updateDraft((draft) => ({
      ...draft,
      position_key: template.source_row_id,
      company_profile_id: template.company_profile_id,
      role_ids: allRoleIds.length > 0 ? allRoleIds : [template.role_id],
      role_labels: allRoleLabels.length > 0 ? allRoleLabels : [template.role_name],
      vertical_names: companyProfile?.vertical_name ? [companyProfile.vertical_name] : [],
      vertical_ids: [],
      sub_verticals: companyProfile?.sub_vertical ? [companyProfile.sub_vertical] : [],
      subvertical_ids: [],
      company_scales: companyProfile?.size_bucket ? [companyProfile.size_bucket] : [],
      company_scale_ids: [],
      title: draft.title.trim() ? draft.title : (template.source_title ?? draft.title),
      vp_point: template.vp_point ?? draft.vp_point,
      decision_context: template.decision_context ?? draft.decision_context,
      pain: template.pain ?? draft.pain,
      job_to_be_done: template.job_to_be_done ?? draft.job_to_be_done,
      outcome_metric: template.outcome_metric ?? draft.outcome_metric
    }));
  }

  function setPositionValue(nextValue: string) {
    const template = segmentTemplateBySourceRowId.get(nextValue);
    if (!template) {
      updateDraft((draft) => ({ ...draft, position_key: "", company_profile_id: "", role_ids: [], role_labels: [] }));
      return;
    }
    setPositionVpFilter(normalizeVpPoint(template.vp_point));
    applyPositionTemplate(template);
  }

  function setStatusValue(nextStatus: string) {
    updateDraft((draft) => {
      const nextDraft = { ...draft, status: nextStatus };
      if (selectedSegmentTemplate) {
        const snapshot = snapshotValuesFromDraft(nextDraft, selectedSegmentTemplate);
        return {
          ...nextDraft,
          vp_point: snapshot.vp_point,
          decision_context: snapshot.decision_context,
          pain: snapshot.pain,
          job_to_be_done: snapshot.job_to_be_done,
          outcome_metric: snapshot.outcome_metric
        };
      }
      return nextDraft;
    });
  }

  function openEditorForRow(rowId: string) {
    if (rowActionsBusy) return;
    if (rowId === selectedRowId && isEditorOpen) {
      if (!confirmDiscardDraft()) return;
      hydrateDraft(selectedRow);
      setIsEditorOpen(false);
      return;
    }
    if (rowId !== selectedRowId && !confirmDiscardDraft()) return;
    if (rowId !== selectedRowId) setDraftDirty(false);
    setSelectedRowId(rowId);
    setIsEditorOpen(true);
  }

  function closeEditor() {
    if (editorBusy) return;
    if (!confirmDiscardDraft()) return;
    hydrateDraft(selectedRow);
    setIsEditorOpen(false);
  }

  async function reloadRegistryRows() {
    if (!registry || rowActionsBusy) return;
    if (!confirmDiscardDraft()) return;
    setDraftDirty(false);
    setIsEditorOpen(false);
    setReloadingRows(true);
    try {
      await loadRows(registry.id);
      setStatus("");
    } finally {
      setReloadingRows(false);
    }
  }

  async function addRow() {
    if (!registry || rowActionsBusy) return;
    if (!confirmDiscardDraft()) return;
    setSelectedRowId("");
    hydrateDraft(null);
    setIsEditorOpen(true);
    setStatus("New hypothesis draft.");
  }

  async function updateRowChannels(rowId: string, nextValues: string[]) {
    if (!supabase || !registry || updatingChannelsRowId === rowId) return;
    const normalized = Array.from(new Set(nextValues.map((item) => item.trim()).filter(Boolean)));
    setUpdatingChannelsRowId(rowId);
    setStatus("Saving channels...");
    try {
      const { error } = await supabase
        .from("sales_hypothesis_rows")
        .update({ signal_speed: normalized.length ? normalized.join(", ") : null })
        .eq("id", rowId);
      if (error) {
        setStatus(`Channels update error: ${error.message}`);
        return;
      }
      setRows((prev) =>
        prev.map((row) => (row.id === rowId ? { ...row, channel: normalized.length ? normalized.join(", ") : null } : row))
      );
      if (selectedRowId === rowId) {
        setRowDraft((draft) => ({ ...draft, channels: normalized }));
      }
      setStatus("Channels saved.");
    } finally {
      setUpdatingChannelsRowId("");
    }
  }

  async function saveRow() {
    if (!supabase) { setStatus("No database connection."); return; }
    if (!registry) { setStatus("Registry not loaded."); return; }
    if (savingRow) { setStatus("Already saving..."); return; }
    const isNewRow = !selectedRowId;
    if (isNewRow && !rowDraft.vp_point.trim()) {
      setStatus("VP Point is required.");
      return;
    }
    if (isNewRow && !rowDraft.company_profile_id) {
      setStatus("Company profile is required. Select a position that has a company profile.");
      return;
    }
    if (isNewRow && rowDraft.role_ids.length < 1) {
      setStatus("At least one role is required.");
      return;
    }
    setSavingRow(true);
    setStatus("Saving row...");
    try {
      const nextRoleIds = rowDraft.role_ids.filter(Boolean);
      const nextRoleLabels = nextRoleIds
        .map((id) => roles.find((item) => item.id === id)?.name ?? "")
        .filter(Boolean);

      const nextVerticalIds: string[] = [];
      const nextVerticalNames: string[] = [];
      for (const value of rowDraft.vertical_names) {
        const normalized = cleanText(value);
        if (!normalized) continue;
        const vertical = await ensureVertical(normalized);
        if (!vertical) continue;
        nextVerticalIds.push(vertical.id);
        nextVerticalNames.push(vertical.name);
      }

      const nextSubverticalIds: string[] = [];
      const nextSubverticalNames: string[] = [];
      if (rowDraft.sub_verticals.length && !nextVerticalIds.length) {
        setStatus("Vertical is required before sub-vertical.");
        setSavingRow(false);
        return;
      }
      for (const value of rowDraft.sub_verticals) {
        const normalized = cleanText(value);
        if (!normalized) continue;
        const existingSubvertical = filteredSubverticalOptions.find((item) => item.name.toLowerCase() === normalized.toLowerCase())
          ?? subverticals.find((item) => item.name.toLowerCase() === normalized.toLowerCase() && (!nextVerticalIds.length || nextVerticalIds.includes(item.vertical_id)));
        if (existingSubvertical) {
          nextSubverticalIds.push(existingSubvertical.id);
          nextSubverticalNames.push(existingSubvertical.name);
          continue;
        }
        if (nextVerticalIds.length !== 1) {
          setStatus("Pick exactly one vertical before creating a new sub-vertical.");
          setSavingRow(false);
          return;
        }
        const subvertical = await ensureSubvertical(nextVerticalIds[0], normalized);
        if (subvertical) {
          nextSubverticalIds.push(subvertical.id);
          nextSubverticalNames.push(subvertical.name);
        }
      }

      const nextCompanyScaleNames: string[] = [];
      const nextCompanyScaleIds: string[] = [];
      for (const value of rowDraft.company_scales) {
        const normalized = cleanText(value);
        if (!normalized) continue;
        const scale = await ensureCompanyScale(normalized);
        if (!scale) continue;
        nextCompanyScaleNames.push(scale.name);
        nextCompanyScaleIds.push(scale.id);
      }

      const snapshot = snapshotValuesFromDraft(rowDraft, selectedSegmentTemplate);
      const payload = {
        title: cleanText(rowDraft.title) ?? cleanText(rowDraft.vp_point),
        ...(talLinkAvailable ? { tal_id: rowDraft.tal_id || null } : {}),
        role_id: nextRoleIds[0] ?? null,
        role_label: nextRoleLabels.length ? nextRoleLabels.join(", ") : null,
        company_profile_id: rowDraft.company_profile_id || null,
        vertical_id: nextVerticalIds[0] ?? null,
        vertical_name: nextVerticalNames.length ? nextVerticalNames.join(", ") : null,
        subvertical_id: nextSubverticalIds[0] ?? null,
        sub_vertical: nextSubverticalNames.length ? nextSubverticalNames.join(", ") : null,
        company_scale_id: nextCompanyScaleIds[0] ?? null,
        company_scale: nextCompanyScaleNames.length ? nextCompanyScaleNames.join(", ") : null,
        signal_speed: rowDraft.channels.length ? rowDraft.channels.join(", ") : null,
        decision_context: cleanText(snapshot.decision_context),
        vp_point: snapshot.vp_point,
        pain: cleanText(snapshot.pain),
        expected_signal: cleanText(rowDraft.expected_signal),
        disqualifiers: cleanText(rowDraft.disqualifiers),
        calls_count: parseIntOrZero(rowDraft.calls_count),
        pain_confirmed_rate: parseNumberOrNull(rowDraft.pain_confirmed_rate),
        severity_rate: parseNumberOrNull(rowDraft.severity_rate),
        interest_rate: parseNumberOrNull(rowDraft.interest_rate),
        decision: cleanText(rowDraft.decision),
        status: rowDraft.status || "draft",
        priority: priorityValueToScore(rowDraft.priority),
        owner_id: rowDraft.owner_id || null,
        notes: serializeHypothesisMeta({
          ...rowDraft,
          job_to_be_done: snapshot.job_to_be_done,
          outcome_metric: snapshot.outcome_metric
        })
      };

      const isCreating = !selectedRowId;
      const response = isCreating
        ? await supabase.from("sales_hypothesis_rows").insert({ workspace_id: registry.id, source: "manual", ...payload }).select("id").single()
        : await supabase.from("sales_hypothesis_rows").update(payload).eq("id", selectedRowId).select("id").single();
      console.log("[saveRow] response", response);
      if (response.error) {
        setStatus(`${isCreating ? "Row insert" : "Row update"} error: ${response.error.message}`);
        setSavingRow(false);
        return;
      }
      setDraftDirty(false);
      await loadRows(registry.id);
      setSelectedRowId(String(response.data?.id ?? ""));
      await Promise.all([loadRoles(), loadTaxonomy()]);
      setIsEditorOpen(false);
      setStatus(isCreating ? "Hypothesis created." : "Hypothesis saved.");
    } catch (error) {
      setStatus(`Hypothesis update error: ${error instanceof Error ? error.message : "Unknown error"}`);
    } finally {
      setSavingRow(false);
    }
  }

  async function deleteRow() {
    if (!supabase || !selectedRowId || !registry || !selectedRow || deletingRow) return;
    if (!confirm(`Delete hypothesis "${selectedRow.title || selectedRow.vp_point}"?`)) return;
    setDeletingRow(true);
    setStatus("Deleting hypothesis...");
    try {
      const { error } = await supabase.from("sales_hypothesis_rows").delete().eq("id", selectedRowId);
      if (error) {
        setStatus(`Hypothesis delete error: ${error.message}`);
        return;
      }
      await loadRows(registry.id);
      setDraftDirty(false);
      setIsEditorOpen(false);
      setStatus("Hypothesis deleted.");
    } finally {
      setDeletingRow(false);
    }
  }

  const filteredRows = rows.filter((row) => {
    const needle = rowSearch.trim().toLowerCase();
    const channelLabel = parseList(row.channel)
      .map((slug) => channelNameBySlug.get(slug) ?? slug)
      .join(", ");
    const talLabel = talNameById.get(row.tal_id ?? "") ?? "";
    if (rowStatusFilter.length && !rowStatusFilter.includes(row.status)) return false;
    if (rowDecisionFilter.length && !rowDecisionFilter.includes(row.decision ?? "")) return false;
    if (!needle) return true;
    const live = liveRowFields.get(row.id);
    return [
      row.title,
      live?.vp_point ?? row.vp_point,
      live?.pain ?? row.pain,
      live?.decision_context ?? row.decision_context,
      row.expected_signal,
      row.disqualifiers,
      verticalNameById.get(row.vertical_id ?? "") ?? row.vertical_name,
      subverticalNameById.get(row.subvertical_id ?? "") ?? row.sub_vertical,
      talLabel,
      channelLabel,
      row.status,
      row.decision,
      roleDisplay(row)
    ]
      .map((value) => String(value ?? "").toLowerCase())
      .some((value) => value.includes(needle));
  });

  const isCreatingRow = !selectedRowId;
  const validatedCount = rows.filter((row) => row.status === "validated").length;
  const draftCount = rows.filter((row) => row.status === "draft").length;
  const activeFilterCount = rowStatusFilter.length + rowDecisionFilter.length + (rowSearch.trim() ? 1 : 0);

  const editorPanel = isEditorOpen ? (
    <>
      <div className="cardHeader">
        <div>
          <div className="cardTitle">{isCreatingRow ? "New Hypothesis" : "Hypothesis Editor"}{draftDirty ? " · Unsaved changes" : ""}</div>
          <div className="cardDesc">
            {isCreatingRow
              ? "Create a new hypothesis directly in the registry."
              : "Edits apply to the selected row directly below the registry."}
          </div>
        </div>
        <div className="btnRow">
          <button className="btn" onClick={closeEditor} disabled={editorBusy}>Close</button>
          <button className="btn btnPrimary" onClick={saveRow} disabled={editorBusy || !registry}>Save hypothesis</button>
          <button className="btn btnDanger" onClick={deleteRow} disabled={!selectedRowId || editorBusy}>Delete hypothesis</button>
        </div>
      </div>
      <div className="cardBody">
        {status ? <div className="notice" style={{ marginBottom: 12 }}>{status}</div> : null}
        {!selectedRow && !isCreatingRow ? (
          <div className="subcard">
            <div style={{ fontWeight: 600, marginBottom: 6 }}>Pick a hypothesis</div>
            <div className="muted2">Select a hypothesis in the registry to edit its fields.</div>
          </div>
        ) : (
          <div className="grid formGridTight">
            <div style={{ gridColumn: "span 4" }}>
              <label className="muted" style={{ fontSize: 12 }}>Title</label>
              <input className="input" value={rowDraft.title} disabled={editorBusy} onChange={(event) => updateDraft((draft) => ({ ...draft, title: event.target.value }))} />
            </div>
            <div style={{ gridColumn: "span 2" }}>
              <label className="muted" style={{ fontSize: 12 }}>Status</label>
              <SelectPopover
                value={rowDraft.status}
                displayValue={statusOptions.find((item) => item.value === rowDraft.status)?.label}
                options={statusOptions}
                placeholder="Status"
                onChange={setStatusValue}
                searchable={false}
                disabled={editorBusy}
              />
            </div>
            <div style={{ gridColumn: "span 2" }}>
              <label className="muted" style={{ fontSize: 12 }}>Priority</label>
              <SelectPopover
                value={rowDraft.priority}
                displayValue={priorityOptions.find((item) => item.value === rowDraft.priority)?.label}
                options={priorityOptions}
                placeholder="Priority"
                onChange={(value) => updateDraft((draft) => ({ ...draft, priority: value }))}
                searchable={false}
                disabled={editorBusy}
              />
            </div>
            <div style={{ gridColumn: "span 2" }}>
              <label className="muted" style={{ fontSize: 12 }}>Decision</label>
              <SelectPopover
                value={rowDraft.decision}
                displayValue={decisionOptions.find((item) => item.value === rowDraft.decision)?.label}
                options={decisionOptions}
                placeholder="—"
                onChange={(value) => updateDraft((draft) => ({ ...draft, decision: value }))}
                searchable={false}
                disabled={editorBusy}
              />
            </div>
            <div style={{ gridColumn: "span 2" }}>
              <label className="muted" style={{ fontSize: 12 }}>Owner</label>
              <SelectPopover
                value={rowDraft.owner_id}
                displayValue={owners.find((o) => o.id === rowDraft.owner_id)?.name}
                options={[{ value: "", label: "—" }, ...owners.map((o) => ({ value: o.id, label: o.name }))]}
                placeholder="—"
                onChange={(value) => updateDraft((draft) => ({ ...draft, owner_id: value }))}
                searchable={false}
                disabled={editorBusy}
              />
            </div>

            <div style={{ gridColumn: "span 12" }}>
              <label className="muted" style={{ fontSize: 12 }}>VP Point</label>
              <SelectPopover
                value={rowDraft.vp_point}
                displayValue={vpPointOptions.find((item) => item.value === rowDraft.vp_point)?.label}
                options={vpPointOptions}
                placeholder="Choose VP Point"
                searchPlaceholder="Search VP Point..."
                emptyMessage="No VP Points in Library yet."
                onChange={(value) => {
                  updateDraft((draft) => ({ ...draft, vp_point: value, position_key: "" }));
                  // Auto-populate roles from library for this VP Point + Company Profile
                  if (rowDraft.company_profile_id) {
                    const vpKey = value.trim().toLowerCase();
                    const matching = segmentTemplates.filter((t) =>
                      (t.vp_point ?? "").trim().toLowerCase() === vpKey
                      && t.company_profile_id === rowDraft.company_profile_id
                    );
                    if (matching.length > 0) {
                      const rIds = Array.from(new Set(matching.map((t) => t.role_id)));
                      const rLabels = rIds.map((id) => roles.find((r) => r.id === id)?.name ?? "").filter(Boolean);
                      setRoleValues(rIds);
                    }
                  }
                }}
                disabled={editorBusy}
              />
            </div>

            <div style={{ gridColumn: "span 12" }}>
              <div className="subcard" style={{ margin: 0 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start", marginBottom: 10 }}>
                  <div>
                    <div style={{ fontWeight: 600, marginBottom: 4 }}>Position source</div>
                    <div className="muted2" style={{ fontSize: 13 }}>
                      {selectedSegmentTemplate
                        ? "Messaging is always synced from Library automatically."
                        : "Pick a position to inherit messaging from the library."}
                    </div>
                  </div>
                </div>
                {rowDraft.vp_point.trim() && (
                  <div style={{ marginTop: 8 }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
                      <label className="muted" style={{ fontSize: 12 }}>Roles ({rowDraft.role_ids.length})</label>
                      <div style={{ display: "flex", gap: 6 }}>
                        <button type="button" className="btn" style={{ fontSize: 11, padding: "2px 8px" }} onClick={() => setRoleValues(roles.map((r) => r.id))}>All</button>
                        <button type="button" className="btn" style={{ fontSize: 11, padding: "2px 8px" }} onClick={() => {
                          const primaryId = rowDraft.role_ids[0];
                          setRoleValues(primaryId ? [primaryId] : []);
                        }}>Reset</button>
                      </div>
                    </div>
                    <div style={{ maxHeight: 160, overflowY: "auto", display: "flex", flexWrap: "wrap", gap: 4 }}>
                      {roles.map((role) => {
                        const checked = rowDraft.role_ids.includes(role.id);
                        return (
                          <label
                            key={role.id}
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              gap: 5,
                              padding: "3px 8px",
                              borderRadius: 5,
                              cursor: "pointer",
                              background: checked ? "rgba(59,130,246,0.12)" : "transparent",
                              border: `1px solid ${checked ? "rgba(59,130,246,0.3)" : "rgba(255,255,255,0.08)"}`,
                              fontSize: 12,
                            }}
                          >
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={() => {
                                const next = checked
                                  ? rowDraft.role_ids.filter((id) => id !== role.id)
                                  : [...rowDraft.role_ids, role.id];
                                setRoleValues(next);
                              }}
                              style={{ margin: 0, width: 13, height: 13 }}
                            />
                            {role.name}
                          </label>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            </div>

            <div style={{ gridColumn: "span 9" }}>
              <label className="muted" style={{ fontSize: 12 }}>Company profile</label>
              <SelectPopover
                value={rowDraft.company_profile_id}
                displayValue={selectedCompanyProfile ? companyProfileLabel(selectedCompanyProfile) : undefined}
                options={companyProfileOptions}
                placeholder="Choose company profile"
                searchPlaceholder="Search company profile..."
                emptyMessage="No company profiles found."
                onChange={setCompanyProfileValue}
                disabled={editorBusy}
              />
              <div className="helpInline">Vertical and sub-vertical are driven by the selected company profile.</div>
            </div>
            <div style={{ gridColumn: "span 6" }}>
              <label className="muted" style={{ fontSize: 12 }}>Company size</label>
              <SelectPopover
                values={rowDraft.company_scales}
                displayValue={rowDraft.company_scales.length ? rowDraft.company_scales.join(", ") : undefined}
                options={companyScaleOptions}
                placeholder="Choose company sizes"
                searchPlaceholder="Search size..."
                emptyMessage="No sizes found."
                onChangeMultiple={setCompanyScaleValues}
                multiple
                disabled={editorBusy}
              />
            </div>
            <div style={{ gridColumn: "span 3" }}>
              <label className="muted" style={{ fontSize: 12 }}>Region</label>
              <SelectPopover
                values={rowDraft.regions}
                displayValue={rowDraft.regions.length ? rowDraft.regions.join(", ") : undefined}
                options={[
                  { value: "EU", label: "EU" },
                  { value: "MENA", label: "MENA" },
                  { value: "APAC", label: "APAC" },
                  { value: "LATAM", label: "LATAM" },
                  { value: "Africa", label: "Africa" },
                  { value: "CIS", label: "CIS" },
                  { value: "Global", label: "Global" },
                ]}
                placeholder="Choose regions"
                searchPlaceholder="Search region..."
                emptyMessage="No regions."
                onChangeMultiple={(next) => updateDraft((d) => ({ ...d, regions: next }))}
                multiple
                disabled={editorBusy}
              />
            </div>
            <div style={{ gridColumn: "span 3" }}>
              <label className="muted" style={{ fontSize: 12 }}>Channels</label>
              {selectedTal ? (
                <>
                  <div className="statField"><span className="statValue">{deriveTalChannels(selectedTal).join(", ") || "—"}</span></div>
                  <div className="helpInline">Auto-derived from TAL campaigns.</div>
                </>
              ) : (
                <>
                  <SelectPopover
                    values={rowDraft.channels}
                    displayValue={rowDraft.channels.length ? rowDraft.channels.map((slug) => channelNameBySlug.get(slug) ?? slug).join(", ") : undefined}
                    options={channelOptions}
                    placeholder="Choose channels"
                    searchPlaceholder="Search channel..."
                    emptyMessage="No channels found."
                    onChangeMultiple={setChannelValues}
                    multiple
                    disabled={editorBusy}
                  />
                  <div className="helpInline">Link a TAL to auto-derive channels, or pick manually.</div>
                </>
              )}
            </div>

            <div style={{ gridColumn: "span 12" }}>
              <label className="muted" style={{ fontSize: 12 }}>Territory Account List</label>
              <SelectPopover
                value={rowDraft.tal_id}
                displayValue={selectedTal?.name}
                options={talOptions}
                placeholder="Choose TAL"
                searchPlaceholder="Search TAL..."
                emptyMessage="No TAL found."
                onChange={(value) => updateDraft((draft) => ({ ...draft, tal_id: value }))}
                disabled={editorBusy}
              />
              <div className="helpInline">
                {!talLinkAvailable
                  ? "Selection is active. Until migration 20260401101500_sales_hypothesis_rows_add_tal_id.sql is applied, TAL is saved in hypothesis metadata."
                  : selectedTal
                  ? `Linked TAL: ${selectedTal.name}${selectedTal.criteria ? ` · ${selectedTal.criteria}` : ""}`
                  : "Optional. Link this hypothesis to a Territory Account List to make the targeting set explicit."}
              </div>
              {selectedTal && (selectedTal.total_meetings > 0 || selectedTal.email_sent > 0 || selectedTal.li_invited > 0 || selectedTal.app_touches > 0 || selectedTal.tg_touches > 0) && (
                <div className="helpInline" style={{ marginTop: 4, lineHeight: 1.6 }}>
                  {(selectedTal.email_sent + selectedTal.email_replies + selectedTal.email_meetings + selectedTal.email_held_meetings > 0) && (
                    <div><strong>Email:</strong> {selectedTal.email_sent} sent, {selectedTal.email_replies} replies ({selectedTal.email_reply_rate ?? 0}%), {selectedTal.email_meetings} booked{selectedTal.email_replies > 0 ? ` (${Math.round((selectedTal.email_meetings / selectedTal.email_replies) * 100)}%)` : ""}, {selectedTal.email_held_meetings} held{selectedTal.email_meetings > 0 ? ` (${Math.round((selectedTal.email_held_meetings / selectedTal.email_meetings) * 100)}%)` : ""}</div>
                  )}
                  {(selectedTal.li_invited + selectedTal.li_accepted + selectedTal.li_replies + selectedTal.li_meetings + selectedTal.li_held_meetings > 0) && (
                    <div><strong>LinkedIn:</strong> {selectedTal.li_invited} invited, {selectedTal.li_accepted} accepted ({selectedTal.li_accept_rate ?? 0}%), {selectedTal.li_replies} replies{selectedTal.li_accepted > 0 ? ` (${Math.round((selectedTal.li_replies / selectedTal.li_accepted) * 100)}%)` : ""}, {selectedTal.li_meetings} booked{selectedTal.li_replies > 0 ? ` (${Math.round((selectedTal.li_meetings / selectedTal.li_replies) * 100)}%)` : ""}, {selectedTal.li_held_meetings} held{selectedTal.li_meetings > 0 ? ` (${Math.round((selectedTal.li_held_meetings / selectedTal.li_meetings) * 100)}%)` : ""}</div>
                  )}
                  {(selectedTal.app_touches + selectedTal.app_replies + selectedTal.app_meetings + selectedTal.app_held_meetings > 0) && (
                    <div><strong>App:</strong> {selectedTal.app_touches} touches, {selectedTal.app_replies} replies ({selectedTal.app_reply_rate ?? 0}%), {selectedTal.app_meetings} booked{selectedTal.app_replies > 0 ? ` (${Math.round((selectedTal.app_meetings / selectedTal.app_replies) * 100)}%)` : ""}, {selectedTal.app_held_meetings} held{selectedTal.app_meetings > 0 ? ` (${Math.round((selectedTal.app_held_meetings / selectedTal.app_meetings) * 100)}%)` : ""}</div>
                  )}
                  {(selectedTal.tg_touches + selectedTal.tg_replies + selectedTal.tg_meetings + selectedTal.tg_held_meetings > 0) && (
                    <div><strong>Telegram:</strong> {selectedTal.tg_touches} touches, {selectedTal.tg_replies} replies ({selectedTal.tg_reply_rate ?? 0}%), {selectedTal.tg_meetings} booked{selectedTal.tg_replies > 0 ? ` (${Math.round((selectedTal.tg_meetings / selectedTal.tg_replies) * 100)}%)` : ""}, {selectedTal.tg_held_meetings} held{selectedTal.tg_meetings > 0 ? ` (${Math.round((selectedTal.tg_held_meetings / selectedTal.tg_meetings) * 100)}%)` : ""}</div>
                  )}
                </div>
              )}
            </div>

            <div style={{ gridColumn: "span 12" }}>
              <div className="subcard" style={{ margin: 0 }}>
                <div style={{ fontWeight: 600, marginBottom: 10 }}>
                  {selectedSegmentTemplate ? "Library messaging (auto-synced)" : "Hypothesis messaging"}
                </div>
                <div className="helpInline" style={{ marginBottom: 12 }}>
                  {selectedSegmentTemplate
                    ? "Messaging is always synced from the Library position. Changes in Library are reflected here automatically."
                    : "Pick a position to auto-sync messaging from the VP Library."}
                </div>
                {selectedSegmentTemplate ? (
                  <div className="helpInline" style={{ marginBottom: 12 }}>
                    Primary role: <span className="mono">{selectedSegmentTemplate.role_name}</span> × <span className="mono">{selectedSegmentTemplate.company_profile_label}</span>
                    {selectedSegmentTemplate.updated_at ? ` · updated ${isoDate(selectedSegmentTemplate.updated_at)}` : ""}
                  </div>
                ) : null}
                <div className="grid formGridTight">
                  <div style={{ gridColumn: "span 12" }}>
                    <label className="muted" style={{ fontSize: 12 }}>VP point</label>
                    <div className="statField" style={{ alignItems: "flex-start", minHeight: 72 }}>
                      <span className="statValue" style={{ fontSize: 14, whiteSpace: "normal", lineHeight: 1.45 }}>
                        {liveDraftSnapshot.vp_point || "—"}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div style={{ gridColumn: "span 12" }}>
              <div className="subcard" style={{ margin: 0 }}>
                <div style={{ fontWeight: 600, marginBottom: 10 }}>Hypothesis criteria</div>
                <div className="helpInline" style={{ marginBottom: 12 }}>
                  These criteria belong to the hypothesis itself. They are not synced from VP Library and should reflect how you validate or kill this specific test.
                </div>
                <div className="grid formGridTight">
                  <div style={{ gridColumn: "span 6" }}>
                    <label className="muted" style={{ fontSize: 12 }}>Expected signal</label>
                    <textarea className="textarea" value={rowDraft.expected_signal} disabled={editorBusy} onChange={(event) => updateDraft((draft) => ({ ...draft, expected_signal: event.target.value }))} />
                  </div>
                  <div style={{ gridColumn: "span 6" }}>
                    <label className="muted" style={{ fontSize: 12 }}>Disqualifiers</label>
                    <textarea className="textarea" value={rowDraft.disqualifiers} disabled={editorBusy} onChange={(event) => updateDraft((draft) => ({ ...draft, disqualifiers: event.target.value }))} />
                  </div>
                </div>
              </div>
            </div>

            <div style={{ gridColumn: "span 3" }}>
              <label className="muted" style={{ fontSize: 12 }}>Updated</label>
              <div className="statField"><span className="statValue">{selectedRow ? isoDate(selectedRow.updated_at || selectedRow.created_at) : "Not saved yet"}</span></div>
            </div>

            <div style={{ gridColumn: "span 12" }}>
              <label className="muted" style={{ fontSize: 12 }}>Notes</label>
              <textarea className="textarea" value={rowDraft.notes} disabled={editorBusy} onChange={(event) => updateDraft((draft) => ({ ...draft, notes: event.target.value }))} />
            </div>
          </div>
        )}
      </div>
    </>
  ) : null;

  return (
    <main className="content">
      <div className="pageWide">
        <AppTopbar title="Hypotheses" subtitle="Single registry" />

      <div className="page grid">
        <div className="card hypRegistryCard" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div className="pageHeader" style={{ width: "100%" }}>
              <div>
                <div className="cardTitle">{registry?.title || "Hypotheses Registry"}</div>
                <div className="cardDesc">
                  One common registry of hypotheses. All taxonomy fields are stored in Supabase lookup tables.
                </div>
              </div>
              <div className="hypToolbar">
                <input className="input hypSearch" placeholder="Search hypotheses…" value={rowSearch} onChange={(event) => setRowSearch(event.target.value)} />
                <SelectPopover
                  width={190}
                  values={rowStatusFilter}
                  displayValue={rowStatusFilter.length ? rowStatusFilter.join(", ") : undefined}
                  options={filterStatusOptions}
                  placeholder="All statuses"
                  onChangeMultiple={setRowStatusFilter}
                  searchable={false}
                  multiple
                />
                <SelectPopover
                  width={190}
                  values={rowDecisionFilter}
                  displayValue={rowDecisionFilter.length ? rowDecisionFilter.join(", ") : undefined}
                  options={filterDecisionOptions}
                  placeholder="All decisions"
                  onChangeMultiple={setRowDecisionFilter}
                  searchable={false}
                  multiple
                />
                <button className="btn" onClick={() => void reloadRegistryRows()} disabled={!registry || rowActionsBusy}>Reload</button>
                <button className="btn btnPrimary" onClick={addRow} disabled={!registry || rowActionsBusy}>New hypothesis</button>
              </div>
            </div>
          </div>
          <div className="cardBody">
            {status ? <div className="notice" style={{ marginBottom: 12 }}>{status}</div> : null}
            {talLinkNotice ? <div className="notice" style={{ marginBottom: 12 }}>{talLinkNotice}</div> : null}
            <div className="hypSummary">
              <span className="hypSummaryItem"><span className="hypSummaryValue">{filteredRows.length}</span> shown</span>
              <span className="hypSummaryItem"><span className="hypSummaryValue">{rows.length}</span> total</span>
              <span className="hypSummaryItem"><span className="hypSummaryValue">{validatedCount}</span> validated</span>
              <span className="hypSummaryItem"><span className="hypSummaryValue">{draftCount}</span> draft</span>
              <span className="hypSummaryItem"><span className="hypSummaryValue">{activeFilterCount}</span> active filters</span>
            </div>
{/* editor is rendered as overlay below */}
            {!registry ? (
              <div className="muted2">Loading registry...</div>
            ) : !rows.length ? (
              <div className="subcard hypRegistryEmpty">
                <div style={{ fontWeight: 600, marginBottom: 6 }}>Registry is empty</div>
                <div className="muted2" style={{ marginBottom: 12 }}>Create the first hypothesis. Vertical, sub-vertical, role and company size will be saved into shared Supabase lookups.</div>
                <button className="btn btnPrimary" onClick={addRow} disabled={rowActionsBusy}>Create first hypothesis</button>
              </div>
            ) : (
              <div className="hypTableWrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Title</th>
                      <th>Owner</th>
                      <th>Role</th>
                      <th>Vertical</th>
                      <th>Company Size</th>
                      <th>TAL</th>
                      <th>Channels</th>
                      <th>Booked</th>
                      <th>Held</th>
                      <th>Priority</th>
                      <th>Decision</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredRows.map((row) => {
                      const isActive = row.id === selectedRowId;
                      const live = liveRowFields.get(row.id);
                      return (
                        <Fragment key={row.id}>
                          <tr className={isActive ? "hypRowActive" : undefined} onClick={() => openEditorForRow(row.id)} style={{ cursor: rowActionsBusy ? "progress" : "pointer" }}>
                            <td><div className="hypRowTitle">{preview(row.title || (live?.vp_point ?? row.vp_point), 56)}</div></td>
                            <td>{owners.find(o => o.id === row.owner_id)?.name ?? "—"}</td>
                            <td>{roleDisplay(row, roles.length)}</td>
                            <td>{preview(verticalNameById.get(row.vertical_id ?? "") ?? row.vertical_name, 40)}</td>
                            <td>{preview(row.company_scale ?? companyScaleNameById.get(row.company_scale_id ?? ""), 40)}</td>
                            <td>{preview(talNameById.get(row.tal_id ?? ""), 40)}</td>
                            <td>{row.tal_id ? deriveTalChannels(talById.get(row.tal_id)).join(", ") || "—" : parseList(row.channel).map((slug) => channelNameBySlug.get(slug) ?? slug).join(", ") || "—"}</td>
                            <td className="mono">{row.tal_id ? (talById.get(row.tal_id)?.total_meetings ?? 0) : "—"}</td>
                            <td className="mono">{row.tal_id ? (talById.get(row.tal_id)?.total_held_meetings ?? 0) : "—"}</td>
                            <td>{priorityValueToLabel(priorityScoreToValue(row.priority))}</td>
                            <td>{row.decision ? formatLabel(row.decision) : "—"}</td>
                            <td><span className="tag">{formatLabel(row.status)}</span></td>
                          </tr>
{/* editor is now an overlay */}
                        </Fragment>
                      );
                    })}
                    {!filteredRows.length ? (
                      <tr>
                        <td colSpan={13} className="muted2">{rowsLoading ? "Loading..." : "No hypotheses match the current filters."}</td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>

      </div>
      </div>
      {isEditorOpen ? (
        <div className="dialogScrim" onClick={closeEditor}>
          <div className="card dialogCard" onClick={(e) => e.stopPropagation()}>
            {editorPanel}
          </div>
        </div>
      ) : null}
    </main>
  );
}
