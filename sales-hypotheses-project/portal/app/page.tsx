// @ts-nocheck
"use client";

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { createClient } from "@supabase/supabase-js";
import { createPortal } from "react-dom";
import { AppTopbar } from "./components/AppTopbar";

function safeJsonParse<T>(s: string | null): T | null {
  if (!s) return null;
  try {
    return JSON.parse(s) as T;
  } catch {
    return null;
  }
}

function DateRangePicker({
  label,
  availableMin,
  availableMax,
  value,
  onChange
}: {
  label: string;
  availableMin: string; // y-m-d
  availableMax: string; // y-m-d
  value: { since: string; until: string }; // inclusive y-m-d
  onChange: (next: { since: string; until: string }, meta?: { period?: "day" | "week" | "month" | "quarter" | "year" }) => void;
}) {
  const [open, setOpen] = useState(false);
  const btnRef = useRef<any>(null);
  const [pos, setPos] = useState<{ left: number; top?: number; bottom?: number; width: number } | null>(null);
  const [periodHint, setPeriodHint] = useState<null | "day" | "week" | "month" | "quarter" | "year">(null);
  const [quickQuarter, setQuickQuarter] = useState<string>("");
  const [quickYear, setQuickYear] = useState<string>("");

  const parseMs = (ymd: string) => Date.parse(`${ymd}T00:00:00.000Z`);
  const fmtYmd = (d: Date) => {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
  };
  const addDays = (ymd: string, delta: number) => fmtYmd(new Date(parseMs(ymd) + delta * 86400000));
  const clampYmd = (ymd: string) => {
    if (!availableMin || !availableMax) return ymd;
    if (ymd < availableMin) return availableMin;
    if (ymd > availableMax) return availableMax;
    return ymd;
  };

  const effectiveSince = value?.since || availableMin || "";
  const effectiveUntil = value?.until || availableMax || "";

  const [draft, setDraft] = useState<{ since: string; until: string }>({ since: effectiveSince, until: effectiveUntil });
  const [viewMonth, setViewMonth] = useState<string>(() => {
    const base = (effectiveUntil || effectiveSince || availableMax || availableMin || "2025-01-01").slice(0, 7);
    return base;
  });

  useEffect(() => {
    if (!open) return;
    const calc = () => {
      const el = btnRef.current as HTMLElement | null;
      if (!el) return;
      const r = el.getBoundingClientRect();
      const estimatedMenuH = 540;
      const spaceBelow = window.innerHeight - r.bottom;
      const canOpenUp = r.top > estimatedMenuH;
      const openUp = spaceBelow < estimatedMenuH && canOpenUp;
      // Popover needs enough width for calendar; still clamp via maxWidth in render.
      const width = Math.max(520, r.width);
      const left = Math.max(12, Math.min(r.left, window.innerWidth - 12 - width));
      if (openUp) setPos({ left, bottom: Math.max(12, window.innerHeight - r.top + 8), width });
      else setPos({ left, top: Math.min(window.innerHeight - 12, r.bottom + 8), width });
    };
    calc();
    window.addEventListener("resize", calc);
    window.addEventListener("scroll", calc, true);
    return () => {
      window.removeEventListener("resize", calc);
      window.removeEventListener("scroll", calc, true);
    };
  }, [open]);

  useEffect(() => {
    if (!open) return;
    setDraft({ since: effectiveSince, until: effectiveUntil });
    const base = (effectiveUntil || effectiveSince || availableMax || availableMin || "2025-01-01").slice(0, 7);
    setViewMonth(base);
    setPeriodHint(null);
    setQuickQuarter("");
    setQuickYear("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const monthToDate = (ym: string) => new Date(Date.parse(`${ym}-01T00:00:00.000Z`));
  const shiftMonth = (ym: string, delta: number) => {
    const d = monthToDate(ym);
    d.setUTCMonth(d.getUTCMonth() + delta);
    return fmtYmd(d).slice(0, 7);
  };

  const makeMonthDays = (ym: string) => {
    const d0 = monthToDate(ym);
    const y = d0.getUTCFullYear();
    const m = d0.getUTCMonth(); // 0..11
    const first = new Date(Date.UTC(y, m, 1));
    const firstDow = (first.getUTCDay() + 6) % 7; // Monday=0
    const nextMonth = new Date(Date.UTC(y, m + 1, 1));
    const daysInMonth = Math.round((nextMonth.getTime() - first.getTime()) / 86400000);
    const cells: Array<{ ymd: string; day: number; off?: boolean }> = [];
    for (let i = 0; i < firstDow; i++) cells.push({ ymd: "", day: 0, off: true });
    for (let day = 1; day <= daysInMonth; day++) {
      const ymd = fmtYmd(new Date(Date.UTC(y, m, day)));
      cells.push({ ymd, day });
    }
    return { y, m: m + 1, cells };
  };

  const inRange = (x: string, a: string, b: string) => {
    if (!x || !a || !b) return false;
    const lo = a <= b ? a : b;
    const hi = a <= b ? b : a;
    return x >= lo && x <= hi;
  };

  const pickDay = (ymd: string) => {
    if (!ymd) return;
    const day = clampYmd(ymd);
    setQuickQuarter("");
    setQuickYear("");
    const hasSince = !!draft.since;
    const hasUntil = !!draft.until;
    // If starting a new selection (no start yet OR range already complete),
    // first click should set start and clear end; second click sets the end.
    if (!hasSince || hasUntil) {
      setDraft({ since: day, until: "" });
      return;
    }
    // only since set -> set until (swap if needed)
    const a = draft.since;
    const b = day;
    if (b < a) setDraft({ since: b, until: a });
    else setDraft({ since: a, until: b });
  };

  const preset = (kind: "7d" | "30d" | "90d" | "ytd" | "1y" | "all") => {
    const max = availableMax || effectiveUntil;
    const min = availableMin || effectiveSince;
    if (!min || !max) return;
    setQuickQuarter("");
    setQuickYear("");
    if (kind === "all") {
      setPeriodHint("month");
      return setDraft({ since: min, until: max });
    }
    const end = max;
    if (kind === "ytd") {
      const y = end.slice(0, 4);
      setPeriodHint("week");
      return setDraft({ since: `${y}-01-01`, until: end });
    }
    const days = kind === "7d" ? 7 : kind === "30d" ? 30 : kind === "90d" ? 90 : 365;
    const start = addDays(end, -(days - 1));
    setPeriodHint(days <= 90 ? "day" : "week");
    return setDraft({ since: clampYmd(start), until: end });
  };

  const quarters = useMemo(() => {
    // last 12 quarters based on availableMax
    if (!availableMax) return [];
    const y = Number(availableMax.slice(0, 4));
    const m = Number(availableMax.slice(5, 7));
    const q = Math.floor((m - 1) / 3) + 1;
    const out: Array<{ key: string; since: string; until: string }> = [];
    let yy = y;
    let qq = q;
    for (let i = 0; i < 12; i++) {
      const startM = (qq - 1) * 3 + 1;
      const since = `${yy}-${String(startM).padStart(2, "0")}-01`;
      const endM = startM + 3;
      const untilEx = fmtYmd(new Date(Date.UTC(yy, endM - 1, 1))); // first of next quarter
      const until = addDays(untilEx, -1);
      out.push({ key: `${yy}-Q${qq}`, since, until });
      qq -= 1;
      if (qq <= 0) {
        qq = 4;
        yy -= 1;
      }
    }
    return out.filter((x) => x.until >= availableMin);
  }, [availableMax, availableMin]);

  const years = useMemo(() => {
    if (!availableMax) return [];
    const yMax = Number(availableMax.slice(0, 4));
    const yMin = availableMin ? Number(availableMin.slice(0, 4)) : yMax - 5;
    const out: Array<{ key: string; since: string; until: string }> = [];
    for (let y = yMax; y >= Math.max(yMin, yMax - 6); y--) {
      out.push({ key: String(y), since: `${y}-01-01`, until: `${y}-12-31` });
    }
    return out;
  }, [availableMax, availableMin]);

  const menu =
    open && pos && typeof document !== "undefined"
      ? createPortal(
        <div style={{ position: "fixed", inset: 0, zIndex: 2600 }} onMouseDown={() => setOpen(false)}>
          <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.10)", backdropFilter: "blur(2px)" }} />
          <div
            className="card"
            style={{
              position: "absolute",
              left: pos.left,
              top: pos.top,
              bottom: pos.bottom,
              width: Math.min(820, pos.width),
              maxWidth: "calc(100vw - 24px)"
            }}
            onMouseDown={(e) => e.stopPropagation()}
          >
            <div className="cardHeader" style={{ padding: "12px 12px" }}>
              <div style={{ minWidth: 0 }}>
                <div className="cardTitle" style={{ fontSize: 13 }}>{label}</div>
                <div className="cardDesc" style={{ marginTop: 2, fontSize: 12 }}>
                  {draft.since && draft.until ? (
                    <>
                      <span className="mono">{draft.since}</span> → <span className="mono">{draft.until}</span>
                    </>
                  ) : (
                    "Pick a range"
                  )}
                </div>
              </div>
              <div className="btnRow" style={{ gap: 8 }}>
                <button className="btn" onClick={() => preset("7d")}>7d</button>
                <button className="btn" onClick={() => preset("30d")}>30d</button>
                <button className="btn" onClick={() => preset("90d")}>90d</button>
                <button className="btn" onClick={() => preset("ytd")}>YTD</button>
                <button className="btn" onClick={() => preset("1y")}>1y</button>
                <button className="btn" onClick={() => preset("all")}>All</button>
              </div>
            </div>

            <div className="cardBody" style={{ padding: 12 }}>
              {(() => {
                const wide = (pos?.width ?? 0) >= 760;
                const gridCols = wide ? "repeat(12, 1fr)" : "1fr";
                const calCol = wide ? "span 8" : "span 12";
                const sideCol = wide ? "span 4" : "span 12";
                const monthOffsets = wide ? [0, 1] : [0];
                return (
                  <div style={{ display: "grid", gridTemplateColumns: gridCols, gap: 10 }}>
                    <div style={{ gridColumn: calCol }}>
                      <div className="calNav">
                        <button className="btn" onClick={() => setViewMonth((m) => shiftMonth(m, -1))}>Prev</button>
                        <div className="mono" style={{ opacity: 0.85 }}>{viewMonth}</div>
                        <button className="btn" onClick={() => setViewMonth((m) => shiftMonth(m, +1))}>Next</button>
                      </div>
                      <div className={`calWrap ${wide ? "calWrap2" : ""}`.trim()}>
                        {monthOffsets.map((i) => {
                          const ym = shiftMonth(viewMonth, i);
                          const { y, m, cells } = makeMonthDays(ym);
                          return (
                            <div key={ym} className="calMonth">
                              <div className="calTitle">{y}-{String(m).padStart(2, "0")}</div>
                              <div className="calDow">
                                {["M", "T", "W", "T", "F", "S", "S"].map((x) => (
                                  <div key={x} className="calDowCell">{x}</div>
                                ))}
                              </div>
                              <div className="calGrid">
                                {cells.map((c, idx) => {
                                  if (c.off) return <div key={idx} className="calCell calCellOff" />;
                                  const disabled = (availableMin && c.ymd < availableMin) || (availableMax && c.ymd > availableMax);
                                  const isStart = c.ymd === draft.since;
                                  const isEnd = c.ymd === draft.until;
                                  const isIn = inRange(c.ymd, draft.since, draft.until);
                                  return (
                                    <button
                                      key={c.ymd}
                                      className={[
                                        "calCell",
                                        isIn ? "calCellIn" : "",
                                        isStart ? "calCellStart" : "",
                                        isEnd ? "calCellEnd" : ""
                                      ].filter(Boolean).join(" ")}
                                      disabled={disabled}
                                      onClick={() => pickDay(c.ymd)}
                                    >
                                      {c.day}
                                    </button>
                                  );
                                })}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </div>

                    <div style={{ gridColumn: sideCol }}>
                      <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Quick pick</div>
                      <div className="card" style={{ background: "rgba(255,255,255,0.02)", border: "1px solid rgba(255,255,255,0.08)" }}>
                        <div className="cardBody" style={{ padding: 12, display: "grid", gap: 10 }}>
                          <div>
                            <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Quarter</div>
                            <select
                              className="select"
                              value={quickQuarter}
                              onChange={(e) => {
                                const k = String(e.target.value);
                                const q = quarters.find((x) => x.key === k);
                                if (q) {
                                  setPeriodHint("quarter");
                                  setQuickQuarter(k);
                                  setQuickYear("");
                                  setDraft({ since: clampYmd(q.since), until: clampYmd(q.until) });
                                }
                              }}
                            >
                              <option value="">Select quarter…</option>
                              {quarters.map((q) => (
                                <option key={q.key} value={q.key}>{q.key}</option>
                              ))}
                            </select>
                          </div>
                          <div>
                            <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Year</div>
                            <select
                              className="select"
                              value={quickYear}
                              onChange={(e) => {
                                const k = String(e.target.value);
                                const y = years.find((x) => x.key === k);
                                if (y) {
                                  setPeriodHint("year");
                                  setQuickYear(k);
                                  setQuickQuarter("");
                                  setDraft({ since: clampYmd(y.since), until: clampYmd(y.until) });
                                }
                              }}
                            >
                              <option value="">Select year…</option>
                              {years.map((y) => (
                                <option key={y.key} value={y.key}>{y.key}</option>
                              ))}
                            </select>
                          </div>
                          <div className="muted2" style={{ fontSize: 12, lineHeight: 1.35 }}>
                            Available: <span className="mono">{availableMin || "—"}</span> → <span className="mono">{availableMax || "—"}</span>
                          </div>
                        </div>
                      </div>

                      <div className="btnRow" style={{ marginTop: 12, justifyContent: "flex-end" }}>
                        <button className="btn" onClick={() => setOpen(false)}>Cancel</button>
                        <button
                          className="btn btnPrimary"
                          onClick={() => {
                            const since = clampYmd(draft.since || availableMin);
                            const until = clampYmd(draft.until || availableMax);
                            const a = since <= until ? since : until;
                            const b = since <= until ? until : since;
                            // Suggest period for UX: short ranges -> day; longer -> week/month.
                            const days = Math.max(1, Math.round((parseMs(b) - parseMs(a)) / 86400000) + 1);
                            const suggested: any = days <= 120 ? "day" : days <= 540 ? "week" : days <= 900 ? "month" : "quarter";
                            onChange({ since: a, until: b }, { period: periodHint || suggested });
                            setOpen(false);
                          }}
                          disabled={!draft.since || !draft.until}
                        >
                          Apply
                        </button>
                      </div>
                    </div>
                  </div>
                );
              })()}
            </div>
          </div>
        </div>,
        document.body
      )
      : null;

  return (
    <div>
      <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>{label}</div>
      <button
        className="selectTrigger"
        ref={btnRef}
        data-open={open ? "1" : "0"}
        onClick={() => setOpen((v) => !v)}
        style={{ width: "100%", justifyContent: "space-between" }}
      >
        <span className="selectTriggerMain">
          <span className="selectTriggerValue">{effectiveSince && effectiveUntil ? "Date range" : "Pick range"}</span>
          <span className="selectTriggerHint">
            {effectiveSince && effectiveUntil ? `${effectiveSince} → ${effectiveUntil}` : "—"}
          </span>
        </span>
        <span className="selectCaret" aria-hidden="true" />
      </button>
      {menu}
    </div>
  );
}

function MultiSelectDropdown({
  label,
  placeholder,
  options,
  selected,
  onChange,
  onDone,
  disabled,
  height
}: {
  label: string;
  placeholder?: string;
  options: Array<{ value: string; label: string; meta?: string }>;
  selected: string[];
  onChange: (next: string[]) => void;
  onDone?: () => void;
  disabled?: boolean;
  height?: number;
}) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const btnRef = useRef<any>(null);
  const [pos, setPos] = useState<{ left: number; top?: number; bottom?: number; width: number } | null>(null);

  const selectedSet = useMemo(() => new Set(selected), [selected]);
  const filtered = useMemo(() => {
    const t = q.trim().toLowerCase();
    if (!t) return options;
    return options.filter((o) => (o.label + " " + (o.meta || "") + " " + o.value).toLowerCase().includes(t));
  }, [options, q]);

  const selectedLabels = useMemo(() => {
    const m = new Map(options.map((o) => [o.value, o.label]));
    return selected.map((v) => m.get(v) ?? v);
  }, [options, selected]);

  useEffect(() => {
    if (!open) return;
    const calc = () => {
      const el = btnRef.current as HTMLElement | null;
      if (!el) return;
      const r = el.getBoundingClientRect();
      // Estimate menu height for placement decisions (search row + buttons + list)
      const estimatedMenuH = Math.min(560, Math.max(320, Number(height ?? 320) + 140));
      const spaceBelow = window.innerHeight - r.bottom;
      const canOpenUp = r.top > estimatedMenuH;
      const openUp = spaceBelow < estimatedMenuH && canOpenUp;

      const width = Math.max(320, r.width);
      const left = Math.max(12, Math.min(r.left, window.innerWidth - 12 - width));

      if (openUp) {
        const bottom = Math.max(12, window.innerHeight - r.top + 8);
        setPos({ left, bottom, width });
      } else {
        const top = Math.min(window.innerHeight - 12, r.bottom + 8);
        setPos({ left, top, width });
      }
    };

    calc();
    window.addEventListener("resize", calc);
    window.addEventListener("scroll", calc, true);
    return () => {
      window.removeEventListener("resize", calc);
      window.removeEventListener("scroll", calc, true);
    };
  }, [open, height]);

  const menu =
    open && pos && typeof document !== "undefined"
      ? createPortal(
        <div
          style={{
            position: "fixed",
            inset: 0,
            zIndex: 2500
          }}
          onMouseDown={() => setOpen(false)}
        >
          <div
            style={{
              position: "absolute",
              inset: 0,
              background: "rgba(0,0,0,0.10)",
              backdropFilter: "blur(2px)"
            }}
          />

          <div
            className="card"
            style={{
              position: "absolute",
              left: pos.left,
              top: pos.top,
              bottom: pos.bottom,
              width: Math.min(760, pos.width),
              maxWidth: "calc(100vw - 24px)"
            }}
            onMouseDown={(e) => e.stopPropagation()}
          >
            <div className="cardHeader" style={{ padding: "12px 12px" }}>
              <div style={{ minWidth: 0 }}>
                <div className="cardTitle" style={{ fontSize: 13 }}>{label}</div>
                <div className="cardDesc" style={{ marginTop: 2, fontSize: 12 }}>
                  {selected.length ? `${selected.length} selected` : "No filter"}
                </div>
              </div>
              <div className="btnRow" style={{ gap: 8 }}>
                <button className="btn" onClick={() => onChange([])} disabled={!selected.length}>Clear</button>
                <button
                  className="btn btnPrimary"
                  onClick={() => {
                    setOpen(false);
                    onDone?.();
                  }}
                >
                  Done
                </button>
              </div>
            </div>

            <div className="cardBody" style={{ padding: 12 }}>
              <input
                className="input"
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="Search..."
                autoFocus
              />

              <div
                style={{
                  marginTop: 10,
                  maxHeight: height ?? 320,
                  overflow: "auto",
                  borderRadius: 12,
                  border: "1px solid rgba(255,255,255,0.08)"
                }}
              >
                {filtered.length ? (
                  <div style={{ display: "grid" }}>
                    {filtered.map((o) => {
                      const checked = selectedSet.has(o.value);
                      return (
                        <label
                          key={o.value}
                          style={{
                            display: "flex",
                            gap: 10,
                            alignItems: "center",
                            padding: "10px 12px",
                            cursor: "pointer",
                            borderBottom: "1px solid rgba(255,255,255,0.06)",
                            background: checked ? "rgba(125,211,252,0.08)" : "transparent"
                          }}
                        >
                          <input
                            type="checkbox"
                            checked={checked}
                            onChange={() => {
                              const next = checked ? selected.filter((x) => x !== o.value) : [...selected, o.value];
                              onChange(next);
                            }}
                          />
                          <div style={{ flex: 1, minWidth: 0 }}>
                            <div style={{ fontSize: 13, opacity: 0.92, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                              {o.label}
                            </div>
                            {o.meta ? (
                              <div className="mono" style={{ fontSize: 11, opacity: 0.55, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                {o.meta}
                              </div>
                            ) : null}
                          </div>
                          <div className="mono" style={{ fontSize: 11, opacity: 0.55 }}>{o.value}</div>
                        </label>
                      );
                    })}
                  </div>
                ) : (
                  <div className="muted2" style={{ padding: 12 }}>No matches.</div>
                )}
              </div>
            </div>
          </div>
        </div>,
        document.body
      )
      : null;

  return (
    <div>
      <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>{label}</div>

      <button
        className="selectTrigger"
        disabled={!!disabled}
        onClick={() => setOpen((v) => !v)}
        ref={btnRef}
        data-open={open ? "1" : "0"}
        style={{
          width: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 10,
          cursor: disabled ? "not-allowed" : "pointer"
        }}
      >
        <span className="selectTriggerMain">
          <span className="selectTriggerValue">
            {selected.length ? `${selected.length} selected` : (placeholder || "Select...")}
          </span>
          {selected.length ? (
            <span className="selectTriggerHint">
              {selectedLabels.slice(0, 2).join(", ")}{selectedLabels.length > 2 ? ` +${selectedLabels.length - 2}` : ""}
            </span>
          ) : null}
        </span>
        <span className="selectCaret" aria-hidden="true" />
      </button>
      {menu}
    </div>
  );
}

export default function HomePage() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";
  const hubspotPortalId = process.env.NEXT_PUBLIC_HUBSPOT_PORTAL_ID ?? "22603597";

  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

  const [email, setEmail] = useState("");
  const [status, setStatus] = useState<string>("");
  const [sessionEmail, setSessionEmail] = useState<string | null>(null);
  const [cooldownUntil, setCooldownUntil] = useState<number | null>(null);

  // Global dashboard (HubSpot TAL snapshots)
  const [hyps, setHyps] = useState<any[]>([]);
  const [snaps, setSnaps] = useState<any[]>([]);
  const [dailySnaps, setDailySnaps] = useState<any[]>([]);
  const [dashStatus, setDashStatus] = useState<string>("");
  // GetSales sync is part of unified /api/sync/all; we don't show a separate UI status anymore.
  const [period, setPeriod] = useState<"day" | "week" | "month" | "quarter" | "year">("day"); // visual granularity (auto from date range)
  const [colorMode, setColorMode] = useState<"channel" | "hypothesis">("channel");
  const [tableRowsByMetric, setTableRowsByMetric] = useState<Record<string, any[]>>({});
  const [tableErrByMetric, setTableErrByMetric] = useState<Record<string, string>>({});
  const [tableLoadingByMetric, setTableLoadingByMetric] = useState<Record<string, boolean>>({});

  // UI filters (instead of env)
  const [pipelines, setPipelines] = useState<any[]>([]);
  const [selectedPipelineIds, setSelectedPipelineIds] = useState<string[]>([]);
  const [selectedStageIds, setSelectedStageIds] = useState<string[]>([]);
  const [viewMode, setViewMode] = useState<"deals" | "activities">("deals");
  const [activitySources, setActivitySources] = useState<string[]>(["linkedin", "email"]);
  const [activitiesOnlyPushed, setActivitiesOnlyPushed] = useState<boolean>(true);
  const [activityReportTab, setActivityReportTab] = useState<"linkedin" | "email">("linkedin");
  const [smartleadCampaigns, setSmartleadCampaigns] = useState<any[]>([]);
  const [smartleadCampaignsErr, setSmartleadCampaignsErr] = useState<string>("");
  const [smartleadCampaignsLoading, setSmartleadCampaignsLoading] = useState<boolean>(false);
  const [smartleadCampaignIds, setSmartleadCampaignIds] = useState<string[]>([]); // SmartLead campaign_id filter for Email tab
  const [kpiStageIds, setKpiStageIds] = useState<string[]>([]);
  const [kpiStageKey, setKpiStageKey] = useState<string>("");
  const [kpiDeltaMode, setKpiDeltaMode] = useState<"all" | "plus" | "minus">("all");
  const [stock, setStock] = useState<any | null>(null);
  const [dateRange, setDateRange] = useState<{ since: string; until: string }>({ since: "", until: "" }); // inclusive y-m-d
  const [dateRangeTouched, setDateRangeTouched] = useState<boolean>(false);
  const [stageConvs, setStageConvs] = useState<any | null>(null);
  const [stageConvsErr, setStageConvsErr] = useState<string>("");
  const [stageConvsLoading, setStageConvsLoading] = useState<boolean>(false);
  const [stageConvsMode, setStageConvsMode] = useState<"cohort_created" | "in_window">("cohort_created");

  const buildStamp = "home-ui-2025-12-28-4";

  // Persist filters to avoid "jumping" when the page reloads (e.g. auth refresh / multiple clients).
  // IMPORTANT: keep this block AFTER the related useState declarations to avoid TDZ during Next.js prerender.
  const filtersStorageKey = sessionEmail ? `sales_home_filters_v1:${String(sessionEmail).toLowerCase()}` : "";
  const restoredRef = useRef<boolean>(false);

  useEffect(() => {
    if (!sessionEmail) return;
    if (restoredRef.current) return;
    restoredRef.current = true;
    if (typeof window === "undefined" || !filtersStorageKey) return;
    const saved = safeJsonParse<{
      pipelines?: string[];
      stages?: string[];
      colorMode?: "channel" | "hypothesis";
      viewMode?: "deals" | "activities";
      activitySources?: string[];
      activitiesOnlyPushed?: boolean;
      activityReportTab?: "linkedin" | "email";
      smartleadCampaignIds?: string[];
      dateRange?: { since: string; until: string };
      dateRangeTouched?: boolean;
    }>(window.localStorage.getItem(filtersStorageKey));
    if (!saved) return;
    if (Array.isArray(saved.pipelines) && saved.pipelines.length) setSelectedPipelineIds(saved.pipelines.map(String));
    if (Array.isArray(saved.stages)) setSelectedStageIds(saved.stages.map(String));
    if (saved.colorMode === "channel" || saved.colorMode === "hypothesis") setColorMode(saved.colorMode);
    if (saved.viewMode === "deals" || saved.viewMode === "activities") setViewMode(saved.viewMode);
    if (Array.isArray(saved.activitySources) && saved.activitySources.length) setActivitySources(saved.activitySources.map(String));
    if (typeof saved.activitiesOnlyPushed === "boolean") setActivitiesOnlyPushed(saved.activitiesOnlyPushed);
    if (saved.activityReportTab === "linkedin" || saved.activityReportTab === "email") setActivityReportTab(saved.activityReportTab);
    if (Array.isArray(saved.smartleadCampaignIds)) {
      const cleaned = saved.smartleadCampaignIds
        .map((x) => Number(x))
        .filter((n) => Number.isFinite(n) && n > 0)
        .map((n) => String(n));
      setSmartleadCampaignIds(cleaned);
    }
    if (saved.dateRange && typeof saved.dateRange.since === "string" && typeof saved.dateRange.until === "string") setDateRange(saved.dateRange);
    if (typeof saved.dateRangeTouched === "boolean") setDateRangeTouched(saved.dateRangeTouched);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionEmail, filtersStorageKey]);

  useEffect(() => {
    if (!sessionEmail) return;
    if (typeof window === "undefined" || !filtersStorageKey) return;
    const payload = {
      pipelines: selectedPipelineIds,
      stages: selectedStageIds,
      colorMode,
      viewMode,
      activitySources,
      activitiesOnlyPushed,
      activityReportTab,
      smartleadCampaignIds,
      dateRange,
      dateRangeTouched
    };
    // Best-effort: ignore quota errors.
    try {
      window.localStorage.setItem(filtersStorageKey, JSON.stringify(payload));
    } catch {
      // ignore
    }
  }, [
    sessionEmail,
    filtersStorageKey,
    selectedPipelineIds.join(","),
    selectedStageIds.join(","),
    colorMode,
    viewMode,
    activitySources.join(","),
    activitiesOnlyPushed,
    activityReportTab,
    smartleadCampaignIds.join(","),
    dateRange.since,
    dateRange.until,
    dateRangeTouched
  ]);

  function stageCategoryFromLabel(labelOrId: string) {
    const t = String(labelOrId ?? "").trim().toLowerCase();
    if (!t) return "unknown";
    if (t.includes("lead")) return "lead";
    if (t.includes("sql")) return "sql";
    if (t.includes("evaluate")) return "evaluate";
    if (t.includes("select")) return "select";
    if (t.includes("negot")) return "negotiate";
    if (t.includes("purchase")) return "purchase";
    if (t.includes("integrat")) return "integration";
    if (t.includes("active")) return "active";
    if (t.includes("lost")) return "lost";
    if (t.includes("dormant")) return "dormant";
    if (t.includes("churn")) return "churn";
    return "unknown";
  }

  function stageIdsForKpi(kpiKey: string) {
    const ps = (pipelines || []).filter((p: any) => selectedPipelineIds.includes(String(p.id)));
    const stages = ps.flatMap((p: any) => (Array.isArray(p.stages) ? p.stages : [])).map((s: any) => ({ id: String(s.id), label: String(s.label || s.id) }));
    const want = new Set<string>();
    for (const s of stages) {
      const cat = stageCategoryFromLabel(s.label);
      if (kpiKey === "leads" && cat === "lead") want.add(s.id);
      else if (kpiKey === "sql" && cat === "sql") want.add(s.id);
      else if (kpiKey === "opportunity" && ["evaluate", "select", "negotiate", "purchase"].includes(cat)) want.add(s.id);
      else if (kpiKey === "clients" && ["integration", "active"].includes(cat)) want.add(s.id);
      else if (kpiKey === "lost" && ["lost", "dormant", "churn"].includes(cat)) want.add(s.id);
    }
    return Array.from(want);
  }

  function sameSet(a: string[], b: string[]) {
    if (a.length !== b.length) return false;
    const s = new Set(a);
    for (const x of b) if (!s.has(x)) return false;
    return true;
  }

  function toggleKpiFilter(kpiKey: string) {
    if (!selectedPipelineIds.length) return;
    const target = stageIdsForKpi(kpiKey);
    if (!target.length) return;
    const cur = (kpiStageIds || []).map(String);
    if (kpiStageKey === kpiKey && sameSet(cur.slice().sort(), target.slice().sort())) {
      setKpiStageKey("");
      setKpiStageIds([]);
    } else {
      setKpiStageKey(kpiKey);
      setKpiStageIds(target);
    }
  }

  const effectiveStageIdsForDeals = useMemo(() => {
    // Dropdown "Stages" is an explicit filter (higher priority).
    if ((selectedStageIds || []).length) return selectedStageIds;
    return kpiStageIds || [];
  }, [selectedStageIds.join(","), kpiStageIds.join(",")]);

  function resetDateRange() {
    setDateRangeTouched(false);
    setDateRange({ since: "", until: "" });
    setPeriod("day");
  }

  useEffect(() => {
    if (!supabase) return;
    supabase.auth.getSession().then(({ data }) => {
      const s = data.session;
      setSessionEmail(s?.user?.email ?? null);
    });
    const { data: sub } = supabase.auth.onAuthStateChange((_event, s) => {
      setSessionEmail(s?.user?.email ?? null);
    });
    return () => sub.subscription.unsubscribe();
  }, [supabase]);

  const canSend = !!supabase && email.trim().length > 3 && (!cooldownUntil || Date.now() >= cooldownUntil);

  async function sendMagicLink() {
    if (!supabase) {
      setStatus("Missing NEXT_PUBLIC_SUPABASE_URL or NEXT_PUBLIC_SUPABASE_ANON_KEY.");
      return;
    }
    const clean = email.trim().toLowerCase();
    if (!clean.endsWith(allowedDomain)) {
      setStatus(`Only emails ending with ${allowedDomain} are allowed.`);
      return;
    }
    setCooldownUntil(Date.now() + 30_000);
    setStatus("Sending magic link...");
    const origin = window.location.origin;
    const { error } = await supabase.auth.signInWithOtp({
      email: clean,
      // After login, land directly on hypotheses list (less confusing than coming back to the login page).
      options: { emailRedirectTo: `${origin}/hypotheses` }
    });
    if (error) {
      setStatus(`Error: ${error.message}`);
      setCooldownUntil(null);
      return;
    }
    setStatus(
      [
        "Magic link sent. Check your email and click the link.",
        "",
        "If it does not arrive:",
        "- check Spam / Promotions",
        "- wait 2-3 minutes",
        "- try again later (rate limits)",
        "- if it never arrives, ask admin to configure SMTP in Supabase Auth settings"
      ].join("\n")
    );
  }

  async function signOut() {
    if (!supabase) return;
    await supabase.auth.signOut();
    setStatus("Signed out.");
  }

  function startOfWeekISO(d: Date) {
    const x = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    const day = x.getUTCDay(); // 0..6 (Sun..Sat)
    const diff = (day + 6) % 7; // Monday=0
    x.setUTCDate(x.getUTCDate() - diff);
    const y = x.getUTCFullYear();
    const m = String(x.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(x.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
  }

  function ymd(d: Date) {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
  }

  function bucketKey(weekStartYmd: string) {
    const d = new Date(`${weekStartYmd}T00:00:00.000Z`);
    const y = d.getUTCFullYear();
    const m = d.getUTCMonth() + 1;
    if (period === "week") return weekStartYmd;
    if (period === "month") return `${y}-${String(m).padStart(2, "0")}`;
    if (period === "quarter") return `${y}-Q${Math.floor((m - 1) / 3) + 1}`;
    return String(y);
  }

  function bucketLabel(key: string) {
    if (period === "week") return key;
    return key;
  }

  function maxBucketsFor(p: string) {
    // Allow year-long views without truncating; still keep a sane cap for perf.
    if (p === "day") return 400;
    if (p === "week") return 120;
    if (p === "month") return 60;
    if (p === "quarter") return 40;
    if (p === "year") return 30;
    return 52;
  }

  const COLORS = ["#7dd3fc", "#a7f3d0", "#fca5a5", "#c4b5fd", "#fde68a", "#f9a8d4", "#93c5fd", "#86efac", "#fdba74", "#fda4af", "#e9d5ff"];

  function toNum(v: any): number {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  }

  function uniq(xs: string[]) {
    return Array.from(new Set(xs));
  }

  function levenshtein(a: string, b: string) {
    const s = String(a ?? "");
    const t = String(b ?? "");
    const n = s.length;
    const m = t.length;
    if (!n) return m;
    if (!m) return n;
    const dp = new Array<number>(m + 1);
    for (let j = 0; j <= m; j++) dp[j] = j;
    for (let i = 1; i <= n; i++) {
      let prev = dp[0];
      dp[0] = i;
      for (let j = 1; j <= m; j++) {
        const tmp = dp[j];
        const cost = s[i - 1] === t[j - 1] ? 0 : 1;
        dp[j] = Math.min(dp[j] + 1, dp[j - 1] + 1, prev + cost);
        prev = tmp;
      }
    }
    return dp[m];
  }

  function canonicalizeChannelLabel(raw: string) {
    /**
     * Canonicalize channel labels for charts.
     *
     * Why: HubSpot/CRM sources can be noisy (different casing, separators, free-form labels).
     * A better canonicalization reduces the "Other channels" long tail so the dashboard is readable.
     */
    const s0 = String(raw ?? "").trim();
    // Treat common placeholders as Unknown (data quality issue, not a real channel).
    if (!s0) return "Unknown";
    if (["--", "-", "—", "na", "n/a", "none", "null"].includes(s0.toLowerCase())) return "Unknown";
    const s = s0.normalize("NFKC").replace(/\s+/g, " ").trim();

    if (s === s.toUpperCase() && /[A-Z]/.test(s)) return s;
    if (!s.includes(" ") && (s.includes("/") || s.includes("."))) {
      // Domain/URL-like values are usually referrers, not stable channels → bucket as Website,
      // with a few high-signal exceptions.
      const d = s.replace(/^https?:\/\//i, "").replace(/\/+$/g, "").toLowerCase();
      if (d.includes("linkedin")) return "LinkedIn";
      if (d.includes("youtube")) return "YouTube";
      if (d.includes("google")) return "Google";
      return "Website";
    }

    const key = s.toLowerCase();
    const keyNoSep = key.replace(/[^a-z0-9]+/g, "");
    const keyWords = key
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    // ------------------------------------------------------------------
    // High-signal bucketing rules (keep this conservative).
    // ------------------------------------------------------------------
    // Core channels
    if (keyNoSep && levenshtein(keyNoSep, "linkedin") <= 1) return "LinkedIn";
    if (keyNoSep && levenshtein(keyNoSep, "google") <= 1) return "Google";
    if (keyNoSep.includes("linkedin")) return "LinkedIn";
    if (keyNoSep.includes("youtube")) return "YouTube";
    if (keyNoSep.includes("socialmedia") || (keyWords.includes("social") && keyWords.includes("media"))) return "Social Media";
    if (keyNoSep.includes("integration")) return "Integration";

    // CRM/UI internal sources
    if (keyNoSep === "crmui" || keyNoSep === "crmui" || keyWords === "crm ui" || keyWords === "crm-ui" || keyWords === "crm_ui") return "CRM_UI";

    // Events / conferences (common free-form labels)
    if (keyNoSep.includes("conference") || keyNoSep.includes("event") || keyNoSep.includes("meetup") || keyNoSep.includes("webinar")) {
      if (keyNoSep.includes("webinar")) return "Webinar";
      return "Conference";
    }

    // Social sub-channels
    if (keyNoSep.includes("instagram")) return "Instagram";
    if (keyNoSep === "twitter" || keyNoSep === "x") return "X";

    // Referral / word of mouth
    if (keyNoSep.includes("friend") || keyWords.includes("a friend") || keyWords.includes("friends")) return "Referral";
    if (keyWords.includes("ceo") || keyWords.includes("founder")) return "Referral";

    // Content / community buckets
    if (keyNoSep.includes("blog") || keyNoSep.includes("news")) return "Content";
    if (keyNoSep.includes("community")) return "Community";

    // Obvious spam / garbage inputs (contact forms, etc.)
    if (
      keyWords.includes("password") ||
      keyWords.includes("recover gmail") ||
      keyWords.includes("guest post") ||
      keyWords.includes("link insertion") ||
      keyWords.includes("do follow") ||
      keyWords.includes("semrush") ||
      keyWords.includes("traffic") ||
      keyWords.includes("usd") ||
      keyWords.includes("job candidate")
    ) {
      return "Noise";
    }
    // Short/meaningless tokens like "D", "E", "J", "1" → Unknown
    if (keyNoSep.length <= 1) return "Unknown";

    // Website / inbound generic buckets
    if (
      keyNoSep.includes("website") ||
      keyNoSep.includes("organic") ||
      keyNoSep.includes("direct") ||
      keyNoSep.includes("seo") ||
      keyNoSep.includes("internet") ||
      keyNoSep.includes("online")
    ) {
      return "Website";
    }

    const words = key
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .split(" ")
      .filter(Boolean);
    if (!words.length) return s;
    return words.map((w) => (w.length <= 2 ? w.toUpperCase() : w[0].toUpperCase() + w.slice(1))).join(" ") || s;
  }

  function topKeysByTotal(seriesByKey: Record<string, number[]>, topN: number) {
    const keys = Object.keys(seriesByKey);
    const totals = keys.map((k) => ({ k, total: (seriesByKey[k] || []).reduce((a, b) => a + (Number(b) || 0), 0) }));
    totals.sort((a, b) => b.total - a.total);
    return totals.slice(0, topN).map((x) => x.k);
  }

  function StackedBars({
    title,
    weeks,
    series,
    right,
    below,
    onBarClick
  }: {
    title: string;
    weeks: string[];
    series: Array<{ key: string; label: string; color: string; values: number[] }>;
    right?: any;
    below?: any;
    onBarClick?: (label: string) => void;
  }) {
    const w = 980;
    const h = 260;
    const padL = 52;
    const padR = 10;
    const padT = 18;
    const padB = 26;

    const [hover, setHover] = useState<{ i: number; x: number; y: number } | null>(null);
    const chartRef = useRef<HTMLDivElement | null>(null);
    const tipRef = useRef<HTMLDivElement | null>(null);
    const [tipPos, setTipPos] = useState<{ left: number; top: number }>({ left: 8, top: 8 });

    const totals = weeks.map((_, i) => series.reduce((sum, s) => sum + (Number(s.values[i] ?? 0) || 0), 0));
    const max = totals.length ? Math.max(...totals, 1) : 1;
    const xFor = (i: number) => {
      const inner = w - padL - padR;
      const step = inner / Math.max(1, weeks.length);
      return padL + i * step;
    };
    const barW = (w - padL - padR) / Math.max(1, weeks.length) - 3;
    const yFor = (v: number) => padT + ((max - v) * (h - padT - padB)) / max;

    const ticks = 4;
    const yTicks = Array.from({ length: ticks + 1 }).map((_, i) => (max * i) / ticks);

    const hoverLabel = hover ? String(weeks[hover.i] ?? "") : "";
    const hoverTotal = hover ? Number(totals[hover.i] ?? 0) || 0 : 0;
    const hoverBreakdown = hover
      ? series
        .map((s) => ({ key: s.key, label: s.label, color: s.color, v: Number(s.values[hover.i] ?? 0) || 0 }))
        .filter((x) => x.v > 0)
        .sort((a, b) => b.v - a.v)
        .slice(0, 8)
      : [];

    useLayoutEffect(() => {
      if (!hover) return;
      const wrap = chartRef.current;
      const tip = tipRef.current;
      if (!wrap || !tip) return;
      const wRect = wrap.getBoundingClientRect();
      const tRect = tip.getBoundingClientRect();
      const pad = 8;
      const offset = 12;
      // hover coords are relative to svg box inside the wrapper.
      const rawLeft = (hover?.x ?? 0) + offset;
      const rawTop = (hover?.y ?? 0) + offset;
      let left = rawLeft;
      let top = rawTop;
      // If overflowing right/bottom, flip to left/top side.
      if (left + tRect.width > wRect.width - pad) left = (hover?.x ?? 0) - tRect.width - offset;
      if (top + tRect.height > wRect.height - pad) top = (hover?.y ?? 0) - tRect.height - offset;
      // Clamp to wrapper bounds
      left = Math.max(pad, Math.min(left, wRect.width - tRect.width - pad));
      top = Math.max(pad, Math.min(top, wRect.height - tRect.height - pad));
      setTipPos({ left, top });
    }, [hover?.i, hover?.x, hover?.y, hoverBreakdown.length, hoverTotal]);

    return (
      <div className="card" style={{ gridColumn: "span 12" }}>
        <div className="cardHeader">
          <div>
            <div className="cardTitle">{title}</div>
            <div className="cardDesc">
              {weeks.length ? (weeks.length === 1 ? weeks[0] : `${weeks[0]} → ${weeks[weeks.length - 1]}`) : "No data"} · {period}
            </div>
          </div>
          {right ? <div className="btnRow">{right}</div> : null}
        </div>
        <div className="cardBody">
          {!weeks.length ? (
            <div className="muted2">No snapshots in selected range. Run “Sync history” on hypotheses.</div>
          ) : (
            <>
              <div ref={chartRef} style={{ position: "relative" }}>
                <svg
                  viewBox={`0 0 ${w} ${h}`}
                  width="100%"
                  height={h}
                  style={{ display: "block" }}
                  onMouseLeave={() => setHover(null)}
                >
                  {yTicks.map((t, i) => {
                    const y = yFor(t);
                    return (
                      <g key={i}>
                        <line x1={padL} y1={y} x2={w - padR} y2={y} stroke="rgba(255,255,255,0.08)" strokeWidth={1} />
                        <text x={padL - 8} y={y + 4} textAnchor="end" fontSize="11" fill="rgba(255,255,255,0.55)">
                          {Math.round(t)}
                        </text>
                      </g>
                    );
                  })}
                  {weeks.map((wk, i) => {
                    const step = Math.max(1, Math.floor(weeks.length / 6));
                    if (i % step !== 0 && i !== weeks.length - 1) return null;
                    const x = xFor(i) + barW / 2;
                    return (
                      <text key={wk} x={x} y={h - 8} textAnchor="middle" fontSize="11" fill="rgba(255,255,255,0.45)">
                        {String(wk).length >= 10 ? String(wk).slice(5) : String(wk)}
                      </text>
                    );
                  })}
                  {weeks.map((wk, i) => {
                    let acc = 0;
                    const x = xFor(i);
                    return (
                      <g key={wk}>
                        {series.map((s) => {
                          const v = Number(s.values[i] ?? 0) || 0;
                          if (!v) return null;
                          const y0 = yFor(acc);
                          const y1 = yFor(acc + v);
                          const rectY = y1;
                          const rectH = Math.max(0.5, y0 - y1);
                          acc += v;
                          return <rect key={s.key} x={x} y={rectY} width={barW} height={rectH} fill={s.color} opacity={0.95} rx={2} ry={2} />;
                        })}

                        {/* hover/click target */}
                        <rect
                          x={x}
                          y={padT}
                          width={barW}
                          height={h - padT - padB}
                          fill="transparent"
                          pointerEvents="all"
                          style={{ cursor: onBarClick ? "pointer" : "default" }}
                          onMouseMove={(e: any) => {
                            const svg = e.currentTarget.ownerSVGElement as any;
                            const rect = svg?.getBoundingClientRect?.();
                            const cx = e.clientX ?? 0;
                            const cy = e.clientY ?? 0;
                            const relX = rect ? cx - rect.left : 0;
                            const relY = rect ? cy - rect.top : 0;
                            setHover({ i, x: relX, y: relY });
                          }}
                          onMouseEnter={() => {
                            // Ensure tooltip appears even if mouse doesn't move (trackpads).
                            const xCenter = x + barW / 2;
                            const yTop = padT + 10;
                            setHover({ i, x: xCenter, y: yTop });
                          }}
                          onClick={() => {
                            if (!onBarClick) return;
                            onBarClick(String(wk));
                          }}
                        />
                      </g>
                    );
                  })}
                </svg>

                {hover ? (
                  <div
                    className="card"
                    style={{
                      position: "absolute",
                      left: tipPos.left,
                      top: tipPos.top,
                      padding: 10,
                      width: 260,
                      background: "rgba(10,12,18,0.92)",
                      border: "1px solid rgba(255,255,255,0.10)",
                      borderRadius: 12,
                      pointerEvents: "none"
                    }}
                    ref={tipRef}
                  >
                    <div className="mono" style={{ fontSize: 12, opacity: 0.85 }}>{hoverLabel}</div>
                    <div style={{ marginTop: 4, fontSize: 18, fontWeight: 700 }} className="mono">{hoverTotal}</div>
                    <div style={{ marginTop: 8, display: "grid", gap: 6 }}>
                      {hoverBreakdown.map((x) => (
                        <div key={x.key} style={{ display: "flex", gap: 8, alignItems: "center" }}>
                          <span style={{ width: 10, height: 10, borderRadius: 2, background: x.color, display: "inline-block" }} />
                          <span style={{ flex: 1, fontSize: 12, opacity: 0.9, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                            {x.label}
                          </span>
                          <span className="mono" style={{ fontSize: 12 }}>{x.v}</span>
                        </div>
                      ))}
                      {!hoverBreakdown.length ? <div className="muted2" style={{ fontSize: 12 }}>No deals</div> : null}
                    </div>
                  </div>
                ) : null}
              </div>
              <div className="btnRow" style={{ flexWrap: "wrap", gap: 10, justifyContent: "flex-start", marginTop: 10 }}>
                {series.slice(0, 12).map((s) => (
                  <span key={s.key} className="tag" style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                    <span style={{ width: 10, height: 10, borderRadius: 2, background: s.color, display: "inline-block" }} />
                    <span>{s.label}</span>
                  </span>
                ))}
              </div>
              {below ? <div style={{ marginTop: 14 }}>{below}</div> : null}
            </>
          )}
        </div>
      </div>
    );
  }

  function StackedBarsWithLine({
    title,
    buckets,
    series,
    line,
    lineLabel,
    right,
    below,
    onBarClick
  }: {
    title: string;
    buckets: Array<{ key: string; label: string }>;
    series: Array<{ key: string; label: string; color: string; values: number[] }>;
    line: number[]; // total line per bucket
    lineLabel: string;
    right?: any;
    below?: any;
    onBarClick?: (bucketKey: string) => void;
  }) {
    const w = 980;
    const h = 290;
    const padL = 52;
    const padR = 52;
    const padT = 18;
    const padB = 26;

    const [hover, setHover] = useState<{ i: number; x: number; y: number } | null>(null);
    const chartRef = useRef<HTMLDivElement | null>(null);
    const tipRef = useRef<HTMLDivElement | null>(null);
    const [tipPos, setTipPos] = useState<{ left: number; top: number }>({ left: 8, top: 8 });

    const labels = buckets.map((b) => String(b.label));
    const totals = labels.map((_, i) => series.reduce((sum, s) => sum + (Number(s.values[i] ?? 0) || 0), 0));
    const maxBars = totals.length ? Math.max(...totals, 1) : 1;
    const maxLine = line.length ? Math.max(...line.map((x) => Number(x) || 0), 1) : 1;

    const xFor = (i: number) => {
      const inner = w - padL - padR;
      const step = inner / Math.max(1, labels.length);
      return padL + i * step;
    };
    const barW = (w - padL - padR) / Math.max(1, labels.length) - 3;
    const yBars = (v: number) => padT + ((maxBars - v) * (h - padT - padB)) / maxBars;
    const yLine = (v: number) => padT + ((maxLine - v) * (h - padT - padB)) / maxLine;

    const ticks = 4;
    const yTicksBars = Array.from({ length: ticks + 1 }).map((_, i) => (maxBars * i) / ticks);
    const yTicksLine = Array.from({ length: ticks + 1 }).map((_, i) => (maxLine * i) / ticks);

    const hoverLabel = hover ? String(labels[hover.i] ?? "") : "";
    const hoverTotal = hover ? Number(totals[hover.i] ?? 0) || 0 : 0;
    const hoverLine = hover ? Number(line[hover.i] ?? 0) || 0 : 0;
    const hoverBreakdown = hover
      ? series
        .map((s) => ({ key: s.key, label: s.label, color: s.color, v: Number(s.values[hover.i] ?? 0) || 0 }))
        .filter((x) => x.v > 0)
        .sort((a, b) => b.v - a.v)
        .slice(0, 8)
      : [];

    useLayoutEffect(() => {
      if (!hover) return;
      const wrap = chartRef.current;
      const tip = tipRef.current;
      if (!wrap || !tip) return;
      const wRect = wrap.getBoundingClientRect();
      const tRect = tip.getBoundingClientRect();
      const pad = 8;
      const offset = 12;
      let left = (hover?.x ?? 0) + offset;
      let top = (hover?.y ?? 0) + offset;
      if (left + tRect.width > wRect.width - pad) left = (hover?.x ?? 0) - tRect.width - offset;
      if (top + tRect.height > wRect.height - pad) top = (hover?.y ?? 0) - tRect.height - offset;
      left = Math.max(pad, Math.min(left, wRect.width - tRect.width - pad));
      top = Math.max(pad, Math.min(top, wRect.height - tRect.height - pad));
      setTipPos({ left, top });
    }, [hover?.i, hover?.x, hover?.y, hoverBreakdown.length, hoverTotal, hoverLine]);

    const linePath = useMemo(() => {
      if (!labels.length) return "";
      const pts = labels.map((_, i) => {
        const x = xFor(i) + barW / 2;
        const y = yLine(Number(line[i] ?? 0) || 0);
        return [x, y] as [number, number];
      });
      return pts
        .map((p, i) => (i === 0 ? `M ${p[0].toFixed(2)} ${p[1].toFixed(2)}` : `L ${p[0].toFixed(2)} ${p[1].toFixed(2)}`))
        .join(" ");
    }, [labels.length, line.join(","), barW]);

    return (
      <div className="card" style={{ gridColumn: "span 12" }}>
        <div className="cardHeader">
          <div>
            <div className="cardTitle">{title}</div>
            <div className="cardDesc">
              {labels.length ? (labels.length === 1 ? labels[0] : `${labels[0]} → ${labels[labels.length - 1]}`) : "No data"} · {period}
              {" "}· Line: <span className="mono">{lineLabel}</span>
            </div>
          </div>
          {right ? <div className="btnRow">{right}</div> : null}
        </div>
        <div className="cardBody">
          {!labels.length ? (
            <div className="muted2">No daily snapshots yet. Press Sync to backfill last year.</div>
          ) : (
            <>
              <div ref={chartRef} style={{ position: "relative" }}>
                <svg
                  viewBox={`0 0 ${w} ${h}`}
                  width="100%"
                  height={h}
                  style={{ display: "block" }}
                  onMouseLeave={() => setHover(null)}
                >
                  {/* left axis (bars) grid */}
                  {yTicksBars.map((t, i) => {
                    const y = yBars(t);
                    return (
                      <g key={i}>
                        <line x1={padL} y1={y} x2={w - padR} y2={y} stroke="rgba(255,255,255,0.08)" strokeWidth={1} />
                        <text x={padL - 8} y={y + 4} textAnchor="end" fontSize="11" fill="rgba(255,255,255,0.55)">
                          {Math.round(t)}
                        </text>
                      </g>
                    );
                  })}
                  {/* right axis (line) ticks */}
                  {yTicksLine.map((t, i) => {
                    const y = yLine(t);
                    return (
                      <text key={i} x={w - padR + 8} y={y + 4} textAnchor="start" fontSize="11" fill="rgba(255,255,255,0.45)">
                        {Math.round(t)}
                      </text>
                    );
                  })}

                  {/* x labels */}
                  {labels.map((lbl, i) => {
                    const step = Math.max(1, Math.floor(labels.length / 6));
                    if (i % step !== 0 && i !== labels.length - 1) return null;
                    const x = xFor(i) + barW / 2;
                    return (
                      <text key={lbl + i} x={x} y={h - 8} textAnchor="middle" fontSize="11" fill="rgba(255,255,255,0.45)">
                        {String(lbl).length >= 10 ? String(lbl).slice(5) : String(lbl)}
                      </text>
                    );
                  })}

                  {/* bars */}
                  {labels.map((lbl, i) => {
                    let acc = 0;
                    const x = xFor(i);
                    return (
                      <g key={lbl + i}>
                        {series.map((s) => {
                          const v = Number(s.values[i] ?? 0) || 0;
                          if (!v) return null;
                          const y0 = yBars(acc);
                          const y1 = yBars(acc + v);
                          const rectY = y1;
                          const rectH = Math.max(0.5, y0 - y1);
                          acc += v;
                          return <rect key={s.key} x={x} y={rectY} width={barW} height={rectH} fill={s.color} opacity={0.95} rx={2} ry={2} />;
                        })}

                        <rect
                          x={x}
                          y={padT}
                          width={barW}
                          height={h - padT - padB}
                          fill="transparent"
                          pointerEvents="all"
                          style={{ cursor: onBarClick ? "pointer" : "default" }}
                          onMouseMove={(e: any) => {
                            const svg = e.currentTarget.ownerSVGElement as any;
                            const rect = svg?.getBoundingClientRect?.();
                            const cx = e.clientX ?? 0;
                            const cy = e.clientY ?? 0;
                            const relX = rect ? cx - rect.left : 0;
                            const relY = rect ? cy - rect.top : 0;
                            setHover({ i, x: relX, y: relY });
                          }}
                          onMouseEnter={() => {
                            const xCenter = x + barW / 2;
                            const yTop = padT + 10;
                            setHover({ i, x: xCenter, y: yTop });
                          }}
                          onClick={() => {
                            if (!onBarClick) return;
                            onBarClick(String(buckets[i]?.key ?? ""));
                          }}
                        />
                      </g>
                    );
                  })}

                  {/* line */}
                  <path d={linePath} fill="none" stroke="rgba(125,211,252,0.95)" strokeWidth={2.5} />
                  {labels.map((_, i) => {
                    const x = xFor(i) + barW / 2;
                    const y = yLine(Number(line[i] ?? 0) || 0);
                    return <circle key={i} cx={x} cy={y} r={2.2} fill="rgba(125,211,252,0.95)" />;
                  })}
                </svg>

                {hover ? (
                  <div
                    className="card"
                    style={{
                      position: "absolute",
                      left: tipPos.left,
                      top: tipPos.top,
                      padding: 10,
                      width: 280,
                      background: "rgba(10,12,18,0.92)",
                      border: "1px solid rgba(255,255,255,0.10)",
                      borderRadius: 12,
                      pointerEvents: "none"
                    }}
                    ref={tipRef}
                  >
                    <div className="mono" style={{ fontSize: 12, opacity: 0.85 }}>{hoverLabel}</div>
                    <div style={{ marginTop: 4, display: "flex", justifyContent: "space-between", gap: 10, alignItems: "baseline" }}>
                      <div>
                        <div className="muted2" style={{ fontSize: 11 }}>New deals</div>
                        <div style={{ fontSize: 18, fontWeight: 700 }} className="mono">{hoverTotal}</div>
                      </div>
                      <div style={{ textAlign: "right" }}>
                        <div className="muted2" style={{ fontSize: 11 }}>{lineLabel}</div>
                        <div style={{ fontSize: 18, fontWeight: 700 }} className="mono">{hoverLine}</div>
                      </div>
                    </div>
                    <div style={{ marginTop: 10, display: "grid", gap: 6 }}>
                      {hoverBreakdown.map((x) => (
                        <div key={x.key} style={{ display: "flex", gap: 8, alignItems: "center" }}>
                          <span style={{ width: 10, height: 10, borderRadius: 2, background: x.color, display: "inline-block" }} />
                          <span style={{ flex: 1, fontSize: 12, opacity: 0.9, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                            {x.label}
                          </span>
                          <span className="mono" style={{ fontSize: 12 }}>{x.v}</span>
                        </div>
                      ))}
                      {!hoverBreakdown.length ? <div className="muted2" style={{ fontSize: 12 }}>No deals</div> : null}
                    </div>
                  </div>
                ) : null}
              </div>

              <div className="btnRow" style={{ flexWrap: "wrap", gap: 10, justifyContent: "flex-start", marginTop: 10 }}>
                {series.slice(0, 12).map((s) => (
                  <span key={s.key} className="tag" style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                    <span style={{ width: 10, height: 10, borderRadius: 2, background: s.color, display: "inline-block" }} />
                    <span>{s.label}</span>
                  </span>
                ))}
                <span className="tag" style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                  <span style={{ width: 14, height: 2, background: "rgba(125,211,252,0.95)", display: "inline-block" }} />
                  <span>{lineLabel}</span>
                </span>
              </div>
              {below ? <div style={{ marginTop: 14 }}>{below}</div> : null}
            </>
          )}
        </div>
      </div>
    );
  }

  function MultiLineChart({
    title,
    days,
    series,
    right,
    below
  }: {
    title: string;
    days: string[];
    series: Array<{ key: string; label: string; color: string; values: number[] }>;
    right?: any;
    below?: any;
  }) {
    const w = 980;
    const h = 290;
    const padL = 52;
    const padR = 20;
    const padT = 18;
    const padB = 26;

    const [hover, setHover] = useState<{ i: number; x: number; y: number } | null>(null);
    const chartRef = useRef<HTMLDivElement | null>(null);
    const tipRef = useRef<HTMLDivElement | null>(null);
    const [tipPos, setTipPos] = useState<{ left: number; top: number }>({ left: 8, top: 8 });

    const max = useMemo(() => {
      let m = 1;
      for (const s of series) for (const v of s.values || []) m = Math.max(m, Number(v) || 0);
      return Math.max(1, m);
    }, [series]);

    const xFor = (i: number) => {
      const inner = w - padL - padR;
      const step = inner / Math.max(1, days.length - 1);
      return padL + i * step;
    };
    const yFor = (v: number) => padT + ((max - v) * (h - padT - padB)) / max;

    const ticks = 4;
    const yTicks = Array.from({ length: ticks + 1 }).map((_, i) => (max * i) / ticks);

    const hoverLabel = hover ? String(days[hover.i] ?? "") : "";
    const hoverBreakdown = hover
      ? series
        .map((s) => ({ key: s.key, label: s.label, color: s.color, v: Number(s.values[hover.i] ?? 0) || 0 }))
        .filter((x) => x.v > 0)
        .sort((a, b) => b.v - a.v)
      : [];
    const hoverTotal = hover ? hoverBreakdown.reduce((sum, x) => sum + (Number(x.v) || 0), 0) : 0;

    useLayoutEffect(() => {
      if (!hover) return;
      const wrap = chartRef.current;
      const tip = tipRef.current;
      if (!wrap || !tip) return;
      const wRect = wrap.getBoundingClientRect();
      const tRect = tip.getBoundingClientRect();
      const pad = 8;
      const offset = 12;
      let left = (hover?.x ?? 0) + offset;
      let top = (hover?.y ?? 0) + offset;
      if (left + tRect.width > wRect.width - pad) left = (hover?.x ?? 0) - tRect.width - offset;
      if (top + tRect.height > wRect.height - pad) top = (hover?.y ?? 0) - tRect.height - offset;
      left = Math.max(pad, Math.min(left, wRect.width - tRect.width - pad));
      top = Math.max(pad, Math.min(top, wRect.height - tRect.height - pad));
      setTipPos({ left, top });
    }, [hover?.i, hover?.x, hover?.y, hoverBreakdown.length, hoverTotal]);

    const paths = useMemo(() => {
      return series.map((s) => {
        const pts = (s.values || []).map((v, i) => {
          const x = xFor(i);
          const y = yFor(Number(v) || 0);
          return [x, y] as [number, number];
        });
        const d = pts.map((p, i) => (i === 0 ? `M ${p[0].toFixed(2)} ${p[1].toFixed(2)}` : `L ${p[0].toFixed(2)} ${p[1].toFixed(2)}`)).join(" ");
        return { key: s.key, color: s.color, d };
      });
    }, [series, days.length, max]);

    return (
      <div className="card" style={{ gridColumn: "span 12" }}>
        <div className="cardHeader">
          <div>
            <div className="cardTitle">{title}</div>
            <div className="cardDesc">
              {days.length ? (days.length === 1 ? days[0] : `${days[0]} → ${days[days.length - 1]}`) : "No data"} · {period}
            </div>
          </div>
          {right ? <div className="btnRow">{right}</div> : null}
        </div>
        <div className="cardBody">
          {!days.length ? (
            <div className="muted2">No activity events in selected range.</div>
          ) : (
            <>
              <div ref={chartRef} style={{ position: "relative" }}>
                <svg viewBox={`0 0 ${w} ${h}`} width="100%" height={h} style={{ display: "block" }} onMouseLeave={() => setHover(null)}>
                  {yTicks.map((t, i) => {
                    const y = yFor(t);
                    return (
                      <g key={i}>
                        <line x1={padL} y1={y} x2={w - padR} y2={y} stroke="rgba(255,255,255,0.08)" strokeWidth={1} />
                        <text x={padL - 8} y={y + 4} textAnchor="end" fontSize="11" fill="rgba(255,255,255,0.55)">
                          {Math.round(t)}
                        </text>
                      </g>
                    );
                  })}

                  {/* x labels */}
                  {days.map((d, i) => {
                    const step = Math.max(1, Math.floor(days.length / 6));
                    if (i % step !== 0 && i !== days.length - 1) return null;
                    const x = xFor(i);
                    return (
                      <text key={d + i} x={x} y={h - 8} textAnchor="middle" fontSize="11" fill="rgba(255,255,255,0.45)">
                        {String(d).slice(5)}
                      </text>
                    );
                  })}

                  {/* lines */}
                  {paths.map((p) => (
                    <path key={p.key} d={p.d} fill="none" stroke={p.color} strokeWidth={2.6} />
                  ))}

                  {/* hover targets */}
                  {days.map((d, i) => {
                    const x = xFor(i);
                    return (
                      <rect
                        key={d + i}
                        x={x - 8}
                        y={padT}
                        width={16}
                        height={h - padT - padB}
                        fill="transparent"
                        pointerEvents="all"
                        onMouseMove={(e: any) => {
                          const svg = e.currentTarget.ownerSVGElement as any;
                          const rect = svg?.getBoundingClientRect?.();
                          const cx = e.clientX ?? 0;
                          const cy = e.clientY ?? 0;
                          const relX = rect ? cx - rect.left : 0;
                          const relY = rect ? cy - rect.top : 0;
                          setHover({ i, x: relX, y: relY });
                        }}
                        onMouseEnter={() => {
                          const yTop = padT + 10;
                          setHover({ i, x, y: yTop });
                        }}
                      />
                    );
                  })}
                </svg>

                {hover ? (
                  <div
                    className="card"
                    style={{
                      position: "absolute",
                      left: tipPos.left,
                      top: tipPos.top,
                      padding: 10,
                      width: 280,
                      background: "rgba(10,12,18,0.92)",
                      border: "1px solid rgba(255,255,255,0.10)",
                      borderRadius: 12,
                      pointerEvents: "none"
                    }}
                    ref={tipRef}
                  >
                    <div className="mono" style={{ fontSize: 12, opacity: 0.85 }}>{hoverLabel}</div>
                    <div style={{ marginTop: 4, fontSize: 18, fontWeight: 700 }} className="mono">{hoverTotal}</div>
                    <div style={{ marginTop: 8, display: "grid", gap: 6 }}>
                      {hoverBreakdown.map((x) => (
                        <div key={x.key} style={{ display: "flex", gap: 8, alignItems: "center" }}>
                          <span style={{ width: 10, height: 10, borderRadius: 2, background: x.color, display: "inline-block" }} />
                          <span style={{ flex: 1, fontSize: 12, opacity: 0.9, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                            {x.label}
                          </span>
                          <span className="mono" style={{ fontSize: 12 }}>{x.v}</span>
                        </div>
                      ))}
                      {!hoverBreakdown.length ? <div className="muted2" style={{ fontSize: 12 }}>No activity</div> : null}
                    </div>
                  </div>
                ) : null}
              </div>
              <div className="btnRow" style={{ flexWrap: "wrap", gap: 10, justifyContent: "flex-start", marginTop: 10 }}>
                {series.slice(0, 12).map((s) => (
                  <span key={s.key} className="tag" style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                    <span style={{ width: 10, height: 10, borderRadius: 2, background: s.color, display: "inline-block" }} />
                    <span>{s.label}</span>
                  </span>
                ))}
              </div>
              {below ? <div style={{ marginTop: 14 }}>{below}</div> : null}
            </>
          )}
        </div>
      </div>
    );
  }

  async function loadGlobal() {
    if (!supabase) return;
    setDashStatus("Loading dashboard...");
    const sess = await supabase.auth.getSession();
    if (!sess.data.session) return setDashStatus("Not signed in.");

    const [hRes, sRes, dRes] = await Promise.all([
      supabase.from("sales_hypotheses").select("id,title,status").order("updated_at", { ascending: false }).limit(500),
      supabase
        .from("sales_hubspot_global_snapshots")
        .select("period_start,window_days,new_leads_count,new_opps_count,new_customers_count,new_churn_count,funnel_by_channel_json,funnel_by_hypothesis_json,updated_at")
        .eq("window_days", 7)
        .order("period_start", { ascending: true })
        .limit(5000)
      ,
      supabase
        .from("sales_hubspot_global_daily_snapshots")
        .select("period_day,pipeline_id,new_deals_count,active_delta_count,new_deals_by_channel_json,new_deals_by_hypothesis_json,updated_at")
        .order("period_day", { ascending: true })
        .limit(5000)
    ]);
    if (hRes.error) return setDashStatus(`hypotheses error: ${hRes.error.message}`);
    if (sRes.error) {
      const msg = String(sRes.error.message || "");
      if (msg.toLowerCase().includes("does not exist")) {
        setHyps((hRes.data ?? []) as any[]);
        setSnaps([]);
        setDailySnaps([]);
        setDashStatus("HubSpot snapshots table is missing. Apply schema update and run Sync history.");
        return;
      }
      return setDashStatus(`snapshots error: ${sRes.error.message}`);
    }
    if (dRes.error) {
      const msg = String(dRes.error.message || "");
      if (msg.toLowerCase().includes("does not exist")) {
        setDailySnaps([]);
      } else {
        setDailySnaps([]);
        // keep weekly working even if daily is missing
        setDashStatus(`daily snapshots error: ${dRes.error.message}`);
        return;
      }
    } else {
      setDailySnaps((dRes.data ?? []) as any[]);
    }
    setHyps((hRes.data ?? []) as any[]);
    setSnaps((sRes.data ?? []) as any[]);
    setDashStatus("");
  }

  async function syncNow() {
    // Manual sync: run the unified sync flow (same as cron), then refresh UI.
    try {
      setDashStatus("Syncing now...");
      const sess = await supabase.auth.getSession();
      const token = sess.data.session?.access_token ?? "";
      if (!token) throw new Error("Not signed in.");

      const res = await fetch("/api/sync/all", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({})
      });
      const text = await res.text();
      let json: any = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch {
        json = { raw: text };
      }

      const failed = Array.isArray(json?.failed) ? json.failed : [];
      const failedMsg = failed
        .map((x: any) => {
          const name = String(x?.name ?? "").trim();
          const err = String(x?.error ?? x?.message ?? "").trim();
          const status = x?.status != null ? String(x.status) : "";
          const head = name || status ? `${name || "step"}${status ? ` (${status})` : ""}` : "";
          return [head, err].filter(Boolean).join(": ");
        })
        .filter(Boolean);

      if (!res.ok || !json?.ok) {
        setDashStatus(failedMsg.length ? `Sync finished with errors: ${failedMsg.slice(0, 3).join(" · ")}` : `Sync error: ${String(json?.error ?? "failed")}`);
        // Still refresh UI: some steps may have succeeded.
        await loadGlobal();
        return;
      }

      if (failed.length) setDashStatus(`Sync finished with errors: ${failedMsg.slice(0, 3).join(" · ")}`);
      else setDashStatus("Synced.");

      await loadGlobal();
    } catch (e: any) {
      setDashStatus(`Sync error: ${String(e?.message || e)}`);
    }
  }

  async function loadDealsTable(metricKey: string, periodStart: string) {
    if (!supabase) return;
    setTableErrByMetric((p) => ({ ...p, [metricKey]: "" }));
    setTableLoadingByMetric((p) => ({ ...p, [metricKey]: true }));
    try {
      const sess = await supabase.auth.getSession();
      const token = sess.data.session?.access_token ?? "";
      if (!token) throw new Error("Not signed in.");
      // Read from snapshots to keep counts consistent with charts.
      const res = await supabase
        .from("sales_hubspot_global_snapshots")
        .select("data_json")
        .eq("window_days", 7)
        .eq("period_start", periodStart)
        .limit(1)
        .maybeSingle();
      if (res.error) throw new Error(res.error.message);
      const dj = (res.data as any)?.data_json ?? null;
      if (!dj || !dj.deals_by_metric) {
        throw new Error("Snapshot has no per-metric deals yet. Press Sync and retry.");
      }
      const deals = dj?.deals_by_metric?.[metricKey] ?? [];
      setTableRowsByMetric((p) => ({ ...p, [metricKey]: Array.isArray(deals) ? deals : [] }));
    } catch (e: any) {
      setTableErrByMetric((p) => ({ ...p, [metricKey]: String(e?.message || e) }));
      setTableRowsByMetric((p) => ({ ...p, [metricKey]: [] }));
    } finally {
      setTableLoadingByMetric((p) => ({ ...p, [metricKey]: false }));
    }
  }

  useEffect(() => {
    if (!sessionEmail) return;
    loadGlobal();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionEmail, period, supabase]);

  // Auto-select a reasonable default pipeline (reduces user steps).
  useEffect(() => {
    if (!pipelines.length) return;
    if (selectedPipelineIds.length) return;
    const p =
      pipelines.find((x: any) => String(x?.label ?? "").toLowerCase().includes("pipeline")) ||
      (pipelines.length === 1 ? pipelines[0] : pipelines[0]) ||
      null;
    const id = p?.id ? String(p.id) : "";
    if (id) setSelectedPipelineIds([id]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pipelines.length]);

  useEffect(() => {
    if (!sessionEmail) return;
    // Background sync is handled by Vercel Cron (/api/sync/all) every 30 minutes.
  }, [sessionEmail]);

  const hypById = useMemo(() => {
    const m = new Map<string, any>();
    for (const h of hyps) m.set(String(h.id), h);
    return m;
  }, [hyps]);

  const allWeeks = useMemo(() => uniq(snaps.map((r) => String(r.period_start))).sort(), [snaps]);

  const lastSyncMs = useMemo(() => {
    const msList: number[] = [];
    for (const r of Array.isArray(snaps) ? snaps : []) {
      const t = Date.parse(String((r as any)?.updated_at ?? ""));
      if (Number.isFinite(t)) msList.push(t);
    }
    for (const r of Array.isArray(dailySnaps) ? dailySnaps : []) {
      const t = Date.parse(String((r as any)?.updated_at ?? ""));
      if (Number.isFinite(t)) msList.push(t);
    }
    return msList.length ? Math.max(...msList) : null;
  }, [snaps, dailySnaps]);

  const lastSyncAgo = useMemo(() => {
    if (!lastSyncMs) return "";
    const delta = Math.max(0, Date.now() - lastSyncMs);
    const mins = Math.round(delta / 60000);
    if (mins < 1) return "just now";
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.round(mins / 60);
    if (hrs < 48) return `${hrs}h ago`;
    const days = Math.round(hrs / 24);
    return `${days}d ago`;
  }, [lastSyncMs]);

  // ===== Activities (GetSales) =====
  const [activityEvents, setActivityEvents] = useState<any[]>([]);
  const [activityErr, setActivityErr] = useState<string>("");
  const [activityLoading, setActivityLoading] = useState<boolean>(false);
  const [getsalesInfluence, setGetsalesInfluence] = useState<any | null>(null);
  const [getsalesInfluenceErr, setGetsalesInfluenceErr] = useState<string>("");
  const [getsalesInfluenceLoading, setGetsalesInfluenceLoading] = useState<boolean>(false);
  const [getsalesInfluenceLookbackDays, setGetsalesInfluenceLookbackDays] = useState<number>(180);

  // ===== SmartLead Email (direct) =====
  const [smartleadEvents, setSmartleadEvents] = useState<any[]>([]);
  const [smartleadEventsErr, setSmartleadEventsErr] = useState<string>("");
  const [smartleadInfluence, setSmartleadInfluence] = useState<any | null>(null);
  const [smartleadInfluenceErr, setSmartleadInfluenceErr] = useState<string>("");
  const [smartleadInfluenceLoading, setSmartleadInfluenceLoading] = useState<boolean>(false);
  const [smartleadInfluenceLookbackDays, setSmartleadInfluenceLookbackDays] = useState<number>(180);

  useEffect(() => {
    // Don't gate on `sessionEmail`: auth state can lag due to multiple GoTrueClient instances / token refresh.
    // If we have a valid Supabase session token, we can load activities and metrics safely.
    if (!supabase) return;
    if (viewMode !== "activities") return;
    const since = String(dateRange?.since || "");
    const until = String(dateRange?.until || "");
    if (!since || !until) return;
    let cancelled = false;
    const t = setTimeout(async () => {
      try {
        setActivityErr("");
        setActivityLoading(true);
        const sess = await supabase.auth.getSession();
        const token = sess.data.session?.access_token ?? "";
        if (!token) throw new Error("Not signed in.");
        // sales_analytics_activities is the source of truth for dashboard analytics.
        const sinceIso = `${since}T00:00:00.000Z`;
        const untilExcl = ymdAddDaysLocal(until, 1);
        const untilIso = `${untilExcl}T00:00:00.000Z`;
        const rows = await listAnalyticsEventsPaged({
          supabase,
          sinceIso,
          untilIso,
          // Avoid truncating end-of-range days when months exceed large volumes.
          maxRows: 1000000,
          sourceSystems: ["getsales", "smartlead"]
        });
        if (cancelled) return;
        setActivityEvents(rows);
        setSmartleadEvents(rows.filter((r: any) => String(r?.source_system ?? "").toLowerCase() === "smartlead"));
        setSmartleadEventsErr("");
      } catch (e: any) {
        if (cancelled) return;
        setActivityEvents([]);
        setActivityErr(String(e?.message || e));
        setSmartleadEvents([]);
        setSmartleadEventsErr(String(e?.message || e));
      } finally {
        if (!cancelled) setActivityLoading(false);
      }
    }, 250);
    return () => {
      cancelled = true;
      clearTimeout(t);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [supabase, viewMode, dateRange.since, dateRange.until]);

  // SmartLead campaigns list (for Email tab filter UI)
  useEffect(() => {
    if (!supabase) return;
    if (viewMode !== "activities") return;
    if (activityReportTab !== "email") return;
    let cancelled = false;
    const t = setTimeout(async () => {
      try {
        setSmartleadCampaignsErr("");
        setSmartleadCampaignsLoading(true);
        const sess = await supabase.auth.getSession();
        const token = sess.data.session?.access_token ?? "";
        if (!token) throw new Error("Not signed in.");
        const res = await fetch("/api/smartlead/campaigns", {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
          body: JSON.stringify({
            include_stats: smartleadCampaignIds.length > 0 && smartleadCampaignIds.length <= 5,
            all: true,
            limit: 200,
            offset: 0
          })
        });
        const json = (await res.json()) as any;
        if (!res.ok || !json?.ok) throw new Error(String(json?.error ?? "Failed"));
        const campaigns = Array.isArray(json?.campaigns) ? json.campaigns : [];
        if (!cancelled) setSmartleadCampaigns(campaigns);
        // Sanitize saved selection: drop invalid ids (like 0) and ids not present in the fetched campaign list.
        const available = new Set(campaigns.map((c: any) => String(Number(c?.id))).filter(Boolean));
        setSmartleadCampaignIds((prev) => {
          const cleaned = (prev || [])
            .map((x) => Number(x))
            .filter((n) => Number.isFinite(n) && n > 0)
            .map((n) => String(n));
          const filtered = cleaned.filter((id) => available.size === 0 || available.has(id));
          return filtered;
        });
      } catch (e: any) {
        if (!cancelled) {
          setSmartleadCampaigns([]);
          setSmartleadCampaignsErr(String(e?.message || e));
        }
      } finally {
        if (!cancelled) setSmartleadCampaignsLoading(false);
      }
    }, 350);
    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [supabase, viewMode, activityReportTab]);

  /**
   * Fetch analytics events in pages to avoid PostgREST row limits.
   *
   * @param {object} opts
   * @param {any} opts.supabase - Supabase client
   * @param {string} opts.sinceIso - ISO start (inclusive)
   * @param {string} opts.untilIso - ISO end (exclusive)
   * @param {number} opts.maxRows - Max rows to fetch
   * @param {string[]} opts.sourceSystems - Optional source filters
   * @returns {Promise<any[]>} Paginated analytics rows
   */
  async function listAnalyticsEventsPaged(opts: {
    supabase: any;
    sinceIso: string;
    untilIso: string;
    maxRows: number;
    sourceSystems?: string[];
  }) {
    const out: any[] = [];
    const max = Math.max(1, Math.min(50000, Number(opts.maxRows || 20000)));
    const pageSize = 1000;
    const sources = Array.isArray(opts.sourceSystems) ? opts.sourceSystems.map((s) => String(s).trim()).filter(Boolean) : [];
    for (let offset = 0; offset < max; offset += pageSize) {
      let q = opts.supabase
        .from("sales_analytics_activities")
        .select("id,occurred_at,activity_type,direction,source_system,source_id,event_kind,message_hash,is_inmail,contact_email,smartlead_campaign_id,lead_uuid,contact_id,hubspot_engagement_id")
        .gte("occurred_at", opts.sinceIso)
        .lt("occurred_at", opts.untilIso)
        .order("occurred_at", { ascending: true })
        .range(offset, offset + pageSize - 1);
      if (sources.length) q = q.in("source_system", sources);
      const res = await q;
      if (res.error) throw new Error(res.error.message);
      const rows = Array.isArray(res.data) ? res.data : [];
      out.push(...rows);
      if (rows.length < pageSize) break;
    }
    return out.slice(0, max);
  }

  /**
   * Build LinkedIn report from analytics events.
   *
   * Uses unique lead_uuid for both connections and message metrics to mirror GetSales UI.
   * InMail is excluded from message counts.
   */
  function buildLinkedinReportFromAnalytics(rows: any[], since: string, until: string) {
    const days = daysInclusive(String(since || ""), String(until || ""));
    const idx = new Map(days.map((d, i) => [d, i]));
    const perDay = {
      connections_sent: days.map(() => new Set<string>()),
      connections_accepted: days.map(() => new Set<string>()),
      messages_sent: days.map(() => new Set<string>()),
      messages_opened: days.map(() => new Set<string>()),
      messages_replied: days.map(() => new Set<string>())
    };
    const totals = {
      connections_sent: new Set<string>(),
      connections_accepted: new Set<string>(),
      messages_sent: new Set<string>(),
      messages_opened: new Set<string>(),
      messages_replied: new Set<string>()
    };

    const connectionSentLeads = new Set<string>();

    for (const r of Array.isArray(rows) ? rows : []) {
      const source = String(r?.source_system ?? "").trim().toLowerCase();
      if (source && source !== "getsales") continue;
      const kind = String(r?.event_kind ?? "").trim();
      if (!kind || kind !== "linkedin_connection_request_sent") continue;
      const leadId = String(r?.lead_uuid ?? "").trim();
      if (leadId) connectionSentLeads.add(leadId);
    }

    for (const r of Array.isArray(rows) ? rows : []) {
      const source = String(r?.source_system ?? "").trim().toLowerCase();
      if (source && source !== "getsales") continue;
      const kind = String(r?.event_kind ?? "").trim();
      const at = String(r?.occurred_at ?? "").trim();
      const day = at ? at.slice(0, 10) : "";
      const i = idx.get(day);
      const isInmail = r?.is_inmail === true;
      const leadId = String(r?.lead_uuid ?? "").trim();

      // Fallback removed: rely only on explicit event_kind to mirror GetSales UI.
      const activityType = String(r?.activity_type ?? "").trim().toLowerCase();
      const direction = String(r?.direction ?? "").trim().toLowerCase();
      const fallbackKind =
        "";
      const eventKind = kind || fallbackKind;

      if (eventKind === "linkedin_connection_request_sent") {
        if (leadId) {
          totals.connections_sent.add(leadId);
          if (i != null) perDay.connections_sent[i].add(leadId);
        }
      } else if (eventKind === "linkedin_connection_request_accepted") {
        if (leadId) {
          totals.connections_accepted.add(leadId);
          if (i != null) perDay.connections_accepted[i].add(leadId);
        }
      } else if (eventKind === "linkedin_message_sent") {
        // GetSales UI counts unique leads, not unique message hashes.
        if (isInmail || !leadId) continue;
        totals.messages_sent.add(leadId);
        if (i != null) perDay.messages_sent[i].add(leadId);
      } else if (eventKind === "linkedin_message_opened") {
        // Keep opens aligned to unique leads (can be lower or higher than replies).
        if (isInmail || !leadId) continue;
        // Match GetSales UI: opened is counted only for leads with a connection sent.
        if (!connectionSentLeads.has(leadId)) continue;
        totals.messages_opened.add(leadId);
        if (i != null) perDay.messages_opened[i].add(leadId);
      } else if (eventKind === "linkedin_message_replied") {
        // Replies are also counted per lead in GetSales UI.
        if (isInmail || !leadId) continue;
        totals.messages_replied.add(leadId);
        if (i != null) perDay.messages_replied[i].add(leadId);
      }
    }

    const series = {
      connections_sent: perDay.connections_sent.map((s) => s.size),
      connections_accepted: perDay.connections_accepted.map((s) => s.size),
      messages_sent: perDay.messages_sent.map((s) => s.size),
      messages_opened: perDay.messages_opened.map((s) => s.size),
      messages_replied: perDay.messages_replied.map((s) => s.size)
    };
    const chartSeries = [
      { key: "connections_sent", label: "Connections Sent", color: "rgba(167,139,250,0.95)", values: series.connections_sent },
      { key: "connections_accepted", label: "Connections Accepted", color: "rgba(74,222,128,0.95)", values: series.connections_accepted },
      { key: "messages_sent", label: "Messages Sent", color: "rgba(251,146,60,0.95)", values: series.messages_sent },
      { key: "messages_opened", label: "Messages Opened", color: "rgba(163,230,53,0.95)", values: series.messages_opened },
      { key: "messages_replied", label: "Messages Replied", color: "rgba(253,224,71,0.95)", values: series.messages_replied }
    ];
    const totalsOut = {
      connections_sent: totals.connections_sent.size,
      connections_accepted: totals.connections_accepted.size,
      messages_sent: totals.messages_sent.size,
      messages_opened: totals.messages_opened.size,
      messages_replied: totals.messages_replied.size
    };

    return { days, series, chartSeries, totals: totalsOut };
  }

  /**
   * Build SmartLead email report from DB events.
   *
   * Uses unique contacts per day for sent/opened/replied (SmartLead UI semantics).
   */
  function buildSmartleadEmailReport(
    rows: any[],
    since: string,
    until: string,
    opts?: { campaignIds?: string[]; onlyPushed?: boolean }
  ) {
    const days = daysInclusive(String(since || ""), String(until || ""));
    const idx = new Map(days.map((d, i) => [d, i]));
    const campaignSet = new Set((opts?.campaignIds || []).map((x) => String(x).trim()).filter(Boolean));
    const perDay = {
      sent: days.map(() => new Set<string>()),
      opened: days.map(() => new Set<string>()),
      replied: days.map(() => new Set<string>())
    };
    const uniqueSent = new Set<string>();
    const uniqueOpened = new Set<string>();

    const uniqueReplied = new Set<string>();
    const sentByDay = new Set<string>();
    const openedByDay = new Set<string>();
    const repliedByDay = new Set<string>();
    const positiveByDay = new Set<string>();
    const oooByDay = new Set<string>();
    const totals = {
      leads_contacted: 0,
      emails_sent: 0,
      emails_opened: 0,
      emails_replied: 0,
      bounced: 0,
      positive_reply: 0,
      replied_ooo: 0
    };

    for (const r of Array.isArray(rows) ? rows : []) {
      const source = String(r?.source_system ?? "").trim().toLowerCase();
      if (source && source !== "smartlead") continue;
      if (opts?.onlyPushed && !r?.hubspot_engagement_id) continue;
      const campaignId = String(r?.smartlead_campaign_id ?? "").trim();
      if (campaignSet.size && !campaignSet.has(campaignId)) continue;
      const t = String(r?.event_kind ?? "").trim().toLowerCase();
      const email = String(r?.contact_email ?? "").trim().toLowerCase();
      const at = String(r?.occurred_at ?? "").trim();

      const day = at ? at.slice(0, 10) : "";
      const i = idx.get(day);
      // SmartLead UI counts sent/opened/replied as unique per lead per day.
      const dayKey = email && day ? `${email}#${day}` : "";
      if (t === "email_sent") {
        if (email) uniqueSent.add(email);
        if (email && i != null) perDay.sent[i].add(email);
        if (dayKey) sentByDay.add(dayKey);
      } else if (t === "email_opened") {
        if (email) uniqueOpened.add(email);
        if (email && i != null) perDay.opened[i].add(email);
        if (dayKey) openedByDay.add(dayKey);
      } else if (t === "email_replied") {
        if (email) uniqueReplied.add(email);
        if (email && i != null) perDay.replied[i].add(email);
        if (dayKey) repliedByDay.add(dayKey);
      } else if (t === "email_bounced") {
        if (dayKey) {
          // Keep bounced aligned with daily unique, even if we later decide to show it.
          totals.bounced += 0;
        }
      } else if (t === "email_positive_reply") {
        if (dayKey) positiveByDay.add(dayKey);
      } else if (t === "email_replied_ooo") {
        if (dayKey) oooByDay.add(dayKey);
      }
    }

    // Unique counts align with SmartLead UI (unique contacts, not raw events).
    totals.leads_contacted = uniqueSent.size;
    totals.emails_sent = sentByDay.size;
    totals.emails_opened = openedByDay.size;
    totals.emails_replied = repliedByDay.size;
    totals.positive_reply = positiveByDay.size;
    totals.replied_ooo = oooByDay.size;
    const series = {
      emails_sent: perDay.sent.map((s) => s.size),
      emails_opened: perDay.opened.map((s) => s.size),
      emails_replied: perDay.replied.map((s) => s.size)
    };

    return { days, series, totals };
  }

  const activitiesAgg = useMemo(() => {
    const since = String(dateRange?.since || "");
    const until = String(dateRange?.until || "");
    const days = daysInclusive(since, until);
    const idx = new Map(days.map((d, i) => [d, i]));
    const bySource: Record<string, number[]> = {};
    const filtered = Array.isArray(activityEvents) ? activityEvents : [];
    for (const ev of filtered) {
      const sourceSystem = String(ev?.source_system ?? "").trim().toLowerCase();
      const activityType = String(ev?.activity_type ?? "").trim().toLowerCase();
      const src = sourceSystem === "smartlead"
        ? "email"
        : sourceSystem === "getsales"
          ? "linkedin"
          : (activityType.includes("email") ? "email" : sourceSystem || "unknown");
      if (activitySources.length && !activitySources.map((x) => String(x).toLowerCase()).includes(src)) continue;
      if (activitiesOnlyPushed && !ev?.hubspot_engagement_id) continue;
      const at = String(ev?.occurred_at ?? "").trim();
      const day = at ? at.slice(0, 10) : "";
      const i = idx.get(day);
      if (i == null) continue;
      if (!bySource[src]) bySource[src] = days.map(() => 0);
      bySource[src][i] += 1;
    }
    const series = Object.keys(bySource)
      .sort()
      .map((src, i) => ({
        key: src,
        label: src,
        color: COLORS[i % COLORS.length],
        values: bySource[src]
      }));
    const total = days.map((_, i) => series.reduce((sum, s) => sum + (Number(s.values[i] ?? 0) || 0), 0));
    return { days, series, total };
  }, [activityEvents, activitySources.join(","), activitiesOnlyPushed, dateRange.since, dateRange.until]);

  const getsalesReport = useMemo(() => {
    const since = String(dateRange?.since || "");
    const until = String(dateRange?.until || "");
    const base = Array.isArray(activityEvents) ? activityEvents : [];
    const filtered = activitiesOnlyPushed ? base.filter((r: any) => !!r?.hubspot_engagement_id) : base;
    return buildLinkedinReportFromAnalytics(filtered, since, until);
  }, [activityEvents, activitiesOnlyPushed, dateRange.since, dateRange.until]);

  const smartleadReport = useMemo(() => {
    const since = String(dateRange?.since || "");
    const until = String(dateRange?.until || "");
    return buildSmartleadEmailReport(smartleadEvents, since, until, {
      campaignIds: smartleadCampaignIds,
      onlyPushed: activitiesOnlyPushed
    });
  }, [smartleadEvents, smartleadCampaignIds.join(","), activitiesOnlyPushed, dateRange.since, dateRange.until]);

  const activityChart = useMemo(() => {
    const baseRaw = Array.isArray(getsalesReport?.chartSeries) ? getsalesReport.chartSeries : [];
    const days = Array.isArray(getsalesReport?.days) ? getsalesReport.days : [];

    if (activityReportTab === "email") {
      const fullDays = Array.isArray(smartleadReport?.days) ? smartleadReport.days : [];
      const series = smartleadReport?.series ?? { emails_sent: [], emails_opened: [], emails_replied: [] };
      const base = fullDays.length > 0
        ? [
          { key: "emails_sent", label: "Emails Sent", color: "rgba(251,146,60,0.95)", values: series.emails_sent || [] },
          { key: "emails_opened", label: "Emails Opened", color: "rgba(163,230,53,0.95)", values: series.emails_opened || [] },
          { key: "emails_replied", label: "Emails Replied", color: "rgba(253,224,71,0.95)", values: series.emails_replied || [] }
        ]
        : [];

      const inf = smartleadInfluence?.series || null;
      if (!inf || !fullDays.length) {
        return { days: fullDays, series: base, bucket_size_days: 1 };
      }

      const mk = (key: string, label: string, color: string) => ({
        key,
        label,
        color,
        values: Array.isArray(inf[key]) ? inf[key] : fullDays.map(() => 0)
      });

      return {
        days: fullDays,
        bucket_size_days: 1,
        series: [
          ...base,
          mk("influenced_deals_created", "Deals created (influenced)", "rgba(125,211,252,0.95)"),
          mk("influenced_leads_created", "Leads created (influenced)", "rgba(96,165,250,0.95)"),
          mk("influenced_opps_created", "Opps created (influenced)", "rgba(34,197,94,0.95)")
        ]
      };
    }

    // LinkedIn: use event-derived series only (internal logic).
    const base = baseRaw;

    const inf = getsalesInfluence?.series || null;
    if (!inf || !days.length) return { days, series: base, bucket_size_days: 1 };
    const mk = (key: string, label: string, color: string) => ({
      key,
      label,
      color,
      values: Array.isArray(inf[key]) ? inf[key] : days.map(() => 0)
    });
    return {
      days,
      bucket_size_days: 1,
      series: [
        ...base,
        mk("influenced_deals_created", "Deals created (influenced)", "rgba(125,211,252,0.95)"),
        mk("influenced_leads_created", "Leads created (influenced)", "rgba(96,165,250,0.95)"),
        mk("influenced_opps_created", "Opps created (influenced)", "rgba(34,197,94,0.95)")
      ]
    };
  }, [
    getsalesReport?.chartSeries,
    getsalesReport?.days,
    getsalesInfluence?.series,
    activityReportTab,
    smartleadReport,
    smartleadInfluence?.series,
    dateRange?.since,
    dateRange?.until
  ]);

  const dealsAgg = useMemo(() => {
    const deals = Array.isArray(tableRowsByMetric?.new_deals) ? tableRowsByMetric.new_deals : [];
    if (!deals.length) return { rows: [] as any[], minDay: "", maxDay: "" };
    const byDay: Record<string, any> = {};
    for (const d of deals) {
      const created = String(d?.createdate ?? "").trim();
      const day = created ? created.slice(0, 10) : "";
      if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) continue;
      if (!byDay[day]) byDay[day] = { day, new_deals_count: 0, by_channel: {}, by_hypothesis: {} };
      byDay[day].new_deals_count += 1;
      const ch = String(d?.channel ?? "Unknown");
      byDay[day].by_channel[ch] = (byDay[day].by_channel[ch] ?? 0) + 1;
      const hk = String(d?.hypothesis_key ?? "__unassigned__");
      byDay[day].by_hypothesis[hk] = (byDay[day].by_hypothesis[hk] ?? 0) + 1;
    }
    const days = Object.keys(byDay).sort();
    const rows = days.map((x) => byDay[x]);
    return { rows, minDay: days[0] || "", maxDay: days[days.length - 1] || "" };
  }, [tableRowsByMetric?.new_deals]);

  const dailyAgg = useMemo(() => {
    // Aggregate across selected pipelines (required, usually 1).
    if (!selectedPipelineIds.length) {
      return { rows: [], lineDaily: [], minDay: "", maxDay: "" };
    }
    const selected = new Set((selectedPipelineIds || []).map(String));
    const byDay: Record<string, any> = {};
    for (const r of Array.isArray(dailySnaps) ? dailySnaps : []) {
      const pid = String(r?.pipeline_id ?? "");
      if (!selected.has(pid)) continue;
      const day = String(r?.period_day ?? "");
      if (!day) continue;
      if (!byDay[day]) {
        byDay[day] = { day, new_deals_count: 0, active_delta_count: 0, by_channel: {}, by_hypothesis: {} };
      }
      byDay[day].new_deals_count += toNum(r?.new_deals_count);
      byDay[day].active_delta_count += toNum(r?.active_delta_count);
      const bc = r?.new_deals_by_channel_json ?? {};
      for (const k of Object.keys(bc || {})) byDay[day].by_channel[k] = (byDay[day].by_channel[k] ?? 0) + toNum(bc[k]);
      const bh = r?.new_deals_by_hypothesis_json ?? {};
      for (const k of Object.keys(bh || {})) byDay[day].by_hypothesis[k] = (byDay[day].by_hypothesis[k] ?? 0) + toNum(bh[k]);
    }
    const days = Object.keys(byDay).sort();
    const rows = days.map((d) => byDay[d]);
    // cumulative active total derived from net deltas; aligned to current active total (A: excludes Lost/Dormant).
    let cum = 0;
    const cumArr: number[] = [];
    for (const r of rows) {
      cum += toNum(r.active_delta_count);
      cumArr.push(cum);
    }
    const lastCum = cumArr.length ? cumArr[cumArr.length - 1] : 0;
    const activeNow =
      (Number(stock?.counts_now?.leads ?? 0) || 0) +
      (Number(stock?.counts_now?.sql ?? 0) || 0) +
      (Number(stock?.counts_now?.opportunity ?? 0) || 0) +
      (Number(stock?.counts_now?.clients ?? 0) || 0);
    const offset = Number.isFinite(activeNow) ? activeNow - lastCum : 0;
    const lineDaily = cumArr.map((x) => x + offset);
    return { rows, lineDaily, minDay: days[0] || "", maxDay: days[days.length - 1] || "" };
  }, [
    dailySnaps,
    selectedPipelineIds.join(","),
    stock?.counts_now?.leads,
    stock?.counts_now?.sql,
    stock?.counts_now?.opportunity,
    stock?.counts_now?.clients
  ]);

  // Default date range: last 90 days ending at latest available day (per selected pipeline).
  useEffect(() => {
    const max = String(dailyAgg?.maxDay ?? "");
    const min = String(dailyAgg?.minDay ?? "");
    if (!min || !max) return;
    if (dateRangeTouched && (dateRange.since || dateRange.until)) return;
    const parseMs = (ymd: string) => Date.parse(`${ymd}T00:00:00.000Z`);
    const fmt = (ms: number) => {
      const d = new Date(ms);
      const y = d.getUTCFullYear();
      const m = String(d.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(d.getUTCDate()).padStart(2, "0");
      return `${y}-${m}-${dd}`;
    };
    const start = fmt(parseMs(max) - (90 - 1) * 86400000);
    setDateRange({ since: start < min ? min : start, until: max });
  }, [dailyAgg?.minDay, dailyAgg?.maxDay, dateRangeTouched, dateRange.since, dateRange.until]);

  function startOfWeekISOFromYmd(dayYmd: string) {
    const d = new Date(`${dayYmd}T00:00:00.000Z`);
    return startOfWeekISO(d);
  }
  function startOfMonthKey(dayYmd: string) {
    const d = new Date(`${dayYmd}T00:00:00.000Z`);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    return `${y}-${m}`;
  }
  function startOfQuarterKey(dayYmd: string) {
    const d = new Date(`${dayYmd}T00:00:00.000Z`);
    const y = d.getUTCFullYear();
    const m = d.getUTCMonth() + 1;
    return `${y}-Q${Math.floor((m - 1) / 3) + 1}`;
  }
  function startOfYearKey(dayYmd: string) {
    const d = new Date(`${dayYmd}T00:00:00.000Z`);
    return String(d.getUTCFullYear());
  }

  const chartBucketsDaily = useMemo(() => {
    // Bars should reflect the same dataset as the table when a stage/KPI filter is applied.
    // We use dealsAgg for reasonably small ranges (fast + consistent with table). Otherwise fallback to daily snapshots.
    const rangeDays = (() => {
      const a = String(dateRange?.since || "");
      const b = String(dateRange?.until || "");
      const ams = a ? Date.parse(`${a}T00:00:00.000Z`) : NaN;
      const bms = b ? Date.parse(`${b}T00:00:00.000Z`) : NaN;
      if (!Number.isFinite(ams) || !Number.isFinite(bms) || bms < ams) return 0;
      return Math.round((bms - ams) / 86400000) + 1;
    })();
    const canUseDealsAgg = (dealsAgg.rows || []).length > 0 && rangeDays > 0 && rangeDays <= 140;
    const rowsAll = canUseDealsAgg ? dealsAgg.rows : (dailyAgg.rows || []);
    const lineAll = dailyAgg.lineDaily || [];
    if (!rowsAll.length) return { buckets: [], line: [] as number[] };

    const since = String(dateRange?.since || dailyAgg?.minDay || "");
    const until = String(dateRange?.until || dailyAgg?.maxDay || "");
    const rows: any[] = [];
    const lineDaily: number[] = [];
    const indexByDay = new Map<string, number>();
    const dailyRows = dailyAgg.rows || [];
    for (let i = 0; i < dailyRows.length; i++) indexByDay.set(String(dailyRows[i]?.day ?? ""), i);

    for (let i = 0; i < rowsAll.length; i++) {
      const day = String(rowsAll[i]?.day ?? "");
      if (!day) continue;
      if (since && day < since) continue;
      if (until && day > until) continue;
      rows.push(rowsAll[i]);
      // Line is always from daily snapshots (overall pipeline), not stage-filtered.
      // We align by day key (best-effort).
      const idx = indexByDay.get(day);
      lineDaily.push(idx != null ? (Number(lineAll[idx] ?? 0) || 0) : 0);
    }
    if (!rows.length) return { buckets: [], line: [] as number[] };

    // If user selected a day range, keep the full range visible on the chart even if some days have no rows.
    // Otherwise the chart "collapses" to only the days that have data.
    if (period === "day" && since && until) {
      const parseMs = (ymdStr: string) => Date.parse(`${ymdStr}T00:00:00.000Z`);
      const fmtYmd = (ms: number) => {
        const d = new Date(ms);
        const y = d.getUTCFullYear();
        const m = String(d.getUTCMonth() + 1).padStart(2, "0");
        const dd = String(d.getUTCDate()).padStart(2, "0");
        return `${y}-${m}-${dd}`;
      };
      const sinceMs = parseMs(since);
      const untilMs = parseMs(until);
      if (Number.isFinite(sinceMs) && Number.isFinite(untilMs) && untilMs >= sinceMs) {
        const rowByDay = new Map<string, any>();
        for (const r of rows) rowByDay.set(String(r.day), r);

        // Initialize last known line value using the last daily snapshot BEFORE the range (forward-fill).
        let lastLine = 0;
        for (let i = 0; i < dailyRows.length; i++) {
          const d = String(dailyRows[i]?.day ?? "");
          if (!d) continue;
          if (d > since) break;
          lastLine = Number(lineAll[i] ?? 0) || 0;
        }

        const filledRows: any[] = [];
        const filledLine: number[] = [];
        for (let ms = sinceMs; ms <= untilMs; ms += 86400000) {
          const dayKey = fmtYmd(ms);
          const r = rowByDay.get(dayKey) ?? { day: dayKey, new_deals_count: 0, by_channel: {}, by_hypothesis: {} };
          filledRows.push(r);
          const idx = indexByDay.get(dayKey);
          if (idx != null) lastLine = Number(lineAll[idx] ?? 0) || 0;
          filledLine.push(lastLine);
        }

        rows.length = 0;
        lineDaily.length = 0;
        rows.push(...filledRows);
        lineDaily.push(...filledLine);
      }
    }

    const byKey: Record<string, any> = {};
    const order: string[] = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const day = String(r.day);
      let key = day;
      if (period === "week") key = startOfWeekISOFromYmd(day);
      else if (period === "month") key = startOfMonthKey(day);
      else if (period === "quarter") key = startOfQuarterKey(day);
      else if (period === "year") key = startOfYearKey(day);

      if (!byKey[key]) {
        byKey[key] = {
          key,
          label: key,
          since: day,
          until: day, // will expand
          by_channel: {},
          by_hypothesis: {},
          total_new: 0,
          line_total: 0
        };
        order.push(key);
      }
      const b = byKey[key];
      b.total_new += toNum(r.new_deals_count);
      for (const k of Object.keys(r.by_channel || {})) b.by_channel[k] = (b.by_channel[k] ?? 0) + toNum(r.by_channel[k]);
      for (const k of Object.keys(r.by_hypothesis || {})) b.by_hypothesis[k] = (b.by_hypothesis[k] ?? 0) + toNum(r.by_hypothesis[k]);
      // set since=min day; until=max day
      if (day < b.since) b.since = day;
      if (day > b.until) b.until = day;
      // line total = last day in bucket (so keep overwriting as we iterate in chronological order)
      b.line_total = Number(lineDaily[i] ?? 0) || 0;
    }

    // normalize "until" to exclusive (add 1 day)
    for (const k of order) {
      const b = byKey[k];
      const untilExcl = ymd(new Date(Date.parse(`${b.until}T00:00:00.000Z`) + 24 * 60 * 60 * 1000));
      b.until_exclusive = untilExcl;
    }

    const maxB = maxBucketsFor(period);
    const recent = order.slice(Math.max(0, order.length - maxB)).map((k) => byKey[k]);
    return { buckets: recent, line: recent.map((b: any) => Number(b.line_total ?? 0) || 0) };
  }, [
    dailyAgg.rows,
    dailyAgg.lineDaily.join(","),
    dailyAgg.minDay,
    dailyAgg.maxDay,
    dealsAgg.rows,
    dateRange.since,
    dateRange.until,
    period,
    effectiveStageIdsForDeals.join(",")
  ]);

  function buildDailySeries() {
    const buckets = chartBucketsDaily.buckets || [];
    if (colorMode === "hypothesis") {
      const seriesByKey: Record<string, number[]> = {};
      for (let i = 0; i < buckets.length; i++) {
        const by = buckets[i]?.by_hypothesis ?? {};
        for (const k of Object.keys(by || {})) {
          if (!seriesByKey[k]) seriesByKey[k] = buckets.map(() => 0);
          seriesByKey[k][i] = toNum(by[k]);
        }
      }
      // Ensure bars show even if breakdown is missing: compute unassigned as remainder from total_new.
      if (!seriesByKey["__unassigned__"]) seriesByKey["__unassigned__"] = buckets.map(() => 0);
      for (let i = 0; i < buckets.length; i++) {
        const total = toNum(buckets[i]?.total_new);
        const assigned = Object.keys(buckets[i]?.by_hypothesis ?? {})
          .filter((k) => k !== "__unassigned__")
          .reduce((sum, k) => sum + toNum((buckets[i]?.by_hypothesis ?? {})[k]), 0);
        const rem = Math.max(0, total - assigned);
        seriesByKey["__unassigned__"][i] = rem;
      }
      const top = topKeysByTotal(seriesByKey, 9).filter((k) => k !== "__unassigned__");
      const otherKeys = Object.keys(seriesByKey).filter((k) => k !== "__unassigned__" && !top.includes(k));
      const series = top.map((hid, i) => ({
        key: hid,
        label: hid === "__unassigned__" ? "Unassigned" : (hypById.get(hid)?.title ?? hid),
        color: COLORS[i % COLORS.length],
        values: seriesByKey[hid] || buckets.map(() => 0)
      }));
      if (otherKeys.length) {
        const values = buckets.map((_: any, i: number) => otherKeys.reduce((sum, k) => sum + (seriesByKey[k]?.[i] ?? 0), 0));
        series.push({ key: "other", label: "Other", color: "rgba(255,255,255,0.15)", values });
      }
      series.push({ key: "__unassigned__", label: "Unassigned deals", color: "rgba(255,255,255,0.25)", values: seriesByKey["__unassigned__"] });
      // If everything is zero (e.g. no hypothesis attribution), show a single Total series.
      const hasAny = series.some((s) => (s.values || []).some((v) => (Number(v) || 0) > 0));
      if (!hasAny) {
        return [{ key: "total", label: "Total", color: "rgba(125,211,252,0.55)", values: buckets.map((b: any) => toNum(b?.total_new)) }];
      }
      return series;
    }

    const seriesByChannel: Record<string, number[]> = {};
    for (let i = 0; i < buckets.length; i++) {
      const by = buckets[i]?.by_channel ?? {};
      for (const ch of Object.keys(by || {})) {
        const canon = ch === "__unassigned__" ? ch : canonicalizeChannelLabel(ch);
        if (!seriesByChannel[canon]) seriesByChannel[canon] = buckets.map(() => 0);
        seriesByChannel[canon][i] = (seriesByChannel[canon][i] ?? 0) + toNum(by[ch]);
      }
    }
    // Compute "Unassigned" remainder from total_new if channel breakdown doesn't include it.
    if (!seriesByChannel["__unassigned__"]) seriesByChannel["__unassigned__"] = buckets.map(() => 0);
    for (let i = 0; i < buckets.length; i++) {
      const total = toNum(buckets[i]?.total_new);
      const assigned = Object.keys(buckets[i]?.by_channel ?? {})
        .filter((k) => k !== "__unassigned__")
        .reduce((sum, k) => sum + toNum((buckets[i]?.by_channel ?? {})[k]), 0);
      const rem = Math.max(0, total - assigned);
      seriesByChannel["__unassigned__"][i] = rem;
    }
    const top = topKeysByTotal(seriesByChannel, 9);
    const otherKeys = Object.keys(seriesByChannel).filter((k) => !top.includes(k));
    const series = top.map((ch, i) => ({
      key: ch,
      label: ch,
      color: COLORS[i % COLORS.length],
      values: seriesByChannel[ch] || buckets.map(() => 0)
    }));
    if (otherKeys.length) {
      const values = buckets.map((_: any, i: number) => otherKeys.reduce((sum, k) => sum + (seriesByChannel[k]?.[i] ?? 0), 0));
      series.push({ key: "other", label: "Other channels", color: "rgba(255,255,255,0.25)", values });
    }
    // If breakdown is empty/zero, show Total series so bars are visible.
    const hasAny = series.some((s) => (s.values || []).some((v) => (Number(v) || 0) > 0));
    if (!hasAny) {
      return [{ key: "total", label: "Total", color: "rgba(125,211,252,0.55)", values: buckets.map((b: any) => toNum(b?.total_new)) }];
    }
    return series;
  }

  function ymdAddDaysLocal(dayYmd: string, delta: number) {
    const ms = Date.parse(`${dayYmd}T00:00:00.000Z`);
    const d = new Date(ms + delta * 86400000);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
  }

  function daysInclusive(sinceYmd: string, untilYmd: string) {
    const out: string[] = [];
    const s = String(sinceYmd || "").trim();
    const u = String(untilYmd || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(s) || !/^\d{4}-\d{2}-\d{2}$/.test(u)) return out;
    const sMs = Date.parse(`${s}T00:00:00.000Z`);
    const uMs = Date.parse(`${u}T00:00:00.000Z`);
    if (!Number.isFinite(sMs) || !Number.isFinite(uMs) || uMs < sMs) return out;
    const days = Math.min(500, Math.floor((uMs - sMs) / 86400000) + 1);
    for (let i = 0; i < days; i++) out.push(ymdAddDaysLocal(s, i));
    return out;
  }


  const buckets = useMemo(() => {
    const byKey: Record<string, any> = {};
    for (const r of snaps) {
      const wk = String(r.period_start);
      const k = bucketKey(wk);
      if (!byKey[k]) {
        byKey[k] = {
          key: k,
          label: bucketLabel(k),
          new_leads_count: 0,
          new_opps_count: 0,
          new_customers_count: 0,
          new_churn_count: 0,
          funnel_by_channel_json: { new_leads: {}, new_opps: {}, new_customers: {}, new_churn: {} },
          funnel_by_hypothesis_json: { new_leads: {}, new_opps: {}, new_customers: {}, new_churn: {} }
        };
      }
      const b = byKey[k];
      b.new_leads_count += toNum(r.new_leads_count);
      b.new_opps_count += toNum(r.new_opps_count);
      b.new_customers_count += toNum(r.new_customers_count);
      b.new_churn_count += toNum(r.new_churn_count);
      for (const metricKey of ["new_leads", "new_opps", "new_customers", "new_churn"]) {
        const ch = r?.funnel_by_channel_json?.[metricKey] ?? {};
        for (const kk of Object.keys(ch)) {
          b.funnel_by_channel_json[metricKey][kk] = (b.funnel_by_channel_json[metricKey][kk] ?? 0) + toNum(ch[kk]);
        }
        const hh = r?.funnel_by_hypothesis_json?.[metricKey] ?? {};
        for (const kk of Object.keys(hh)) {
          b.funnel_by_hypothesis_json[metricKey][kk] = (b.funnel_by_hypothesis_json[metricKey][kk] ?? 0) + toNum(hh[kk]);
        }
      }
    }
    const ks = Object.keys(byKey).sort();
    const maxB = maxBucketsFor(period);
    const recent = ks.slice(Math.max(0, ks.length - maxB));
    return recent.map((k) => byKey[k]);
  }, [snaps, period]);

  function latestKpi(metric: string) {
    const last = buckets.length ? buckets[buckets.length - 1] : null;
    return last ? toNum(last?.[metric]) : 0;
  }

  function stockCount(label: string) {
    const top = Array.isArray(stock?.top_stages) ? stock.top_stages : [];
    const m = new Map(top.map((x: any) => [String(x.label), Number(x.count || 0)]));
    return Number(m.get(label) ?? 0) || 0;
  }

  function stockBucket(key: string) {
    return Number(stock?.counts_now?.[key] ?? 0) || 0;
  }

  function stockDelta(key: string) {
    return Number(stock?.delta?.[key] ?? 0) || 0;
  }

  function stockPlus(key: string) {
    return Number(stock?.delta_details?.plus?.[key] ?? 0) || 0;
  }
  function stockMinus(key: string) {
    return Number(stock?.delta_details?.minus?.[key] ?? 0) || 0;
  }
  function stockMinusForward(key: string) {
    return Number(stock?.delta_details?.minus_forward?.[key] ?? 0) || 0;
  }
  function stockMinusToLost(key: string) {
    return Number(stock?.delta_details?.minus_to_lost?.[key] ?? 0) || 0;
  }
  function stockMinusBackward(key: string) {
    return Number(stock?.delta_details?.minus_backward?.[key] ?? 0) || 0;
  }

  function deltaBadge(delta: number) {
    if (!Number.isFinite(delta) || delta === 0) return { text: "0", color: "rgba(255,255,255,0.55)" };
    if (delta > 0) return { text: `+${delta}`, color: "rgba(34,197,94,0.95)" };
    return { text: `${delta}`, color: "rgba(239,68,68,0.95)" };
  }

  function msToYmd(ms: any) {
    const n = Number(ms);
    if (!Number.isFinite(n)) return "";
    const d = new Date(n);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
  }

  function buildSeries(metricKey: "new_deals" | "new_leads" | "new_opps" | "new_customers" | "new_churn") {
    if (colorMode === "hypothesis") {
      // global snapshot has per-week funnel_by_hypothesis_json.<metricKey>
      const seriesByKey: Record<string, number[]> = {};
      for (let wi = 0; wi < buckets.length; wi++) {
        const row = buckets[wi] ?? null;
        const by = row?.funnel_by_hypothesis_json?.[metricKey] ?? {};
        for (const k of Object.keys(by || {})) {
          if (!seriesByKey[k]) seriesByKey[k] = buckets.map(() => 0);
          seriesByKey[k][wi] = toNum(by[k]);
        }
      }
      // Ensure unassigned exists
      if (!seriesByKey["__unassigned__"]) seriesByKey["__unassigned__"] = buckets.map(() => 0);

      const top = topKeysByTotal(seriesByKey, 9).filter((k) => k !== "__unassigned__");
      const series = top.map((hid, i) => ({
        key: hid,
        label: hid === "__unassigned__" ? "Unassigned" : (hypById.get(hid)?.title ?? hid),
        color: COLORS[i % COLORS.length],
        values: seriesByKey[hid] || buckets.map(() => 0)
      }));
      // Add unassigned (grey) always
      series.push({ key: "__unassigned__", label: "Unassigned deals", color: "rgba(255,255,255,0.25)", values: seriesByKey["__unassigned__"] });
      return series;
    }

    // channel mode
    const seriesByChannel: Record<string, number[]> = {};
    for (let wi = 0; wi < buckets.length; wi++) {
      const row = buckets[wi] ?? null;
      const by = row?.funnel_by_channel_json?.[metricKey] ?? {};
      for (const ch of Object.keys(by || {})) {
        const canon = ch === "__unassigned__" ? ch : canonicalizeChannelLabel(ch);
        if (!seriesByChannel[canon]) seriesByChannel[canon] = buckets.map(() => 0);
        seriesByChannel[canon][wi] = (seriesByChannel[canon][wi] ?? 0) + toNum(by[ch]);
      }
    }
    const top = topKeysByTotal(seriesByChannel, 9);
    const otherKeys = Object.keys(seriesByChannel).filter((k) => !top.includes(k));
    const series = top.map((ch, i) => ({
      key: ch,
      label: ch,
      color: COLORS[i % COLORS.length],
      values: seriesByChannel[ch] || buckets.map(() => 0)
    }));
    if (otherKeys.length) {
      const values = buckets.map((_, i) => otherKeys.reduce((sum, k) => sum + (seriesByChannel[k]?.[i] ?? 0), 0));
      series.push({ key: "other", label: "Other channels", color: "rgba(255,255,255,0.25)", values });
    }
    return series;
  }

  function isYmd(s: string) {
    return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
  }

  return (
    <main>
      <AppTopbar
        title="Oversecured Sales"
        subtitle="Main dashboard"
        showSync={!!sessionEmail}
        onSync={syncNow}
      />

      <div className="page grid">
        {sessionEmail ? (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardHeader">
              <div>
                <div className="cardTitle">Main dashboard</div>
                <div className="cardDesc">
                  HubSpot TAL snapshots · Period: <span className="mono">{period}</span> · Colors by{" "}
                  <span className="mono">{colorMode}</span>
                  {lastSyncAgo ? (
                    <>
                      {" "}· Last sync <span className="mono">{lastSyncAgo}</span>
                    </>
                  ) : null}
                </div>
              </div>
            </div>
            <div className="cardBody">
              {dashStatus ? <div className="notice" style={{ marginBottom: 10 }}>{dashStatus}</div> : null}

              <div className="card" style={{ marginBottom: 12, background: "rgba(255,255,255,0.02)", border: "1px solid rgba(255,255,255,0.08)" }}>
                <div className="cardHeader">
                  <div>
                    <div className="cardTitle" style={{ fontSize: 14 }}>
                      Filters · <span className="mono">{viewMode === "deals" ? "Deals" : "Activities"}</span>
                    </div>
                    <div className="cardDesc">
                      {viewMode === "deals"
                        ? "Select the funnel pipeline (and optionally stages). Counts refresh automatically."
                        : "Activity view (GetSales): LinkedIn + Email events aggregated by day."}
                    </div>
                  </div>
                  <div className="btnRow">
                    <select
                      className="select"
                      value={viewMode}
                      onChange={(e: any) => {
                        const next = String(e.target.value) === "activities" ? "activities" : "deals";
                        setViewMode(next);
                        setKpiStageKey("");
                        setKpiStageIds([]);
                      }}
                    >
                      <option value="deals">Deals</option>
                      <option value="activities">Activities</option>
                    </select>
                    <button
                      className="btn"
                      onClick={() => {
                        setSelectedPipelineIds([]);
                        setSelectedStageIds([]);
                        setStock(null);
                        resetDateRange();
                      }}
                    >
                      Reset
                    </button>
                  </div>
                </div>
                <div className="cardBody">
                  <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10 }}>
                    {viewMode === "deals" ? (
                      <>
                        <div style={{ gridColumn: "span 6" }}>
                          <MultiSelectDropdown
                            label="Pipelines"
                            placeholder="All pipelines (no filter)"
                            options={(pipelines || []).map((p: any) => ({
                              value: String(p.id),
                              label: String(p.label || p.id),
                              meta: "Pipeline"
                            }))}
                            selected={selectedPipelineIds}
                            onChange={(next) => {
                              setSelectedPipelineIds(next);
                              setSelectedStageIds([]);
                            }}
                            disabled={!pipelines.length}
                            height={220}
                          />
                        </div>

                        <div style={{ gridColumn: "span 6" }}>
                          <MultiSelectDropdown
                            label="Stages (optional)"
                            placeholder="All stages (no filter)"
                            options={(() => {
                              const ps = (pipelines || []).filter((p: any) => selectedPipelineIds.includes(String(p.id)));
                              const stages = ps.length
                                ? ps.flatMap((p: any) => Array.isArray(p.stages) ? p.stages.map((s: any) => ({ ...s, pipelineId: p.id, pipelineLabel: p.label })) : [])
                                : (pipelines || []).flatMap((p: any) => Array.isArray(p.stages) ? p.stages.map((s: any) => ({ ...s, pipelineId: p.id, pipelineLabel: p.label })) : []);
                              const uniqById = new Map<string, any>();
                              for (const s of stages) if (s?.id && !uniqById.has(String(s.id))) uniqById.set(String(s.id), s);
                              return Array.from(uniqById.values())
                                .sort((a: any, b: any) => (Number(a.displayOrder ?? 0) - Number(b.displayOrder ?? 0)))
                                .map((s: any) => ({
                                  value: String(s.id),
                                  label: String(s.label || s.id),
                                  meta: String(s.pipelineLabel || s.pipelineId || "")
                                }));
                            })()}
                            selected={selectedStageIds}
                            onChange={(next) => setSelectedStageIds(next)}
                            disabled={!pipelines.length}
                            height={220}
                          />
                        </div>
                      </>
                    ) : (
                      <>
                        <div style={{ gridColumn: "span 6" }}>
                          <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Report</div>
                          <div className="btnRow" style={{ gap: 8 }}>
                            <button
                              className={activityReportTab === "linkedin" ? "btn btnPrimary" : "btn"}
                              onClick={() => {
                                setActivityReportTab("linkedin");
                                setActivitySources(["linkedin"]);
                              }}
                            >
                              LinkedIn
                            </button>
                            <button
                              className={activityReportTab === "email" ? "btn btnPrimary" : "btn"}
                              onClick={() => {
                                setActivityReportTab("email");
                                setActivitySources(["email"]);
                              }}
                            >
                              Email
                            </button>
                          </div>
                        </div>
                        <div style={{ gridColumn: "span 6" }}>
                          <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Only pushed to HubSpot</div>
                          <select className="select" value={activitiesOnlyPushed ? "yes" : "no"} onChange={(e: any) => setActivitiesOnlyPushed(String(e.target.value) === "yes")}>
                            <option value="yes">Yes (has engagement)</option>
                            <option value="no">No (include not pushed)</option>
                          </select>
                        </div>
                        {activityReportTab === "email" ? (
                          <div style={{ gridColumn: "span 12" }}>
                            <MultiSelectDropdown
                              label="SmartLead campaigns (optional)"
                              placeholder="All configured campaigns"
                              options={(smartleadCampaigns || []).map((c: any) => {
                                const id = String(c?.id ?? "").trim();
                                const name = String(c?.name ?? "").trim();
                                const st = String(c?.status ?? "unknown").trim();
                                const stLabel = st === "active" ? "Active" : st === "paused" ? "Paused" : "Unknown";
                                return {
                                  value: id,
                                  label: name ? `${name} (#${id})` : `Campaign #${id}`,
                                  meta: stLabel
                                };
                              })}
                              selected={smartleadCampaignIds}
                              onChange={(next) => setSmartleadCampaignIds(next)}
                              disabled={smartleadCampaignsLoading}
                              height={260}
                            />
                            {smartleadCampaignsErr ? (
                              <div className="muted2" style={{ fontSize: 12, marginTop: 6 }}>
                                Campaigns load error: <span className="mono">{smartleadCampaignsErr}</span>
                              </div>
                            ) : smartleadCampaignsLoading ? (
                              <div className="muted2" style={{ fontSize: 12, marginTop: 6 }}>Loading SmartLead campaigns…</div>
                            ) : null}
                          </div>
                        ) : null}
                      </>
                    )}

                    <div style={{ gridColumn: "span 6" }}>
                      <DateRangePicker
                        label="Date range"
                        availableMin={String(dailyAgg?.minDay ?? "")}
                        availableMax={(() => {
                          const max = String(dailyAgg?.maxDay ?? "").trim();
                          const now = new Date();
                          const y = now.getUTCFullYear();
                          const m = String(now.getUTCMonth() + 1).padStart(2, "0");
                          const d = String(now.getUTCDate()).padStart(2, "0");
                          const today = `${y}-${m}-${d}`;
                          // Allow selecting up to today even if we don't yet have daily snapshots for those days.
                          return max ? (today > max ? today : max) : today;
                        })()}
                        value={dateRange}
                        onChange={(r, meta) => {
                          setDateRangeTouched(true);
                          setDateRange(r);
                          if (meta?.period) setPeriod(meta.period);
                        }}
                      />
                    </div>
                    {viewMode === "deals" ? (
                      <div style={{ gridColumn: "span 3" }}>
                        <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>Colors</div>
                        <select className="select" value={colorMode} onChange={(e) => setColorMode(e.target.value)}>
                          <option value="channel">Channels</option>
                          <option value="hypothesis">Hypotheses</option>
                        </select>
                      </div>
                    ) : (
                      <div style={{ gridColumn: "span 3" }}>
                        <div className="muted2" style={{ fontSize: 12, marginBottom: 6 }}>View</div>
                        <select className="select" value={"lines"} disabled>
                          <option value="lines">Lines</option>
                        </select>
                      </div>
                    )}
                    <div style={{ gridColumn: "span 3", paddingTop: 18 }}>
                      <button className="btn" onClick={resetDateRange} disabled={!dateRangeTouched && !dateRange.since && !dateRange.until}>
                        Reset range
                      </button>
                    </div>
                  </div>
                  {viewMode === "activities" ? (
                    <div style={{ marginTop: 10 }}>
                      {activityErr ? <div className="notice">Activities error: <span className="mono">{activityErr}</span></div> : null}
                      {activityLoading ? <div className="muted2">Loading activities…</div> : null}
                    </div>
                  ) : null}
                </div>
              </div>
              {viewMode === "deals" ? (
                <>
                  <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10 }}>
                    <div style={{ gridColumn: "span 12" }} className="kpiRow">
                      {[
                        { key: "leads", label: "Leads", value: stockBucket("leads") },
                        { key: "sql", label: "SQL", value: stockBucket("sql") },
                        { key: "opportunity", label: "Opportunity", value: stockBucket("opportunity") },
                        { key: "clients", label: "Clients", value: stockBucket("clients") },
                        { key: "lost", label: "Lost", value: stockBucket("lost") }
                      ].map((kpi) => {
                        const deltaTitle = dateRangeTouched && dateRange?.since
                          ? `Delta since ${String(dateRange.since)}`
                          : "Delta since week start";
                        const plus = stockPlus(kpi.key);
                        const minus = stockMinus(kpi.key);
                        const minusFwd = stockMinusForward(kpi.key);
                        const minusLost = stockMinusToLost(kpi.key);
                        const minusBack = stockMinusBackward(kpi.key);
                        const targetStages = selectedPipelineIds.length ? stageIdsForKpi(kpi.key) : [];
                        const isActive = kpiStageKey === kpi.key && targetStages.length
                          ? sameSet((kpiStageIds || []).slice().sort(), targetStages.slice().sort())
                          : false;
                        const conv = stageConvs?.conversions ?? null;
                        const pct = (x: any) => {
                          const n = Number(x);
                          if (!Number.isFinite(n)) return "—";
                          return `${Math.round(n * 100)}%`;
                        };
                        let convText: string | null = null;
                        if (conv) {
                          if (kpi.key === "leads") convText = `→ SQL ${pct(conv.lead_to_sql?.rate)} (${conv.lead_to_sql?.to ?? 0}/${conv.lead_to_sql?.from ?? 0})`;
                          else if (kpi.key === "sql") convText = `→ Opp ${pct(conv.sql_to_opportunity?.rate)} (${conv.sql_to_opportunity?.to ?? 0}/${conv.sql_to_opportunity?.from ?? 0})`;
                          else if (kpi.key === "opportunity") convText = `→ Clients ${pct(conv.opportunity_to_clients?.rate)} (${conv.opportunity_to_clients?.to ?? 0}/${conv.opportunity_to_clients?.from ?? 0})`;
                        }
                        return (
                          <div
                            key={kpi.key}
                            className={`card kpiCard ${isActive ? "kpiCardActive" : ""}`.trim()}
                            role="button"
                            tabIndex={0}
                            title={selectedPipelineIds.length ? "Click to filter by these stages" : "Select a pipeline first"}
                            onClick={() => toggleKpiFilter(kpi.key)}
                            onKeyDown={(e: any) => {
                              if (e.key === "Enter" || e.key === " ") {
                                e.preventDefault();
                                toggleKpiFilter(kpi.key);
                              }
                            }}
                          >
                            <div className="cardBody">
                              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 10 }}>
                                <div className="muted2" style={{ fontSize: 12 }}>{kpi.label}</div>
                                <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
                                  <div style={{ display: "flex", gap: 6, alignItems: "baseline" }} title={`${deltaTitle} (breakdown)`}>
                                    {(() => {
                                      const isLost = kpi.key === "lost";
                                      const plusColor = isLost ? "rgba(239,68,68,0.95)" : "rgba(34,197,94,0.95)";
                                      const good = "rgba(34,197,94,0.95)";
                                      const bad = "rgba(239,68,68,0.95)";
                                      return (
                                        <>
                                          {plus ? <span className="mono" style={{ fontSize: 11, color: plusColor }}>{`+${plus}`}</span> : null}
                                          {minus ? (
                                            isLost ? (
                                              <span className="mono" style={{ fontSize: 11, color: good }}>{`-${minus}`}</span>
                                            ) : (
                                              <span className="mono" style={{ fontSize: 11, display: "inline-flex", gap: 6 }}>
                                                {minusFwd ? <span style={{ color: good }}>{`-fwd:${minusFwd}`}</span> : null}
                                                {minusLost ? <span style={{ color: bad }}>{`-lost:${minusLost}`}</span> : null}
                                                {minusBack ? <span style={{ color: bad }}>{`-back:${minusBack}`}</span> : null}
                                                {!minusFwd && !minusLost && !minusBack ? <span style={{ color: "rgba(255,255,255,0.65)" }}>{`-${minus}`}</span> : null}
                                              </span>
                                            )
                                          ) : null}
                                          {!plus && !minus ? <span className="mono" style={{ fontSize: 11, color: "rgba(255,255,255,0.55)" }}>0</span> : null}
                                        </>
                                      );
                                    })()}
                                  </div>
                                </div>
                              </div>
                              <div style={{ fontSize: 28, fontWeight: 700, marginTop: 6 }} className="mono">
                                {String(kpi.value)}
                              </div>
                              {convText ? (
                                <div className="muted2" style={{ fontSize: 12, marginTop: 6 }}>
                                  Conv (new deals): <span className="mono">{convText}</span>
                                </div>
                              ) : null}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                    {kpiStageKey && stock?.change_deals ? (
                      <div style={{ gridColumn: "span 12" }}>
                        <div className="card" style={{ background: "rgba(255,255,255,0.02)", border: "1px solid rgba(255,255,255,0.08)" }}>
                          <div className="cardHeader">
                            <div>
                              <div className="cardTitle">Stage changes (delta)</div>
                              <div className="cardDesc">
                                KPI: <span className="mono">{kpiStageKey}</span> · Window since{" "}
                                <span className="mono">{String(stock?.window?.since_ymd ?? "") || "—"}</span>
                              </div>
                            </div>
                            <div className="btnRow">
                              <select className="select" value={kpiDeltaMode} onChange={(e: any) => setKpiDeltaMode(e.target.value)}>
                                <option value="all">All</option>
                                <option value="plus">Entered (+)</option>
                                <option value="minus">Left (-)</option>
                              </select>
                            </div>
                          </div>
                          <div className="cardBody">
                            {(() => {
                              const bucketKey = String(kpiStageKey);
                              const src = stock?.change_deals?.[bucketKey] ?? { plus: [], minus: [] };
                              const plusRows = Array.isArray(src?.plus) ? src.plus : [];
                              const minusRows = Array.isArray(src?.minus) ? src.minus : [];
                              const rows = kpiDeltaMode === "plus" ? plusRows : kpiDeltaMode === "minus" ? minusRows : plusRows.concat(minusRows);
                              if (!rows.length) return <div className="muted2">No stage changes in this window.</div>;
                              const isLost = bucketKey === "lost";
                              const showLostReason = bucketKey === "lost" || rows.some((r: any) => String(r?.to_bucket ?? "") === "lost" || String(r?.from_bucket ?? "") === "lost");

                              // ------------------------------------------------------------------
                              // Lost reasons summary (donut + top reasons)
                              //
                              // Goal: provide a quick read of WHY deals ended up in Lost, without
                              // adding 3 separate columns (Closed Lost / Disqual / Unified).
                              //
                              // We attribute a "reason" only to transitions INTO Lost.
                              // ------------------------------------------------------------------
                              const toLostRows = showLostReason ? rows.filter((r: any) => String(r?.to_bucket ?? "") === "lost") : [];
                              const reasonKeyFor = (r: any) => {
                                const field = String(r?.lost_reason_field ?? "").trim();
                                const value = String(r?.lost_reason_value ?? "").trim();
                                if (!value) return null;
                                // Key includes field to avoid collisions between enum/text fields.
                                return { key: `${field}::${value}`, field, value };
                              };
                              const reasonCounts = new Map<string, { field: string; value: string; count: number }>();
                              for (const r of toLostRows) {
                                const k = reasonKeyFor(r);
                                if (!k) continue;
                                const prev = reasonCounts.get(k.key);
                                if (prev) prev.count += 1;
                                else reasonCounts.set(k.key, { field: k.field, value: k.value, count: 1 });
                              }
                              const sortedReasons = Array.from(reasonCounts.values()).sort((a, b) => b.count - a.count);
                              const topN = 10;
                              const topReasons = sortedReasons.slice(0, topN);
                              const otherCount = sortedReasons.slice(topN).reduce((acc, x) => acc + (Number(x?.count ?? 0) || 0), 0);
                              const reasonTotal = topReasons.reduce((acc, x) => acc + (Number(x?.count ?? 0) || 0), 0) + otherCount;
                              const donutItems = [
                                ...topReasons.map((x) => ({ label: x.value || x.field || "—", field: x.field, value: x.value, count: x.count })),
                                ...(otherCount ? [{ label: "Other", field: "", value: "", count: otherCount }] : [])
                              ];
                              const donutColors = [
                                "#60a5fa", // blue
                                "#34d399", // green
                                "#fbbf24", // yellow
                                "#f87171", // red
                                "#a78bfa", // violet
                                "#fb7185", // rose
                                "#22d3ee", // cyan
                                "#f97316", // orange
                                "#4ade80", // green 2
                                "#38bdf8", // sky
                                "#94a3b8"  // other
                              ];
                              const donutGradient = (() => {
                                if (!reasonTotal) return "";
                                let acc = 0;
                                const parts: string[] = [];
                                for (let i = 0; i < donutItems.length; i++) {
                                  const it = donutItems[i];
                                  const p = Math.max(0, (Number(it?.count ?? 0) || 0) / reasonTotal);
                                  const start = acc;
                                  const end = acc + p;
                                  acc = end;
                                  const c = donutColors[Math.min(i, donutColors.length - 1)] || "#94a3b8";
                                  const sPct = Math.round(start * 10000) / 100;
                                  const ePct = Math.round(end * 10000) / 100;
                                  parts.push(`${c} ${sPct}% ${ePct}%`);
                                }
                                return parts.join(", ");
                              })();

                              const rank = (b: string) => (b === "leads" ? 0 : b === "sql" ? 1 : b === "opportunity" ? 2 : b === "clients" ? 3 : b === "lost" ? 9 : 99);
                              const colorForRow = (r: any) => {
                                const fromB = String(r?.from_bucket ?? "");
                                const toB = String(r?.to_bucket ?? "");
                                const kind = String(r?.kind ?? "");
                                if (kind === "created") return isLost ? "rgba(239,68,68,0.95)" : "rgba(34,197,94,0.95)";
                                if (isLost) {
                                  if (fromB === "lost" && toB !== "lost") return "rgba(34,197,94,0.95)";
                                  if (toB === "lost" && fromB !== "lost") return "rgba(239,68,68,0.95)";
                                  return "rgba(255,255,255,0.65)";
                                }
                                if (fromB === bucketKey && toB === "lost") return "rgba(239,68,68,0.95)";
                                if (fromB === bucketKey && rank(toB) > rank(fromB)) return "rgba(34,197,94,0.95)";
                                if (fromB === bucketKey && rank(toB) <= rank(fromB)) return "rgba(239,68,68,0.95)";
                                if (toB === bucketKey && fromB !== bucketKey) return "rgba(34,197,94,0.95)";
                                return "rgba(255,255,255,0.65)";
                              };
                              return (
                                <div style={{ overflow: "auto" }}>
                                  {showLostReason && reasonTotal ? (
                                    <div style={{ display: "flex", gap: 16, alignItems: "stretch", padding: "6px 0 14px 0" }}>
                                      <div style={{ flex: "0 0 auto", display: "flex", alignItems: "center", gap: 14 }}>
                                        <div
                                          title={`Top reasons among deals that entered Lost: ${toLostRows.length}`}
                                          style={{
                                            width: 120,
                                            height: 120,
                                            borderRadius: 999,
                                            background: donutGradient ? `conic-gradient(${donutGradient})` : "rgba(148,163,184,0.65)",
                                            position: "relative",
                                            border: "1px solid rgba(255,255,255,0.12)"
                                          }}
                                        >
                                          <div
                                            style={{
                                              position: "absolute",
                                              inset: 18,
                                              borderRadius: 999,
                                              background: "rgba(15,17,23,0.92)",
                                              border: "1px solid rgba(255,255,255,0.08)"
                                            }}
                                          />
                                          <div
                                            style={{
                                              position: "absolute",
                                              inset: 0,
                                              display: "flex",
                                              alignItems: "center",
                                              justifyContent: "center",
                                              flexDirection: "column"
                                            }}
                                          >
                                            <div className="mono" style={{ fontSize: 18, fontWeight: 750 }}>{String(reasonTotal)}</div>
                                            <div className="muted2" style={{ fontSize: 11, marginTop: 2 }}>Lost in window</div>
                                          </div>
                                        </div>
                                      </div>
                                      <div style={{ minWidth: 260, flex: "1 1 auto" }}>
                                        <div className="muted2" style={{ fontSize: 12, marginBottom: 8 }}>
                                          Top reasons for deals that <span className="mono">entered Lost</span> (top 10 + other)
                                        </div>
                                        <div style={{ display: "grid", gridTemplateColumns: "1fr auto auto", gap: "6px 12px", alignItems: "baseline" }}>
                                          {donutItems.slice(0, 11).map((it, idx) => {
                                            const c = donutColors[Math.min(idx, donutColors.length - 1)] || "#94a3b8";
                                            const pct = reasonTotal ? Math.round(((Number(it.count) || 0) / reasonTotal) * 1000) / 10 : 0;
                                            const label = String(it.label || "—");
                                            const title = it.field && it.value ? `${it.field}: ${it.value}` : label;
                                            return (
                                              <div key={`${label}-${idx}`} style={{ display: "contents" }}>
                                                <div title={title} style={{ display: "flex", gap: 8, alignItems: "baseline", minWidth: 0 }}>
                                                  <span style={{ width: 10, height: 10, borderRadius: 3, background: c, flex: "0 0 auto", marginTop: 2 }} />
                                                  <span className="mono" style={{ fontSize: 12, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                                                    {label}
                                                  </span>
                                                </div>
                                                <div className="mono" style={{ fontSize: 12, opacity: 0.85, textAlign: "right" }}>{String(it.count)}</div>
                                                <div className="mono" style={{ fontSize: 12, opacity: 0.65, textAlign: "right" }}>{pct ? `${pct}%` : "—"}</div>
                                              </div>
                                            );
                                          })}
                                        </div>
                                      </div>
                                    </div>
                                  ) : null}
                                  <table className="table">
                                    <thead>
                                      <tr>
                                        <th>Deal</th>
                                        <th>Pipeline</th>
                                        <th>Change</th>
                                        {showLostReason ? <th>Lost reason</th> : null}
                                        <th>Created</th>
                                        <th>Last modified</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {rows.slice(0, 400).map((r: any) => (
                                        <tr key={String(r.id) + String(r.from_stage_id) + String(r.to_stage_id) + String(r.kind)}>
                                          <td>
                                            {(() => {
                                              const id = String(r.id ?? "").trim();
                                              const href = hubspotPortalId && id ? `https://app.hubspot.com/contacts/${hubspotPortalId}/record/0-3/${id}/` : "";
                                              if (!href) return <span style={{ fontWeight: 650 }}>{String(r.dealname || r.id)}</span>;
                                              return (
                                                <a
                                                  href={href}
                                                  target="_blank"
                                                  rel="noreferrer"
                                                  style={{ color: "inherit", textDecoration: "underline", textUnderlineOffset: 3, fontWeight: 650 }}
                                                  title="Open in HubSpot"
                                                >
                                                  {String(r.dealname || r.id)}
                                                </a>
                                              );
                                            })()}
                                            <div className="mono" style={{ fontSize: 11, opacity: 0.6 }}>{String(r.id)}</div>
                                          </td>
                                          <td className="mono">{String(r.pipeline || "—")}</td>
                                          <td>
                                            <span className="mono" style={{ fontSize: 12, color: colorForRow(r) }}>
                                              {String(r.kind) === "created"
                                                ? `created → ${String(r.to_stage_label || r.to_bucket)}`
                                                : `${String(r.from_stage_label || r.from_bucket)} → ${String(r.to_stage_label || r.to_bucket)}`}
                                            </span>
                                          </td>
                                          {showLostReason ? (() => {
                                            const field = String(r?.lost_reason_field ?? "").trim();
                                            const value = String(r?.lost_reason_value ?? "").trim();
                                            if (!field || !value) return <td className="mono" style={{ opacity: 0.7 }}>—</td>;
                                            return (
                                              <td title={`${field}: ${value}`}>
                                                <div className="muted2" style={{ fontSize: 11 }}>{field}</div>
                                                <div className="mono" style={{ fontSize: 12, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: 360 }}>
                                                  {value}
                                                </div>
                                              </td>
                                            );
                                          })() : null}
                                          <td className="mono">{r.createdate ? msToYmd(r.createdate) : "—"}</td>
                                          <td className="mono">{r.lastmodified ? msToYmd(r.lastmodified) : "—"}</td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                  {rows.length > 400 ? <div className="muted2" style={{ marginTop: 8, fontSize: 12 }}>Showing first 400 changes.</div> : null}
                                </div>
                              );
                            })()}
                          </div>
                        </div>
                      </div>
                    ) : null}
                    <div style={{ gridColumn: "span 12" }}>
                      <div className="card" style={{ background: "rgba(255,255,255,0.02)", border: "1px solid rgba(255,255,255,0.08)" }}>
                        <div className="cardHeader">
                          <div>
                            <div className="cardTitle">Conversions</div>
                            <div className="cardDesc">
                              {stageConvsMode === "in_window"
                                ? "Stage transitions that happened inside the selected date range (based on dealstage history timestamps)."
                                : "Cohort view: from deals created in the selected date range, which stages they ever reached (based on dealstage history)."}
                            </div>
                          </div>
                          <div className="btnRow">
                            <select className="select" value={stageConvsMode} onChange={(e: any) => setStageConvsMode(e.target.value)}>
                              <option value="cohort_created">Cohort (created in range)</option>
                              <option value="in_window">In-window (moves in range)</option>
                            </select>
                            {stageConvsLoading ? <span className="tag">Calculating…</span> : null}
                            {stageConvs?.cohort != null ? (
                              <span className="tag">Cohort: <span className="mono">{String(stageConvs.cohort)}</span></span>
                            ) : null}
                          </div>
                        </div>
                        <div className="cardBody">
                          {stageConvsErr ? (
                            <div className="notice">Conversions error: <span className="mono">{stageConvsErr}</span></div>
                          ) : !stageConvs?.conversions ? (
                            <div className="muted2">Select a pipeline and date range to calculate conversions.</div>
                          ) : (
                            <div className="grid" style={{ gridTemplateColumns: "repeat(12,1fr)", gap: 10 }}>
                              {[
                                { label: "Lead → SQL", k: "lead_to_sql" },
                                { label: "SQL → Opportunity", k: "sql_to_opportunity" },
                                { label: "Opportunity → Clients", k: "opportunity_to_clients" },
                                { label: "Lead → Clients", k: "lead_to_clients" }
                              ].map((x) => {
                                const base = stageConvsMode === "in_window" ? stageConvs?.in_window?.conversions : stageConvs?.conversions;
                                const c = (base ?? {})[x.k] ?? null;
                                const from = Number(c?.from ?? 0) || 0;
                                const to = Number(c?.to ?? 0) || 0;
                                const rate = Number(c?.rate ?? 0);
                                return (
                                  <div key={x.k} style={{ gridColumn: "span 3" }} className="card">
                                    <div className="cardBody" style={{ padding: 12 }}>
                                      <div className="muted2" style={{ fontSize: 12 }}>{x.label}</div>
                                      <div className="mono" style={{ fontSize: 18, fontWeight: 700, marginTop: 6 }}>
                                        {Number.isFinite(rate) ? `${Math.round(rate * 100)}%` : "—"}
                                      </div>
                                      <div className="muted2" style={{ fontSize: 12, marginTop: 6 }}>
                                        <span className="mono">{to}</span> / <span className="mono">{from}</span>
                                      </div>
                                    </div>
                                  </div>
                                );
                              })}

                              <div style={{ gridColumn: "span 12" }} className="card">
                                <div className="cardBody" style={{ padding: 12, display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center", justifyContent: "space-between" }}>
                                  <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center" }}>
                                    <span className="tag">
                                      Leads per Client:{" "}
                                      <span className="mono">
                                        {stageConvsMode === "in_window"
                                          ? "—"
                                          : Number.isFinite(Number(stageConvs?.leads_per_client))
                                            ? `${Math.round(Number(stageConvs.leads_per_client) * 10) / 10}`
                                            : "—"}
                                      </span>
                                    </span>
                                    <span className="tag">
                                      Median time to Clients:{" "}
                                      <span className="mono">
                                        {stageConvsMode === "in_window" ? "—" : (() => {
                                          const v = Number(stageConvs?.time_to_clients_days?.median_days);
                                          if (!Number.isFinite(v)) return "—";
                                          if (v < 1) return "<1d";
                                          return `${Math.round(v)}d`;
                                        })()}
                                      </span>
                                    </span>
                                    <span className="tag">
                                      P75 time to Clients:{" "}
                                      <span className="mono">
                                        {stageConvsMode === "in_window" ? "—" : (() => {
                                          const v = Number(stageConvs?.time_to_clients_days?.p75_days);
                                          if (!Number.isFinite(v)) return "—";
                                          if (v < 1) return "<1d";
                                          return `${Math.round(v)}d`;
                                        })()}
                                      </span>
                                    </span>
                                    <span className="tag">
                                      Clients sample:{" "}
                                      <span className="mono">
                                        {stageConvsMode === "in_window"
                                          ? "—"
                                          : Number.isFinite(Number(stageConvs?.time_to_clients_days?.n))
                                            ? String(stageConvs.time_to_clients_days.n)
                                            : "—"}
                                      </span>
                                    </span>
                                    <span className="tag">
                                      Opportunity → Lost:{" "}
                                      <span className="mono">
                                        {(() => {
                                          const base = stageConvsMode === "in_window" ? stageConvs?.in_window?.conversions : stageConvs?.conversions;
                                          const c = (base ?? {})?.opportunity_to_lost;
                                          const rate = Number(c?.rate);
                                          const to = Number(c?.to ?? 0) || 0;
                                          const from = Number(c?.from ?? 0) || 0;
                                          if (!Number.isFinite(rate)) return "—";
                                          return `${Math.round(rate * 100)}% (${to}/${from})`;
                                        })()}
                                      </span>
                                    </span>
                                  </div>
                                  <div className="muted2" style={{ fontSize: 12 }}>
                                    {stageConvsMode === "in_window"
                                      ? "In-window rates are computed as transitions / deals that were in the FROM bucket during the window (best-effort)."
                                      : "Time stats are computed only for deals in cohort that reached Clients."}
                                  </div>
                                </div>
                              </div>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>

                  <StackedBarsWithLine
                    title="New deals (created)"
                    buckets={(chartBucketsDaily.buckets || []).map((b: any) => ({ key: String(b.key), label: String(b.label) }))}
                    series={buildDailySeries()}
                    line={chartBucketsDaily.line || []}
                    lineLabel="Active deals in funnel"
                    onBarClick={(bucketKey) => {
                      if (!bucketKey) return;
                      const b = (chartBucketsDaily.buckets || []).find((x: any) => String(x.key) === String(bucketKey));
                      if (!b) return;
                      // Clicking a bar narrows the DateRange to that bucket, so the table matches the bar.
                      setDateRangeTouched(true);
                      setDateRange({ since: String(b.since ?? ""), until: String(b.until ?? "") });
                    }}
                    right={<span className="tag">Metric: <span className="mono">new_deals</span></span>}
                    below={
                      <>
                        {(() => {
                          // ------------------------------------------------------------------
                          // Channels summary (donut + top channels)
                          //
                          // Goal: quick read of which channels bring the most new deals/leads
                          // in the selected date range (same dataset as the daily chart).
                          //
                          // NOTE: This is shown only in "Channels" color mode.
                          // ------------------------------------------------------------------
                          if (colorMode !== "channel") return null;

                          const buckets = Array.isArray(chartBucketsDaily.buckets) ? chartBucketsDaily.buckets : [];
                          const totalsByChannel = new Map<string, number>();
                          const add = (k: string, v: number) => {
                            const kk = String(k || "").trim();
                            if (!kk) return;
                            totalsByChannel.set(kk, (totalsByChannel.get(kk) ?? 0) + (Number(v) || 0));
                          };

                          for (const b of buckets) {
                            const totalNew = toNum(b?.total_new);
                            const by = b?.by_channel ?? {};
                            let assigned = 0;
                            for (const rawKey of Object.keys(by || {})) {
                              const canon = rawKey === "__unassigned__" ? "__unassigned__" : canonicalizeChannelLabel(rawKey);
                              const v = toNum(by[rawKey]);
                              assigned += v;
                              add(canon, v);
                            }
                            // Keep Unassigned as remainder, same as buildDailySeries().
                            const rem = Math.max(0, totalNew - assigned);
                            if (rem) add("__unassigned__", rem);
                          }

                          const items = Array.from(totalsByChannel.entries())
                            .map(([key, count]) => ({ key, label: key === "__unassigned__" ? "Unassigned" : key, count: Number(count) || 0 }))
                            .filter((x) => x.count > 0)
                            .sort((a, b) => b.count - a.count);

                          if (!items.length) return null;

                          const topN = 10;
                          const top = items.slice(0, topN);
                          const otherCount = items.slice(topN).reduce((acc, x) => acc + (Number(x.count) || 0), 0);
                          const total = top.reduce((acc, x) => acc + (Number(x.count) || 0), 0) + otherCount;

                          const donutItems = [
                            ...top,
                            ...(otherCount ? [{ key: "other", label: "Other channels", count: otherCount }] : [])
                          ];

                          const colorForIdx = (idx: number, key: string) => {
                            // Match chart semantics: Unassigned and Other are grey.
                            if (key === "__unassigned__" || key === "other") return "rgba(255,255,255,0.25)";
                            return COLORS[idx % COLORS.length];
                          };

                          const donutGradient = (() => {
                            if (!total) return "";
                            let acc = 0;
                            const parts: string[] = [];
                            for (let i = 0; i < donutItems.length; i++) {
                              const it = donutItems[i];
                              const p = Math.max(0, (Number(it?.count ?? 0) || 0) / total);
                              const start = acc;
                              const end = acc + p;
                              acc = end;
                              const c = colorForIdx(i, it.key);
                              const sPct = Math.round(start * 10000) / 100;
                              const ePct = Math.round(end * 10000) / 100;
                              parts.push(`${c} ${sPct}% ${ePct}%`);
                            }
                            return parts.join(", ");
                          })();

                          return (
                            <div style={{ marginTop: 12, marginBottom: 14 }}>
                              <div className="card" style={{ background: "rgba(255,255,255,0.02)", border: "1px solid rgba(255,255,255,0.08)" }}>
                                <div className="cardBody" style={{ padding: 12, display: "flex", gap: 16, alignItems: "stretch" }}>
                                  <div style={{ flex: "0 0 auto", display: "flex", alignItems: "center", gap: 14 }}>
                                    <div
                                      title="Top channels by new deals created"
                                      style={{
                                        width: 120,
                                        height: 120,
                                        borderRadius: 999,
                                        background: donutGradient ? `conic-gradient(${donutGradient})` : "rgba(148,163,184,0.65)",
                                        position: "relative",
                                        border: "1px solid rgba(255,255,255,0.12)"
                                      }}
                                    >
                                      <div
                                        style={{
                                          position: "absolute",
                                          inset: 18,
                                          borderRadius: 999,
                                          background: "rgba(15,17,23,0.92)",
                                          border: "1px solid rgba(255,255,255,0.08)"
                                        }}
                                      />
                                      <div
                                        style={{
                                          position: "absolute",
                                          inset: 0,
                                          display: "flex",
                                          alignItems: "center",
                                          justifyContent: "center",
                                          flexDirection: "column"
                                        }}
                                      >
                                        <div className="mono" style={{ fontSize: 18, fontWeight: 750 }}>{String(total)}</div>
                                        <div className="muted2" style={{ fontSize: 11, marginTop: 2 }}>New deals</div>
                                      </div>
                                    </div>
                                  </div>
                                  <div style={{ minWidth: 260, flex: "1 1 auto" }}>
                                    <div className="muted2" style={{ fontSize: 12, marginBottom: 8 }}>
                                      Top channels for <span className="mono">new deals (created)</span> (top 10 + other)
                                    </div>
                                    <div style={{ display: "grid", gridTemplateColumns: "1fr auto auto", gap: "6px 12px", alignItems: "baseline" }}>
                                      {donutItems.slice(0, 11).map((it, idx) => {
                                        const c = colorForIdx(idx, it.key);
                                        const pct = total ? Math.round(((Number(it.count) || 0) / total) * 1000) / 10 : 0;
                                        const label = String(it.label || "—");
                                        return (
                                          <div key={`${it.key}-${label}-${idx}`} style={{ display: "contents" }}>
                                            <div title={label} style={{ display: "flex", gap: 8, alignItems: "baseline", minWidth: 0 }}>
                                              <span style={{ width: 10, height: 10, borderRadius: 3, background: c, flex: "0 0 auto", marginTop: 2 }} />
                                              <span className="mono" style={{ fontSize: 12, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                                                {label}
                                              </span>
                                            </div>
                                            <div className="mono" style={{ fontSize: 12, opacity: 0.85, textAlign: "right" }}>{String(it.count)}</div>
                                            <div className="mono" style={{ fontSize: 12, opacity: 0.65, textAlign: "right" }}>{pct ? `${pct}%` : "—"}</div>
                                          </div>
                                        );
                                      })}
                                    </div>
                                  </div>
                                </div>
                              </div>
                            </div>
                          );
                        })()}
                        <MetricTable metricKey="new_deals" />
                      </>
                    }
                  />
                  <div
                    className="muted2"
                    style={{
                      gridColumn: "span 12",
                      marginTop: 10,
                      fontSize: 12,
                      display: "flex",
                      alignItems: "center",
                      flexWrap: "wrap",
                      gap: 8
                    }}
                  >
                    <span>Tip: for advanced funnel movements/activities, open</span>
                    <a className="btn" href="/dashboard">Advanced dashboard</a>
                    <span>.</span>
                  </div>
                </>
              ) : (
                <>
                  {activityErr ? <div className="notice" style={{ gridColumn: "span 12" }}>Activities error: <span className="mono">{activityErr}</span></div> : null}
                  {activityLoading ? <div className="muted2" style={{ gridColumn: "span 12" }}>Loading activities…</div> : null}

                  {activityReportTab === "linkedin" ? (
                    <div
                      className="card"
                      style={{
                        gridColumn: "span 12",
                        marginBottom: 12,
                        background: "rgba(255,255,255,0.02)",
                        border: "1px solid rgba(255,255,255,0.08)"
                      }}
                    >
                      <div className="cardBody" style={{ padding: 12 }}>
                        {(() => {
                          // Main dashboard KPIs should follow our internal logic (derived from events).
                          const t = getsalesReport?.totals ?? {};
                          const cs = Number((t as any)?.connections_sent ?? 0) || 0;
                          const ca = Number((t as any)?.connections_accepted ?? 0) || 0;
                          const ms = Number((t as any)?.messages_sent ?? 0) || 0;
                          const moRaw = Number((t as any)?.messages_opened ?? 0) || 0;
                          const replies = Number((t as any)?.messages_replied ?? 0) || 0;
                          // Keep raw opened count to mirror GetSales UI (it can be < replies).
                          const openedAdj = moRaw;

                          const pct = (a: number, b: number) => (b > 0 ? `${Math.round((a / b) * 1000) / 10}%` : "—");
                          const per = (a: number, b: number) => (b > 0 ? `${Math.round((a / b) * 1000) / 10}%` : "—");

                          const Tile = (p: { title: string; value: number; sub?: string; badge?: string }) => (
                            <div
                              className="card"
                              style={{
                                // IMPORTANT: `.card` defaults to `grid-column: span 12` (page grid).
                                // Override it here so KPI tiles can sit in a compact row grid.
                                gridColumn: "auto",
                                background: "rgba(255,255,255,0.02)",
                                border: "1px solid rgba(255,255,255,0.08)"
                              }}
                            >
                              <div className="cardBody" style={{ padding: 14 }}>
                                <div className="muted2" style={{ fontSize: 13 }}>{p.title}</div>
                                <div style={{ display: "flex", gap: 10, alignItems: "baseline", justifyContent: "space-between", marginTop: 6 }}>
                                  <div className="mono" style={{ fontSize: 28, fontWeight: 800 }}>{p.value}</div>
                                  {p.badge ? <span className="tag"><span className="mono">{p.badge}</span></span> : null}
                                </div>
                                {p.sub ? <div className="muted2" style={{ fontSize: 12, marginTop: 6 }}>{p.sub}</div> : null}
                              </div>
                            </div>
                          );

                          return (
                            <div
                              style={{
                                display: "grid",
                                gridTemplateColumns: "repeat(5, minmax(0, 1fr))",
                                gap: 10
                              }}
                            >
                              <Tile title="Connections Sent" value={cs} />
                              <Tile title="Connections Accepted" value={ca} sub={`${ca} of ${cs}`} badge={pct(ca, cs)} />
                              <Tile title="Messages Sent" value={ms} sub={`${ms} of ${ca}`} badge={per(ms, ca)} />
                              <Tile title="Messages Opened" value={openedAdj} sub={`${openedAdj} of ${ms}`} badge={pct(openedAdj, ms)} />
                              <Tile
                                title="Messages Replied"
                                value={replies}
                                badge={pct(replies, ms)}
                              />
                            </div>
                          );
                        })()}
                      </div>
                    </div>
                  ) : activityReportTab === "email" ? (
                    <div
                      className="card"
                      style={{
                        gridColumn: "span 12",
                        marginBottom: 12,
                        background: "rgba(255,255,255,0.02)",
                        border: "1px solid rgba(255,255,255,0.08)"
                      }}
                    >
                      <div className="cardBody" style={{ padding: 12 }}>
                        {(() => {
                          const totals = smartleadReport?.totals ?? {
                            leads_contacted: 0,
                            emails_sent: 0,
                            emails_opened: 0,
                            emails_replied: 0,
                            bounced: 0,
                            positive_reply: 0,
                            replied_ooo: 0
                          };
                          const leadsContacted = Number(totals.leads_contacted ?? 0) || 0;
                          const messagesSent = Number(totals.emails_sent ?? 0) || 0;
                          const openedAdj = Number(totals.emails_opened ?? 0) || 0;
                          const replied = Number(totals.emails_replied ?? 0) || 0;
                          const positiveFromMetrics = Number(totals.positive_reply ?? 0) || 0;

                          const pct = (a: number, b: number) => (b > 0 ? `${Math.round((a / b) * 1000) / 10}%` : "—");
                          // Campaign list stats are best-effort and not date-ranged (display only).
                          const selectedSet = new Set((smartleadCampaignIds || []).map(String));
                          const campaigns = Array.isArray(smartleadCampaigns) ? smartleadCampaigns : [];
                          const filteredCampaigns = selectedSet.size
                            ? campaigns.filter((c: any) => selectedSet.has(String(c?.id ?? "")))
                            : campaigns;

                          const bannerPct = (a: number, b: number, digits = 2) => {
                            if (!(b > 0)) return "";
                            const v = (a / b) * 100;
                            const pow = Math.pow(10, digits);
                            return `(${Math.round(v * pow) / pow}%)`;
                          };
                          return (
                            <div style={{ display: "grid", gridTemplateColumns: "repeat(12, 1fr)", gap: 10 }}>
                              {smartleadEventsErr ? (
                                <div className="muted2" style={{ gridColumn: "span 12", fontSize: 12 }}>
                                  SmartLead events unavailable: <span className="mono">{smartleadEventsErr}</span>
                                </div>
                              ) : null}

                              {/* SmartLead-like campaign summary banner (best-effort) */}
                              <div className="card" style={{ gridColumn: "span 12" }}>
                                <div className="cardBody" style={{ padding: 12 }}>
                                  <div style={{ display: "flex", gap: 10, alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", marginBottom: 10 }}>
                                    <div style={{ minWidth: 260 }}>
                                      <div style={{ fontWeight: 750 }}>
                                        {selectedSet.size === 1
                                          ? (filteredCampaigns[0]?.name ? String(filteredCampaigns[0].name) : `Campaign #${String(filteredCampaigns[0]?.id ?? "")}`)
                                          : selectedSet.size > 1
                                            ? `Selected campaigns (${selectedSet.size})`
                                            : "All configured campaigns"}
                                      </div>
                                      <div className="muted2" style={{ fontSize: 12, marginTop: 4 }}>
                                        {(() => {
                                          const byStatus = { active: 0, paused: 0, unknown: 0 };
                                          for (const c of filteredCampaigns) {
                                            const s = String(c?.status ?? "unknown");
                                            if (s === "active") byStatus.active++;
                                            else if (s === "paused") byStatus.paused++;
                                            else byStatus.unknown++;
                                          }
                                          const parts = [];
                                          if (byStatus.active) parts.push(`Active: ${byStatus.active}`);
                                          if (byStatus.paused) parts.push(`Paused: ${byStatus.paused}`);
                                          if (byStatus.unknown) parts.push(`Unknown: ${byStatus.unknown}`);
                                          return parts.length ? parts.join(" · ") : "—";
                                        })()}
                                      </div>
                                    </div>
                                    <div className="muted2" style={{ fontSize: 12 }}>
                                      Source:{" "}
                                      <span className="mono">
                                        db_events
                                      </span>
                                    </div>
                                  </div>

                                  <div
                                    style={{
                                      display: "flex",
                                      flexWrap: "wrap",
                                      gap: 10,
                                      alignItems: "stretch"
                                    }}
                                  >
                                    {[
                                      { k: "leads_contacted", label: "Leads contacted", v: leadsContacted, pct: "" },
                                      { k: "emails_sent", label: "Messages sent", v: messagesSent, pct: "" },
                                      {
                                        k: "opened",
                                        label: "Opened",
                                        v: openedAdj,
                                        pct: bannerPct(openedAdj, leadsContacted)
                                      },
                                      {
                                        k: "replied",
                                        label: "Replied",
                                        v: replied,
                                        pct: bannerPct(replied, leadsContacted)
                                      },
                                      {
                                        k: "pos",
                                        label: "Positive reply",
                                        v: positiveFromMetrics,
                                        pct: bannerPct(positiveFromMetrics, replied)
                                      },
                                    ].map((x) => (
                                      <div
                                        key={x.k}
                                        className="card"
                                        style={{
                                          background: "rgba(255,255,255,0.02)",
                                          border: "1px solid rgba(255,255,255,0.08)",
                                          flex: "1 1 140px",
                                          minWidth: 140
                                        }}
                                      >
                                        <div className="cardBody" style={{ padding: 12 }}>
                                          <div className="muted2" style={{ fontSize: 12 }}>{x.label}</div>
                                          <div style={{ display: "flex", gap: 10, alignItems: "baseline", justifyContent: "space-between", marginTop: 6 }}>
                                            <div className="mono" style={{ fontSize: 22, fontWeight: 800 }}>{x.v}</div>
                                            {x.pct ? <span className="tag"><span className="mono">{x.pct}</span></span> : null}
                                          </div>
                                        </div>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              </div>
                            </div>
                          );
                        })()}
                      </div>
                    </div>
                  ) : null}

                  <div
                    className="card"
                    style={{
                      gridColumn: "span 12",
                      marginBottom: 12,
                      background: "rgba(255,255,255,0.02)",
                      border: "1px solid rgba(255,255,255,0.08)"
                    }}
                  >
                    <div className="cardBody" style={{ padding: 12 }}>
                      <div className="muted2" style={{ fontSize: 13, marginBottom: 10, display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
                        <span>
                          {activityReportTab === "email"
                            ? "HubSpot created deals influenced by SmartLead (email match)"
                            : "HubSpot created deals influenced by GetSales (email/company match)"}
                        </span>
                        <span className="tag">
                          Lookback:
                          {" "}
                          <select
                            className="select"
                            value={String(activityReportTab === "email" ? smartleadInfluenceLookbackDays : getsalesInfluenceLookbackDays)}
                            onChange={(e: any) => {
                              const v = Number(String(e.target.value)) || 180;
                              if (activityReportTab === "email") setSmartleadInfluenceLookbackDays(v);
                              else setGetsalesInfluenceLookbackDays(v);
                            }}
                            style={{ marginLeft: 8 }}
                          >
                            <option value="30">30d</option>
                            <option value="60">60d</option>
                            <option value="90">90d</option>
                            <option value="180">180d</option>
                          </select>
                        </span>
                      </div>
                      {activityReportTab === "email" ? (
                        <>
                          {smartleadInfluenceErr ? <div className="notice">Influence error: <span className="mono">{smartleadInfluenceErr}</span></div> : null}
                          {smartleadInfluenceLoading ? <div className="muted2">Loading influenced deals…</div> : null}
                        </>
                      ) : (
                        <>
                          {getsalesInfluenceErr ? <div className="notice">Influence error: <span className="mono">{getsalesInfluenceErr}</span></div> : null}
                          {getsalesInfluenceLoading ? <div className="muted2">Loading influenced deals…</div> : null}
                        </>
                      )}
                      {(() => {
                        const active = activityReportTab === "email" ? smartleadInfluence : getsalesInfluence;
                        const t = active?.totals || {};
                        const leadsCreated = Number(t.influenced_leads_created ?? 0) || 0;
                        const oppsCreated = Number(t.influenced_opps_created ?? 0) || 0;
                        const clientsCreated = Number(t.influenced_clients_created ?? 0) || 0;
                        const Tile = (p: { title: string; value: number; sub?: string }) => (
                          <div
                            className="card"
                            style={{
                              gridColumn: "auto",
                              background: "rgba(255,255,255,0.02)",
                              border: "1px solid rgba(255,255,255,0.08)"
                            }}
                          >
                            <div className="cardBody" style={{ padding: 14 }}>
                              <div className="muted2" style={{ fontSize: 13 }}>{p.title}</div>
                              <div className="mono" style={{ fontSize: 26, fontWeight: 800, marginTop: 6 }}>{p.value}</div>
                              {p.sub ? <div className="muted2" style={{ fontSize: 12, marginTop: 6 }}>{p.sub}</div> : null}
                            </div>
                          </div>
                        );
                        return (
                          <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 10 }}>
                            <Tile title="Leads created" value={leadsCreated} sub="(influenced)" />
                            <Tile title="Opps created" value={oppsCreated} sub="(influenced)" />
                          </div>
                        );
                      })()}

                      {(activityReportTab === "email" ? smartleadInfluence?.debug : getsalesInfluence?.debug) ? (
                        <div className="muted2" style={{ fontSize: 12, marginTop: 10 }}>
                          {(() => {
                            const d = (activityReportTab === "email" ? smartleadInfluence?.debug : getsalesInfluence?.debug) ?? {};
                            return (
                              <>
                                Debug: deals_total <span className="mono">{String((d as any)?.deals_total ?? "—")}</span>, scanned{" "}
                                <span className="mono">{String((d as any)?.deals_scanned ?? "—")}</span>, with_contacts{" "}
                                <span className="mono">{String((d as any)?.deals_with_contacts ?? "—")}</span>, with_companies{" "}
                                <span className="mono">{String((d as any)?.deals_with_companies ?? "—")}</span>, events_with_email{" "}
                                <span className="mono">{String((d as any)?.events_with_email ?? "—")}</span>, matched{" "}
                                <span className="mono">{String((d as any)?.deals_matched ?? "—")}</span>.
                              </>
                            );
                          })()}
                        </div>
                      ) : null}

                      {(() => {
                        const active = activityReportTab === "email" ? smartleadInfluence : getsalesInfluence;
                        const rows = Array.isArray(active?.influencedDeals) ? active.influencedDeals : [];
                        if (!rows.length) return null;
                        return (
                          <div style={{ marginTop: 12, overflow: "auto" }}>
                            <table className="table">
                              <thead>
                                <tr>
                                  <th>Created</th>
                                  <th>Deal</th>
                                  <th>Stage</th>
                                  <th>Email (matched)</th>
                                  <th>Hypothesis</th>
                                </tr>
                              </thead>
                              <tbody>
                                {rows.slice(0, 80).map((r: any) => {
                                  const href = String(r?.url ?? "").trim();
                                  const name = String(r?.dealname ?? r?.id ?? "—");
                                  const created = String(r?.createdate ?? "").replace("T", " ").slice(0, 16) || "—";
                                  const stage = String(r?.dealstage_label ?? r?.dealstage_id ?? "—");
                                  const email = String(r?.influenced_email ?? "—");
                                  const hyp = String(r?.hypothesis_title ?? "—");
                                  return (
                                    <tr key={String(r?.id ?? name)}>
                                      <td className="mono">{created}</td>
                                      <td style={{ maxWidth: 420 }}>
                                        {href ? (
                                          <a href={href} target="_blank" rel="noreferrer" style={{ color: "inherit", textDecoration: "underline", textUnderlineOffset: 3 }}>
                                            {name || "—"}
                                          </a>
                                        ) : (
                                          <span className="mono">{name || "—"}</span>
                                        )}
                                      </td>
                                      <td className="mono">{stage}</td>
                                      <td className="mono">{email}</td>
                                      <td style={{ maxWidth: 360 }}>
                                        <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={hyp || ""}>
                                          {hyp || "—"}
                                        </div>
                                      </td>
                                    </tr>
                                  );
                                })}
                              </tbody>
                            </table>
                            {rows.length > 80 ? <div className="muted2" style={{ marginTop: 8, fontSize: 12 }}>Showing first 80 influenced deals (of {rows.length}). Narrow the date range to explore more.</div> : null}
                          </div>
                        );
                      })()}
                      {(activityReportTab === "email" ? smartleadInfluence?.truncated : getsalesInfluence?.truncated) ? (
                        <div className="muted2" style={{ fontSize: 12, marginTop: 10 }}>
                          Note: influence was computed on a capped subset of deals (to stay within API limits). Narrow the date range for full accuracy.
                        </div>
                      ) : null}
                    </div>
                  </div>

                  <MultiLineChart
                    title={activityReportTab === "linkedin" ? "LinkedIn report (GetSales)" : "Email report (SmartLead)"}
                    days={activityChart.days}
                    series={activityChart.series}
                    right={
                      <span className="tag">
                        Metric: <span className="mono">{activityReportTab}</span>
                        {activityReportTab === "email" && Number(activityChart?.bucket_size_days ?? 1) > 1 ? (
                          <>
                            {" "}
                            · bucket <span className="mono">{String(activityChart.bucket_size_days)}d</span>
                          </>
                        ) : null}
                      </span>
                    }
                    below={
                      <div className="card" style={{ background: "rgba(255,255,255,0.02)", border: "1px solid rgba(255,255,255,0.08)" }}>
                        <div className="cardHeader">
                          <div>
                            <div className="cardTitle">Events</div>
                            <div className="cardDesc">
                              Range <span className="mono">{String(dateRange?.since || "—")}</span> →{" "}
                              <span className="mono">{String(dateRange?.until || "—")}</span>
                            </div>
                          </div>
                          <div className="btnRow">
                            <span className="tag">
                              Rows: <span className="mono">{String(activityReportTab === "email" ? (smartleadEvents || []).length : (activityEvents || []).length)}</span>
                            </span>
                          </div>
                        </div>
                        <div className="cardBody">
                          {activityReportTab === "email" ? (
                            <>
                              {smartleadEventsErr ? <div className="notice">SmartLead events error: <span className="mono">{smartleadEventsErr}</span></div> : null}
                              {!smartleadEvents?.length ? (
                                <div className="muted2">No SmartLead events in range. Run sync to ingest today’s email activity.</div>
                              ) : (
                                <div style={{ overflow: "auto" }}>
                                  <table className="table">
                                    <thead>
                                      <tr>
                                        <th>Time</th>
                                        <th>Event</th>
                                        <th>Campaign</th>
                                        <th>Email</th>
                                        <th>HubSpot</th>
                                        <th>IDs</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {(smartleadEvents || []).slice(0, 500).map((r: any) => {
                                        const cid = String(r?.contact_id ?? "").trim();
                                        const eid = String(r?.hubspot_engagement_id ?? "").trim();
                                        const contactHref = hubspotPortalId && cid ? `https://app.hubspot.com/contacts/${hubspotPortalId}/record/0-1/${cid}/` : "";
                                        const engagementHref = hubspotPortalId && eid ? `https://app.hubspot.com/contacts/${hubspotPortalId}/record/0-38/${eid}/` : "";
                                        const eventType = String(r?.event_kind ?? "—");
                                        const campaignId = String(r?.smartlead_campaign_id ?? "—");
                                        const ids = String(r?.source_id ?? "") || "—";
                                        return (
                                          <tr key={String(r.id)}>
                                            <td className="mono">{String(r?.occurred_at ?? "").replace("T", " ").slice(0, 16) || "—"}</td>
                                            <td className="mono">{eventType}</td>
                                            <td className="mono">{campaignId}</td>
                                            <td className="mono">{String(r?.contact_email ?? "—")}</td>
                                            <td>
                                              {contactHref ? (
                                                <a href={contactHref} target="_blank" rel="noreferrer" style={{ color: "inherit", textDecoration: "underline", textUnderlineOffset: 3 }}>
                                                  Contact
                                                </a>
                                              ) : (
                                                <span className="muted2">—</span>
                                              )}
                                              {engagementHref ? (
                                                <>
                                                  {" "}·{" "}
                                                  <a href={engagementHref} target="_blank" rel="noreferrer" style={{ color: "inherit", textDecoration: "underline", textUnderlineOffset: 3 }}>
                                                    Engagement
                                                  </a>
                                                </>
                                              ) : null}
                                            </td>
                                            <td className="mono" style={{ fontSize: 11, opacity: 0.75 }}>
                                              {ids || <span className="muted2">—</span>}
                                            </td>
                                          </tr>
                                        );
                                      })}
                                    </tbody>
                                  </table>
                                  {(smartleadEvents || []).length > 500 ? <div className="muted2" style={{ marginTop: 8, fontSize: 12 }}>Showing first 500 events.</div> : null}
                                </div>
                              )}
                            </>
                          ) : (
                            <>
                              {!activityEvents?.length ? (
                                <div className="muted2">No events in range.</div>
                              ) : (
                                <div style={{ overflow: "auto" }}>
                                  <table className="table">
                                    <thead>
                                      <tr>
                                        <th>Time</th>
                                        <th>Source</th>
                                        <th>Event</th>
                                        <th>Activity</th>
                                        <th>Email</th>
                                        <th>Message hash</th>
                                        <th>HubSpot</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {(activityEvents || [])
                                        .filter((r: any) => String(r?.source_system ?? "").toLowerCase() === "getsales")
                                        .slice(0, 500)
                                        .map((r: any) => {
                                          const cid = String(r?.contact_id ?? "").trim();
                                          const eid = String(r?.hubspot_engagement_id ?? "").trim();
                                          const contactHref = hubspotPortalId && cid ? `https://app.hubspot.com/contacts/${hubspotPortalId}/record/0-1/${cid}/` : "";
                                          const engagementHref = hubspotPortalId && eid ? `https://app.hubspot.com/contacts/${hubspotPortalId}/record/0-38/${eid}/` : "";
                                          const text = String(r?.message_hash ?? "").trim();
                                          return (
                                            <tr key={String(r.id)}>
                                              <td className="mono">{String(r?.occurred_at ?? "").replace("T", " ").slice(0, 16) || "—"}</td>
                                              <td className="mono">{String(r?.source_system ?? "—")}</td>
                                              <td className="mono">{String(r?.event_kind ?? "—")}</td>
                                              <td className="mono">{String(r?.activity_type ?? "—")}</td>
                                              <td className="mono">{String(r?.contact_email ?? "—")}</td>
                                              <td style={{ maxWidth: 420 }}>
                                                <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={text || ""}>
                                                  {text || <span className="muted2">—</span>}
                                                </div>
                                              </td>
                                              <td>
                                                {contactHref ? (
                                                  <a href={contactHref} target="_blank" rel="noreferrer" style={{ color: "inherit", textDecoration: "underline", textUnderlineOffset: 3 }}>
                                                    Contact
                                                  </a>
                                                ) : (
                                                  <span className="muted2">—</span>
                                                )}
                                                {engagementHref ? (
                                                  <>
                                                    {" "}·{" "}
                                                    <a href={engagementHref} target="_blank" rel="noreferrer" style={{ color: "inherit", textDecoration: "underline", textUnderlineOffset: 3 }}>
                                                      Engagement
                                                    </a>
                                                  </>
                                                ) : null}
                                              </td>
                                            </tr>
                                          );
                                        })}
                                    </tbody>
                                  </table>
                                  {(activityEvents || []).length > 500 ? <div className="muted2" style={{ marginTop: 8, fontSize: 12 }}>Showing first 500 events.</div> : null}
                                </div>
                              )}
                            </>
                          )}
                        </div>
                      </div>
                    }
                  />
                </>
              )}
            </div>
          </div>
        ) : (
          <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardHeader">
              <div>
                <div className="cardTitle">Login</div>
                <div className="cardDesc">Sign in with a magic link to manage hypotheses and weekly check-ins.</div>
              </div>
            </div>
            <div className="cardBody">
              {!supabaseUrl || !supabaseAnonKey ? (
                <div className="notice">
                  Missing env vars: <span className="mono">NEXT_PUBLIC_SUPABASE_URL</span> and{" "}
                  <span className="mono">NEXT_PUBLIC_SUPABASE_ANON_KEY</span>
                </div>
              ) : null}

              {sessionEmail ? (
                <>
                  <div className="muted">
                    Signed in as <b>{sessionEmail}</b>
                  </div>
                  <div className="btnRow" style={{ marginTop: 12 }}>
                    <a className="btn btnPrimary" href="/hypotheses">
                      Open hypotheses
                    </a>
                    <a className="btn" href="/dashboard">Dashboard</a>
                    <button className="btn btnGhost" onClick={signOut}>
                      Sign out
                    </button>
                  </div>
                </>
              ) : (
                <>
                  <label className="muted" style={{ display: "block", marginBottom: 8, fontSize: 13 }}>
                    Work email
                  </label>
                  <div className="grid" style={{ gridTemplateColumns: "repeat(12, 1fr)", gap: 10 }}>
                    <div style={{ gridColumn: "span 8" }}>
                      <input
                        className="input"
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        placeholder={`name${allowedDomain}`}
                      />
                      <div className="muted2" style={{ marginTop: 8, fontSize: 12 }}>
                        Allowed domain: <span className="mono">{allowedDomain}</span>
                      </div>
                    </div>
                    <div style={{ gridColumn: "span 4", display: "flex", alignItems: "end" }}>
                      <button className="btn btnPrimary" disabled={!canSend} onClick={sendMagicLink} style={{ width: "100%" }}>
                        Send magic link
                      </button>
                    </div>
                  </div>
                </>
              )}

              {status ? (
                <div className="notice" style={{ marginTop: 12 }}>
                  {status}
                </div>
              ) : null}
            </div>
          </div>
        )}
      </div>
    </main>
  );
}


