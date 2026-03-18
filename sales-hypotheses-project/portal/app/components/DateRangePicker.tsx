// @ts-nocheck
"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

export function DateRangePicker({
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
    if (!hasSince || hasUntil) {
      setDraft({ since: day, until: "" });
      return;
    }
    const a = draft.since;
    const b = day;
    if (b < a) setDraft({ since: b, until: a });
    else setDraft({ since: a, until: b });
  };

  const preset = (kind: "7d" | "30d" | "90d" | "ytd" | "1y" | "all") => {
    const today = fmtYmd(new Date());
    const max = availableMax || effectiveUntil || today;
    const min = availableMin || effectiveSince || addDays(today, -365);
    setQuickQuarter("");
    setQuickYear("");
    if (kind === "all") {
      onChange({ since: "", until: "" });
      setOpen(false);
      return;
    }
    let since: string, until: string, hint: "day" | "week" | "month" | "quarter" | "year";
    if (kind === "ytd") {
      since = `${max.slice(0, 4)}-01-01`; until = max; hint = "week";
    } else {
      const days = kind === "7d" ? 7 : kind === "30d" ? 30 : kind === "90d" ? 90 : 365;
      since = clampYmd(addDays(max, -(days - 1)));
      until = max;
      hint = days <= 90 ? "day" : "week";
    }
    // Apply immediately — no need to click Apply for preset shortcuts
    onChange({ since, until }, { period: hint });
    setOpen(false);
  };

  const quarters = useMemo(() => {
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
      const untilEx = fmtYmd(new Date(Date.UTC(yy, endM - 1, 1)));
      const until = addDays(untilEx, -1);
      out.push({ key: `${yy}-Q${qq}`, since, until });
      qq -= 1;
      if (qq <= 0) { qq = 4; yy -= 1; }
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
                                {["M", "T", "W", "T", "F", "S", "S"].map((x, xi) => (
                                  <div key={xi} className="calDowCell">{x}</div>
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
