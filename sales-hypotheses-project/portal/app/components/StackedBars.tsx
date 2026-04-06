"use client";

import { useLayoutEffect, useMemo, useRef, useState } from "react";

export function StackedBars({
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
    const h = 290;
    const padL = 52;
    const padR = 20;
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
    }, [hover?.i, hover?.x, hover?.y, hoverBreakdown.length, hoverTotal]);

    return (
        <div className="card" style={{ gridColumn: "span 12" }}>
            <div className="cardHeader">
                <div>
                    <div className="cardTitle">{title}</div>
                    <div className="cardDesc">
                        {weeks.length ? (weeks.length === 1 ? weeks[0] : `${weeks[0]} → ${weeks[weeks.length - 1]}`) : "No data"}
                    </div>
                </div>
                {right ? <div className="btnRow">{right}</div> : null}
            </div>
            <div className="cardBody">
                {!weeks.length ? (
                    <div className="muted2">No data in selected range.</div>
                ) : (
                    <>
                        <div ref={chartRef} style={{ position: "relative" }}>
                            <svg viewBox={`0 0 ${w} ${h}`} width="100%" height={h} style={{ display: "block" }} onMouseLeave={() => setHover(null)}>
                                {yTicks.map((t, i) => {
                                    const y = yFor(t);
                                    return (
                                        <g key={i}>
                                            <line x1={padL} y1={y} x2={w - padR} y2={y} stroke="var(--chartGrid)" strokeWidth={1} />
                                            <text x={padL - 8} y={y + 4} textAnchor="end" fontSize="11" fill="var(--chartAxis)">
                                                {Math.round(t)}
                                            </text>
                                        </g>
                                    );
                                })}

                                {/* x labels: specific logic to avoid crowding */}
                                {weeks.map((wk, i) => {
                                    const step = Math.max(1, Math.floor(weeks.length / 6));
                                    if (i % step !== 0 && i !== weeks.length - 1) return null;
                                    const x = xFor(i) + barW / 2;
                                    return (
                                        <text key={wk + i} x={x} y={h - 8} textAnchor="middle" fontSize="11" fill="var(--chartAxis)">
                                            {String(wk).length >= 10 ? String(wk).slice(5) : String(wk)}
                                        </text>
                                    );
                                })}

                                {/* bars */}
                                {weeks.map((wk, i) => {
                                    let acc = 0;
                                    const x = xFor(i);
                                    return (
                                        <g key={wk + i}>
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
                                    className="chartTooltip"
                                    style={{
                                        left: tipPos.left,
                                        top: tipPos.top,
                                    }}
                                    ref={tipRef}
                                >
                                    <div className="mono" style={{ fontSize: 12, opacity: 0.85 }}>{hoverLabel}</div>
                                    <div style={{ marginTop: 4, fontSize: 18, fontWeight: 700 }} className="mono">{hoverTotal}</div>
                                    <div className="chartTooltipRows">
                                        {hoverBreakdown.map((x) => (
                                            <div key={x.key} className="chartTooltipRow">
                                                <span style={{ width: 10, height: 10, borderRadius: 2, background: x.color, display: "inline-block" }} />
                                                <span className="chartTooltipLabel" style={{ opacity: 0.9 }}>
                                                    {x.label}
                                                </span>
                                                <span className="mono chartTooltipValue">{x.v}</span>
                                            </div>
                                        ))}
                                        {!hoverBreakdown.length ? <div className="muted2" style={{ fontSize: 12 }}>No deals</div> : null}
                                    </div>
                                </div>
                            ) : null}
                        </div>
                        <div className="chartLegend">
                            {series.slice(0, 12).map((s) => (
                                <span key={s.key} className="chartLegendItem">
                                    <span className="chartLegendSwatch" style={{ width: 10, height: 10, background: s.color }} />
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
