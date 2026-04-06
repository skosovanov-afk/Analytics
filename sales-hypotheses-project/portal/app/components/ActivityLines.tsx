"use client";

import { useLayoutEffect, useMemo, useRef, useState } from "react";

export function ActivityLines({
    title,
    weeks,
    series,
    right,
    below,
    onPointClick
}: {
    title: string;
    weeks: string[];
    series: Array<{ key: string; label: string; color: string; values: number[] }>;
    right?: any;
    below?: any;
    onPointClick?: (label: string) => void;
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

    // Calculate max value across all series
    const allValues = series.flatMap(s => s.values);
    const max = allValues.length ? Math.max(...allValues, 5) : 5; // min max 5

    const xFor = (i: number) => {
        const inner = w - padL - padR;
        const step = inner / Math.max(1, weeks.length - 1);
        return padL + i * step;
    };
    const yFor = (v: number) => padT + ((max - v) * (h - padT - padB)) / max;

    const ticks = 4;
    const yTicks = Array.from({ length: ticks + 1 }).map((_, i) => (max * i) / ticks);

    const hoverLabel = hover ? String(weeks[hover.i] ?? "") : "";
    const hoverBreakdown = hover
        ? series
            .map((s) => ({ key: s.key, label: s.label, color: s.color, v: Number(s.values[hover.i] ?? 0) || 0 }))
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
    }, [hover?.i, hover?.x, hover?.y]);

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
                                {/* Y-axis grid */}
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

                                {/* X-axis labels */}
                                {weeks.map((wk, i) => {
                                    const step = Math.max(1, Math.floor(weeks.length / 6));
                                    if (i % step !== 0 && i !== weeks.length - 1) return null;
                                    const x = xFor(i);
                                    return (
                                        <text key={wk + i} x={x} y={h - 8} textAnchor="middle" fontSize="11" fill="var(--chartAxis)">
                                            {String(wk).length >= 10 ? String(wk).slice(5) : String(wk)}
                                        </text>
                                    );
                                })}

                                {/* Lines */}
                                {series.map((s) => {
                                    const points = weeks.map((_, i) => {
                                        const v = Number(s.values[i] ?? 0) || 0;
                                        return `${xFor(i)},${yFor(v)}`;
                                    }).join(" ");
                                    return (
                                        <polyline
                                            key={s.key}
                                            points={points}
                                            fill="none"
                                            stroke={s.color}
                                            strokeWidth={2}
                                            strokeLinecap="round"
                                            strokeLinejoin="round"
                                            opacity={0.9}
                                        />
                                    );
                                })}

                                {/* Dots (only visible on hover or if few points) */}
                                {series.map((s) => (
                                    <g key={`dots:${s.key}`}>
                                        {weeks.map((_, i) => {
                                            const v = Number(s.values[i] ?? 0) || 0;
                                            const x = xFor(i);
                                            const y = yFor(v);
                                            const isHovered = hover?.i === i;
                                            return (
                                                <circle
                                                    key={i}
                                                    cx={x}
                                                    cy={y}
                                                    r={isHovered ? 4 : 2}
                                                    fill={s.color}
                                                    opacity={isHovered ? 1 : 0.6}
                                                />
                                            );
                                        })}
                                    </g>
                                ))}

                                {/* Hover overlay columns */}
                                {weeks.map((_, i) => {
                                    const x = xFor(i);
                                    // width based on spacing
                                    const nextX = i < weeks.length - 1 ? xFor(i + 1) : w - padR;
                                    const prevX = i > 0 ? xFor(i - 1) : padL;
                                    const colW = weeks.length > 1 ? (xFor(1) - xFor(0)) : (w - padL - padR);
                                    
                                    return (
                                        <rect
                                            key={`hover:${i}`}
                                            x={x - colW / 2}
                                            y={padT}
                                            width={colW}
                                            height={h - padT - padB}
                                            fill="transparent"
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
                                                const xCenter = x;
                                                const yTop = padT + 10;
                                                setHover({ i, x: xCenter, y: yTop });
                                            }}
                                            onClick={() => {
                                                if (!onPointClick) return;
                                                onPointClick(String(weeks[i]));
                                            }}
                                            style={{ cursor: onPointClick ? "pointer" : "default" }}
                                        />
                                    );
                                })}
                            </svg>

                            {hover ? (
                                <div
                                    className="chartTooltip"
                                    style={{
                                        left: tipPos.left,
                                        top: tipPos.top,
                                        zIndex: 10
                                    }}
                                    ref={tipRef}
                                >
                                    <div className="mono" style={{ fontSize: 12, opacity: 0.85 }}>{hoverLabel}</div>
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
