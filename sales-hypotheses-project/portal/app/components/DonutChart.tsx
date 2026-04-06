"use client";

import { useState } from "react";

export type DonutSlice = {
  label: string;
  value: number;
  color: string;
};

function num(v: number) {
  return v.toLocaleString();
}

export function DonutChart({
  slices,
  title,
  size = 200,
  thickness = 36,
}: {
  slices: DonutSlice[];
  title?: string;
  size?: number;
  thickness?: number;
}) {
  const [hovered, setHovered] = useState<number | null>(null);

  const total = slices.reduce((s, d) => s + d.value, 0);
  if (!total) {
    return (
      <div style={{ textAlign: "center", padding: 24 }}>
        <div className="muted2" style={{ fontSize: 13 }}>No data</div>
      </div>
    );
  }

  const cx = size / 2;
  const cy = size / 2;
  const r = (size - thickness) / 2;
  const circumference = 2 * Math.PI * r;

  // Build arcs
  let offset = 0;
  const arcs = slices.filter(s => s.value > 0).map((s, i) => {
    const pct = s.value / total;
    const dash = pct * circumference;
    const gap = circumference - dash;
    const rotation = (offset / total) * 360 - 90;
    offset += s.value;
    return { ...s, pct, dash, gap, rotation, index: i };
  });

  const hoveredSlice = hovered !== null ? arcs[hovered] : null;

  return (
    <div style={{ display: "flex", alignItems: "center", gap: 32, flexWrap: "wrap", justifyContent: "center" }}>
      {/* Donut */}
      <div style={{ position: "relative", width: size, height: size, flexShrink: 0 }}>
        <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size}>
          {/* Background circle */}
          <circle cx={cx} cy={cy} r={r} fill="none" stroke="rgba(255,255,255,0.04)" strokeWidth={thickness} />
          {/* Slices */}
          {arcs.map((a, i) => (
            <circle
              key={i}
              cx={cx} cy={cy} r={r}
              fill="none"
              stroke={a.color}
              strokeWidth={hovered === i ? thickness + 6 : thickness}
              strokeDasharray={`${a.dash} ${a.gap}`}
              strokeDashoffset={0}
              transform={`rotate(${a.rotation} ${cx} ${cy})`}
              style={{
                transition: "stroke-width 150ms, opacity 150ms",
                opacity: hovered !== null && hovered !== i ? 0.35 : 1,
                cursor: "pointer",
              }}
              onMouseEnter={() => setHovered(i)}
              onMouseLeave={() => setHovered(null)}
            />
          ))}
        </svg>
        {/* Center text */}
        <div style={{
          position: "absolute", inset: 0,
          display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
          pointerEvents: "none",
        }}>
          <div style={{ fontSize: 22, fontWeight: 700, lineHeight: 1.1 }}>
            {hoveredSlice ? num(hoveredSlice.value) : num(total)}
          </div>
          <div className="muted2" style={{ fontSize: 11, marginTop: 2 }}>
            {hoveredSlice ? `${(hoveredSlice.pct * 100).toFixed(1)}%` : (title || "total")}
          </div>
        </div>
      </div>

      {/* Legend */}
      <div style={{ display: "flex", flexDirection: "column", gap: 6, minWidth: 160 }}>
        {arcs.map((a, i) => (
          <div
            key={i}
            style={{
              display: "flex", alignItems: "center", gap: 8, fontSize: 12,
              cursor: "pointer",
              opacity: hovered !== null && hovered !== i ? 0.4 : 1,
              transition: "opacity 150ms",
            }}
            onMouseEnter={() => setHovered(i)}
            onMouseLeave={() => setHovered(null)}
          >
            <span style={{
              width: 10, height: 10, borderRadius: 3,
              background: a.color, flexShrink: 0,
            }} />
            <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {a.label}
            </span>
            <span className="muted2" style={{ fontFamily: "var(--mono)", fontSize: 11 }}>
              {num(a.value)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
