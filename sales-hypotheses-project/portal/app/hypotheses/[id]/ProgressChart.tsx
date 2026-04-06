"use client";

import { useEffect, useRef, useState } from "react";

type Snapshot = {
    period_start: string;
    companies_in_tal_count?: number | null;
    deals_in_tal_count?: number | null;
    new_deals_count?: number | null;
};

export function ProgressChart({ snapshots }: { snapshots: Snapshot[] }) {
    const containerRef = useRef<HTMLDivElement>(null);
    const canvasRef = useRef<HTMLCanvasElement>(null);
    const [canvasWidth, setCanvasWidth] = useState(900);

    useEffect(() => {
        const container = containerRef.current;
        if (!container) return;
        const observer = new ResizeObserver((entries) => {
            for (const entry of entries) {
                const w = Math.round(entry.contentRect.width);
                if (w > 0) setCanvasWidth(w);
            }
        });
        observer.observe(container);
        // Set initial width
        const w = container.clientWidth;
        if (w > 0) setCanvasWidth(w);
        return () => observer.disconnect();
    }, []);

    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas || snapshots.length < 2) return;

        const ctx = canvas.getContext("2d");
        if (!ctx) return;

        // Sort and limit to last 12 weeks
        const sorted = [...snapshots]
            .sort((a, b) => new Date(a.period_start || 0).getTime() - new Date(b.period_start || 0).getTime())
            .slice(-12);

        if (sorted.length < 2) return;

        const width = canvas.width;
        const height = canvas.height;
        const padding = { top: 30, right: 50, bottom: 50, left: 60 };
        const chartWidth = width - padding.left - padding.right;
        const chartHeight = height - padding.top - padding.bottom;

        // Clear
        ctx.clearRect(0, 0, width, height);

        // Data
        const companiesData = sorted.map((s) => Number(s.companies_in_tal_count ?? 0));
        const dealsData = sorted.map((s) => Number(s.deals_in_tal_count ?? 0));

        const maxCompanies = Math.max(...companiesData, 1);
        const maxDeals = Math.max(...dealsData, 1);

        // Helpers
        const xPos = (i: number) => padding.left + (i / (sorted.length - 1)) * chartWidth;
        const yPosCompanies = (val: number) => padding.top + chartHeight - (val / maxCompanies) * chartHeight;
        const yPosDeals = (val: number) => padding.top + chartHeight - (val / maxDeals) * chartHeight;

        // Grid
        ctx.strokeStyle = "rgba(255,255,255,0.05)";
        ctx.lineWidth = 1;
        for (let i = 0; i <= 4; i++) {
            const y = padding.top + (i / 4) * chartHeight;
            ctx.beginPath();
            ctx.moveTo(padding.left, y);
            ctx.lineTo(width - padding.right, y);
            ctx.stroke();
        }

        // X-axis labels
        ctx.fillStyle = "rgba(255,255,255,0.4)";
        ctx.font = "11px monospace";
        ctx.textAlign = "center";
        sorted.forEach((s, i) => {
            const date = new Date(s.period_start);
            const label = `${date.getMonth() + 1}/${date.getDate()}`;
            ctx.fillText(label, xPos(i), height - padding.bottom + 20);
        });

        // Y-axis labels (left = companies, right = deals)
        ctx.textAlign = "right";
        ctx.font = "10px monospace";
        ctx.fillStyle = "rgba(100,200,255,0.6)";
        for (let i = 0; i <= 4; i++) {
            const val = Math.round((maxCompanies / 4) * (4 - i));
            const y = padding.top + (i / 4) * chartHeight;
            ctx.fillText(String(val), padding.left - 10, y + 4);
        }

        ctx.textAlign = "left";
        ctx.fillStyle = "rgba(255,180,100,0.6)";
        for (let i = 0; i <= 4; i++) {
            const val = Math.round((maxDeals / 4) * (4 - i));
            const y = padding.top + (i / 4) * chartHeight;
            ctx.fillText(String(val), width - padding.right + 10, y + 4);
        }

        // Legend
        ctx.font = "12px sans-serif";
        ctx.textAlign = "left";
        ctx.fillStyle = "rgba(100,200,255,0.9)";
        ctx.fillText("● Companies (TAL)", padding.left + 10, 20);
        ctx.fillStyle = "rgba(255,180,100,0.9)";
        ctx.fillText("● Deals (TAL)", padding.left + 180, 20);

        // Lines
        ctx.strokeStyle = "rgba(100,200,255,0.8)";
        ctx.lineWidth = 2;
        ctx.beginPath();
        companiesData.forEach((val, i) => {
            const x = xPos(i);
            const y = yPosCompanies(val);
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.stroke();

        ctx.strokeStyle = "rgba(255,180,100,0.8)";
        ctx.lineWidth = 2;
        ctx.beginPath();
        dealsData.forEach((val, i) => {
            const x = xPos(i);
            const y = yPosDeals(val);
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.stroke();

        // Points
        companiesData.forEach((val, i) => {
            ctx.fillStyle = "rgba(100,200,255,0.9)";
            ctx.beginPath();
            ctx.arc(xPos(i), yPosCompanies(val), 4, 0, Math.PI * 2);
            ctx.fill();
        });
        dealsData.forEach((val, i) => {
            ctx.fillStyle = "rgba(255,180,100,0.9)";
            ctx.beginPath();
            ctx.arc(xPos(i), yPosDeals(val), 4, 0, Math.PI * 2);
            ctx.fill();
        });
    }, [snapshots, canvasWidth]);

    return (
        <div ref={containerRef} style={{ width: "100%" }}>
            <canvas
                ref={canvasRef}
                width={canvasWidth}
                height={320}
                style={{ width: "100%", height: "auto", maxHeight: 320 }}
            />
        </div>
    );
}
