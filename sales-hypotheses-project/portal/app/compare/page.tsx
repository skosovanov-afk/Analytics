"use client";

import { AppTopbar } from "../components/AppTopbar";

export default function ComparePage() {
  // Deprecated: kept for old bookmarks. Use /dashboard.
  if (typeof window !== "undefined") {
    window.location.href = "/dashboard";
  }
  return (
    <main>
      <AppTopbar title="Redirecting…" subtitle="Opening dashboard." />
      <div className="page" style={{ marginTop: 12 }}>
        <div className="btnRow" style={{ justifyContent: "flex-end" }}>
          <a className="btn btnPrimary" href="/dashboard">Open dashboard</a>
        </div>
      </div>
      <div className="page grid">
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardBody">
            <div className="muted2">If you were not redirected automatically, click “Open dashboard”.</div>
          </div>
        </div>
      </div>
    </main>
  );
}

