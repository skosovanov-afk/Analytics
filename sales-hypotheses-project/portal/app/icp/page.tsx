"use client";

import { AppTopbar } from "../components/AppTopbar";

export default function IcpIndexPage() {
  return (
    <main>
      <AppTopbar
        title="Sales Library"
        subtitle="Shared building blocks: roles, company profiles, VP matrix, channels."
      />

      <div className="page grid">
        <div className="card" style={{ gridColumn: "span 6" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Roles</div>
              <div className="cardDesc">Personas / titles / decision role.</div>
            </div>
          </div>
          <div className="cardBody">
            <a className="btn btnPrimary" href="/icp/roles">Manage roles</a>
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 6" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Company profiles</div>
              <div className="cardDesc">Vertical + sub-vertical + size/region/tech.</div>
            </div>
          </div>
          <div className="cardBody">
            <a className="btn btnPrimary" href="/icp/companies">Manage company profiles</a>
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">VP Matrix</div>
              <div className="cardDesc">VP is defined per intersection: Role × CompanyProfile (incl. sub-vertical).</div>
            </div>
          </div>
          <div className="cardBody">
            <a className="btn btnPrimary" href="/icp/matrix">Open VP matrix</a>
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Channels</div>
              <div className="cardDesc">Hypotheses select channels from this list. Weekly check-ins prompt per selected channel.</div>
            </div>
          </div>
          <div className="cardBody">
            <a className="btn btnPrimary" href="/icp/channels">Manage channels</a>
          </div>
        </div>

        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Metrics</div>
              <div className="cardDesc">Hypotheses select metrics from this list. Weekly check-ins collect values for selected metrics.</div>
            </div>
          </div>
          <div className="cardBody">
            <a className="btn btnPrimary" href="/icp/metrics">Manage metrics</a>
          </div>
        </div>
      </div>
    </main>
  );
}


