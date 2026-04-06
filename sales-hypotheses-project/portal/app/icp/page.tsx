"use client";

import Link from "next/link";
import { AppTopbar } from "../components/AppTopbar";

const sections = [
  {
    label: "ICP Foundation",
    desc: "Базовые строительные блоки — роли, профили компаний, каналы.",
    items: [
      {
        title: "Roles",
        desc: "Персоны, тайтлы и decision roles которые таргетируешь.",
        href: "/icp/roles",
        cta: "Manage roles",
      },
      {
        title: "Company profiles",
        desc: "Вертикали, суб-вертикали, размер, регион, тех-стек.",
        href: "/icp/companies",
        cta: "Manage profiles",
      },
      {
        title: "Channels",
        desc: "Каналы из которых собираются гипотезы.",
        href: "/icp/channels",
        cta: "Manage channels",
      },
    ],
  },
  {
    label: "Strategy",
    desc: "Что и кому говоришь — мессаджинг в разрезе ролей и профилей.",
    items: [
      {
        title: "VP Library",
        desc: "VP Point × Role × Company profile. Видишь где и как тестируется каждый angle.",
        href: "/icp/matrix",
        cta: "Open VP library",
        wide: true,
      },
    ],
  },
  {
    label: "Execution",
    desc: "Операционная единица — списки компаний с привязанными кампаниями и аналитикой.",
    items: [
      {
        title: "TAL",
        desc: "Territory Account Lists — группируешь кампании Smartlead и Expandi по сегментам. Аналитика агрегируется по Email + LinkedIn в одном месте.",
        href: "/tals",
        cta: "Open TAL",
        wide: true,
      },
    ],
  },
];

export default function IcpIndexPage() {
  return (
    <main>
      <AppTopbar
        title="Library"
        subtitle="ICP foundation, messaging strategy, and execution tracking."
      />

      <div className="icpSections">
        {sections.map((section) => (
          <div key={section.label}>
            {/* Section header */}
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: "#666", marginBottom: 4 }}>
                {section.label}
              </div>
              <div style={{ fontSize: 14, color: "#888" }}>{section.desc}</div>
            </div>

            {/* Cards */}
            <div className={section.items.length === 3 ? "icpSectionGrid icpSectionGridThree" : "icpSectionGrid"}>
              {section.items.map((item) => (
                <Link
                  key={item.href}
                  href={item.href}
                  style={{ textDecoration: "none", color: "inherit" }}
                >
                  <div
                    className="card"
                    style={{
                      display: "flex",
                      flexDirection: "column",
                      justifyContent: "space-between",
                      gap: 20,
                      padding: 24,
                      cursor: "pointer",
                      transition: "border-color 150ms ease",
                      height: "100%",
                      boxSizing: "border-box",
                    }}
                  >
                    <div>
                      <div style={{ fontSize: 16, fontWeight: 600, marginBottom: 6 }}>{item.title}</div>
                      <div style={{ fontSize: 13, color: "#888", lineHeight: 1.5 }}>{item.desc}</div>
                    </div>
                    <div>
                      <span className="btn btnPrimary" style={{ fontSize: 13, padding: "6px 14px" }}>
                        {item.cta}
                      </span>
                    </div>
                  </div>
                </Link>
              ))}
            </div>
          </div>
        ))}
      </div>
    </main>
  );
}
