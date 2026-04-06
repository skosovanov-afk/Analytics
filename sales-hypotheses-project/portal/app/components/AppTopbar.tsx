"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { usePathname } from "next/navigation";
import Link from "next/link";
import { getSupabase } from "../lib/supabase";

const NAV = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/hypotheses", label: "Hypotheses" },
  { href: "/icp", label: "Library" },
  { href: "/expandi", label: "LinkedIn" },
  { href: "/smartlead", label: "Email" },
  { href: "/app", label: "App" },
  { href: "/telegram", label: "Telegram" },
  { href: "/replies", label: "Replies" },
  { href: "/manual-stats", label: "Manual" },
];

function isNavActive(pathname: string, href: string) {
  if (href === "/dashboard") return pathname === "/" || pathname === "/dashboard";
  if (href === "/dashboard/advanced") return pathname === "/dashboard/advanced";
  if (href === "/icp") return pathname === "/icp" || pathname.startsWith("/icp/") || pathname === "/tals" || pathname.startsWith("/tals/");
  return pathname === href || pathname.startsWith(`${href}/`);
}

export function AppTopbar(props: {
  title: string;
  subtitle?: string;
  showSync?: boolean;
  onSync?: () => void;
}) {
  const pathname = usePathname();

  const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "";

  const supabase = useMemo(() => getSupabase(), []);

  const [sessionEmail, setSessionEmail] = useState<string | null>(null);

  useEffect(() => {
    if (!supabase) return;
    supabase.auth.getSession().then(({ data }) =>
      setSessionEmail(data.session?.user?.email ?? null)
    );
    const { data: sub } = supabase.auth.onAuthStateChange((_event, s) =>
      setSessionEmail(s?.user?.email ?? null)
    );
    return () => sub.subscription.unsubscribe();
  }, [supabase]);

  async function signOut() {
    if (!supabase) return;
    await supabase.auth.signOut();
  }

  // ─── Sliding pill ──────────────────────────────────────────────────────────

  const navRef = useRef<HTMLDivElement>(null);
  const initialized = useRef(false);
  const [pill, setPill] = useState({ left: 0, top: 0, width: 0, height: 0 });
  const [pillVisible, setPillVisible] = useState(false);
  const [pillTransition, setPillTransition] = useState("none");

  function measurePill() {
    const container = navRef.current;
    if (!container) return;
    const active = container.querySelector<HTMLElement>('[data-active="true"]');
    if (!active) { setPillVisible(false); return; }

    const cRect = container.getBoundingClientRect();
    const aRect = active.getBoundingClientRect();

    setPill({
      left: aRect.left - cRect.left,
      top: aRect.top - cRect.top,
      width: aRect.width,
      height: aRect.height,
    });

    // No transition on first paint — just appear
    if (!initialized.current) {
      setPillTransition("none");
      initialized.current = true;
    } else {
      setPillTransition(
        "left 220ms cubic-bezier(0.4,0,0.2,1), top 220ms cubic-bezier(0.4,0,0.2,1), width 220ms cubic-bezier(0.4,0,0.2,1)"
      );
    }

    setPillVisible(true);
  }

  useEffect(() => {
    const id = setTimeout(measurePill, 16); // wait one frame for DOM layout
    return () => clearTimeout(id);
  }, [pathname, sessionEmail, props.showSync]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    window.addEventListener("resize", measurePill);
    return () => window.removeEventListener("resize", measurePill);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ──────────────────────────────────────────────────────────────────────────

  return (
    <div className="topbar" style={{ display: "flex", alignItems: "center", gap: 24 }}>
      <div className="brand" style={{ flexShrink: 0 }}>
        <div className="brandMark" />
        <div>
          <div className="brandTitle">{props.title}</div>
          <div className="muted2" style={{ fontSize: 13 }}>
            {props.subtitle
              ? props.subtitle
              : sessionEmail
              ? `Signed in as ${sessionEmail}`
              : `domain: ${allowedDomain}`}
          </div>
        </div>
      </div>

      <div ref={navRef} style={{ position: "relative", display: "flex", gap: 6, flexWrap: "wrap", flex: 1 }}>
        {pillVisible && (
          <span
            aria-hidden
            style={{
              position: "absolute",
              left: pill.left,
              top: pill.top,
              width: pill.width,
              height: pill.height,
              background: "var(--pill-bg, #ffffff)",
              transition: pillTransition,
              zIndex: 0,
              pointerEvents: "none",
            }}
          />
        )}

        {NAV.map(({ href, label }) => {
          const isActive = isNavActive(pathname, href);
          const useFallback = isActive && !pillVisible;
          return (
            <Link
              key={href}
              href={href}
              data-active={isActive ? "true" : undefined}
              className={`btn${useFallback ? " btnPrimary" : ""}`}
              style={{
                position: "relative",
                zIndex: 1,
                transition: "color 100ms ease, background 100ms ease, border-color 100ms ease",
                ...(isActive && pillVisible
                  ? { color: "var(--pill-text, #111827)", fontWeight: 600, borderColor: "transparent", background: "transparent" }
                  : !isActive
                  ? { borderColor: "transparent", background: "transparent" }
                  : {}),
              }}
            >
              {label}
            </Link>
          );
        })}
      </div>

      <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
        {props.showSync && (
          <button className="btn btnPrimary" onClick={props.onSync}>Sync</button>
        )}
        {sessionEmail && (
          <button className="btn btnGhost" onClick={signOut}>Sign out</button>
        )}
      </div>
    </div>
  );
}
