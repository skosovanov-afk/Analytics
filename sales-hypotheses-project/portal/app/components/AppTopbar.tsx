"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { usePathname } from "next/navigation";
import Link from "next/link";
import { createClient } from "@supabase/supabase-js";

const NAV = [
  { href: "/", label: "Main dashboard" },
  { href: "/hypotheses", label: "Hypotheses" },
  { href: "/dashboard", label: "Advanced dashboard" },
  { href: "/icp", label: "Library" },
  { href: "/expandi", label: "LinkedIn" },
  { href: "/smartlead", label: "Email" },
  { href: "/app", label: "App" },
  { href: "/telegram", label: "Telegram" },
  { href: "/manual-stats", label: "Manual" },
  { href: "/checkins/new", label: "Submit report" },
];

export function AppTopbar(props: {
  title: string;
  subtitle?: string;
  showSync?: boolean;
  onSync?: () => void;
}) {
  const pathname = usePathname();

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
  const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@oversecured.com";

  const supabase = useMemo(() => {
    if (!supabaseUrl || !supabaseAnonKey) return null;
    return createClient(supabaseUrl, supabaseAnonKey);
  }, [supabaseUrl, supabaseAnonKey]);

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
  }, [pathname]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    window.addEventListener("resize", measurePill);
    return () => window.removeEventListener("resize", measurePill);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ──────────────────────────────────────────────────────────────────────────

  return (
    <div className="topbar">
      <div className="brand">
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

      <div className="btnRow">
        {props.showSync && (
          <button className="btn btnPrimary" onClick={props.onSync}>Sync</button>
        )}

        {/* Pill nav — sliding white indicator follows active link */}
        <div ref={navRef} style={{ position: "relative", display: "flex", gap: 6, flexWrap: "wrap" }}>
          {pillVisible && (
            <span
              aria-hidden
              style={{
                position: "absolute",
                left: pill.left,
                top: pill.top,
                width: pill.width,
                height: pill.height,
                background: "#ffffff",
                transition: pillTransition,
                zIndex: 0,
                pointerEvents: "none",
              }}
            />
          )}

          {NAV.map(({ href, label }) => {
            const isActive = pathname === href;
            // When pill hasn't positioned yet, show active link as btnPrimary immediately
            // so there's never a frame with no visual highlight. Once pill is visible,
            // hand off: button goes transparent, pill provides the white background.
            const useFallback = isActive && !pillVisible;
            return (
              <Link
                key={href}
                href={href}
                scroll={false}
                data-active={isActive ? "true" : undefined}
                className={`btn${useFallback ? " btnPrimary" : ""}`}
                style={{
                  position: "relative",
                  zIndex: 1,
                  transition: "color 100ms ease, background 100ms ease, border-color 100ms ease",
                  ...(isActive && pillVisible
                    ? { color: "#0a0a0a", fontWeight: 500, borderColor: "transparent", background: "transparent" }
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

        <Link
          href="/hypotheses/new"
          scroll={false}
          className="btn btnPrimary"
          style={{ position: "relative", zIndex: 1 }}
        >
          New hypothesis
        </Link>

        {sessionEmail && (
          <button className="btn btnGhost" onClick={signOut}>Sign out</button>
        )}
      </div>
    </div>
  );
}
