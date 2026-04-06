"use client";

import { useEffect, useMemo, useState } from "react";
import { getSupabase } from "../lib/supabase";

export function AuthGate({ children }: { children: React.ReactNode }) {
  const supabase = useMemo(() => getSupabase(), []);
  const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "@inxy.io";

  const [checking, setChecking] = useState(true);
  const [email, setEmail] = useState<string | null>(null);
  const [inputEmail, setInputEmail] = useState("");
  const [status, setStatus] = useState("");
  const [sent, setSent] = useState(false);

  useEffect(() => {
    if (!supabase) { setChecking(false); return; }
    supabase.auth.getSession().then(({ data: { session } }) => {
      setEmail(session?.user?.email ?? null);
      setChecking(false);
    });
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
      setEmail(session?.user?.email ?? null);
    });
    return () => subscription.unsubscribe();
  }, [supabase]);

  if (checking) {
    return (
      <div style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100vh" }}>
        <div className="muted2" style={{ fontSize: 14 }}>Loading...</div>
      </div>
    );
  }

  if (email) {
    return <>{children}</>;
  }

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault();
    if (!supabase) return;
    const trimmed = inputEmail.trim().toLowerCase();
    if (!trimmed) { setStatus("Enter your email"); return; }
    if (allowedDomain && !trimmed.endsWith(allowedDomain)) {
      setStatus(`Only ${allowedDomain} emails are allowed`);
      return;
    }
    setStatus("Sending magic link...");
    const { error } = await supabase.auth.signInWithOtp({
      email: trimmed,
      options: { emailRedirectTo: window.location.origin + "/dashboard" },
    });
    if (error) {
      setStatus(error.message);
    } else {
      setSent(true);
      setStatus("Check your email for the magic link");
    }
  }

  return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100vh", padding: 24 }}>
      <div className="card" style={{ width: "100%", maxWidth: 420 }}>
        <div className="cardHeader">
          <div>
            <div className="cardTitle" style={{ fontFamily: "var(--sans)", fontWeight: 700 }}>Sign in</div>
            <div className="cardDesc">Use your {allowedDomain} email to access the portal</div>
          </div>
        </div>
        <div className="cardBody">
          {sent ? (
            <div>
              <div style={{ fontSize: 14, marginBottom: 12 }}>{status}</div>
              <button className="btn" onClick={() => { setSent(false); setStatus(""); }}>
                Try again
              </button>
            </div>
          ) : (
            <form onSubmit={handleLogin}>
              <input
                type="email"
                className="input"
                placeholder={`you${allowedDomain}`}
                value={inputEmail}
                onChange={(e) => setInputEmail(e.target.value)}
                autoFocus
                style={{ width: "100%", padding: "10px 14px", fontSize: 14, marginBottom: 12 }}
              />
              <button type="submit" className="btn btnPrimary" style={{ width: "100%", padding: "10px 14px", fontSize: 14 }}>
                Send magic link
              </button>
              {status && (
                <div className="notice" style={{ marginTop: 12 }}>{status}</div>
              )}
            </form>
          )}
        </div>
      </div>
    </div>
  );
}
