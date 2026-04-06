"use client";

import { useEffect } from "react";
import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import { AppTopbar } from "../../components/AppTopbar";

export default function HypothesisLegacyDetailPage() {
  const params = useParams();
  const id = String(params?.id ?? "").trim();
  const router = useRouter();

  useEffect(() => {
    const timer = window.setTimeout(() => {
      router.replace(`/hypotheses?focus=${encodeURIComponent(id)}`);
    }, 250);
    return () => window.clearTimeout(timer);
  }, [id, router]);

  return (
    <main>
      <AppTopbar title="Hypothesis" subtitle="Route moved to the registry editor." />
      <div className="page grid">
        <div className="card" style={{ gridColumn: "span 12" }}>
          <div className="cardBody">
            <div style={{ fontWeight: 600, marginBottom: 8 }}>This screen moved to `Hypotheses`.</div>
            <div className="muted2" style={{ marginBottom: 14 }}>
              The legacy detail page used an outdated schema and has been replaced with the inline registry editor.
            </div>
            <div className="btnRow">
              <Link className="btn btnPrimary" href={`/hypotheses?focus=${encodeURIComponent(id)}`}>Open hypotheses registry</Link>
            </div>
          </div>
        </div>
      </div>
    </main>
  );
}
