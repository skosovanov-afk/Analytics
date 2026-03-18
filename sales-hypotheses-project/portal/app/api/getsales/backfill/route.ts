import { NextResponse } from "next/server";
import { POST as syncPost } from "../sync/route";

function jsonError(status: number, message: string) {
  return NextResponse.json({ ok: false, error: message }, { status });
}

function isoMonthsAgo(months: number) {
  const m = Math.max(1, Math.min(24, Number(months) || 6));
  const d = new Date();
  d.setMonth(d.getMonth() - m);
  return d.toISOString();
}

// Backfill is a thin wrapper around /api/getsales/sync that:
// - defaults since=now-6 months
// - avoids mutating sales_getsales_sync_state (skip_state_update)
// - returns cursor info so the caller can loop
export async function POST(req: Request) {
  const body = (await req.json().catch(() => ({}))) as {
    months?: number;
    since?: string;
    max?: number;
    dry_run?: boolean;
  };

  const since = String(body?.since || "").trim() || isoMonthsAgo(Number(body?.months ?? 6));
  const payload = {
    since,
    max: body?.max ?? 200,
    dry_run: !!body?.dry_run,
    skip_state_update: true,
    include_cursor: true
  };

  // Re-create Request for sync handler (body can be read only once).
  const headers = new Headers(req.headers);
  headers.set("Content-Type", "application/json");

  const url = req.url.replace(/\/api\/getsales\/backfill\b/, "/api/getsales/sync");
  if (!url.includes("/api/getsales/sync")) return jsonError(500, "Bad backfill route url");

  return await syncPost(
    new Request(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload)
    })
  );
}


