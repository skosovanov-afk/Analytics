/**
 * Shared SmartLead API utilities.
 * Import from here instead of duplicating in each route.
 */

export async function smartleadFetch(path: string, init?: RequestInit) {
  const apiKey = String(process.env.SMARTLEAD_API_KEY ?? "").trim();
  if (!apiKey) throw new Error("Missing SMARTLEAD_API_KEY");
  const baseUrl = String(process.env.SMARTLEAD_BASE_URL ?? "https://server.smartlead.ai")
    .trim()
    .replace(/\/+$/g, "");
  const url = `${baseUrl}${path}${path.includes("?") ? "&" : "?"}api_key=${encodeURIComponent(apiKey)}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      accept: "application/json",
      ...(init?.body != null ? { "content-type": "application/json" } : {}),
      ...(init?.headers ?? {})
    }
  });
  const text = await res.text();
  let json: any = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }
  if (!res.ok) throw new Error(String(json?.message || json?.error || text || "SmartLead API error"));
  return json;
}

export function pickArrayFromSmartleadListResponse(json: any): any[] {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json?.data)) return json.data;
  if (Array.isArray(json?.results)) return json.results;
  if (Array.isArray(json?.campaigns)) return json.campaigns;
  return [];
}

export async function smartleadListCampaignsBestEffort(opts?: { limit?: number; offset?: number }) {
  const limit = Math.max(1, Math.min(200, Number(opts?.limit ?? 100)));
  const offset = Math.max(0, Number(opts?.offset ?? 0));
  const qs = new URLSearchParams();
  qs.set("limit", String(limit));
  qs.set("offset", String(offset));
  const q = qs.toString();
  const attempts = [`/api/v1/campaigns?${q}`, `/api/v1/campaigns/list?${q}`, `/api/v1/campaigns`];
  let lastErr: any = null;
  for (const path of attempts) {
    try {
      const json = await smartleadFetch(path, { method: "GET" });
      return pickArrayFromSmartleadListResponse(json);
    } catch (e: any) {
      lastErr = e;
    }
  }
  throw lastErr ?? new Error("Failed to list campaigns");
}

export function parseCsvEnv(name: string): string[] {
  const raw = String(process.env[name] ?? "").trim();
  if (!raw) return [];
  return raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}
