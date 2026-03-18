export function isoDate(d: string | Date | null | undefined) {
  if (!d) return "";
  const x = typeof d === "string" ? new Date(d) : d;
  if (Number.isNaN(x.getTime())) return "";
  return x.toISOString().slice(0, 10);
}

export function startOfWeekISO(date = new Date()) {
  const d = new Date(date);
  const day = d.getDay(); // 0..6, 0=Sun
  const diff = (day === 0 ? -6 : 1) - day; // Monday start
  d.setDate(d.getDate() + diff);
  d.setHours(0, 0, 0, 0);
  return isoDate(d);
}

export function safeJsonStringify(v: any) {
  try {
    return JSON.stringify(v ?? {}, null, 2);
  } catch {
    return "{}";
  }
}

export function safeJsonParse(text: string) {
  const t = String(text ?? "").trim();
  if (!t) return {};
  try {
    return JSON.parse(t);
  } catch {
    return {};
  }
}


