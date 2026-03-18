export type IcpJson = {
  role?: {
    persona?: string;
    decision_role?: "DecisionMaker" | "Influencer" | "User" | "";
    seniority?: string;
    titles?: string[];
    notes?: string;
  };
  company?: {
    region?: string;
    size_bucket?: string;
    employees?: number | null;
    revenue?: string | null;
    tech_stack?: string[];
    notes?: string;
  };
  constraints?: {
    compliance?: string[];
    notes?: string;
  };
};

export type CjmJson = {
  channels?: string[];
  stages?: string[];
  notes?: string;
};

export type VpJson = {
  value_proposition?: string; // single statement: "Why should I care?"
};

export const CHANNEL_OPTIONS = [
  "OutboundEmail",
  "LinkedIn",
  "Inbound",
  "Ads",
  "Partners",
  "Events",
  "Content",
  "ColdCalls",
  "Communities"
] as const;

export function normalizeStringArray(xs: any): string[] {
  const out: string[] = [];
  for (const x of Array.isArray(xs) ? xs : []) {
    const t = String(x ?? "").trim();
    if (!t) continue;
    if (!out.includes(t)) out.push(t);
  }
  return out;
}

export function normalizeAssets(xs: any): Array<{ name?: string; url?: string; notes?: string }> {
  const out: Array<{ name?: string; url?: string; notes?: string }> = [];
  for (const x of Array.isArray(xs) ? xs : []) {
    const name = String(x?.name ?? "").trim();
    const url = String(x?.url ?? "").trim();
    const notes = String(x?.notes ?? "").trim();
    if (!name && !url && !notes) continue;
    out.push({ name: name || undefined, url: url || undefined, notes: notes || undefined });
  }
  return out;
}

export function parseIcp(v: any): IcpJson {
  const x = (v && typeof v === "object") ? v : {};
  return {
    role: {
      persona: String(x?.role?.persona ?? "").trim() || "",
      decision_role: (["DecisionMaker", "Influencer", "User"].includes(String(x?.role?.decision_role)) ? x.role.decision_role : "") as any,
      seniority: String(x?.role?.seniority ?? "").trim() || "",
      titles: normalizeStringArray(x?.role?.titles),
      notes: String(x?.role?.notes ?? "").trim() || ""
    },
    company: {
      region: String(x?.company?.region ?? "").trim() || "",
      size_bucket: String(x?.company?.size_bucket ?? "").trim() || "",
      employees: Number.isFinite(Number(x?.company?.employees)) ? Number(x.company.employees) : null,
      revenue: String(x?.company?.revenue ?? "").trim() || "",
      tech_stack: normalizeStringArray(x?.company?.tech_stack),
      notes: String(x?.company?.notes ?? "").trim() || ""
    },
    constraints: {
      compliance: normalizeStringArray(x?.constraints?.compliance),
      notes: String(x?.constraints?.notes ?? "").trim() || ""
    }
  };
}

export function parseCjm(v: any): CjmJson {
  const x = (v && typeof v === "object") ? v : {};
  return {
    channels: normalizeStringArray(x?.channels),
    stages: normalizeStringArray(x?.stages),
    notes: String(x?.notes ?? "").trim() || ""
  };
}

export function parseVp(v: any): VpJson {
  const x = (v && typeof v === "object") ? v : {};
  // backward-compat: accept older shapes but only expose the single statement
  const value = String(x?.value_proposition ?? x?.statement ?? "").trim();
  return { value_proposition: value };
}


