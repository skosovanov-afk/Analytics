import { NextResponse } from "next/server";

function jsonError(status: number, message: string) {
    return NextResponse.json({ ok: false, error: message }, { status });
}

async function hubspotFetch(url: string, init?: RequestInit) {
    const token = process.env.HUBSPOT_PRIVATE_APP_TOKEN ?? "";
    if (!token) throw new Error("Missing HUBSPOT_PRIVATE_APP_TOKEN");

    const res = await fetch(url, {
        ...init,
        headers: { Authorization: `Bearer ${token}`, ...(init?.headers ?? {}) }
    });
    if (res.status === 429) {
        throw new Error("HubSpot rate limit (429)");
    }
    return res;
}

async function findGetSalesNotes(since: Date, max: number) {
    const notes: any[] = [];
    let after: string | undefined;

    // Increase limit to scan deeper
    while (notes.length < max) {
        const res = await hubspotFetch("https://api.hubapi.com/crm/v3/objects/notes/search", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                filterGroups: [
                    {
                        filters: [
                            { propertyName: "hs_note_body", operator: "CONTAINS_TOKEN", value: "[GetSales]" },
                            { propertyName: "hs_createdate", operator: "GTE", value: since.getTime() }
                        ]
                    }
                ],
                properties: ["hs_note_body", "hs_createdate"],
                sorts: [{ propertyName: "hs_createdate", direction: "DESCENDING" }],
                limit: 100,
                after
            })
        });

        if (!res.ok) {
            const txt = await res.text();
            throw new Error(`HubSpot search failed: ${res.status} ${txt}`);
        }

        const json = await res.json();
        const results = Array.isArray(json?.results) ? json.results : [];
        if (!results.length) break;
        notes.push(...results);

        if (!json?.paging?.next?.after) break;
        after = json.paging.next.after;
    }
    return notes;
}

async function batchGetNoteAssociations(noteIds: string[]) {
    if (!noteIds.length) return {};

    // Batch read associations: Note -> Contact
    // HubSpot limit for batch read inputs is 100. Need to chunk.
    const map: Record<string, string> = {};

    const chunks: string[][] = [];
    for (let i = 0; i < noteIds.length; i += 100) {
        chunks.push(noteIds.slice(i, i + 100));
    }

    for (const chunk of chunks) {
        const res = await hubspotFetch("https://api.hubapi.com/crm/v3/associations/NOTES/CONTACTS/batch/read", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                inputs: chunk.map(id => ({ id }))
            })
        });

        if (!res.ok) {
            console.warn("HubSpot batch associations failed for chunk", res.status);
            continue;
        }

        const json = await res.json();
        if (Array.isArray(json?.results)) {
            for (const r of json.results) {
                const nid = r.from?.id;
                const contacts = r.to || [];
                if (nid && contacts.length > 0) {
                    map[nid] = String(contacts[0].id);
                }
            }
        }

        // small delay to avoid 429
        await new Promise(r => setTimeout(r, 100));
    }

    return map;
}

function parseGetSalesUuid(body: string) {
    const m = body.match(/GetSales (?:email|linkedin) uuid:\s*([a-zA-Z0-9-]+)/i);
    return m ? m[1].trim() : null;
}

async function updateEventContact(uuid: string, contactId: string) {
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";
    if (!supabaseUrl || !serviceRoleKey) return;

    const payload = { hubspot_contact_id: contactId };

    const headers = {
        "Content-Type": "application/json",
        apikey: serviceRoleKey,
        Authorization: `Bearer ${serviceRoleKey}`,
        Prefer: "return=minimal"
    };

    // Try update EMAIL source
    await fetch(`${supabaseUrl}/rest/v1/sales_getsales_events?getsales_uuid=eq.email:${uuid}`, {
        method: "PATCH",
        headers,
        body: JSON.stringify(payload)
    });

    // Try update LINKEDIN source
    await fetch(`${supabaseUrl}/rest/v1/sales_getsales_events?getsales_uuid=eq.linkedin:${uuid}`, {
        method: "PATCH",
        headers,
        body: JSON.stringify(payload)
    });
}

export async function POST(req: Request) {
    try {
        const { days = 30, max = 5000 } = await req.json().catch(() => ({}));

        const since = new Date();
        since.setDate(since.getDate() - Number(days));

        // 1. Find Notes (increased limit)
        const notes = await findGetSalesNotes(since, Number(max));

        // 2. Get Associations
        const noteIds = notes.map(n => n.id);
        const associations = await batchGetNoteAssociations(noteIds);

        let updated = 0;

        // 3. Parse and Update
        // Process in parallel chunks to speed up? Supabase calls might bottleneck.
        // Let's do batch of 10.

        const BATCH_SIZE = 10;
        for (let i = 0; i < notes.length; i += BATCH_SIZE) {
            const chunk = notes.slice(i, i + BATCH_SIZE);
            await Promise.all(chunk.map(async (n) => {
                const body = String(n.properties?.hs_note_body || "");
                const uuid = parseGetSalesUuid(body);
                const contactId = associations[n.id];

                if (uuid && contactId) {
                    await updateEventContact(uuid, contactId);
                    updated++;
                }
            }));
        }

        return NextResponse.json({ ok: true, scanned: notes.length, updated });
    } catch (e: any) {
        return jsonError(500, String(e.message || e));
    }
}
