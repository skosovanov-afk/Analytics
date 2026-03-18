import dotenv from 'dotenv';
dotenv.config({ path: '.env' });

async function main() {
    const token = process.env.GETSALES_API_TOKEN ?? process.env.GETSALES_BEARER_TOKEN;
    const baseUrl = (process.env.GETSALES_BASE_URL ?? "https://amazing.getsales.io").replace(/\/+$/, "");

    if (!token) {
        console.error("Missing GETSALES_API_TOKEN");
        process.exit(1);
    }

    // Example lead UUID - replace with a known one or find one from messages
    const leadUuid = process.argv[2];

    if (!leadUuid) {
        console.log("Fetching recent LinkedIn messages to find a lead UUID...");
        const res = await fetch(`${baseUrl}/flows/api/linkedin-messages?limit=5`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        if (!res.ok) {
            console.error("Failed to list messages:", await res.text());
            process.exit(1);
        }
        const json = await res.json();
        const messages = json.data || json;
        console.log(`Found ${messages.length} messages.`);
        if (messages.length > 0) {
            const msg = messages[0];
            console.log("Using lead_uuid from first message:", msg.lead_uuid);
            await testGetLead(baseUrl, token, msg.lead_uuid);
        } else {
            console.log("No messages found to extract lead UUID.");
        }
    } else {
        await testGetLead(baseUrl, token, leadUuid);
    }
}

async function testGetLead(baseUrl, token, uuid) {
    console.log(`\n--- Testing getLead(${uuid}) ---`);
    const url = `${baseUrl}/leads/api/leads/${encodeURIComponent(uuid)}`;
    console.log("URL:", url);

    const res = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` }
    });

    console.log("Status:", res.status);
    const text = await res.text();
    console.log("Raw Response Body:", text.slice(0, 1000) + (text.length > 1000 ? "..." : ""));

    try {
        const json = JSON.parse(text);
        const data = json.data ?? json; // Imitate logic from route.ts
        const lead = data?.lead ?? data;
        console.log("\nParsed Lead Data (normalized):");
        console.log(JSON.stringify(lead, null, 2));

        // Check finding email
        const email = pickContactEmailFromLead(lead);
        console.log("\nExtracted Email:", email || "(none)");

    } catch (e) {
        console.error("JSON parse error:", e.message);
    }
}

function pickContactEmailFromLead(lead) {
    const w = String(lead?.work_email ?? lead?.workEmail ?? lead?.email ?? "").trim().toLowerCase();
    const p = String(lead?.personal_email ?? lead?.personalEmail ?? "").trim().toLowerCase();
    return w || p || "";
}

main();
