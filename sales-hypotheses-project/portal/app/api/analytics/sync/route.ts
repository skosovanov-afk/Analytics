import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";

// RPC Name to check
const RPC_NAME = "sales_analytics_sync";

type SupabaseUserResponse = { email?: string | null };

function jsonError(status: number, message: string) {
    return NextResponse.json({ ok: false, error: message }, { status });
}

async function getSupabaseUserFromAuthHeader(authHeader: string | null) {
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";
    if (!supabaseUrl || !supabaseAnonKey) return null;
    if (!authHeader?.startsWith("Bearer ")) return null;

    const res = await fetch(`${supabaseUrl}/auth/v1/user`, {
        method: "GET",
        headers: { apikey: supabaseAnonKey, Authorization: authHeader }
    });
    if (!res.ok) return null;
    return (await res.json()) as SupabaseUserResponse;
}

async function runSql(query: string) {
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
    const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";

    // Use the raw /v1/query endpoint if available or just raw postgrest if we can?
    // Actually, simple way via REST client if using pg-postgres or just use supabase-js text?
    // The supabase-js client doesn't expose raw SQL execution unless via RPC.

    // Fallback: we cannot easily run DDL via supabase-js client if RPC doesn't exist.
    // But we can try to use the REST API 'POST /rest/v1/rpc/...' assuming we have a privileged user?
    // No, standard PostgREST doesn't allow raw SQL.

    // However, since we are stuck without MCP, we'll try to rely on the fact that maybe the migration actually applied
    // despite the EOF error (sometimes happens).
    // Or we just return error telling user to run it manually.

    console.log("Cannot run raw SQL from Next.js without a specific driver/RPC.");
    return false;
}

export async function POST(req: Request) {
    try {
        const authHeader = req.headers.get("authorization");
        const user = await getSupabaseUserFromAuthHeader(authHeader);
        if (!user?.email) return jsonError(401, "Not authorized");
        const allowedDomain = process.env.NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN ?? "";
        const email = String(user.email || "").toLowerCase();
        if (allowedDomain && !email.endsWith(String(allowedDomain).toLowerCase())) return jsonError(403, "Forbidden");

        const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
        const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";

        if (!serviceRoleKey) return jsonError(500, "Missing SUPABASE_SERVICE_ROLE_KEY");

        // 1. Try to call the RPC
        const rpcUrl = `${supabaseUrl}/rest/v1/rpc/${RPC_NAME}`;
        let res = await fetch(rpcUrl, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                apikey: serviceRoleKey,
                Authorization: `Bearer ${serviceRoleKey}`
            },
            body: "{}"
        });

        // 2. If 404, it means RPC doesn't exist.
        if (res.status === 404) {
            // We cannot auto-heal DDL from here easily without a direct PG connection.
            // We will return a specific error asking user to run the SQL manually.
            return jsonError(500, "Setup required: The `sales_analytics_sync` RPC is missing. Please run `supabase/schema-analytics.sql` in your Supabase SQL Editor manually.");
        }

        if (!res.ok) {
            const txt = await res.text();
            return jsonError(res.status, `RPC failed: ${txt}`);
        }

        const json = await res.json();
        return NextResponse.json({ ok: true, stats: json });

    } catch (e: any) {
        return jsonError(500, String(e.message || e));
    }
}

/**
 * Cron-friendly GET handler for Vercel cron jobs
 *
 * @param {Request} req - Incoming request
 * @returns {Promise<NextResponse>} Proxy response from POST handler
 */
export async function GET(req: Request) {
    // Reuse POST logic to keep auth and error handling consistent.
    return POST(req);
}
