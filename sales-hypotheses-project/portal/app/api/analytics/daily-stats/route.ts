import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

// Init independent client (or use createRouteHandlerClient if we need auth context, 
// but here we just need data access and likely rely on the API token or service role if internal).
// For simplicity and consistency with other routes, let's use the service role if possible 
// to ensure we can read all analytics, OR use the user's session if RLS is set up.
// Given previous patterns, we check for a token.

export async function POST(req: Request) {
    try {
        const json = await req.json();
        const { hypothesisId, days = 30, tal_list_id } = json ?? {};

        const talListId = String(tal_list_id ?? "").trim();

        if (!hypothesisId && !talListId) {
            return NextResponse.json({ ok: false, error: "Missing hypothesisId or tal_list_id" }, { status: 400 });
        }

        const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
        const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY ?? "").trim();
        if (!supabaseUrl || !serviceRoleKey) {
            return NextResponse.json({ ok: false, error: "Missing SUPABASE_SERVICE_ROLE_KEY" }, { status: 500 });
        }
        const supabase = createClient(supabaseUrl, serviceRoleKey);

        // Calc "since" and "until" bounds.
        const sinceDate = new Date();
        sinceDate.setDate(sinceDate.getDate() - Math.max(1, Math.min(365, Number(days) || 30)));
        const sinceIso = sinceDate.toISOString();
        const untilIso = new Date().toISOString();

        // Use the RPC for complete daily series when TAL list id is available.
        if (talListId) {
            const { data, error } = await supabase.rpc("sales_hypothesis_activity_stats_daily", {
                p_tal_list_id: talListId,
                p_since: sinceIso,
                p_until: untilIso
            });
            if (error) throw error;
            return NextResponse.json({ ok: true, stats: Array.isArray(data) ? data : [] });
        }

        // Fallback: aggregate by hypothesis_id directly (best-effort).
        const { data, error } = await supabase
            .from("sales_analytics_activities")
            .select("occurred_at, activity_type, direction")
            .eq("hypothesis_id", hypothesisId)
            .gte("occurred_at", sinceIso)
            .lte("occurred_at", untilIso);

        if (error) throw error;
        if (!data) return NextResponse.json({ ok: true, stats: [] });

        // Aggregate in memory (used only when RPC path is unavailable).
        const statsMap = new Map<string, { emails: number; linkedin: number; replies: number }>();
        for (const row of data) {
            const day = row.occurred_at.slice(0, 10); // ISO string YYYY-MM-DD
            if (!statsMap.has(day)) statsMap.set(day, { emails: 0, linkedin: 0, replies: 0 });
            const entry = statsMap.get(day)!;

            // Categorize by channel and direction.
            if (row.direction === "inbound") {
                entry.replies++;
            } else if (row.activity_type === "email") {
                entry.emails++;
            } else if (row.activity_type === "linkedin" || row.activity_type === "linkedin_connection") {
                entry.linkedin++;
            }
        }

        const result = Array.from(statsMap.entries()).map(([day, counts]) => ({
            day,
            emails_sent_count: counts.emails,
            linkedin_sent_count: counts.linkedin,
            replies_count: counts.replies
        }));
        result.sort((a, b) => b.day.localeCompare(a.day));

        return NextResponse.json({ ok: true, stats: result });
    } catch (e: any) {
        console.error("Error in daily-stats:", e);
        return NextResponse.json({ ok: false, error: String(e.message || e) }, { status: 500 });
    }
}
