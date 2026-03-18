# Activity Graph "No Data" Issue - Diagnosis & Solutions

## Problem Summary

The Activity Graph in the Hypothesis Details page shows "No Data" despite high activity totals (Emails: 5987, etc.) being displayed correctly on the right side.

## Root Cause Analysis

### 1. **Primary Issue: Missing Historical Data** ⭐ **MOST LIKELY**

**Problem**: The `activities_json` field in [`sales_hubspot_tal_snapshots`](99-applications/sales/supabase/schema-hypotheses.sql:258) was recently added. Existing historical snapshots have this field as `NULL`.

**Evidence**:
- The graph logic in [`page.tsx:228`](99-applications/sales/portal/app/hypotheses/[id]/page.tsx:228) filters for `activities_json` data
- Line 228: `const emails = sorted.map(s => Number(s.activities_json?.emails_sent ?? 0) || 0);`
- If `activities_json` is `NULL`, all values become `0`, resulting in "No data"

**Impact**: Historical snapshots created before the field was added show as empty in the graph.

### 2. **Secondary Issue: Data Source Mismatch**

**Problem**: The totals and graph use different data sources:
- **Totals** (right side): Come from `talContactedStats` via [`/api/hubspot/tal/contacted-stats`](99-applications/sales/portal/app/api/hubspot/tal/contacted-stats/route.ts)
- **Graph**: Uses `hubspotHistory` from [`sales_hubspot_tal_snapshots`](99-applications/sales/supabase/schema-hypotheses.sql:240) table

**Evidence**: 
- Totals show current aggregate data (5987 emails)
- Graph shows historical weekly snapshots, which may be missing or incomplete

### 3. **Potential Issue: Weekly vs Daily Precision**

**Problem**: The current implementation uses weekly snapshots, but the user wants daily precision like the main dashboard.

**Evidence**:
- [`sales_hubspot_tal_snapshots`](99-applications/sales/supabase/schema-hypotheses.sql:240) stores weekly data (`window_days: 7`)
- Main dashboard might use daily snapshots or real-time aggregation

## Solutions

### 🔧 **Solution 1: Backfill Missing Activities Data** (IMMEDIATE FIX)

**Action**: Run the backfill script to populate `activities_json` for existing snapshots.

```bash
# Test first
node backfill_activities_json.mjs --dry-run

# Apply changes
node backfill_activities_json.mjs

# Target specific hypothesis
node backfill_activities_json.mjs --hypothesis-id=your-uuid-here
```

**How it works**:
1. Finds snapshots with `activities_json = NULL`
2. For each snapshot, calls [`sales_hypothesis_activity_stats`](99-applications/sales/supabase/schema-hypothesis-activities.sql:8) RPC
3. Updates the snapshot with computed activity data

**Expected Result**: Historical data appears in the Activity Graph immediately.

### 🔧 **Solution 2: Fix Daily vs Weekly Precision** (ENHANCEMENT)

**Option A**: Create daily snapshots table
```sql
CREATE TABLE sales_hubspot_tal_daily_snapshots (
  -- Similar structure to weekly snapshots but with daily granularity
  period_day date not null,
  activities_json jsonb not null default '{}'::jsonb,
  -- ... other fields
);
```

**Option B**: Use real-time aggregation for daily view
- Modify the graph to call [`sales_hypothesis_activity_stats`](99-applications/sales/supabase/schema-hypothesis-activities.sql:8) for each day
- Cache results client-side or use a lighter caching strategy

**Recommendation**: Start with Option A for consistency with the existing architecture.

### 🔧 **Solution 3: Validate Coverage Calculation** (VERIFICATION)

**Issue**: User mentioned low "Coverage" (Companies/Contacts).

**Action**: Verify the [`sales_hypothesis_contacted_stats`](99-applications/sales/supabase/schema-hypothesis-contacted-stats.sql) RPC correctly identifies "contacted" entities.

**Check**:
1. Ensure HubSpot engagement timestamps are properly captured
2. Verify the logic for determining "contacted" vs "total" entities
3. Test with known data to confirm accuracy

## Implementation Priority

1. **🚨 HIGH**: Run backfill script (immediate fix for "No Data")
2. **📊 MEDIUM**: Implement daily precision if required
3. **🔍 LOW**: Validate coverage calculation accuracy

## Technical Details

### Data Flow
```
HubSpot Activities → SmartLead/GetSales Events → sales_hypothesis_activity_stats() → activities_json → Activity Graph
```

### Key Files
- **Frontend**: [`page.tsx:215-240`](99-applications/sales/portal/app/hypotheses/[id]/page.tsx:215) (activityChartData memo)
- **API**: [`cache-sync/route.ts:1019-1034`](99-applications/sales/portal/app/api/hubspot/tal/cache-sync/route.ts:1019) (activities_json creation)
- **RPC**: [`schema-hypothesis-activities.sql:8`](99-applications/sales/supabase/schema-hypothesis-activities.sql:8) (activity aggregation)
- **Schema**: [`schema-hypotheses.sql:258`](99-applications/sales/supabase/schema-hypotheses.sql:258) (activities_json field)

### Validation Commands

```bash
# Check for snapshots with missing activities_json
SELECT hypothesis_id, period_start, tal_list_id, 
       CASE WHEN activities_json IS NULL THEN 'MISSING' ELSE 'OK' END as status
FROM sales_hubspot_tal_snapshots 
ORDER BY period_start DESC;

# Test activity stats RPC
SELECT * FROM sales_hypothesis_activity_stats('your-tal-list-id', '2025-01-01'::timestamptz);

# Verify current totals
SELECT * FROM sales_hypothesis_contacted_stats('your-tal-list-id', 90);
```

## Expected Outcome

After running the backfill script:
1. ✅ Activity Graph shows historical weekly data
2. ✅ Graph data matches the pattern of actual activity
3. ✅ "No Data" message disappears
4. ✅ User can see activity trends over time

The graph should display weekly bars showing email sends, LinkedIn messages, and replies, providing the visual trend analysis that was intended in the original design.