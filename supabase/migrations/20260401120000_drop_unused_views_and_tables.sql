-- Drop unused views and tables
-- All CREATE statements preserved in supabase/sql/ and earlier migrations for rollback

-- === Views: drop dependents first, then parents ===

-- expandi_campaign_performance_v depends on expandi_conversation_facts
drop view if exists public.expandi_campaign_performance_v;
-- expandi_conversation_facts depends on expandi_kpi_daily_v (which we keep)
drop view if exists public.expandi_conversation_facts;

-- expandi KPI aggregation views (depend on expandi_kpi_daily_v which we keep)
drop view if exists public.expandi_kpi_alltime_v;
drop view if exists public.expandi_kpi_monthly_v;
drop view if exists public.expandi_kpi_weekly_v;

-- NOTE: expandi_campaign_daily_mv stays - expandi_campaign_daily_v depends on it,
-- which chains to linkedin_kpi_daily_v2 → tal_analytics_v (all used in app)

-- smartlead KPI views (nobody reads them)
drop view if exists public.smartlead_kpi_daily_v;
drop view if exists public.smartlead_kpi_weekly_v;
drop view if exists public.smartlead_kpi_monthly_v;
drop view if exists public.smartlead_kpi_touch_v;

-- NOTE: linkedin_live_snapshot_v2 stays - linkedin_kpi_alltime_v2 depends on it
-- NOTE: linkedin_legacy_manual_activity_v2 stays - depends on linkedin_live_snapshot_v2

-- linkedin diagnostic view
drop view if exists public.linkedin_campaign_mapping_conflicts_v;

-- smartlead reply categorization views
drop view if exists public.smartlead_reply_events_categorized_v;
drop view if exists public.smartlead_reply_category_daily_v;

-- === Tables ===

-- NOTE: linkedin_campaign_mapping stays - linkedin_legacy_manual_activity_v2 depends on it

-- expandi_stats_daily: legacy, replaced by expandi_kpi_daily_v
drop table if exists public.expandi_stats_daily;

-- backup tables from hardening migrations
drop table if exists public.manual_stats_backup_20260327_phase1;
drop table if exists public.smartlead_events_backup_20260327_phase1;
