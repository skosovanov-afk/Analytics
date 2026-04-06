-- =============================================================================
-- Expandi cleanup: remove unused tables and views
-- Safe to run — all objects below are replaced by expandi_kpi_v2 views.
--
-- Replaced by:
--   expandi_conversation_facts    → expandi_kpi_daily_v
--   expandi_campaign_performance_v → expandi_kpi_alltime_v
--   expandi_stats_daily           → expandi_kpi_daily_v / weekly / monthly
--   expandi_raw                   → store_raw: false in cron, not used
-- =============================================================================

-- 1. Old analytics views (replaced by expandi_kpi_*)
drop view if exists public.expandi_campaign_performance_v;
drop view if exists public.expandi_conversation_facts;

-- 2. Old stats table (empty, replaced by views)
drop table if exists public.expandi_stats_daily;

-- 3. Raw API payloads table (cron runs with store_raw: false, data stale)
drop table if exists public.expandi_raw;
