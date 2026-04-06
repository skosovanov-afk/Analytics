-- =============================================================================
-- Fix generated key columns in manual_stats
-- The previous migration used an incorrect empty-string literal and produced
-- "'" instead of "" for null account_name / campaign_name keys.
-- =============================================================================

drop index if exists manual_stats_fact_key_uidx;
drop index if exists manual_stats_channel_record_date_idx;

alter table public.manual_stats
  drop column if exists account_name_key,
  drop column if exists campaign_name_key;

alter table public.manual_stats
  add column account_name_key text
    generated always as (coalesce(btrim(account_name), ''::text)) stored;

alter table public.manual_stats
  add column campaign_name_key text
    generated always as (coalesce(btrim(campaign_name), ''::text)) stored;

create unique index if not exists manual_stats_fact_key_uidx
  on public.manual_stats (
    record_date,
    channel,
    account_name_key,
    campaign_name_key,
    metric_name
  );

create index if not exists manual_stats_channel_record_date_idx
  on public.manual_stats (channel, record_date desc);
