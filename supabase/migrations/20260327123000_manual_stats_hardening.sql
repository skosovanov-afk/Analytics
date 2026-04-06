-- =============================================================================
-- Phase 1: manual_stats hardening
-- - backup current rows
-- - normalize logical key with generated columns
-- - deduplicate existing facts
-- - add unique/indexes for safe upsert
-- =============================================================================

do $$
begin
  if to_regclass('public.manual_stats_backup_20260327_phase1') is null then
    execute '
      create table public.manual_stats_backup_20260327_phase1 as
      select *
      from public.manual_stats
    ';
  end if;
end
$$;

alter table public.manual_stats
  add column if not exists account_name_key text
    generated always as (coalesce(btrim(account_name), '''')) stored;

alter table public.manual_stats
  add column if not exists campaign_name_key text
    generated always as (coalesce(btrim(campaign_name), '''')) stored;

with ranked as (
  select
    id,
    row_number() over (
      partition by record_date, channel, metric_name, account_name_key, campaign_name_key
      order by created_at desc nulls last, id desc
    ) as rn
  from public.manual_stats
)
delete from public.manual_stats ms
using ranked r
where ms.id = r.id
  and r.rn > 1;

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
