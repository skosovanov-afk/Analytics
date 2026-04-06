-- Expandi autopilot + hygiene
-- Goal:
-- 1) keep campaign/message stats updated automatically (no Cursor runs)
-- 2) enforce data scope:
--    - only campaign_instance_id present in current catalog (expandi_campaign_instances)
--    - no analytics rows before 2025-01-01
--
-- Usage:
-- 1) Replace <PROJECT_REF> and <SYNC_SECRET>.
-- 2) Run this script in Supabase SQL Editor once.
-- 3) Verify checks at the bottom.

begin;

create extension if not exists pg_cron;
create extension if not exists pg_net;

create or replace function public.expandi_enforce_catalog_cutoff(
  p_cutoff date default date '2025-01-01'
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_deleted_messages_invalid int := 0;
  v_deleted_messages_old int := 0;
  v_deleted_messengers_invalid int := 0;
  v_deleted_messengers_old int := 0;
  v_deleted_stats int := 0;
  v_deleted_snapshots int := 0;
begin
  -- 1) Remove messages outside campaign catalog.
  delete from public.expandi_messages m
  where m.campaign_instance_id is null
     or not exists (
       select 1
       from public.expandi_campaign_instances c
       where c.id = m.campaign_instance_id
     );
  get diagnostics v_deleted_messages_invalid = row_count;

  -- 2) Remove old messages.
  delete from public.expandi_messages m
  where coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source)::date < p_cutoff;
  get diagnostics v_deleted_messages_old = row_count;

  -- 3) Remove messengers outside campaign catalog.
  delete from public.expandi_messengers em
  where em.campaign_instance_id is null
     or not exists (
       select 1
       from public.expandi_campaign_instances c
       where c.id = em.campaign_instance_id
     );
  get diagnostics v_deleted_messengers_invalid = row_count;

  -- 4) Remove old messengers with no remaining messages.
  delete from public.expandi_messengers em
  where coalesce(
          em.replied_at,
          em.first_inbound_at,
          em.first_outbound_at,
          em.connected_at,
          em.invited_at,
          em.last_datetime,
          em.synced_at
        )::date < p_cutoff
    and not exists (
      select 1
      from public.expandi_messages m
      where m.messenger_id = em.id
    );
  get diagnostics v_deleted_messengers_old = row_count;

  -- 5) Remove old/invalid daily stats and snapshots.
  delete from public.expandi_stats_daily d
  where d.campaign_instance_id is null
     or not exists (
       select 1
       from public.expandi_campaign_instances c
       where c.id = d.campaign_instance_id
     )
     or d.date < p_cutoff;
  get diagnostics v_deleted_stats = row_count;

  delete from public.expandi_campaign_stats_snapshots s
  where not exists (
          select 1
          from public.expandi_campaign_instances c
          where c.id = s.campaign_instance_id
        )
     or s.snapshot_date < p_cutoff;
  get diagnostics v_deleted_snapshots = row_count;

  return jsonb_build_object(
    'ok', true,
    'cutoff', p_cutoff,
    'deleted', jsonb_build_object(
      'messages_invalid', v_deleted_messages_invalid,
      'messages_old', v_deleted_messages_old,
      'messengers_invalid', v_deleted_messengers_invalid,
      'messengers_old', v_deleted_messengers_old,
      'stats_daily', v_deleted_stats,
      'campaign_stats_snapshots', v_deleted_snapshots
    )
  );
end;
$$;

commit;

-- Remove old duplicated jobs if script is re-run.
do $$
declare
  r record;
begin
  for r in
    select jobid
    from cron.job
    where jobname in (
      'expandi-campaigns-sync-main',
      'expandi-messages-sync-main',
      'expandi-hygiene-daily'
    )
  loop
    perform cron.unschedule(r.jobid);
  end loop;
end $$;

-- Campaigns metadata freshness (lightweight).
select cron.schedule(
  'expandi-campaigns-sync-main',
  '*/5 * * * *',
  $$
  select net.http_post(
    url := 'https://<PROJECT_REF>.supabase.co/functions/v1/expandi-sync',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'x-sync-secret', '<SYNC_SECRET>'
    ),
    body := jsonb_build_object(
      'mode', 'campaigns_tick',
      'store_raw', false,
      'accounts_per_run', 1,
      'max_pages_accounts', 1,
      'max_pages_campaigns', 10
    )
  );
  $$
);

-- Messages/messengers incremental ingestion for conversion metrics.
select cron.schedule(
  'expandi-messages-sync-main',
  '*/10 * * * *',
  $$
  select net.http_post(
    url := 'https://<PROJECT_REF>.supabase.co/functions/v1/expandi-sync',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'x-sync-secret', '<SYNC_SECRET>'
    ),
    body := jsonb_build_object(
      'mode', 'backfill_tick',
      'store_raw', false,
      'accounts_per_run', 1,
      'max_messengers_per_run', 120,
      'max_pages_accounts', 1,
      'max_pages_campaigns', 3,
      'max_pages_messengers', 20,
      'max_pages_messages', 4
    )
  );
  $$
);

-- Daily data hygiene guardrail.
select cron.schedule(
  'expandi-hygiene-daily',
  '30 2 * * *',
  $$
  select public.expandi_enforce_catalog_cutoff('2025-01-01'::date);
  $$
);

-- Checks:
-- select jobid, jobname, schedule, active from cron.job where jobname like 'expandi-%' order by jobname;
-- select public.expandi_enforce_catalog_cutoff('2025-01-01'::date);
-- select
--   (select min(day) from public.expandi_campaign_daily_v) as min_day,
--   (select max(day) from public.expandi_campaign_daily_v) as max_day,
--   (select count(*) from public.expandi_campaign_instances) as campaigns,
--   (select count(*) from public.expandi_messengers) as messengers,
--   (select count(*) from public.expandi_messages) as messages;
