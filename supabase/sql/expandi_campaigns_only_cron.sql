-- Expandi campaigns-only autonomous sync (no messengers/messages/raw-heavy backfill)
--
-- Goal:
-- - pull only accounts + campaign_instances (+ stats snapshot)
-- - keep data fresh automatically, similar to smartlead-sync pattern
--
-- How to use:
-- 1) Replace <PROJECT_REF> and <SYNC_SECRET>.
-- 2) Run this script once in Supabase SQL Editor.
-- 3) Verify checks below.

create extension if not exists pg_cron;
create extension if not exists pg_net;

-- Remove old campaign-only job if re-running this script.
do $$
declare
  r record;
begin
  for r in
    select jobid
    from cron.job
    where jobname in ('expandi-campaigns-sync-main')
  loop
    perform cron.unschedule(r.jobid);
  end loop;
end $$;

-- One lightweight job every 5 minutes.
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
      'max_pages_campaigns', 5
    )
  );
  $$
);

-- Checks:
-- select jobid, jobname, schedule, active from cron.job where jobname like 'expandi-campaigns-%';
--
-- One-time immediate backfill run (all accounts at once):
-- select net.http_post(
--   url := 'https://<PROJECT_REF>.supabase.co/functions/v1/expandi-sync',
--   headers := jsonb_build_object(
--     'Content-Type', 'application/json',
--     'x-sync-secret', '<SYNC_SECRET>'
--   ),
--   body := jsonb_build_object(
--     'mode', 'campaigns_tick',
--     'store_raw', false,
--     'accounts_per_run', 20,
--     'max_pages_accounts', 5,
--     'max_pages_campaigns', 30
--   )
-- );
--
-- select id, status_code, timed_out, left(content::text, 220) as content
-- from net._http_response
-- order by id desc
-- limit 20;
--
-- select
--   (select count(*) from public.expandi_accounts) as accounts_loaded,
--   (select count(*) from public.expandi_campaign_instances) as campaigns_loaded,
--   (select count(*) from public.expandi_campaign_stats_snapshots) as snapshots_loaded,
--   now() as checked_at;
