-- Expandi auto-update via pg_cron + pg_net
-- Goal: autonomous sync without manual runs from Cursor.
--
-- How to use:
-- 1) Replace <PROJECT_REF> and <SYNC_SECRET>
-- 2) Run this whole script in Supabase SQL Editor once
-- 3) Verify with the checks at the bottom
--
-- Why this config:
-- - Uses ONE cron job (prevents DB overload from parallel jobs).
-- - Runs every 3 minutes (fast enough for near-real-time, but safer on compute).
-- - Processes bounded chunks each run.

create extension if not exists pg_cron;
create extension if not exists pg_net;

-- 1) Stop old/duplicated Expandi jobs (if any).
do $$
declare
  r record;
begin
  for r in
    select jobid, jobname
    from cron.job
    where jobname like 'expandi-backfill-fast-%'
       or jobname like 'expandi-backfill-%'
       or jobname = 'expandi-auto-sync-main'
  loop
    perform cron.unschedule(r.jobid);
  end loop;
end $$;

-- 2) Create a single autonomous sync job.
select cron.schedule(
  'expandi-auto-sync-main',
  '*/3 * * * *',
  $$
  select net.http_post(
    url := 'https://<PROJECT_REF>.supabase.co/functions/v1/expandi-sync',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'x-sync-secret', '<SYNC_SECRET>'
    ),
    body := jsonb_build_object(
      'mode', 'backfill_tick',
      'accounts_per_run', 1,
      'max_messengers_per_run', 60,
      'max_pages_accounts', 1,
      'max_pages_campaigns', 2,
      'max_pages_messengers', 20,
      'max_pages_messages', 2
    )
  );
  $$
);

-- 3) Checks.
-- Active cron jobs:
-- select jobid, jobname, schedule, active from cron.job order by jobid;
--
-- Recent HTTP results for pg_net:
-- select id, status_code, timed_out, left(content::text, 200) as content
-- from net._http_response
-- order by id desc
-- limit 20;
--
-- Current Expandi counts:
-- select
--   (select count(*) from public.expandi_accounts) as accounts_loaded,
--   (select count(*) from public.expandi_campaign_instances) as campaigns_loaded,
--   (select count(*) from public.expandi_messengers) as messengers_loaded,
--   (select count(*) from public.expandi_messages) as messages_loaded,
--   now() as checked_at;
