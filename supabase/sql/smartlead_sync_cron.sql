-- SmartLead -> Supabase autonomous sync (Edge Function + pg_cron)
-- 1) Replace placeholders: <PROJECT_REF>, <SYNC_SECRET>
-- 2) Run in Supabase SQL Editor.

create extension if not exists pg_net with schema extensions;
create extension if not exists pg_cron with schema extensions;

-- Optional: remove old job if exists
select cron.unschedule('smartlead-sync-every-15m')
where exists (
  select 1 from cron.job where jobname = 'smartlead-sync-every-15m'
);

-- Schedule every 15 minutes
select cron.schedule(
  'smartlead-sync-every-15m',
  '*/15 * * * *',
  $$
  select
    net.http_post(
      url := 'https://<PROJECT_REF>.functions.supabase.co/smartlead-sync',
      headers := jsonb_build_object(
        'Content-Type', 'application/json',
        'x-sync-secret', '<SYNC_SECRET>'
      ),
      body := jsonb_build_object(
        'campaigns_per_run', 3,
        'page_size', 100,
        'batch_size', 500
      )
    ) as request_id;
  $$
);

-- Optional: trigger once immediately (manual smoke test)
select net.http_post(
  url := 'https://<PROJECT_REF>.functions.supabase.co/smartlead-sync',
  headers := jsonb_build_object(
    'Content-Type', 'application/json',
    'x-sync-secret', '<SYNC_SECRET>'
  ),
  body := jsonb_build_object(
    'campaigns_per_run', 1,
    'page_size', 50,
    'batch_size', 200
  )
) as request_id;
