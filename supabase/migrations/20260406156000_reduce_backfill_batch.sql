-- Fix: reduce max_messengers_per_run from 60 to 20 to avoid WORKER_LIMIT error
-- Edge Function exhausts compute resources with 60 messengers per tick

do $$
declare r record;
begin
  for r in select jobid from cron.job where jobname = 'expandi-backfill-sync-main'
  loop perform cron.unschedule(r.jobid); end loop;
end $$;

select cron.schedule(
  'expandi-backfill-sync-main',
  '*/3 * * * *',
  $$
  select net.http_post(
    url := 'https://lnuzkvoutworlsbkoxzw.supabase.co/functions/v1/expandi-sync',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'x-sync-secret', 'sjdbsv34viwshv4e'
    ),
    body := jsonb_build_object(
      'mode', 'backfill_tick',
      'store_raw', false,
      'accounts_per_run', 1,
      'max_messengers_per_run', 20,
      'max_pages_accounts', 1,
      'max_pages_campaigns', 2,
      'max_pages_messengers', 10,
      'max_pages_messages', 2
    )
  );
  $$
);
