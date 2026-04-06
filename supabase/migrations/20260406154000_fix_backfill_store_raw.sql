-- Fix: disable store_raw for backfill_tick to avoid statement timeout on expandi_raw INSERT
-- expandi_raw has 124K+ rows and upsert causes timeout

-- Drop and recreate backfill job with store_raw=false
do $$
declare r record;
begin
  for r in select jobid from cron.job where jobname = 'expandi-backfill-sync-main'
  loop
    perform cron.unschedule(r.jobid);
  end loop;
end $$;

select cron.schedule(
  'expandi-backfill-sync-main',
  '*/3 * * * *',
  $$
  select net.http_post(
    url := 'https://lnuzkvoutworlsbkoxzw.supabase.co/functions/v1/expandi-sync',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer sb_publishable_6hQOreDBwgAn7nMWK0O9vQ_TRAp1ULi',
      'x-sync-secret', 'sjdbsv34viwshv4e'
    ),
    body := jsonb_build_object(
      'mode', 'backfill_tick',
      'store_raw', false,
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
