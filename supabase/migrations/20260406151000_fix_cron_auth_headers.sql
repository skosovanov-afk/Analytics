-- Fix: add Authorization header to all cron jobs calling Edge Functions
-- Root cause: Edge Functions return 401 "Missing authorization header"
-- because pg_net calls only send x-sync-secret but not Authorization: Bearer <anon_key>

-- ── 1. Drop all existing sync cron jobs ──────────────────────────────────────

do $$
declare r record;
begin
  for r in
    select jobid, jobname from cron.job
    where jobname like 'expandi%' or jobname like 'smartlead%'
  loop
    raise notice 'Unscheduling: %', r.jobname;
    perform cron.unschedule(r.jobid);
  end loop;
end $$;

-- ── 2. Recreate Expandi campaigns_tick (every 5 min) ─────────────────────────

select cron.schedule(
  'expandi-campaigns-sync-main',
  '*/5 * * * *',
  $$
  select net.http_post(
    url := 'https://lnuzkvoutworlsbkoxzw.supabase.co/functions/v1/expandi-sync',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer sb_publishable_6hQOreDBwgAn7nMWK0O9vQ_TRAp1ULi',
      'x-sync-secret', 'sjdbsv34viwshv4e'
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

-- ── 3. Recreate Expandi backfill_tick (every 3 min) ──────────────────────────

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

-- ── 4. Recreate Expandi MV refresh (every 30 min) ───────────────────────────

select cron.schedule(
  'expandi-campaign-daily-mv-refresh',
  '*/30 * * * *',
  $$ refresh materialized view concurrently public.expandi_campaign_daily_mv; $$
);

-- ── 5. Recreate SmartLead sync (every 15 min) ───────────────────────────────
-- Note: smartlead has TWO jobs with different URLs - consolidate into one correct one

select cron.schedule(
  'smartlead-sync-main',
  '*/15 * * * *',
  $$
  select net.http_post(
    url := 'https://lnuzkvoutworlsbkoxzw.supabase.co/functions/v1/smartlead-sync',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer sb_publishable_6hQOreDBwgAn7nMWK0O9vQ_TRAp1ULi',
      'x-sync-secret', 'skjdvisunv39vs'
    ),
    body := jsonb_build_object(
      'campaigns_per_run', 3,
      'page_size', 100,
      'batch_size', 500
    )
  );
  $$
);

-- ── 6. Cleanup: drop diagnostic views ────────────────────────────────────────

drop view if exists public._cron_jobs_diag;
drop view if exists public._net_responses_diag;
