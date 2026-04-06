-- =============================================================================
-- Fix: пересоздать Expandi cron-джобы с реальным SYNC_SECRET
-- Причина: expandi_autopilot_hygiene.sql создал джобы с плейсхолдером <SYNC_SECRET>
-- Применить: вставить в Supabase SQL Editor, заменить <REAL_SYNC_SECRET> и запустить
-- =============================================================================

-- 1. Удалить все сломанные Expandi cron-джобы
do $$
declare r record;
begin
  for r in
    select jobid, jobname from cron.job
    where jobname like 'expandi%'
  loop
    raise notice 'Unscheduling: %', r.jobname;
    perform cron.unschedule(r.jobid);
  end loop;
end $$;

-- 2. Пересоздать campaigns_tick (каждые 5 минут)
select cron.schedule(
  'expandi-campaigns-sync-main',
  '*/5 * * * *',
  format($$
  select net.http_post(
    url := 'https://lnuzkvoutworlsbkoxzw.supabase.co/functions/v1/expandi-sync',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'x-sync-secret', '%s'
    ),
    body := jsonb_build_object(
      'mode', 'campaigns_tick',
      'store_raw', false,
      'accounts_per_run', 1,
      'max_pages_accounts', 1,
      'max_pages_campaigns', 10
    )
  );
  $$, '<REAL_SYNC_SECRET>')
);

-- 3. Пересоздать backfill_tick (каждые 3 минуты, для messages/messengers)
select cron.schedule(
  'expandi-backfill-sync-main',
  '*/3 * * * *',
  format($$
  select net.http_post(
    url := 'https://lnuzkvoutworlsbkoxzw.supabase.co/functions/v1/expandi-sync',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'x-sync-secret', '%s'
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
  $$, '<REAL_SYNC_SECRET>')
);

-- 4. Убрать сломанный hygiene (ссылается на дропнутую expandi_stats_daily)
drop function if exists public.expandi_hygiene_hourly(date, int);
drop function if exists public.expandi_hygiene_hourly();

-- =============================================================================
-- ПРОВЕРКИ (запусти после ожидания 5-10 минут)
-- =============================================================================

-- Активные джобы:
-- select jobid, jobname, schedule, active from cron.job where jobname like 'expandi%' order by jobname;

-- Появились ли новые ingest_runs:
-- select mode, ok, requests_total, requests_failed, started_at from expandi_ingest_runs order by started_at desc limit 5;

-- Последние HTTP-ответы от pg_net:
-- select id, status_code, timed_out, left(content::text, 200) as content from net._http_response order by id desc limit 10;
