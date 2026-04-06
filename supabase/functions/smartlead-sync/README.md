# smartlead-sync (Supabase Edge Function)

Автономный синк SmartLead -> Supabase без локального сервера.

## Что делает

- Берет кампании из SmartLead.
- На каждом запуске обрабатывает только часть кампаний (`campaigns_per_run`) по курсору.
- Обновляет таблицы:
  - `smartlead_leads`
  - `smartlead_events`
  - `smartlead_stats_daily`
- Обновляет `sync_state` ключи:
  - `smartlead_campaign_cursor`
  - `smartlead_last_sync_ts`

## Переменные (Secrets)

Задай в Supabase Edge Function Secrets:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SMARTLEAD_API_KEY`
- `SMARTLEAD_BASE_URL` (опционально, default `https://server.smartlead.ai`)
- `SMARTLEAD_SYNC_SECRET` (обязательно для cron/webhook auth)
- `SMARTLEAD_CAMPAIGNS_PER_RUN` (опционально, default `3`)
- `SMARTLEAD_PAGE_SIZE` (опционально, default `100`)
- `SMARTLEAD_BATCH_SIZE` (опционально, default `500`)

## Deploy

```bash
supabase functions deploy smartlead-sync --no-verify-jwt
```

## Ручной запуск

```bash
curl -X POST "https://<PROJECT_REF>.functions.supabase.co/smartlead-sync" \
  -H "Content-Type: application/json" \
  -H "x-sync-secret: <SMARTLEAD_SYNC_SECRET>" \
  -d '{"campaigns_per_run":1,"page_size":50,"batch_size":200}'
```

## Cron

Используй SQL из `supabase/sql/smartlead_sync_cron.sql`.
