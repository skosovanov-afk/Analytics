-- Dedup smartlead_events: remove duplicate rows accumulated during sync backfill.
-- Root cause: ~56k legacy rows had NULL event_identity_key, so ON CONFLICT upsert
-- did not dedupe them and each sync cycle created new duplicates.

BEGIN;

-- Bump statement timeout for this operation
SET LOCAL statement_timeout = '10min';

-- ─── Step 1: Dedup records with stats_id ────────────────────────────────────
-- For each (campaign_id, event_type, stats_id) keep the row with smallest id
DELETE FROM public.smartlead_events a
USING public.smartlead_events b
WHERE a.id > b.id
  AND a.campaign_id = b.campaign_id
  AND lower(a.event_type) = lower(b.event_type)
  AND a.stats_id IS NOT NULL
  AND b.stats_id IS NOT NULL
  AND a.stats_id = b.stats_id;

-- ─── Step 2: Dedup records without stats_id ─────────────────────────────────
-- Fall back to (campaign_id, event_type, email, seq, occurred_at) composite
DELETE FROM public.smartlead_events a
USING public.smartlead_events b
WHERE a.id > b.id
  AND a.campaign_id = b.campaign_id
  AND lower(a.event_type) = lower(b.event_type)
  AND (a.stats_id IS NULL OR a.stats_id = '')
  AND (b.stats_id IS NULL OR b.stats_id = '')
  AND lower(coalesce(a.email,'')) = lower(coalesce(b.email,''))
  AND coalesce(a.sequence_number, -1) = coalesce(b.sequence_number, -1)
  AND a.occurred_at = b.occurred_at;

-- ─── Step 3: Backfill event_identity_key for remaining NULL rows ────────────
UPDATE public.smartlead_events
SET event_identity_key =
  CASE
    WHEN stats_id IS NOT NULL AND stats_id != ''
      THEN campaign_id::text || '|' || lower(event_type) || '|' || stats_id
    ELSE
      campaign_id::text || '|' || lower(event_type) || '|'
        || lower(coalesce(email,'')) || '|'
        || coalesce(sequence_number::text, '') || '|'
        || coalesce(occurred_at::text, '')
  END
WHERE event_identity_key IS NULL;

COMMIT;

-- ─── Step 4: Rebuild smartlead_stats_daily from clean data ──────────────────
BEGIN;
SET LOCAL statement_timeout = '10min';

TRUNCATE public.smartlead_stats_daily;

INSERT INTO public.smartlead_stats_daily (
  date, campaign_id, campaign_name, touch_number,
  sent_count, reply_count, open_count, click_count,
  unique_leads_count, updated_at
)
SELECT
  occurred_at::date AS date,
  campaign_id,
  MAX(campaign_name) AS campaign_name,
  coalesce(sequence_number, 0) AS touch_number,
  sum(CASE WHEN lower(event_type)='sent' THEN 1 ELSE 0 END) AS sent_count,
  sum(CASE WHEN lower(event_type)='reply' THEN 1 ELSE 0 END) AS reply_count,
  sum(coalesce(open_count, 0)) AS open_count,
  sum(coalesce(click_count, 0)) AS click_count,
  count(DISTINCT lower(email)) FILTER (WHERE email IS NOT NULL AND email != '') AS unique_leads_count,
  now() AS updated_at
FROM public.smartlead_events
WHERE occurred_at IS NOT NULL
  AND campaign_id IS NOT NULL
GROUP BY occurred_at::date, campaign_id, coalesce(sequence_number, 0);

COMMIT;

-- ─── Step 5: Refresh table statistics for planner ───────────────────────────
ANALYZE public.smartlead_events;
ANALYZE public.smartlead_stats_daily;
