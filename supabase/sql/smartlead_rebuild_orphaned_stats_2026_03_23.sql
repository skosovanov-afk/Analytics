-- ============================================================
-- Fix: rebuild smartlead_stats_daily for campaigns that have
-- events in smartlead_events but no rows in smartlead_stats_daily.
-- This happens when campaigns are deleted/archived in SmartLead
-- and the sync cursor never reaches them again.
--
-- Run once to fix current gap, then the edge function patch
-- will prevent this from happening again.
-- ============================================================

-- Step 1: Find orphaned campaign IDs
-- (have events but no stats)
WITH orphaned AS (
  SELECT DISTINCT e.campaign_id
  FROM smartlead_events e
  LEFT JOIN smartlead_stats_daily s ON s.campaign_id = e.campaign_id
  WHERE s.campaign_id IS NULL
    AND e.campaign_id IS NOT NULL
)
SELECT campaign_id FROM orphaned;
-- Expected: 2446885 (Fintech), 2454548 (Telecom), 2457545 (SBC Conference)

-- Step 2: Rebuild stats for orphaned campaigns
INSERT INTO smartlead_stats_daily (
  date, campaign_id, campaign_name, touch_number,
  sent_count, reply_count, open_count, click_count,
  unique_leads_count, updated_at
)
SELECT
  (e.occurred_at AT TIME ZONE 'UTC')::date AS date,
  e.campaign_id,
  e.campaign_name,
  COALESCE(e.sequence_number, 0) AS touch_number,
  COUNT(*) FILTER (WHERE e.event_type = 'sent') AS sent_count,
  COUNT(*) FILTER (WHERE e.event_type = 'reply') AS reply_count,
  COALESCE(SUM(e.open_count) FILTER (WHERE e.event_type = 'sent'), 0) AS open_count,
  COALESCE(SUM(e.click_count) FILTER (WHERE e.event_type = 'sent'), 0) AS click_count,
  COUNT(DISTINCT e.email) FILTER (WHERE e.email IS NOT NULL AND e.email != '') AS unique_leads_count,
  NOW() AS updated_at
FROM smartlead_events e
WHERE e.campaign_id IN (
  SELECT DISTINCT ev.campaign_id
  FROM smartlead_events ev
  LEFT JOIN smartlead_stats_daily sd ON sd.campaign_id = ev.campaign_id
  WHERE sd.campaign_id IS NULL
    AND ev.campaign_id IS NOT NULL
)
GROUP BY (e.occurred_at AT TIME ZONE 'UTC')::date, e.campaign_id, e.campaign_name, COALESCE(e.sequence_number, 0);

-- Step 3: Create RPC function for the edge function to call
-- This rebuilds stats for any campaign that has events but no stats rows.
CREATE OR REPLACE FUNCTION rebuild_orphaned_smartlead_stats()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  orphaned_ids int[];
  rows_inserted int := 0;
BEGIN
  -- Find campaigns with events but no stats
  SELECT ARRAY_AGG(DISTINCT campaign_id)
  INTO orphaned_ids
  FROM smartlead_events e
  WHERE e.campaign_id IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM smartlead_stats_daily s
      WHERE s.campaign_id = e.campaign_id
    );

  IF orphaned_ids IS NULL OR array_length(orphaned_ids, 1) IS NULL THEN
    RETURN jsonb_build_object('ok', true, 'orphaned_campaigns', 0, 'rows_inserted', 0);
  END IF;

  INSERT INTO smartlead_stats_daily (
    date, campaign_id, campaign_name, touch_number,
    sent_count, reply_count, open_count, click_count,
    unique_leads_count, updated_at
  )
  SELECT
    (e.occurred_at AT TIME ZONE 'UTC')::date AS date,
    e.campaign_id,
    e.campaign_name,
    COALESCE(e.sequence_number, 0) AS touch_number,
    COUNT(*) FILTER (WHERE e.event_type = 'sent') AS sent_count,
    COUNT(*) FILTER (WHERE e.event_type = 'reply') AS reply_count,
    COALESCE(SUM(e.open_count) FILTER (WHERE e.event_type = 'sent'), 0) AS open_count,
    COALESCE(SUM(e.click_count) FILTER (WHERE e.event_type = 'sent'), 0) AS click_count,
    COUNT(DISTINCT e.email) FILTER (WHERE e.email IS NOT NULL AND e.email != '') AS unique_leads_count,
    NOW() AS updated_at
  FROM smartlead_events e
  WHERE e.campaign_id = ANY(orphaned_ids)
  GROUP BY (e.occurred_at AT TIME ZONE 'UTC')::date, e.campaign_id, e.campaign_name, COALESCE(e.sequence_number, 0);

  GET DIAGNOSTICS rows_inserted = ROW_COUNT;

  RETURN jsonb_build_object(
    'ok', true,
    'orphaned_campaigns', array_length(orphaned_ids, 1),
    'campaign_ids', to_jsonb(orphaned_ids),
    'rows_inserted', rows_inserted
  );
END;
$$;
