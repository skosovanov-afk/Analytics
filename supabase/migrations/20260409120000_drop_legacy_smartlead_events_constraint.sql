-- Drop legacy unique constraint that conflicts with event_identity_key upsert.
-- The old constraint on (campaign_id, stats_id, event_type) predates the
-- event_identity_key column and causes 23505 errors during sync.
-- The correct upsert target is the event_identity_key unique index.

ALTER TABLE public.smartlead_events
  DROP CONSTRAINT IF EXISTS uq_smartlead_events_campaign_stats_event;

DROP INDEX IF EXISTS uq_smartlead_events_campaign_stats_event;
