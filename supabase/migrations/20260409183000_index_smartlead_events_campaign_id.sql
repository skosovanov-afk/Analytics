-- Add index on campaign_id to speed up sync queries that scan by campaign
CREATE INDEX IF NOT EXISTS idx_smartlead_events_campaign_id
  ON public.smartlead_events (campaign_id);
