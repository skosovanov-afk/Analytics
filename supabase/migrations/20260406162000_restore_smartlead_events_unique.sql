-- Restore unique constraint on smartlead_events.event_identity_key
-- Required by smartlead-sync edge function for ON CONFLICT upsert
create unique index if not exists smartlead_events_identity_key_uniq
  on public.smartlead_events (event_identity_key);
