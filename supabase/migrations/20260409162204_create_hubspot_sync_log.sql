CREATE TABLE hubspot_sync_log (
  id bigint generated always as identity primary key,
  source text not null,
  source_key text not null,
  email text not null,
  hubspot_contact_id text,
  hubspot_lead_id text,
  status text not null default 'pending',
  error_text text,
  payload jsonb,
  synced_at timestamptz,
  created_at timestamptz default now(),
  UNIQUE(source, source_key)
);

COMMENT ON TABLE hubspot_sync_log IS 'Лог синхронизации лидов из outreach-каналов в HubSpot';
