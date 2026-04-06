-- Add "current snapshot presence" tracking for smartlead_leads
-- Run once in Supabase SQL Editor before deploying updated edge function.

alter table if exists public.smartlead_leads
  add column if not exists is_present_now boolean not null default false,
  add column if not exists last_seen_at timestamptz null;

-- Required for REST upsert on conflict campaign_id,lead_id
create unique index if not exists smartlead_leads_campaign_lead_uidx
  on public.smartlead_leads (campaign_id, lead_id);

-- Helpful for fast filtering by current snapshot
create index if not exists smartlead_leads_campaign_present_idx
  on public.smartlead_leads (campaign_id, is_present_now);

