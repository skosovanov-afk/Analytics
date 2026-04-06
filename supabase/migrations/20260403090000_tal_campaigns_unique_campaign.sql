-- =============================================================================
-- Constraint: one campaign can only belong to one TAL
-- A campaign is identified by (channel, source_campaign_key).
-- Prevents the same campaign from being linked to multiple TALs.
-- =============================================================================

-- Unique index on source_campaign_key (non-null) per channel.
-- This means: for a given channel, the same source_campaign_key can only appear once
-- across ALL TALs (not just within one TAL).
create unique index if not exists tal_campaigns_unique_source_key
  on public.tal_campaigns (channel, source_campaign_key)
  where source_campaign_key is not null;

-- Fallback: for campaigns without source_campaign_key, use campaign_name.
create unique index if not exists tal_campaigns_unique_name_fallback
  on public.tal_campaigns (channel, campaign_name)
  where source_campaign_key is null;
