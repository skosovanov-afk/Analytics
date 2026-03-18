-- ==============================================================================
-- UNIFIED SALES ANALYTICS (STAR SCHEMA)
-- ==============================================================================
-- A consolidated Fact Table for all sales activities (email, linkedin, etc.)
-- Source of Truth for Dashboard Charts & Metrics.
-- ==============================================================================

-- HARD RESET: Drop to ensure type definition updates (UUID -> BIGINT mismatch fix)
drop table if exists public.sales_analytics_activities cascade;

create table if not exists public.sales_analytics_activities (
  id uuid primary key default gen_random_uuid(),
  
  -- Dimensions (Context)
  hypothesis_id uuid, -- Link to sales_hypotheses
  company_id bigint,  -- Link to sales_hubspot_tal_companies (BIGINT)
  contact_id bigint,  -- Link to sales_hubspot_tal_contacts (BIGINT)
  deal_id bigint,     -- Link to sales_hubspot_deals (BIGINT)
  
  -- Facts (Event Details)
  occurred_at timestamptz not null,
  activity_type text not null, -- 'email', 'linkedin', 'call', 'meeting', 'note'
  direction text not null,     -- 'inbound', 'outbound'
  
  -- ============================================================================
  -- Event-level analytics fields (used for KPIs + charts)
  -- ============================================================================
  event_kind text,             -- normalized event (linkedin_message_sent, email_opened, etc)
  message_hash text,           -- LinkedIn message hash (unique per message)
  is_inmail boolean,           -- LinkedIn InMail flag (excluded from metrics)
  contact_email text,          -- SmartLead contact email (unique per lead)
  smartlead_campaign_id bigint,-- SmartLead campaign id (for campaign filters)
  lead_uuid text,              -- GetSales lead UUID (unique per lead)
  
  -- Lineage / Audit
  source_system text not null, -- 'getsales', 'smartlead', 'hubspot'
  source_id text not null,     -- Original UUID/ID in the source system
  hubspot_engagement_id bigint,-- Matching ID in HubSpot (Recocillation Key)
  
  -- Metadata
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  
  -- Uniqueness: One source event = One analytic row
  unique (source_system, source_id)
);

-- Index for dashboard graphs (filtered by hypothesis + date)
create index if not exists sales_analytics_activities_hyp_date_idx 
  on public.sales_analytics_activities (hypothesis_id, occurred_at desc);

-- Index for deal-level metrics (lead/opportunity counts)
create index if not exists sales_analytics_activities_deal_idx
  on public.sales_analytics_activities (deal_id);

-- Index for analytics aggregations (event kind + date)
create index if not exists sales_analytics_activities_kind_date_idx
  on public.sales_analytics_activities (event_kind, occurred_at desc);

-- Index for unique message/lead lookups
create index if not exists sales_analytics_activities_message_hash_idx
  on public.sales_analytics_activities (message_hash);

create index if not exists sales_analytics_activities_contact_email_idx
  on public.sales_analytics_activities (contact_email);

create index if not exists sales_analytics_activities_sl_campaign_idx
  on public.sales_analytics_activities (smartlead_campaign_id);

-- Activity -> deal mapping (many-to-many via contact associations).
-- Used for deal counts without relying on a single deal_id per activity.
create table if not exists public.sales_analytics_activity_deals (
  activity_id uuid not null,
  deal_id bigint not null,
  created_at timestamptz not null default now(),
  primary key (activity_id, deal_id)
);

create index if not exists sales_analytics_activity_deals_deal_idx
  on public.sales_analytics_activity_deals (deal_id);

-- Index for reconciliation (find missing hubspot links)
create index if not exists sales_analytics_activities_hs_missing_idx 
  on public.sales_analytics_activities (source_system, hubspot_engagement_id) 
  where hubspot_engagement_id is null;


-- ==============================================================================
-- SYNC LOGIC (The "Button")
-- ==============================================================================

create or replace function public.sales_analytics_sync()
returns jsonb
language plpgsql
security definer
as $$
declare
  v_inserted int := 0;
  v_updated int := 0;
begin
  -- Bypass RLS for internal data processing to ensure completeness
  perform set_config('row_security', 'off', true);

  -- 1. SYNC GETSALES EVENTS
  with processed as (
    insert into public.sales_analytics_activities (
      occurred_at,
      activity_type,
      direction,
      event_kind,
      message_hash,
      is_inmail,
      contact_email,
      smartlead_campaign_id,
      lead_uuid,
      source_system,
      source_id,
      hubspot_engagement_id,
      contact_id,
      company_id,
      deal_id,
      hypothesis_id
    )
    select
      e.occurred_at,
      case 
        when e.source = 'linkedin_connection' then 'linkedin_connection'
        else e.source 
      end as activity_type,
      case 
        when lower(coalesce(e.payload->>'event_kind', e.payload->>'eventKind', '')) in ('linkedin_message_replied', 'linkedin_connection_request_accepted') then 'inbound'
        when (e.payload->>'type') in ('reply', 'inbox', 'linkedin_message_replied', 'linkedin_connection_request_accepted') then 'inbound'
        else 'outbound'
      end as direction,
      case
        when coalesce(nullif(e.payload->>'event_kind', ''), nullif(e.payload->>'eventKind', '')) is not null
          then coalesce(nullif(e.payload->>'event_kind', ''), nullif(e.payload->>'eventKind', ''))
        when e.source = 'linkedin_connection' then
          case
            when lower(coalesce(e.payload->>'status', '')) like '%accept%' or lower(coalesce(e.payload->>'status', '')) like '%connected%' then 'linkedin_connection_request_accepted'
            else 'linkedin_connection_request_sent'
          end
        else null
      end as event_kind,
      nullif(coalesce(e.payload->>'message_hash', e.payload->>'messageHash', ''), '') as message_hash,
      (
        lower(coalesce(e.payload->>'status', '')) like '%inmail%' or
        lower(coalesce(e.payload->>'type', '')) like '%inmail%' or
        lower(coalesce(e.payload->>'text', '')) like '%inmail%'
      ) as is_inmail,
      nullif(lower(coalesce(e.contact_email, '')), '') as contact_email,
      null::bigint as smartlead_campaign_id,
      nullif(e.lead_uuid, '') as lead_uuid,
      'getsales' as source_system,
      e.getsales_uuid as source_id,
      e.hubspot_engagement_id,
      c.contact_id as contact_id,
      cc.company_id as company_id, 
      dc.deal_id as deal_id,
      h.id as hypothesis_id
    from (
      /**
       * De-dup within the sync batch to avoid ON CONFLICT updating the same row twice.
       *
       * We pick the latest event row per (source, getsales_uuid) across all creators.
       */
      select distinct on (source, getsales_uuid)
        *
      from public.sales_getsales_events
      where getsales_uuid is not null
      order by source, getsales_uuid, updated_at desc nulls last, occurred_at desc
    ) e
    -- Join: raw event -> cached HubSpot contact
    -- Left join keeps all GetSales events even if the contact is not in TAL yet.
    left join public.sales_hubspot_tal_contacts c on c.contact_id::text = e.hubspot_contact_id
    -- Join: contact -> company (pick ONE company to avoid duplicate insert rows)
    left join lateral (
      select cc.company_id
      from public.sales_hubspot_company_contacts cc
      where cc.contact_id = c.contact_id
      order by cc.company_id asc
      limit 1
    ) cc on true
    -- Join: contact -> deal (pick ONE deal to avoid duplicate insert rows)
    left join lateral (
      select dc.deal_id
      from public.sales_hubspot_deal_contacts dc
      where dc.contact_id = c.contact_id
      order by dc.deal_id asc
      limit 1
    ) dc on true
    -- Join: hypothesis (via List ID in HubSpot URL)
    left join public.sales_hypotheses h on 
       substring(h.hubspot_tal_url from '/(?:lists|objectLists)/([0-9]+)') = c.tal_list_id
    
    on conflict (source_system, source_id) do update set
      hubspot_engagement_id = excluded.hubspot_engagement_id,
      contact_id = excluded.contact_id,
      company_id = excluded.company_id,
      deal_id = excluded.deal_id,
      hypothesis_id = excluded.hypothesis_id,
      event_kind = excluded.event_kind,
      message_hash = excluded.message_hash,
      is_inmail = excluded.is_inmail,
      contact_email = excluded.contact_email,
      smartlead_campaign_id = excluded.smartlead_campaign_id,
      lead_uuid = excluded.lead_uuid,
      updated_at = now()
    returning (xmax = 0) as is_insert
  )
  select 
    count(*) filter (where is_insert), 
    count(*) filter (where not is_insert)
  into v_inserted, v_updated
  from processed;

  -- 2. SYNC SMARTLEAD EVENTS
  with processed_sl as (
    insert into public.sales_analytics_activities (
      occurred_at,
      activity_type,
      direction,
      event_kind,
      message_hash,
      is_inmail,
      contact_email,
      smartlead_campaign_id,
      lead_uuid,
      source_system,
      source_id,
      hubspot_engagement_id,
      contact_id,
      company_id,
      deal_id,
      hypothesis_id
    )
    select
      e.occurred_at,
      'email' as activity_type,
      case 
        when upper(e.event_type) in ('REPLIED', 'POSITIVE_REPLY', 'REPLIED_OOO') then 'inbound'
        else 'outbound'
      end as direction,
      case
        when upper(e.event_type) = 'SENT' then 'email_sent'
        when upper(e.event_type) = 'OPENED' then 'email_opened'
        when upper(e.event_type) = 'REPLIED' then 'email_replied'
        when upper(e.event_type) = 'BOUNCED' then 'email_bounced'
        when upper(e.event_type) = 'POSITIVE_REPLY' then 'email_positive_reply'
        when upper(e.event_type) = 'REPLIED_OOO' then 'email_replied_ooo'
        else null
      end as event_kind,
      null::text as message_hash,
      null::boolean as is_inmail,
      nullif(lower(coalesce(e.contact_email, '')), '') as contact_email,
      nullif(e.smartlead_campaign_id, 0) as smartlead_campaign_id,
      null::text as lead_uuid,
      'smartlead' as source_system,
      e.smartlead_event_id as source_id,
      e.hubspot_engagement_id,
      c.contact_id as contact_id,
      cc.company_id as company_id,
      dc.deal_id as deal_id,
      h.id as hypothesis_id
    from (
      /**
       * De-dup within the sync batch to avoid ON CONFLICT updating the same row twice.
       *
       * We pick the latest event row per smartlead_event_id across all creators.
       */
      select distinct on (smartlead_event_id)
        *
      from public.sales_smartlead_events
      where smartlead_event_id is not null
      order by smartlead_event_id, updated_at desc nulls last, occurred_at desc
    ) e
    inner join public.sales_hubspot_tal_contacts c on c.contact_id::text = e.hubspot_contact_id
    -- Join: contact -> company (pick ONE company to avoid duplicate insert rows)
    left join lateral (
      select cc.company_id
      from public.sales_hubspot_company_contacts cc
      where cc.contact_id = c.contact_id
      order by cc.company_id asc
      limit 1
    ) cc on true
    -- Join: contact -> deal (pick ONE deal to avoid duplicate insert rows)
    left join lateral (
      select dc.deal_id
      from public.sales_hubspot_deal_contacts dc
      where dc.contact_id = c.contact_id
      order by dc.deal_id asc
      limit 1
    ) dc on true
    left join public.sales_hypotheses h on 
       substring(h.hubspot_tal_url from '/(?:lists|objectLists)/([0-9]+)') = c.tal_list_id
    
    where upper(e.event_type) in ('SENT', 'OPENED', 'REPLIED', 'BOUNCED', 'POSITIVE_REPLY', 'REPLIED_OOO')
    
    on conflict (source_system, source_id) do update set
      hubspot_engagement_id = excluded.hubspot_engagement_id,
      contact_id = excluded.contact_id,
      company_id = excluded.company_id,
      deal_id = excluded.deal_id,
      hypothesis_id = excluded.hypothesis_id,
      event_kind = excluded.event_kind,
      message_hash = excluded.message_hash,
      is_inmail = excluded.is_inmail,
      contact_email = excluded.contact_email,
      smartlead_campaign_id = excluded.smartlead_campaign_id,
      lead_uuid = excluded.lead_uuid,
      updated_at = now()
    returning (xmax = 0) as is_insert
  )
  select 
    v_inserted + count(*) filter (where is_insert), 
    v_updated + count(*) filter (where not is_insert)
  into v_inserted, v_updated
  from processed_sl;

  return jsonb_build_object(
    'inserted', v_inserted,
    'updated', v_updated
  );
end;
$$;

-- ==============================================================================
-- BACKFILL + METRICS (Deal attribution via analytics)
-- ==============================================================================

-- Backfill activity -> deal mappings for existing analytics rows.
-- Why: older rows were created before we stored deal associations.
create or replace function public.sales_analytics_backfill_deal_ids()
returns jsonb
language plpgsql
security definer
as $$
declare
  v_updated int := 0;
begin
  perform set_config('row_security', 'off', true);

  with inserted_rows as (
    insert into public.sales_analytics_activity_deals (activity_id, deal_id)
    select a.id, dc.deal_id
    from public.sales_analytics_activities a
    join public.sales_hubspot_deal_contacts dc
      on dc.contact_id = a.contact_id
    where a.contact_id is not null
    on conflict (activity_id, deal_id) do nothing
    returning 1
  )
  select count(*) into v_updated from inserted_rows;

  return jsonb_build_object('inserted', v_updated);
end;
$$;

-- Trigger: keep activity -> deal mappings fresh on insert/update.
create or replace function public.sales_analytics_activity_deals_sync()
returns trigger
language plpgsql
security definer
as $$
begin
  if new.contact_id is null then
    return new;
  end if;

  insert into public.sales_analytics_activity_deals (activity_id, deal_id)
  select new.id, dc.deal_id
  from public.sales_hubspot_deal_contacts dc
  where dc.contact_id = new.contact_id
  on conflict (activity_id, deal_id) do nothing;

  return new;
end;
$$;

drop trigger if exists sales_analytics_activity_deals_trg on public.sales_analytics_activities;
create trigger sales_analytics_activity_deals_trg
after insert or update of contact_id
on public.sales_analytics_activities
for each row
execute function public.sales_analytics_activity_deals_sync();

-- Compute deal counts (Leads/Opps) from analytics + deal metadata.
-- Uses distinct deal_id from analytics rows for the TAL hypothesis.
create or replace function public.sales_hypothesis_deal_counts_from_analytics(
  p_tal_list_id text,
  p_pipeline_ids text[] default null
)
returns table (
  deals_count int,
  leads_count int,
  opps_count int
)
language plpgsql
stable
security definer
as $$
declare
  v_hyp_id uuid;
begin
  perform set_config('row_security', 'off', true);

  if p_tal_list_id is null or p_tal_list_id = '' then
    return;
  end if;

  select id into v_hyp_id
  from public.sales_hypotheses
  where substring(hubspot_tal_url from '/(?:lists|objectLists)/([0-9]+)') = p_tal_list_id
  limit 1;

  if v_hyp_id is null then
    return;
  end if;

  return query
    with distinct_deals as (
      select distinct ad.deal_id
      from public.sales_analytics_activity_deals ad
      join public.sales_analytics_activities a on a.id = ad.activity_id
      where a.hypothesis_id = v_hyp_id
    ),
    filtered as (
      select d.deal_id,
             d.pipeline_id,
             lower(coalesce(d.stage_category, '')) as stage_category
      from public.sales_hubspot_deals d
      join distinct_deals dd on dd.deal_id = d.deal_id
      where p_pipeline_ids is null
        or array_length(p_pipeline_ids, 1) is null
        or d.pipeline_id = any(p_pipeline_ids)
    )
    select
      count(*)::int as deals_count,
      count(*) filter (where stage_category = 'lead')::int as leads_count,
      count(*) filter (where stage_category = 'opportunity')::int as opps_count
    from filtered;
end;
$$;

-- List deals for a TAL hypothesis using analytics as the source of truth.
-- Returns only deals referenced by analytics activity -> deal mappings.
create or replace function public.sales_hypothesis_deals_from_analytics(
  p_tal_list_id text,
  p_pipeline_ids text[] default null,
  p_limit int default 200
)
returns table (
  deal_id bigint,
  pipeline_id text,
  dealstage_id text,
  stage_label text,
  stage_category text,
  owner_id text,
  tal_category text,
  dealname text,
  amount text,
  channel text,
  createdate timestamptz
)
language plpgsql
stable
security definer
as $$
declare
  v_hyp_id uuid;
  v_limit int := greatest(1, least(coalesce(p_limit, 200), 2000));
begin
  perform set_config('row_security', 'off', true);

  if p_tal_list_id is null or p_tal_list_id = '' then
    return;
  end if;

  select id into v_hyp_id
  from public.sales_hypotheses
  where substring(hubspot_tal_url from '/(?:lists|objectLists)/([0-9]+)') = p_tal_list_id
  limit 1;

  if v_hyp_id is null then
    return;
  end if;

  return query
    with distinct_deals as (
      select distinct ad.deal_id
      from public.sales_analytics_activity_deals ad
      join public.sales_analytics_activities a on a.id = ad.activity_id
      where a.hypothesis_id = v_hyp_id
        and ad.deal_id is not null
    )
    select
      d.deal_id,
      d.pipeline_id,
      d.dealstage_id,
      d.stage_label,
      d.stage_category,
      d.owner_id,
      d.tal_category,
      d.dealname,
      d.amount,
      d.channel,
      d.createdate
    from public.sales_hubspot_deals d
    join distinct_deals dd on dd.deal_id = d.deal_id
    where p_pipeline_ids is null
      or array_length(p_pipeline_ids, 1) is null
      or d.pipeline_id = any(p_pipeline_ids)
    order by d.deal_id desc
    limit v_limit;
end;
$$;
