-- Sales Hypotheses (PMF experiments) - Supabase schema add-on
-- Paste into Supabase SQL Editor for the Calls project.
-- Source of truth: Supabase; Sales portal + MD sync read these tables.

create extension if not exists pgcrypto;

-- ======================
-- Users directory (reuse Calls system user_profiles)
-- ======================
-- NOTE: Calls schema keeps public.user_profiles with strict RLS (self/admin only).
-- For Sales UI we need a safe "list users" primitive to populate owner pickers.
-- This RPC returns only minimal fields (id/email/display_name) and bypasses RLS internally.
create or replace function public.sales_list_users()
returns table (user_id uuid, email text, display_name text)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  return query
    select up.user_id, up.email, up.display_name
    from public.user_profiles up
    where up.email is not null
    order by coalesce(up.display_name, ''), up.email;
end;
$$;

grant execute on function public.sales_list_users() to anon, authenticated;

-- Tables
-- ======================
-- ICP Library (shared)
-- ======================

create table if not exists public.sales_icp_roles (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  decision_role text, -- DecisionMaker|Influencer|User (legacy; keep for backward-compat)
  decision_roles text[] not null default '{}'::text[], -- Multi-select (preferred)
  seniority text,
  titles text[] not null default '{}'::text[],
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Migration (2026-01-20):
-- decision_role -> decision_roles (multi-select)
-- Keep the old column for backward-compat; new UI should write both.
alter table public.sales_icp_roles
  add column if not exists decision_roles text[] not null default '{}'::text[];

update public.sales_icp_roles
set decision_roles = array[decision_role]
where (decision_roles is null or array_length(decision_roles, 1) is null)
  and decision_role is not null
  and decision_role <> '';

create index if not exists sales_icp_roles_name_idx on public.sales_icp_roles(lower(name));

create table if not exists public.sales_verticals (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  sort_order int not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists sales_verticals_name_uidx on public.sales_verticals(lower(name));
create index if not exists sales_verticals_active_idx on public.sales_verticals(is_active, sort_order, lower(name));

create table if not exists public.sales_subverticals (
  id uuid primary key default gen_random_uuid(),
  vertical_id uuid not null references public.sales_verticals(id) on delete cascade,
  name text not null,
  sort_order int not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists sales_subverticals_vertical_name_uidx
  on public.sales_subverticals(vertical_id, lower(name));
create index if not exists sales_subverticals_active_idx on public.sales_subverticals(vertical_id, is_active, sort_order, lower(name));

create table if not exists public.sales_company_scales (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  sort_order int not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists sales_company_scales_name_uidx on public.sales_company_scales(lower(name));
create index if not exists sales_company_scales_active_idx on public.sales_company_scales(is_active, sort_order, lower(name));

create table if not exists public.sales_icp_company_profiles (
  id uuid primary key default gen_random_uuid(),
  vertical_name text,
  sub_vertical text, -- e.g. Finance / Payments / Trading ...
  region text,
  size_bucket text,
  tech_stack text[] not null default '{}'::text[],
  constraints_json jsonb not null default '{}'::jsonb,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists sales_icp_company_profiles_vertical_idx on public.sales_icp_company_profiles(lower(coalesce(vertical_name,'')), lower(coalesce(sub_vertical,'')));

-- Segment = Role x CompanyProfile. VP is defined per segment (no versioning for now).
create table if not exists public.sales_icp_segments (
  id uuid primary key default gen_random_uuid(),
  role_id uuid not null references public.sales_icp_roles(id) on delete cascade,
  company_profile_id uuid not null references public.sales_icp_company_profiles(id) on delete cascade,
  vp_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (role_id, company_profile_id)
);

create index if not exists sales_icp_segments_role_idx on public.sales_icp_segments(role_id);
create index if not exists sales_icp_segments_company_idx on public.sales_icp_segments(company_profile_id);

-- Channels library (hypotheses select channels from here; weekly check-ins prompt per selected channel)
create table if not exists public.sales_channels (
  id uuid primary key default gen_random_uuid(),
  slug text not null unique,
  name text not null,
  sort_order int not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists sales_channels_active_idx on public.sales_channels(is_active, sort_order, lower(name));

-- Metrics library (hypotheses select metrics from here; weekly check-ins collect values)
create table if not exists public.sales_metrics (
  id uuid primary key default gen_random_uuid(),
  slug text not null unique,
  name text not null,
  input_type text not null default 'number', -- number|text
  unit text,
  sort_order int not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists sales_metrics_active_idx on public.sales_metrics(is_active, sort_order, lower(name));

create table if not exists public.sales_hypotheses (
  id uuid primary key default gen_random_uuid(),
  parent_hypothesis_id uuid references public.sales_hypotheses(id) on delete set null,
  version int not null default 1,

  title text not null,
  status text not null default 'draft', -- draft|active|paused|won|lost
  priority int not null default 0,

  owner_user_id uuid not null default auth.uid(),
  owner_email text,

  vertical_name text,
  vertical_hubspot_url text,

  hubspot_deals_view_url text,
  hubspot_deal_tal_category text,
  hubspot_tal_url text,
  hubspot_contacts_list_url text,
  hubspot_deals_owner_email text,
  -- SmartLead campaigns filter (optional, used for hypothesis-level activity graphs)
  smartlead_campaign_ids int[],
  -- GetSales automations filter (optional, used for hypothesis-level activity graphs)
  getsales_flow_uuids text[],

  pricing_model text,

  opps_in_progress_count int not null default 0,

  timebox_days int not null default 28,
  win_criteria text,
  kill_criteria text,

  tal_companies_count_baseline int,
  contacts_count_baseline int,

  one_sentence_pitch text,
  product_description text,
  company_profile_text text,
  pain_points_text text,

  -- Legacy/free-form (kept for backward compatibility; preferred workflow uses ICP library + segments)
  icp_json jsonb not null default '{}'::jsonb,
  cjm_json jsonb not null default '{}'::jsonb,
  vp_json jsonb not null default '{}'::jsonb,
  compliance_json jsonb not null default '{}'::jsonb,
  competing_events_json jsonb not null default '{}'::jsonb,

  timeline_start timestamptz,
  timeline_end timestamptz,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  unique (parent_hypothesis_id, version)
);

create index if not exists sales_hypotheses_updated_idx on public.sales_hypotheses(updated_at desc);
create index if not exists sales_hypotheses_owner_idx on public.sales_hypotheses(owner_user_id, updated_at desc);

-- Migration safety: create table if not exists does not add new columns.
alter table if exists public.sales_hypotheses
  add column if not exists pricing_model text;
alter table if exists public.sales_hypotheses
  add column if not exists hubspot_contacts_list_url text;

alter table if exists public.sales_hypotheses
  add column if not exists hubspot_deals_owner_email text;

alter table if exists public.sales_hypotheses
  add column if not exists hubspot_deal_tal_category text;

-- Optional SmartLead campaign filter for hypothesis activity graphs.
alter table if exists public.sales_hypotheses
  add column if not exists smartlead_campaign_ids int[];
-- Optional GetSales automation filter for hypothesis activity graphs.
alter table if exists public.sales_hypotheses
  add column if not exists getsales_flow_uuids text[];

alter table if exists public.sales_hypotheses
  add column if not exists company_profile_text text;
alter table if exists public.sales_hypotheses
  add column if not exists pain_points_text text;

-- HubSpot TAL: store simple funnel breakdown derived from cached deals
alter table if exists public.sales_hypotheses
  add column if not exists hubspot_tal_leads_count int;
alter table if exists public.sales_hypotheses
  add column if not exists hubspot_tal_opps_count int;

create table if not exists public.sales_hypothesis_checkins (
  id uuid primary key default gen_random_uuid(),
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  week_start date not null,

  opps_in_progress_count int,
  tal_companies_count int,
  contacts_count int,

  notes text,
  blockers text,
  next_steps text,
  channel_activity_json jsonb not null default '{}'::jsonb,
  metrics_snapshot_json jsonb not null default '{}'::jsonb,

  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  unique (hypothesis_id, week_start)
);

create index if not exists sales_checkins_hypothesis_idx on public.sales_hypothesis_checkins(hypothesis_id, week_start desc);

create table if not exists public.sales_hypothesis_calls (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  call_id uuid not null references public.calls(id) on delete cascade,
  tag text,
  notes text,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, call_id)
);

create index if not exists sales_hypothesis_calls_call_idx on public.sales_hypothesis_calls(call_id);

-- Hypothesis sheet rows (operational unit inside a workspace / hypothesis container)
create table if not exists public.sales_hypothesis_rows (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  row_code text,
  title text,
  tal_id uuid references public.tals(id) on delete set null,
  role_id uuid references public.sales_icp_roles(id) on delete set null,
  role_label text,
  company_profile_id uuid references public.sales_icp_company_profiles(id) on delete set null,
  vertical_id uuid references public.sales_verticals(id) on delete set null,
  subvertical_id uuid references public.sales_subverticals(id) on delete set null,
  company_scale_id uuid references public.sales_company_scales(id) on delete set null,
  vertical_name text,
  sub_vertical text,
  company_scale text,
  decision_context text,
  vp_point text not null,
  pain text,
  expected_signal text,
  disqualifiers text,
  calls_count int not null default 0,
  pain_confirmed_rate numeric(5,2),
  severity_rate numeric(5,2),
  interest_rate numeric(5,2),
  opportunities_count int not null default 0,
  signal_speed text,
  decision text,
  status text not null default 'new', -- new|in_test|validated|paused|archived
  priority int not null default 0,
  owner_user_id uuid,
  notes text,
  source text not null default 'manual',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists sales_hypothesis_rows_workspace_idx on public.sales_hypothesis_rows(workspace_id, updated_at desc);
create index if not exists sales_hypothesis_rows_status_idx on public.sales_hypothesis_rows(status, updated_at desc);
create index if not exists sales_hypothesis_rows_decision_idx on public.sales_hypothesis_rows(decision, updated_at desc);
create index if not exists sales_hypothesis_rows_tal_id_idx on public.sales_hypothesis_rows(tal_id);
create index if not exists sales_hypothesis_rows_role_idx on public.sales_hypothesis_rows(role_id);
create index if not exists sales_hypothesis_rows_vertical_idx on public.sales_hypothesis_rows(vertical_id);
create index if not exists sales_hypothesis_rows_subvertical_idx on public.sales_hypothesis_rows(subvertical_id);
create index if not exists sales_hypothesis_rows_company_scale_idx on public.sales_hypothesis_rows(company_scale_id);
create unique index if not exists sales_hypothesis_rows_workspace_code_uidx
  on public.sales_hypothesis_rows(workspace_id, lower(row_code))
  where row_code is not null and btrim(row_code) <> '';

alter table if exists public.sales_hypothesis_rows
  add column if not exists vertical_id uuid references public.sales_verticals(id) on delete set null;
alter table if exists public.sales_hypothesis_rows
  add column if not exists subvertical_id uuid references public.sales_subverticals(id) on delete set null;
alter table if exists public.sales_hypothesis_rows
  add column if not exists company_scale_id uuid references public.sales_company_scales(id) on delete set null;

-- ======================
-- HubSpot snapshots (derived, weekly history per hypothesis)
-- ======================
-- Purpose:
-- - Store time-series snapshots for HubSpot TAL analytics (new deals, stage moves, activities)
-- - Enable "history since hypothesis creation" without re-querying HubSpot every time
create table if not exists public.sales_hubspot_tal_snapshots (
  id uuid primary key default gen_random_uuid(),
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  period_start date not null,          -- week start (ISO)
  period_end date not null,            -- period_start + 7 days
  window_days int not null default 7,  -- usually 7
  tal_list_id text,                    -- parsed list id
  companies_in_tal_count int,
  deals_in_tal_count int,
  new_deals_count int,
  stage_moves_count int,
  -- Funnel counters derived from weekly snapshots (week-to-week stage category transitions)
  new_leads_count int,
  new_opps_count int,
  new_customers_count int,
  new_churn_count int,
  -- Breakdown by "channel" (best-effort using deal source properties)
  funnel_by_channel_json jsonb not null default '{}'::jsonb,
  activities_json jsonb not null default '{}'::jsonb,
  data_json jsonb not null default '{}'::jsonb, -- full payload (for debugging / richer UI)
  llm_summary text,
  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (hypothesis_id, period_start, window_days)
);

create index if not exists sales_hubspot_snapshots_hypothesis_idx on public.sales_hubspot_tal_snapshots(hypothesis_id, period_start desc);
create index if not exists sales_hubspot_snapshots_period_idx on public.sales_hubspot_tal_snapshots(period_start desc);

-- ======================
-- HubSpot TAL cache (exact counts for large TALs)
-- ======================
-- Purpose:
-- - Cache TAL membership (companies) in Supabase so we don't re-fetch 1000s of ids from HubSpot every UI click
-- - Cache associations to compute exact counts:
--   - Deals in TAL: distinct deals associated with companies in the TAL
--   - Contacts in TAL: distinct contacts associated with companies in the TAL (all company contacts)
--
-- Notes:
-- - Company/Deal/Contact ids in HubSpot are numeric; store as bigint for efficient ordering/cursors.
-- - Writes are performed by the portal backend using SUPABASE_SERVICE_ROLE_KEY.

create table if not exists public.sales_hubspot_tal_companies (
  tal_list_id text not null,
  company_id bigint not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tal_list_id, company_id)
);

create index if not exists sales_hubspot_tal_companies_tal_idx on public.sales_hubspot_tal_companies(tal_list_id, company_id);

create table if not exists public.sales_hubspot_company_deals (
  company_id bigint not null,
  deal_id bigint not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (company_id, deal_id)
);

create index if not exists sales_hubspot_company_deals_company_idx on public.sales_hubspot_company_deals(company_id, deal_id);
create index if not exists sales_hubspot_company_deals_deal_idx on public.sales_hubspot_company_deals(deal_id);

-- HubSpot deals cache (pipeline + stage metadata).
-- Populated by TAL cache sync to enable DB-side lead/opportunity counts.
create table if not exists public.sales_hubspot_deals (
  deal_id bigint primary key,
  pipeline_id text,
  dealstage_id text,
  stage_label text,
  stage_category text, -- lead | opportunity | other
  owner_id text,
  tal_category text,
  dealname text,
  amount text,
  channel text,
  createdate timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists sales_hubspot_deals_pipeline_idx on public.sales_hubspot_deals(pipeline_id);
create index if not exists sales_hubspot_deals_stage_cat_idx on public.sales_hubspot_deals(stage_category);

-- Migration safety: create table if not exists does not add new columns.
alter table if exists public.sales_hubspot_deals
  add column if not exists dealname text;
alter table if exists public.sales_hubspot_deals
  add column if not exists amount text;
alter table if exists public.sales_hubspot_deals
  add column if not exists channel text;
alter table if exists public.sales_hubspot_deals
  add column if not exists createdate timestamptz;

create table if not exists public.sales_hubspot_deal_contacts (
  deal_id bigint not null,
  contact_id bigint not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (deal_id, contact_id)
);

create index if not exists sales_hubspot_deal_contacts_deal_idx on public.sales_hubspot_deal_contacts(deal_id, contact_id);
create index if not exists sales_hubspot_deal_contacts_contact_idx on public.sales_hubspot_deal_contacts(contact_id);

-- All contacts associated with a company (not just deal-linked contacts).
create table if not exists public.sales_hubspot_company_contacts (
  company_id bigint not null,
  contact_id bigint not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (company_id, contact_id)
);

create index if not exists sales_hubspot_company_contacts_company_idx on public.sales_hubspot_company_contacts(company_id, contact_id);
create index if not exists sales_hubspot_company_contacts_contact_idx on public.sales_hubspot_company_contacts(contact_id);

-- Contacts list cache (optional): contacts list derived from HubSpot filters (if provided).
create table if not exists public.sales_hubspot_tal_contacts (
  tal_list_id text not null,
  contact_id bigint not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tal_list_id, contact_id)
);

create index if not exists sales_hubspot_tal_contacts_tal_idx on public.sales_hubspot_tal_contacts(tal_list_id, contact_id);

-- HubSpot "touch" cache (exclude NOTE/TASK; focus on EMAIL/MEETING/CALL).
create table if not exists public.sales_hubspot_contact_touches (
  contact_id bigint primary key,
  last_touch_at timestamptz,
  last_touch_type text,
  updated_at timestamptz not null default now()
);

create index if not exists sales_hubspot_contact_touches_last_idx on public.sales_hubspot_contact_touches(last_touch_at desc);

create table if not exists public.sales_hubspot_company_touches (
  company_id bigint primary key,
  last_touch_at timestamptz,
  last_touch_type text,
  updated_at timestamptz not null default now()
);

create index if not exists sales_hubspot_company_touches_last_idx on public.sales_hubspot_company_touches(last_touch_at desc);

create table if not exists public.sales_hubspot_tal_touch_jobs (
  id uuid primary key default gen_random_uuid(),
  tal_list_id text not null,
  status text not null default 'queued', -- queued|running|done|failed
  phase text not null default 'companies',    -- companies|contacts|done
  last_deal_id bigint not null default 0,
  last_company_id bigint not null default 0,
  last_contact_id bigint not null default 0,
  deals_processed int not null default 0,
  companies_processed int not null default 0,
  contacts_processed int not null default 0,
  error text,
  started_at timestamptz,
  finished_at timestamptz,
  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists sales_hubspot_tal_touch_jobs_tal_idx on public.sales_hubspot_tal_touch_jobs(tal_list_id, updated_at desc);

create table if not exists public.sales_hubspot_tal_cache_jobs (
  id uuid primary key default gen_random_uuid(),
  tal_list_id text not null,
  hypothesis_id uuid references public.sales_hypotheses(id) on delete set null,
  status text not null default 'queued', -- queued|running|done|failed
  phase text not null default 'memberships', -- memberships|contacts_list|contact_deals|company_contacts|company_deals|deal_contacts|finalize
  contacts_list_id text, -- optional HubSpot contacts list id
  -- HubSpot memberships paging cursor (string)
  memberships_after text,
  contacts_after text,
  -- Cursor for company_id processing (bigint ids)
  last_company_id bigint not null default 0,
  -- Cursor for contact_id processing (bigint ids)
  last_contact_id bigint not null default 0,
  -- Cursor for deal_id processing (bigint ids)
  last_deal_id bigint not null default 0,
  -- Progress counters (best-effort)
  companies_total int,
  companies_processed int not null default 0,
  contacts_list_processed int not null default 0,
  deals_processed int not null default 0,
  contacts_processed int not null default 0,
  error text,
  started_at timestamptz,
  finished_at timestamptz,
  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Migration safety: add contacts_list_id for existing installs.
alter table if exists public.sales_hubspot_tal_cache_jobs
  add column if not exists contacts_list_id text;
-- Migration safety: add contacts_after for existing installs.
alter table if exists public.sales_hubspot_tal_cache_jobs
  add column if not exists contacts_after text;
-- Migration safety: add contacts_list_processed for existing installs.
alter table if exists public.sales_hubspot_tal_cache_jobs
  add column if not exists contacts_list_processed int not null default 0;
-- Migration safety: add last_contact_id for existing installs.
alter table if exists public.sales_hubspot_tal_cache_jobs
  add column if not exists last_contact_id bigint not null default 0;

create index if not exists sales_hubspot_tal_cache_jobs_tal_idx on public.sales_hubspot_tal_cache_jobs(tal_list_id, updated_at desc);
create index if not exists sales_hubspot_tal_cache_jobs_hypothesis_idx on public.sales_hubspot_tal_cache_jobs(hypothesis_id, updated_at desc);

-- ======================
-- HubSpot global snapshots (derived, weekly history for the whole pipeline)
-- ======================
-- Purpose:
-- - Store weekly funnel metrics (lead/opp/customer/churn) for ALL deals in HubSpot
-- - Provide "unassigned" bucket when a deal is not mapped to any active hypothesis TAL
create table if not exists public.sales_hubspot_global_snapshots (
  id uuid primary key default gen_random_uuid(),
  period_start date not null,
  period_end date not null,
  window_days int not null default 7,
  -- funnel totals
  new_leads_count int,
  new_opps_count int,
  new_customers_count int,
  new_churn_count int,
  -- breakdowns for stacked charts
  funnel_by_channel_json jsonb not null default '{}'::jsonb,
  funnel_by_hypothesis_json jsonb not null default '{}'::jsonb, -- includes "__unassigned__"
  -- debug payload for troubleshooting
  data_json jsonb not null default '{}'::jsonb,
  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (period_start, window_days)
);

create index if not exists sales_hubspot_global_snapshots_period_idx on public.sales_hubspot_global_snapshots(period_start desc);

-- ======================
-- HubSpot global daily snapshots (derived, daily history for the whole funnel pipeline)
-- ======================
-- Purpose:
-- - Store daily "new deals created" counts for ALL deals in the selected funnel pipeline
-- - Enable seasonality analysis and attribution by channel/hypothesis without re-querying HubSpot
create table if not exists public.sales_hubspot_global_daily_snapshots (
  id uuid primary key default gen_random_uuid(),
  period_day date not null,      -- day start (UTC)
  pipeline_id text not null,     -- HubSpot pipeline id
  new_deals_count int,
  -- Net change of ACTIVE deals for this day (A: excludes Lost/Dormant). Can be negative.
  active_delta_count int,
  -- breakdowns for stacked charts
  new_deals_by_channel_json jsonb not null default '{}'::jsonb,
  new_deals_by_hypothesis_json jsonb not null default '{}'::jsonb, -- includes "__unassigned__"
  -- debug payload for troubleshooting / UI table view caching (optional)
  data_json jsonb not null default '{}'::jsonb,
  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (period_day, pipeline_id)
);

create index if not exists sales_hubspot_global_daily_snapshots_day_idx on public.sales_hubspot_global_daily_snapshots(period_day desc);
create index if not exists sales_hubspot_global_daily_snapshots_pipeline_idx on public.sales_hubspot_global_daily_snapshots(pipeline_id, period_day desc);

-- Backward compatible migration (for projects that already created the table earlier):
do $$
begin
  if exists (
    select 1
    from information_schema.tables
    where table_schema = 'public'
      and table_name = 'sales_hubspot_global_daily_snapshots'
  ) then
    if not exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'sales_hubspot_global_daily_snapshots'
        and column_name = 'active_delta_count'
    ) then
      alter table public.sales_hubspot_global_daily_snapshots
        add column active_delta_count int;
    end if;
  end if;
end $$;

-- ======================
-- GetSales -> HubSpot activities sync (derived)
-- ======================
-- Purpose:
-- - Store a deduplicated log of GetSales events that were pushed to HubSpot (per user)
-- - Enable incremental sync by persisting a cursor (last synced timestamp)

create table if not exists public.sales_getsales_sync_state (
  created_by uuid primary key default auth.uid(),
  last_synced_at timestamptz, -- max processed activity timestamp (best-effort)
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.sales_getsales_events (
  id uuid primary key default gen_random_uuid(),
  source text not null, -- 'email' | 'linkedin'
  getsales_uuid text not null,
  lead_uuid text,
  contact_email text,
  occurred_at timestamptz,
  hubspot_contact_id text,
  hubspot_engagement_id bigint,
  payload jsonb not null default '{}'::jsonb,
  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (created_by, source, getsales_uuid)
);

create index if not exists sales_getsales_events_occ_idx on public.sales_getsales_events(created_by, occurred_at desc);
create index if not exists sales_getsales_events_occurred_at_idx on public.sales_getsales_events(occurred_at desc);

-- ======================
-- SmartLead -> HubSpot (derived)
-- ======================
-- Purpose:
-- - When a HubSpot Deal enters SQL stage, enroll associated Contact(s) into a SmartLead campaign
-- - When SmartLead lead finishes (status=COMPLETED), attach outcome as HubSpot NOTE on the deal/contact
-- - Provide incremental cron sync via persisted cursors + strong idempotency (no duplicate enrolls)

create table if not exists public.sales_smartlead_sync_state (
  created_by uuid primary key default auth.uid(),
  last_sql_synced_at timestamptz, -- last processed HubSpot SQL transition time (best-effort)
  last_completed_synced_at timestamptz, -- last processed SmartLead completion event time (best-effort)
  last_events_synced_at timestamptz, -- last processed SmartLead activity timestamp (email events; best-effort)
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Migration safety: add new cursor column if the table already existed.
alter table if exists public.sales_smartlead_sync_state
  add column if not exists last_events_synced_at timestamptz;

create table if not exists public.sales_smartlead_enrollments (
  id uuid primary key default gen_random_uuid(),
  smartlead_campaign_id int not null,
  hubspot_deal_id text not null,
  hubspot_contact_id text not null,
  contact_email text,
  -- SmartLead uses leadMapId for per-lead sequence details endpoint.
  smartlead_lead_map_id text,
  status text not null default 'enrolled', -- enrolled|completed|failed|skipped
  sql_entered_at timestamptz,
  enrolled_at timestamptz,
  completed_at timestamptz,
  hubspot_engagement_id bigint, -- NOTE engagement id (legacy engagements API)
  error text,
  raw_enroll_response jsonb not null default '{}'::jsonb,
  raw_completed_payload jsonb not null default '{}'::jsonb,
  raw_sequence_details jsonb not null default '{}'::jsonb,
  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (created_by, smartlead_campaign_id, hubspot_deal_id, hubspot_contact_id)
);

create index if not exists sales_smartlead_enrollments_status_idx on public.sales_smartlead_enrollments(created_by, status, updated_at desc);
create index if not exists sales_smartlead_enrollments_deal_idx on public.sales_smartlead_enrollments(created_by, hubspot_deal_id);
create index if not exists sales_smartlead_enrollments_email_idx on public.sales_smartlead_enrollments(created_by, smartlead_campaign_id, contact_email);

-- SmartLead email activity events (sent/open/reply etc) for accurate reporting + attribution
create table if not exists public.sales_smartlead_events (
  id uuid primary key default gen_random_uuid(),
  smartlead_event_id text not null, -- idempotency key derived from campaign/lead/time/type
  smartlead_campaign_id int,
  smartlead_lead_map_id text,
  contact_email text,
  event_type text not null, -- sent|opened|replied|bounced|unsubscribed|other
  occurred_at timestamptz,
  payload jsonb not null default '{}'::jsonb,
  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (created_by, smartlead_event_id)
);

-- Migration safety: create table if not exists does not add new columns.
-- These are optional linkage fields used by the portal UI (events table links + "Only pushed" filter).
alter table if exists public.sales_smartlead_events
  add column if not exists hubspot_contact_id text;
alter table if exists public.sales_smartlead_events
  add column if not exists hubspot_engagement_id bigint;

create index if not exists sales_smartlead_events_occ_idx on public.sales_smartlead_events(created_by, occurred_at desc);
create index if not exists sales_smartlead_events_email_idx on public.sales_smartlead_events(created_by, contact_email);
create index if not exists sales_smartlead_events_campaign_idx on public.sales_smartlead_events(created_by, smartlead_campaign_id, occurred_at desc);

-- ======================
-- Website contact forms -> HubSpot (derived)
-- ======================
-- Purpose:
-- - Website writes inbound leads into public.contact_form_leads
-- - Portal sync job reads from that table and creates HubSpot Contact + Deal (lead)
-- - This table stores per-lead sync status to avoid duplicates and to support retries
--
-- NOTE: contact_form_leads is created elsewhere in some environments; keep a best-effort definition here.
create table if not exists public.contact_form_leads (
  id bigint generated by default as identity primary key,
  created_at timestamptz not null default now(),
  first_name text,
  last_name text,
  corporate_email text,
  company text,
  reason_for_contact text,
  referral_source text,
  source text,
  user_id text,
  remote_address text
);

-- Migration safety: create table if not exists does not add new columns.
alter table if exists public.contact_form_leads
  add column if not exists referral_source text;
alter table if exists public.contact_form_leads
  add column if not exists source text;
alter table if exists public.contact_form_leads
  add column if not exists user_id text;
alter table if exists public.contact_form_leads
  add column if not exists remote_address text;

create table if not exists public.contact_form_leads_hubspot (
  lead_id bigint primary key references public.contact_form_leads(id) on delete cascade,
  hubspot_contact_id text,
  hubspot_deal_id text,
  hubspot_task_id bigint,
  status text not null default 'queued', -- queued|processing|done|failed
  attempts int not null default 0,
  error text,
  synced_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists contact_form_leads_hubspot_status_idx on public.contact_form_leads_hubspot(status, updated_at desc);

-- Hypothesis <-> ICP segments (hypothesis can include multiple segments)
create table if not exists public.sales_hypothesis_segments (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  segment_id uuid not null references public.sales_icp_segments(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, segment_id)
);

create index if not exists sales_hypothesis_segments_segment_idx on public.sales_hypothesis_segments(segment_id);

-- ======================
-- VP per hypothesis (new model)
-- ======================

create table if not exists public.sales_hypothesis_roles (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  role_id uuid not null references public.sales_icp_roles(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, role_id)
);

create index if not exists sales_hypothesis_roles_role_idx on public.sales_hypothesis_roles(role_id);

create table if not exists public.sales_hypothesis_company_profiles (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  company_profile_id uuid not null references public.sales_icp_company_profiles(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, company_profile_id)
);

create index if not exists sales_hypothesis_company_profiles_company_idx on public.sales_hypothesis_company_profiles(company_profile_id);

create table if not exists public.sales_hypothesis_vps (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  role_id uuid not null references public.sales_icp_roles(id) on delete cascade,
  company_profile_id uuid not null references public.sales_icp_company_profiles(id) on delete cascade,
  vp_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (hypothesis_id, role_id, company_profile_id)
);

create index if not exists sales_hypothesis_vps_lookup_idx on public.sales_hypothesis_vps(role_id, company_profile_id, updated_at desc);

-- Pain points per hypothesis (Role x CompanyProfile). Two multiline text blocks per segment.
create table if not exists public.sales_hypothesis_pains (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  role_id uuid not null references public.sales_icp_roles(id) on delete cascade,
  company_profile_id uuid not null references public.sales_icp_company_profiles(id) on delete cascade,
  pain_json jsonb not null default '{}'::jsonb, -- { pain_points: string, product_solution: string }
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (hypothesis_id, role_id, company_profile_id)
);

create index if not exists sales_hypothesis_pains_lookup_idx on public.sales_hypothesis_pains(role_id, company_profile_id, updated_at desc);

-- Hypothesis <-> Metrics (hypothesis can include multiple metrics)
create table if not exists public.sales_hypothesis_metrics (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  metric_id uuid not null references public.sales_metrics(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, metric_id)
);

create index if not exists sales_hypothesis_metrics_metric_idx on public.sales_hypothesis_metrics(metric_id);

-- Hypothesis <-> Channel <-> Metrics (channel-specific metrics for this hypothesis)
create table if not exists public.sales_hypothesis_channel_metrics (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  channel_id uuid not null references public.sales_channels(id) on delete cascade,
  metric_id uuid not null references public.sales_metrics(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, channel_id, metric_id)
);

create index if not exists sales_hypothesis_channel_metrics_channel_idx on public.sales_hypothesis_channel_metrics(channel_id);
create index if not exists sales_hypothesis_channel_metrics_metric_idx on public.sales_hypothesis_channel_metrics(metric_id);

-- Hypothesis <-> Channel owners (responsibles). Multiple owners per channel.
create table if not exists public.sales_hypothesis_channel_owners (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  channel_id uuid not null references public.sales_channels(id) on delete cascade,
  owner_email text not null,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, channel_id, owner_email)
);

create index if not exists sales_hypothesis_channel_owners_channel_idx on public.sales_hypothesis_channel_owners(channel_id);
create index if not exists sales_hypothesis_channel_owners_owner_idx on public.sales_hypothesis_channel_owners(lower(owner_email));

-- Hypothesis <-> Channel <-> Metric owners (responsible for a specific metric). Multiple owners per metric.
create table if not exists public.sales_hypothesis_channel_metric_owners (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  channel_id uuid not null references public.sales_channels(id) on delete cascade,
  metric_id uuid not null references public.sales_metrics(id) on delete cascade,
  owner_email text not null,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, channel_id, metric_id, owner_email)
);

create index if not exists sales_hypothesis_channel_metric_owners_channel_idx on public.sales_hypothesis_channel_metric_owners(channel_id);
create index if not exists sales_hypothesis_channel_metric_owners_metric_idx on public.sales_hypothesis_channel_metric_owners(metric_id);
create index if not exists sales_hypothesis_channel_metric_owners_owner_idx on public.sales_hypothesis_channel_metric_owners(lower(owner_email));

-- Triggers (requires public.set_updated_at() from Calls schema)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_hypotheses_updated_at') then
    create trigger trg_sales_hypotheses_updated_at
    before update on public.sales_hypotheses
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_checkins_updated_at') then
    create trigger trg_sales_checkins_updated_at
    before update on public.sales_hypothesis_checkins
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_hubspot_snapshots_updated_at') then
    create trigger trg_sales_hubspot_snapshots_updated_at
    before update on public.sales_hubspot_tal_snapshots
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_hubspot_global_snapshots_updated_at') then
    create trigger trg_sales_hubspot_global_snapshots_updated_at
    before update on public.sales_hubspot_global_snapshots
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_hubspot_global_daily_snapshots_updated_at') then
    create trigger trg_sales_hubspot_global_daily_snapshots_updated_at
    before update on public.sales_hubspot_global_daily_snapshots
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_icp_roles_updated_at') then
    create trigger trg_sales_icp_roles_updated_at
    before update on public.sales_icp_roles
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_verticals_updated_at') then
    create trigger trg_sales_verticals_updated_at
    before update on public.sales_verticals
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_subverticals_updated_at') then
    create trigger trg_sales_subverticals_updated_at
    before update on public.sales_subverticals
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_company_scales_updated_at') then
    create trigger trg_sales_company_scales_updated_at
    before update on public.sales_company_scales
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_icp_company_profiles_updated_at') then
    create trigger trg_sales_icp_company_profiles_updated_at
    before update on public.sales_icp_company_profiles
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_icp_segments_updated_at') then
    create trigger trg_sales_icp_segments_updated_at
    before update on public.sales_icp_segments
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_channels_updated_at') then
    create trigger trg_sales_channels_updated_at
    before update on public.sales_channels
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_metrics_updated_at') then
    create trigger trg_sales_metrics_updated_at
    before update on public.sales_metrics
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_hypothesis_vps_updated_at') then
    create trigger trg_sales_hypothesis_vps_updated_at
    before update on public.sales_hypothesis_vps
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_hypothesis_pains_updated_at') then
    create trigger trg_sales_hypothesis_pains_updated_at
    before update on public.sales_hypothesis_pains
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_hypothesis_rows_updated_at') then
    create trigger trg_sales_hypothesis_rows_updated_at
    before update on public.sales_hypothesis_rows
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_getsales_sync_state_updated_at') then
    create trigger trg_sales_getsales_sync_state_updated_at
    before update on public.sales_getsales_sync_state
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_getsales_events_updated_at') then
    create trigger trg_sales_getsales_events_updated_at
    before update on public.sales_getsales_events
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_smartlead_sync_state_updated_at') then
    create trigger trg_sales_smartlead_sync_state_updated_at
    before update on public.sales_smartlead_sync_state
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_smartlead_enrollments_updated_at') then
    create trigger trg_sales_smartlead_enrollments_updated_at
    before update on public.sales_smartlead_enrollments
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_smartlead_events_updated_at') then
    create trigger trg_sales_smartlead_events_updated_at
    before update on public.sales_smartlead_events
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_contact_form_leads_hubspot_updated_at') then
    create trigger trg_contact_form_leads_hubspot_updated_at
    before update on public.contact_form_leads_hubspot
    for each row execute function public.set_updated_at();
  end if;
end $$;

-- RLS
alter table public.sales_icp_roles enable row level security;
alter table public.sales_verticals enable row level security;
alter table public.sales_subverticals enable row level security;
alter table public.sales_company_scales enable row level security;
alter table public.sales_icp_company_profiles enable row level security;
alter table public.sales_icp_segments enable row level security;
alter table public.sales_channels enable row level security;
alter table public.sales_hypotheses enable row level security;
alter table public.sales_hypothesis_checkins enable row level security;
alter table public.sales_hypothesis_calls enable row level security;
alter table public.sales_hypothesis_rows enable row level security;
alter table public.sales_hypothesis_segments enable row level security;
alter table public.sales_metrics enable row level security;
alter table public.sales_hypothesis_metrics enable row level security;
alter table public.sales_hypothesis_roles enable row level security;
alter table public.sales_hypothesis_company_profiles enable row level security;
alter table public.sales_hypothesis_vps enable row level security;
alter table public.sales_hypothesis_pains enable row level security;
alter table public.sales_hypothesis_channel_metrics enable row level security;
alter table public.sales_hypothesis_channel_owners enable row level security;
alter table public.sales_hypothesis_channel_metric_owners enable row level security;
alter table public.sales_hubspot_tal_snapshots enable row level security;
alter table public.sales_hubspot_global_snapshots enable row level security;
alter table public.sales_hubspot_global_daily_snapshots enable row level security;
alter table public.sales_hubspot_tal_companies enable row level security;
alter table public.sales_hubspot_company_deals enable row level security;
alter table public.sales_hubspot_deal_contacts enable row level security;
alter table public.sales_hubspot_company_contacts enable row level security;
alter table public.sales_hubspot_tal_contacts enable row level security;
alter table public.sales_hubspot_tal_cache_jobs enable row level security;
alter table public.sales_hubspot_contact_touches enable row level security;
alter table public.sales_hubspot_company_touches enable row level security;
alter table public.sales_hubspot_tal_touch_jobs enable row level security;

-- Migration safety: add new columns to touch jobs table if it existed earlier.
alter table if exists public.sales_hubspot_tal_touch_jobs
  add column if not exists last_company_id bigint;
alter table if exists public.sales_hubspot_tal_touch_jobs
  add column if not exists companies_processed int;
alter table public.sales_getsales_sync_state enable row level security;
alter table public.sales_getsales_events enable row level security;
alter table public.sales_smartlead_sync_state enable row level security;
alter table public.sales_smartlead_enrollments enable row level security;
alter table public.sales_smartlead_events enable row level security;
alter table public.contact_form_leads_hubspot enable row level security;

-- Helper: can edit hypothesis
--
-- IMPORTANT (Jan 2026):
-- We intentionally allow ANY authenticated user to edit Sales hypotheses and related rows
-- (VP / pains / channels / metrics), to reduce friction for collaborative work.
-- Destructive actions are guarded separately (see can_delete_sales_hypothesis + trigger below).
create or replace function public.can_edit_sales_hypothesis(p_hypothesis_id uuid)
returns boolean
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  -- We still keep this helper (instead of replacing all policies) because many RLS policies
  -- reference it across multiple hypothesis-related tables.
  return auth.role() = 'authenticated';
end;
$$;

grant execute on function public.can_edit_sales_hypothesis(uuid) to anon, authenticated;

-- Helper: can delete hypothesis (owner or admin).
--
-- We keep delete restricted even though edits are collaborative, because deletes are destructive
-- and hard to undo.
create or replace function public.can_delete_sales_hypothesis(p_hypothesis_id uuid)
returns boolean
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  ok boolean;
begin
  perform set_config('row_security', 'off', true);
  select true into ok
  from public.sales_hypotheses h
  where h.id = p_hypothesis_id
    and (h.owner_user_id = auth.uid() or public.is_admin())
  limit 1;
  return coalesce(ok, false);
end;
$$;

grant execute on function public.can_delete_sales_hypothesis(uuid) to anon, authenticated;

-- Guardrail: only admins can change hypothesis owner_user_id.
--
-- RLS alone cannot restrict changes to a single column. This trigger prevents a non-admin
-- from taking ownership of someone else's hypothesis (or removing the owner).
create or replace function public.sales_hypotheses_guard_owner_change()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.owner_user_id is distinct from old.owner_user_id then
    if not public.is_admin() then
      raise exception 'Only admins can change hypothesis owner_user_id';
    end if;
  end if;
  return new;
end;
$$;

drop trigger if exists trg_sales_hypotheses_guard_owner_change on public.sales_hypotheses;
create trigger trg_sales_hypotheses_guard_owner_change
before update on public.sales_hypotheses
for each row
execute function public.sales_hypotheses_guard_owner_change();

-- Helper: can submit weekly check-in (owner/admin OR responsible for a channel OR responsible for a channel-metric)
create or replace function public.can_submit_sales_checkin(p_hypothesis_id uuid)
returns boolean
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  ok boolean;
  email text;
begin
  perform set_config('row_security', 'off', true);
  email := lower(coalesce(auth.jwt() ->> 'email', ''));
  if email = '' then
    return false;
  end if;

  if public.can_edit_sales_hypothesis(p_hypothesis_id) then
    return true;
  end if;

  select true into ok
  from public.sales_hypothesis_channel_owners o
  where o.hypothesis_id = p_hypothesis_id
    and lower(o.owner_email) = email
  limit 1;
  if coalesce(ok, false) then
    return true;
  end if;

  select true into ok
  from public.sales_hypothesis_channel_metric_owners mo
  where mo.hypothesis_id = p_hypothesis_id
    and lower(mo.owner_email) = email
  limit 1;

  return coalesce(ok, false);
end;
$$;

grant execute on function public.can_submit_sales_checkin(uuid) to anon, authenticated;

-- HubSpot TAL cache helpers (server-side exact counts + batching)
create or replace function public.sales_hubspot_tal_exact_counts(p_tal_list_id text)
returns table (companies_count int, deals_count int, contacts_count int)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  return query
    with list_contacts as (
      select tl.contact_id
      from public.sales_hubspot_tal_contacts tl
      where tl.tal_list_id = p_tal_list_id
    ),
    list_exists as (
      select exists(select 1 from list_contacts) as has_list
    ),
    base_contacts as (
      select lc.contact_id
      from list_contacts lc
      where (select has_list from list_exists)
      union
      select cc.contact_id
      from public.sales_hubspot_tal_companies tc
      join public.sales_hubspot_company_contacts cc on cc.company_id = tc.company_id
      where tc.tal_list_id = p_tal_list_id
        and not (select has_list from list_exists)
      union
      select dc.contact_id
      from public.sales_hubspot_tal_companies tc
      join public.sales_hubspot_company_deals cd on cd.company_id = tc.company_id
      join public.sales_hubspot_deal_contacts dc on dc.deal_id = cd.deal_id
      where tc.tal_list_id = p_tal_list_id
        and not (select has_list from list_exists)
    ),
    base_deals as (
      select cd.deal_id
      from public.sales_hubspot_tal_companies tc
      join public.sales_hubspot_company_deals cd on cd.company_id = tc.company_id
      where tc.tal_list_id = p_tal_list_id
      union
      select dc.deal_id
      from list_contacts lc
      join public.sales_hubspot_deal_contacts dc on dc.contact_id = lc.contact_id
      where (select has_list from list_exists)
    )
    select
      (select count(*)::int from public.sales_hubspot_tal_companies tc where tc.tal_list_id = p_tal_list_id) as companies_count,
      (select count(distinct bd.deal_id)::int from base_deals bd) as deals_count,
      (select count(distinct bc.contact_id)::int from base_contacts bc) as contacts_count;
end;
$$;

grant execute on function public.sales_hubspot_tal_exact_counts(text) to anon, authenticated;

create or replace function public.sales_hubspot_tal_next_deals(p_tal_list_id text, p_last_deal_id bigint, p_limit int)
returns table (deal_id bigint)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  return query
    with list_contacts as (
      select tl.contact_id
      from public.sales_hubspot_tal_contacts tl
      where tl.tal_list_id = p_tal_list_id
    ),
    list_exists as (
      select exists(select 1 from list_contacts) as has_list
    ),
    base_deals as (
      select cd.deal_id
      from public.sales_hubspot_tal_companies tc
      join public.sales_hubspot_company_deals cd on cd.company_id = tc.company_id
      where tc.tal_list_id = p_tal_list_id
      union
      select dc.deal_id
      from list_contacts lc
      join public.sales_hubspot_deal_contacts dc on dc.contact_id = lc.contact_id
      where (select has_list from list_exists)
    )
    select distinct bd.deal_id
    from base_deals bd
    where bd.deal_id > coalesce(p_last_deal_id, 0)
    order by bd.deal_id asc
    limit greatest(1, least(coalesce(p_limit, 50), 500));
end;
$$;

grant execute on function public.sales_hubspot_tal_next_deals(text, bigint, int) to anon, authenticated;

create or replace function public.sales_hubspot_tal_next_company_deals(p_tal_list_id text, p_last_deal_id bigint, p_limit int)
returns table (deal_id bigint)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  return query
    with base_deals as (
      select cd.deal_id
      from public.sales_hubspot_tal_companies tc
      join public.sales_hubspot_company_deals cd on cd.company_id = tc.company_id
      where tc.tal_list_id = p_tal_list_id
    )
    select distinct bd.deal_id
    from base_deals bd
    where bd.deal_id > coalesce(p_last_deal_id, 0)
    order by bd.deal_id asc
    limit greatest(1, least(coalesce(p_limit, 50), 500));
end;
$$;

grant execute on function public.sales_hubspot_tal_next_company_deals(text, bigint, int) to anon, authenticated;

create or replace function public.sales_hubspot_tal_next_contacts(p_tal_list_id text, p_last_contact_id bigint, p_limit int)
returns table (contact_id bigint)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  return query
    with list_contacts as (
      select tl.contact_id
      from public.sales_hubspot_tal_contacts tl
      where tl.tal_list_id = p_tal_list_id
    ),
    list_exists as (
      select exists(select 1 from list_contacts) as has_list
    ),
    base_contacts as (
      select lc.contact_id
      from list_contacts lc
      where (select has_list from list_exists)
      union
      select cc.contact_id
      from public.sales_hubspot_tal_companies tc
      join public.sales_hubspot_company_contacts cc on cc.company_id = tc.company_id
      where tc.tal_list_id = p_tal_list_id
        and not (select has_list from list_exists)
    )
    select distinct bc.contact_id
    from base_contacts bc
    where bc.contact_id > coalesce(p_last_contact_id, 0)
    order by bc.contact_id asc
    limit greatest(1, least(coalesce(p_limit, 50), 500));
end;
$$;

grant execute on function public.sales_hubspot_tal_next_contacts(text, bigint, int) to anon, authenticated;

create or replace function public.sales_hubspot_tal_touch_counts(p_tal_list_id text, p_since timestamptz)
returns table (touched_contacts_count int, touched_companies_count int)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  return query
    with tal_companies as (
      select tc.company_id
      from public.sales_hubspot_tal_companies tc
      where tc.tal_list_id = p_tal_list_id
    ),
    list_contacts as (
      select tl.contact_id
      from public.sales_hubspot_tal_contacts tl
      where tl.tal_list_id = p_tal_list_id
    ),
    list_exists as (
      select exists(select 1 from list_contacts) as has_list
    ),
    tal_contacts as (
      select distinct lc.contact_id
      from list_contacts lc
      where (select has_list from list_exists)
      union
      select distinct cc.contact_id
      from tal_companies tc
      join public.sales_hubspot_company_contacts cc on cc.company_id = tc.company_id
      where not (select has_list from list_exists)
    ),
    touched_contacts as (
      select ct.contact_id
      from tal_contacts tc
      join public.sales_hubspot_contact_touches ct on ct.contact_id = tc.contact_id
      where ct.last_touch_at >= p_since
    ),
    touched_companies as (
      select distinct cc.company_id
      from tal_companies tc
      join public.sales_hubspot_company_contacts cc on cc.company_id = tc.company_id
      join tal_contacts tcon on tcon.contact_id = cc.contact_id
      join public.sales_hubspot_contact_touches ct on ct.contact_id = cc.contact_id
      where ct.last_touch_at >= p_since
    )
    select
      (select count(*)::int from touched_contacts) as touched_contacts_count,
      (select count(*)::int from touched_companies) as touched_companies_count;
end;
$$;

grant execute on function public.sales_hubspot_tal_touch_counts(text, timestamptz) to anon, authenticated;

create or replace function public.sales_hubspot_tal_recent_contact_touches(p_tal_list_id text, p_since timestamptz, p_limit int)
returns table (contact_id bigint, company_id bigint, last_touch_at timestamptz, last_touch_type text)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  return query
    with list_contacts as (
      select tl.contact_id
      from public.sales_hubspot_tal_contacts tl
      where tl.tal_list_id = p_tal_list_id
    ),
    list_exists as (
      select exists(select 1 from list_contacts) as has_list
    ),
    base_contacts as (
      select lc.contact_id
      from list_contacts lc
      where (select has_list from list_exists)
      union
      select cc.contact_id
      from public.sales_hubspot_tal_companies tc
      join public.sales_hubspot_company_contacts cc on cc.company_id = tc.company_id
      where tc.tal_list_id = p_tal_list_id
        and not (select has_list from list_exists)
    )
    select
      ct.contact_id,
      cc.company_id,
      ct.last_touch_at,
      ct.last_touch_type
    from base_contacts bc
    join public.sales_hubspot_contact_touches ct on ct.contact_id = bc.contact_id
    left join public.sales_hubspot_company_contacts cc on cc.contact_id = ct.contact_id
    where ct.last_touch_at is not null
      and ct.last_touch_at >= p_since
    order by ct.last_touch_at desc
    limit greatest(1, least(coalesce(p_limit, 10), 50));
end;
$$;

grant execute on function public.sales_hubspot_tal_recent_contact_touches(text, timestamptz, int) to anon, authenticated;

create or replace function public.sales_hubspot_tal_recent_company_touches(p_tal_list_id text, p_since timestamptz, p_limit int)
returns table (company_id bigint, last_touch_at timestamptz, last_touch_type text)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  return query
    with list_contacts as (
      select tl.contact_id
      from public.sales_hubspot_tal_contacts tl
      where tl.tal_list_id = p_tal_list_id
    ),
    list_exists as (
      select exists(select 1 from list_contacts) as has_list
    ),
    base_contacts as (
      select lc.contact_id
      from list_contacts lc
      where (select has_list from list_exists)
      union
      select cc.contact_id
      from public.sales_hubspot_tal_companies tc
      join public.sales_hubspot_company_contacts cc on cc.company_id = tc.company_id
      where tc.tal_list_id = p_tal_list_id
        and not (select has_list from list_exists)
    ),
    ranked as (
      select
        cc.company_id,
        ct.last_touch_at,
        ct.last_touch_type,
        row_number() over (partition by cc.company_id order by ct.last_touch_at desc) as rn
      from base_contacts bc
      join public.sales_hubspot_company_contacts cc on cc.contact_id = bc.contact_id
      join public.sales_hubspot_contact_touches ct on ct.contact_id = bc.contact_id
      where ct.last_touch_at >= p_since
    )
    select ranked.company_id, ranked.last_touch_at, ranked.last_touch_type
    from ranked
    where ranked.rn = 1
    order by ranked.last_touch_at desc
    limit greatest(1, least(coalesce(p_limit, 10), 50));
end;
$$;

grant execute on function public.sales_hubspot_tal_recent_company_touches(text, timestamptz, int) to anon, authenticated;

create or replace function public.sales_hubspot_tal_contacted_counts(p_tal_list_id text, p_since timestamptz)
returns table (contacted_contacts_count int, contacted_companies_count int)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  return query
    with tal_companies as (
      select tc.company_id
      from public.sales_hubspot_tal_companies tc
      where tc.tal_list_id = p_tal_list_id
    ),
    tal_contacts as (
      select distinct cc.contact_id
      from tal_companies tc
      join public.sales_hubspot_company_contacts cc on cc.company_id = tc.company_id
    ),
    active_contacts as (
      select distinct (e.hubspot_contact_id::bigint) as contact_id
      from public.sales_getsales_events e
      where e.occurred_at >= p_since
        and e.hubspot_contact_id is not null
        and e.hubspot_contact_id ~ '^[0-9]+$'
      union
      select distinct (e.hubspot_contact_id::bigint) as contact_id
      from public.sales_smartlead_events e
      where e.occurred_at >= p_since
        and e.hubspot_contact_id is not null
        and e.hubspot_contact_id ~ '^[0-9]+$'
      union
      select distinct (e.hubspot_contact_id::bigint) as contact_id
      from public.sales_smartlead_enrollments e
      where coalesce(e.completed_at, e.updated_at) >= p_since
        and e.hubspot_contact_id is not null
        and e.hubspot_contact_id ~ '^[0-9]+$'
    ),
    contacted_contacts as (
      select ac.contact_id
      from active_contacts ac
      join tal_contacts tc on tc.contact_id = ac.contact_id
    ),
    contacted_companies as (
      select distinct cc.company_id
      from public.sales_hubspot_company_contacts cc
      join tal_companies tco on tco.company_id = cc.company_id
      join contacted_contacts ctc on ctc.contact_id = cc.contact_id
    )
    select
      (select count(*)::int from contacted_contacts) as contacted_contacts_count,
      (select count(*)::int from contacted_companies) as contacted_companies_count;
end;
$$;

grant execute on function public.sales_hubspot_tal_contacted_counts(text, timestamptz) to anon, authenticated;

-- Policies
-- ICP library: readable by authenticated; writable by authenticated (MVP). Tighten later if needed.
drop policy if exists sales_icp_roles_select on public.sales_icp_roles;
create policy sales_icp_roles_select on public.sales_icp_roles
for select using (auth.role() = 'authenticated');
drop policy if exists sales_icp_roles_write on public.sales_icp_roles;
create policy sales_icp_roles_write on public.sales_icp_roles
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_verticals_select on public.sales_verticals;
create policy sales_verticals_select on public.sales_verticals
for select using (auth.role() = 'authenticated');
drop policy if exists sales_verticals_write on public.sales_verticals;
create policy sales_verticals_write on public.sales_verticals
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_subverticals_select on public.sales_subverticals;
create policy sales_subverticals_select on public.sales_subverticals
for select using (auth.role() = 'authenticated');
drop policy if exists sales_subverticals_write on public.sales_subverticals;
create policy sales_subverticals_write on public.sales_subverticals
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_company_scales_select on public.sales_company_scales;
create policy sales_company_scales_select on public.sales_company_scales
for select using (auth.role() = 'authenticated');
drop policy if exists sales_company_scales_write on public.sales_company_scales;
create policy sales_company_scales_write on public.sales_company_scales
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_icp_company_select on public.sales_icp_company_profiles;
create policy sales_icp_company_select on public.sales_icp_company_profiles
for select using (auth.role() = 'authenticated');
drop policy if exists sales_icp_company_write on public.sales_icp_company_profiles;
create policy sales_icp_company_write on public.sales_icp_company_profiles
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_icp_segments_select on public.sales_icp_segments;
create policy sales_icp_segments_select on public.sales_icp_segments
for select using (auth.role() = 'authenticated');
drop policy if exists sales_icp_segments_write on public.sales_icp_segments;
create policy sales_icp_segments_write on public.sales_icp_segments
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_channels_select on public.sales_channels;
create policy sales_channels_select on public.sales_channels
for select using (auth.role() = 'authenticated');
drop policy if exists sales_channels_write on public.sales_channels;
create policy sales_channels_write on public.sales_channels
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_metrics_select on public.sales_metrics;
create policy sales_metrics_select on public.sales_metrics
for select using (auth.role() = 'authenticated');
drop policy if exists sales_metrics_write on public.sales_metrics;
create policy sales_metrics_write on public.sales_metrics
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_hypotheses_select on public.sales_hypotheses;
create policy sales_hypotheses_select
on public.sales_hypotheses
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hypotheses_insert on public.sales_hypotheses;
create policy sales_hypotheses_insert
on public.sales_hypotheses
for insert
with check (auth.role() = 'authenticated' and owner_user_id = auth.uid());

drop policy if exists sales_hypotheses_update on public.sales_hypotheses;
create policy sales_hypotheses_update
on public.sales_hypotheses
for update
using (public.can_edit_sales_hypothesis(id))
with check (public.can_edit_sales_hypothesis(id));

drop policy if exists sales_hypotheses_delete on public.sales_hypotheses;
create policy sales_hypotheses_delete
on public.sales_hypotheses
for delete
using (public.can_delete_sales_hypothesis(id));

drop policy if exists sales_checkins_select on public.sales_hypothesis_checkins;
create policy sales_checkins_select
on public.sales_hypothesis_checkins
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_checkins_insert on public.sales_hypothesis_checkins;
create policy sales_checkins_insert
on public.sales_hypothesis_checkins
for insert
with check (auth.role() = 'authenticated' and public.can_submit_sales_checkin(hypothesis_id));

drop policy if exists sales_checkins_update on public.sales_hypothesis_checkins;
create policy sales_checkins_update
on public.sales_hypothesis_checkins
for update
using (public.can_submit_sales_checkin(hypothesis_id))
with check (public.can_submit_sales_checkin(hypothesis_id));

drop policy if exists sales_checkins_delete on public.sales_hypothesis_checkins;
create policy sales_checkins_delete
on public.sales_hypothesis_checkins
for delete
using (public.can_delete_sales_hypothesis(hypothesis_id));

drop policy if exists sales_hypothesis_calls_select on public.sales_hypothesis_calls;
create policy sales_hypothesis_calls_select
on public.sales_hypothesis_calls
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hypothesis_calls_insert on public.sales_hypothesis_calls;
create policy sales_hypothesis_calls_insert
on public.sales_hypothesis_calls
for insert
with check (
  auth.role() = 'authenticated'
  and public.can_submit_sales_checkin(hypothesis_id)
  and public.can_read_call(call_id)
);

drop policy if exists sales_hypothesis_calls_delete on public.sales_hypothesis_calls;
create policy sales_hypothesis_calls_delete
on public.sales_hypothesis_calls
for delete
using (
  public.can_edit_sales_hypothesis(hypothesis_id)
  and public.can_read_call(call_id)
);

-- HubSpot snapshots: readable by authenticated; writable by check-in submitters (owner/channel owners/metric owners)
drop policy if exists sales_hubspot_snapshots_select on public.sales_hubspot_tal_snapshots;
create policy sales_hubspot_snapshots_select
on public.sales_hubspot_tal_snapshots
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_snapshots_insert on public.sales_hubspot_tal_snapshots;
create policy sales_hubspot_snapshots_insert
on public.sales_hubspot_tal_snapshots
for insert
with check (auth.role() = 'authenticated' and public.can_submit_sales_checkin(hypothesis_id));

drop policy if exists sales_hubspot_snapshots_update on public.sales_hubspot_tal_snapshots;
create policy sales_hubspot_snapshots_update
on public.sales_hubspot_tal_snapshots
for update
using (public.can_submit_sales_checkin(hypothesis_id))
with check (public.can_submit_sales_checkin(hypothesis_id));

drop policy if exists sales_hubspot_snapshots_delete on public.sales_hubspot_tal_snapshots;
create policy sales_hubspot_snapshots_delete
on public.sales_hubspot_tal_snapshots
for delete
using (public.can_delete_sales_hypothesis(hypothesis_id));

-- HubSpot TAL cache: readable by authenticated; written by service role only (no write policies).
drop policy if exists sales_hubspot_tal_companies_select on public.sales_hubspot_tal_companies;
create policy sales_hubspot_tal_companies_select
on public.sales_hubspot_tal_companies
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_company_deals_select on public.sales_hubspot_company_deals;
create policy sales_hubspot_company_deals_select
on public.sales_hubspot_company_deals
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_deals_select on public.sales_hubspot_deals;
create policy sales_hubspot_deals_select
on public.sales_hubspot_deals
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_deal_contacts_select on public.sales_hubspot_deal_contacts;
create policy sales_hubspot_deal_contacts_select
on public.sales_hubspot_deal_contacts
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_company_contacts_select on public.sales_hubspot_company_contacts;
create policy sales_hubspot_company_contacts_select
on public.sales_hubspot_company_contacts
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_tal_contacts_select on public.sales_hubspot_tal_contacts;
create policy sales_hubspot_tal_contacts_select
on public.sales_hubspot_tal_contacts
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_contact_touches_select on public.sales_hubspot_contact_touches;
create policy sales_hubspot_contact_touches_select
on public.sales_hubspot_contact_touches
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_company_touches_select on public.sales_hubspot_company_touches;
create policy sales_hubspot_company_touches_select
on public.sales_hubspot_company_touches
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_tal_touch_jobs_select on public.sales_hubspot_tal_touch_jobs;
create policy sales_hubspot_tal_touch_jobs_select
on public.sales_hubspot_tal_touch_jobs
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_tal_cache_jobs_select on public.sales_hubspot_tal_cache_jobs;
create policy sales_hubspot_tal_cache_jobs_select
on public.sales_hubspot_tal_cache_jobs
for select
using (auth.role() = 'authenticated');

-- HubSpot global snapshots: readable/writable by authenticated (MVP). Tighten later if needed.
drop policy if exists sales_hubspot_global_snapshots_select on public.sales_hubspot_global_snapshots;
create policy sales_hubspot_global_snapshots_select
on public.sales_hubspot_global_snapshots
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_global_snapshots_write on public.sales_hubspot_global_snapshots;
create policy sales_hubspot_global_snapshots_write
on public.sales_hubspot_global_snapshots
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

-- HubSpot global daily snapshots: readable/writable by authenticated (MVP). Tighten later if needed.
drop policy if exists sales_hubspot_global_daily_snapshots_select on public.sales_hubspot_global_daily_snapshots;
create policy sales_hubspot_global_daily_snapshots_select
on public.sales_hubspot_global_daily_snapshots
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_hubspot_global_daily_snapshots_write on public.sales_hubspot_global_daily_snapshots;
create policy sales_hubspot_global_daily_snapshots_write
on public.sales_hubspot_global_daily_snapshots
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

-- GetSales sync state: per-user (cursor). Read/write only own rows.
drop policy if exists sales_getsales_sync_state_select on public.sales_getsales_sync_state;
create policy sales_getsales_sync_state_select
on public.sales_getsales_sync_state
for select
using (auth.role() = 'authenticated' and created_by = auth.uid());

drop policy if exists sales_getsales_sync_state_write on public.sales_getsales_sync_state;
create policy sales_getsales_sync_state_write
on public.sales_getsales_sync_state
for all
using (auth.role() = 'authenticated' and created_by = auth.uid())
with check (auth.role() = 'authenticated' and created_by = auth.uid());

drop policy if exists sales_getsales_events_select on public.sales_getsales_events;
create policy sales_getsales_events_select
on public.sales_getsales_events
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_getsales_events_write on public.sales_getsales_events;
create policy sales_getsales_events_write
on public.sales_getsales_events
for all
using (auth.role() = 'authenticated' and created_by = auth.uid())
with check (auth.role() = 'authenticated' and created_by = auth.uid());

-- SmartLead sync state + enrollments: per-user. Read/write only own rows.
drop policy if exists sales_smartlead_sync_state_select on public.sales_smartlead_sync_state;
create policy sales_smartlead_sync_state_select
on public.sales_smartlead_sync_state
for select
using (auth.role() = 'authenticated' and created_by = auth.uid());

drop policy if exists sales_smartlead_sync_state_write on public.sales_smartlead_sync_state;
create policy sales_smartlead_sync_state_write
on public.sales_smartlead_sync_state
for all
using (auth.role() = 'authenticated' and created_by = auth.uid())
with check (auth.role() = 'authenticated' and created_by = auth.uid());

drop policy if exists sales_smartlead_enrollments_select on public.sales_smartlead_enrollments;
create policy sales_smartlead_enrollments_select
on public.sales_smartlead_enrollments
for select
using (auth.role() = 'authenticated' and created_by = auth.uid());

drop policy if exists sales_smartlead_enrollments_write on public.sales_smartlead_enrollments;
create policy sales_smartlead_enrollments_write
on public.sales_smartlead_enrollments
for all
using (auth.role() = 'authenticated' and created_by = auth.uid())
with check (auth.role() = 'authenticated' and created_by = auth.uid());

-- SmartLead email events: company-wide readable (CEO-friendly); writable only for own created_by (sync uses service role)
drop policy if exists sales_smartlead_events_select on public.sales_smartlead_events;
create policy sales_smartlead_events_select
on public.sales_smartlead_events
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_smartlead_events_write on public.sales_smartlead_events;
create policy sales_smartlead_events_write
on public.sales_smartlead_events
for all
using (auth.role() = 'authenticated' and created_by = auth.uid())
with check (auth.role() = 'authenticated' and created_by = auth.uid());

-- Website contact form -> HubSpot sync log: admin-only (portal uses service role for sync)
drop policy if exists contact_form_leads_hubspot_select on public.contact_form_leads_hubspot;
create policy contact_form_leads_hubspot_select
on public.contact_form_leads_hubspot
for select
using (public.is_admin());

drop policy if exists contact_form_leads_hubspot_write on public.contact_form_leads_hubspot;
create policy contact_form_leads_hubspot_write
on public.contact_form_leads_hubspot
for all
using (public.is_admin())
with check (public.is_admin());

-- Hypothesis segments: readable by authenticated; writable by hypothesis editor
drop policy if exists sales_hypothesis_segments_select on public.sales_hypothesis_segments;
create policy sales_hypothesis_segments_select on public.sales_hypothesis_segments
for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_segments_insert on public.sales_hypothesis_segments;
create policy sales_hypothesis_segments_insert on public.sales_hypothesis_segments
for insert
with check (auth.role() = 'authenticated' and public.can_edit_sales_hypothesis(hypothesis_id));
drop policy if exists sales_hypothesis_segments_delete on public.sales_hypothesis_segments;
create policy sales_hypothesis_segments_delete on public.sales_hypothesis_segments
for delete
using (public.can_edit_sales_hypothesis(hypothesis_id));

drop policy if exists sales_hypothesis_rows_select on public.sales_hypothesis_rows;
create policy sales_hypothesis_rows_select on public.sales_hypothesis_rows
for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_rows_insert on public.sales_hypothesis_rows;
create policy sales_hypothesis_rows_insert on public.sales_hypothesis_rows
for insert
with check (auth.role() = 'authenticated' and public.can_edit_sales_hypothesis(workspace_id));
drop policy if exists sales_hypothesis_rows_update on public.sales_hypothesis_rows;
create policy sales_hypothesis_rows_update on public.sales_hypothesis_rows
for update
using (public.can_edit_sales_hypothesis(workspace_id))
with check (public.can_edit_sales_hypothesis(workspace_id));
drop policy if exists sales_hypothesis_rows_delete on public.sales_hypothesis_rows;
create policy sales_hypothesis_rows_delete on public.sales_hypothesis_rows
for delete
using (public.can_edit_sales_hypothesis(workspace_id));

drop policy if exists sales_hypothesis_metrics_select on public.sales_hypothesis_metrics;
create policy sales_hypothesis_metrics_select on public.sales_hypothesis_metrics
for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_metrics_insert on public.sales_hypothesis_metrics;
create policy sales_hypothesis_metrics_insert on public.sales_hypothesis_metrics
for insert
with check (auth.role() = 'authenticated' and public.can_edit_sales_hypothesis(hypothesis_id));
drop policy if exists sales_hypothesis_metrics_delete on public.sales_hypothesis_metrics;
create policy sales_hypothesis_metrics_delete on public.sales_hypothesis_metrics
for delete
using (public.can_edit_sales_hypothesis(hypothesis_id));

drop policy if exists sales_hypothesis_channel_metrics_select on public.sales_hypothesis_channel_metrics;
create policy sales_hypothesis_channel_metrics_select on public.sales_hypothesis_channel_metrics
for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_channel_metrics_insert on public.sales_hypothesis_channel_metrics;
create policy sales_hypothesis_channel_metrics_insert on public.sales_hypothesis_channel_metrics
for insert
with check (auth.role() = 'authenticated' and public.can_edit_sales_hypothesis(hypothesis_id));
drop policy if exists sales_hypothesis_channel_metrics_delete on public.sales_hypothesis_channel_metrics;
create policy sales_hypothesis_channel_metrics_delete on public.sales_hypothesis_channel_metrics
for delete
using (public.can_edit_sales_hypothesis(hypothesis_id));

drop policy if exists sales_hypothesis_channel_owners_select on public.sales_hypothesis_channel_owners;
create policy sales_hypothesis_channel_owners_select on public.sales_hypothesis_channel_owners
for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_channel_owners_insert on public.sales_hypothesis_channel_owners;
create policy sales_hypothesis_channel_owners_insert on public.sales_hypothesis_channel_owners
for insert
with check (auth.role() = 'authenticated' and public.can_edit_sales_hypothesis(hypothesis_id));
drop policy if exists sales_hypothesis_channel_owners_delete on public.sales_hypothesis_channel_owners;
create policy sales_hypothesis_channel_owners_delete on public.sales_hypothesis_channel_owners
for delete
using (public.can_edit_sales_hypothesis(hypothesis_id));

drop policy if exists sales_hypothesis_channel_metric_owners_select on public.sales_hypothesis_channel_metric_owners;
create policy sales_hypothesis_channel_metric_owners_select on public.sales_hypothesis_channel_metric_owners
for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_channel_metric_owners_insert on public.sales_hypothesis_channel_metric_owners;
create policy sales_hypothesis_channel_metric_owners_insert on public.sales_hypothesis_channel_metric_owners
for insert
with check (auth.role() = 'authenticated' and public.can_edit_sales_hypothesis(hypothesis_id));
drop policy if exists sales_hypothesis_channel_metric_owners_delete on public.sales_hypothesis_channel_metric_owners;
create policy sales_hypothesis_channel_metric_owners_delete on public.sales_hypothesis_channel_metric_owners
for delete
using (public.can_edit_sales_hypothesis(hypothesis_id));

drop policy if exists sales_hypothesis_roles_select on public.sales_hypothesis_roles;
create policy sales_hypothesis_roles_select on public.sales_hypothesis_roles
for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_roles_insert on public.sales_hypothesis_roles;
create policy sales_hypothesis_roles_insert on public.sales_hypothesis_roles
for insert
with check (auth.role() = 'authenticated' and public.can_edit_sales_hypothesis(hypothesis_id));
drop policy if exists sales_hypothesis_roles_delete on public.sales_hypothesis_roles;
create policy sales_hypothesis_roles_delete on public.sales_hypothesis_roles
for delete
using (public.can_edit_sales_hypothesis(hypothesis_id));

drop policy if exists sales_hypothesis_company_profiles_select on public.sales_hypothesis_company_profiles;
create policy sales_hypothesis_company_profiles_select on public.sales_hypothesis_company_profiles
for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_company_profiles_insert on public.sales_hypothesis_company_profiles;
create policy sales_hypothesis_company_profiles_insert on public.sales_hypothesis_company_profiles
for insert
with check (auth.role() = 'authenticated' and public.can_edit_sales_hypothesis(hypothesis_id));
drop policy if exists sales_hypothesis_company_profiles_delete on public.sales_hypothesis_company_profiles;
create policy sales_hypothesis_company_profiles_delete on public.sales_hypothesis_company_profiles
for delete
using (public.can_edit_sales_hypothesis(hypothesis_id));

drop policy if exists sales_hypothesis_vps_select on public.sales_hypothesis_vps;
create policy sales_hypothesis_vps_select on public.sales_hypothesis_vps
for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_vps_upsert on public.sales_hypothesis_vps;
create policy sales_hypothesis_vps_upsert on public.sales_hypothesis_vps
for all
using (public.can_edit_sales_hypothesis(hypothesis_id))
with check (public.can_edit_sales_hypothesis(hypothesis_id));

drop policy if exists sales_hypothesis_pains_select on public.sales_hypothesis_pains;
create policy sales_hypothesis_pains_select on public.sales_hypothesis_pains
for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_pains_upsert on public.sales_hypothesis_pains;
create policy sales_hypothesis_pains_upsert on public.sales_hypothesis_pains
for all
using (public.can_edit_sales_hypothesis(hypothesis_id))
with check (public.can_edit_sales_hypothesis(hypothesis_id));

-- ============================================================================
-- Migration: TAL deals from cache (open deals)
-- ============================================================================
-- Date: 2026-01-30
-- Purpose: Return TAL deals directly from cached HubSpot data (no activity requirement)
-- ============================================================================

create or replace function public.sales_tal_deals_from_cache(
  p_tal_list_id text,
  p_pipeline_ids text[] default null,
  p_tal_category text default null,
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
  v_limit int := greatest(1, least(coalesce(p_limit, 200), 2000));
begin
  perform set_config('row_security', 'off', true);

  if p_tal_list_id is null or p_tal_list_id = '' then
    return;
  end if;

  return query
    with tal_contacts as (
      select contact_id
      from public.sales_hubspot_tal_contacts
      where tal_list_id = p_tal_list_id
    ),
    tal_contacts_present as (
      select count(*) > 0 as has_contacts from tal_contacts
    ),
    tal_contact_ids as (
      select contact_id
      from tal_contacts
      where (select has_contacts from tal_contacts_present)
      union
      select distinct cc.contact_id
      from public.sales_hubspot_tal_companies tc
      join public.sales_hubspot_company_contacts cc on cc.company_id = tc.company_id
      where tc.tal_list_id = p_tal_list_id
        and not (select has_contacts from tal_contacts_present)
    ),
    distinct_deals as (
      select distinct dc.deal_id
      from public.sales_hubspot_deal_contacts dc
      join tal_contact_ids t on t.contact_id = dc.contact_id
      where dc.deal_id is not null
    ),
    base as (
      -- If TAL category is provided, trust it as the source of truth.
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
      where p_tal_category is not null
        and p_tal_category <> ''
        and lower(p_tal_category) = any(
          array_remove(
            string_to_array(lower(coalesce(d.tal_category, '')), ';'),
            ''
          )
        )
      union
      -- Otherwise fall back to TAL list membership (contacts/companies).
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
      where p_tal_category is null or p_tal_category = ''
    )
    select *
    from base b
    where p_pipeline_ids is null
      or array_length(p_pipeline_ids, 1) is null
      or b.pipeline_id = any(p_pipeline_ids)
    order by deal_id desc
    limit v_limit;
end;
$$;

create or replace function public.sales_tal_deal_counts_from_cache(
  p_tal_list_id text,
  p_pipeline_ids text[] default null,
  p_tal_category text default null
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
begin
  perform set_config('row_security', 'off', true);

  if p_tal_list_id is null or p_tal_list_id = '' then
    return;
  end if;

  return query
    with tal_contacts as (
      select contact_id
      from public.sales_hubspot_tal_contacts
      where tal_list_id = p_tal_list_id
    ),
    tal_contacts_present as (
      select count(*) > 0 as has_contacts from tal_contacts
    ),
    tal_contact_ids as (
      select contact_id
      from tal_contacts
      where (select has_contacts from tal_contacts_present)
      union
      select distinct cc.contact_id
      from public.sales_hubspot_tal_companies tc
      join public.sales_hubspot_company_contacts cc on cc.company_id = tc.company_id
      where tc.tal_list_id = p_tal_list_id
        and not (select has_contacts from tal_contacts_present)
    ),
    distinct_deals as (
      select distinct dc.deal_id
      from public.sales_hubspot_deal_contacts dc
      join tal_contact_ids t on t.contact_id = dc.contact_id
      where dc.deal_id is not null
    ),
    base as (
      -- If TAL category is provided, trust it as the source of truth.
      select d.deal_id,
             lower(coalesce(d.stage_category, '')) as stage_category,
             d.pipeline_id
      from public.sales_hubspot_deals d
      where p_tal_category is not null
        and p_tal_category <> ''
        and lower(p_tal_category) = any(
          array_remove(
            string_to_array(lower(coalesce(d.tal_category, '')), ';'),
            ''
          )
        )
      union
      -- Otherwise fall back to TAL list membership (contacts/companies).
      select d.deal_id,
             lower(coalesce(d.stage_category, '')) as stage_category,
             d.pipeline_id
      from public.sales_hubspot_deals d
      join distinct_deals dd on dd.deal_id = d.deal_id
      where p_tal_category is null or p_tal_category = ''
    ),
    filtered as (
      select deal_id, stage_category
      from base b
      where p_pipeline_ids is null
        or array_length(p_pipeline_ids, 1) is null
        or b.pipeline_id = any(p_pipeline_ids)
    )
    select
      count(*)::int as deals_count,
      count(*) filter (where stage_category = 'lead')::int as leads_count,
      count(*) filter (where stage_category = 'opportunity')::int as opps_count
    from filtered;
end;
$$;
