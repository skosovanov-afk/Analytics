-- ============================================================================
-- 8C Auto-Qualification System - Database Schema
-- ============================================================================
-- Purpose: Automatic 8C qualification for client calls with RAG context,
--          HubSpot integration, and Slack notifications
-- Date: 2026-01-21
-- Owner: Egor Moskvin
--
-- This schema extends the Calls system (99-applications/calls/supabase/schema.sql)
-- and Sales hypotheses system (schema-hypotheses.sql) with 8C qualification support.
-- ============================================================================

-- ============================================================================
-- Migration: Add category column to calls table
-- ============================================================================
-- Date: 2026-01-21
-- Purpose: Store call category directly in calls table for efficient filtering
--          Category is synced from AI outputs (fireflies_ai_outputs.response_json->>'call_category')

-- Add category column
alter table public.calls add column if not exists category text;

-- Add index for efficient filtering by category
create index if not exists calls_category_idx on public.calls(category) where category is not null;

-- Backfill existing categories from AI outputs
update public.calls c
set category = (
  select o.response_json->>'call_category'
  from public.fireflies_ai_outputs o
  where o.call_id = c.id
  order by o.source_created_at desc nulls last, o.created_at desc
  limit 1
)
where c.category is null
  and exists (
    select 1 from public.fireflies_ai_outputs o
    where o.call_id = c.id
      and o.response_json->>'call_category' is not null
  );

-- Auto-sync category when AI output is created/updated
-- This ensures calls.category stays in sync with the latest AI categorization
create or replace function sync_call_category_from_ai()
returns trigger
language plpgsql security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  
  update public.calls
  set category = NEW.response_json->>'call_category',
      updated_at = now()
  where id = NEW.call_id;
  
  return NEW;
end;
$$;

drop trigger if exists trg_sync_call_category on public.fireflies_ai_outputs;
create trigger trg_sync_call_category
  after insert or update of response_json
  on public.fireflies_ai_outputs
  for each row
  when (NEW.response_json ? 'call_category')
  execute function sync_call_category_from_ai();

-- ============================================================================
-- Migration: Add summary_main column to calls table
-- ============================================================================
-- Date: 2026-01-21
-- Purpose: Store call summary directly in calls table for efficient RAG context
--          Summary is synced from AI outputs (fireflies_ai_outputs.response_json->>'summary_main')

-- Add summary_main column
alter table public.calls add column if not exists summary_main text;

-- Add index for efficient access
create index if not exists calls_summary_main_idx on public.calls(id) where summary_main is not null;

-- Backfill existing summaries from AI outputs
update public.calls c
set summary_main = (
  select o.response_json->>'summary_main'
  from public.fireflies_ai_outputs o
  where o.call_id = c.id
  order by o.source_created_at desc nulls last, o.created_at desc
  limit 1
)
where c.summary_main is null
  and exists (
    select 1 from public.fireflies_ai_outputs o
    where o.call_id = c.id
      and o.response_json->>'summary_main' is not null
  );

-- Auto-sync summary_main when AI output is created/updated
-- This ensures calls.summary_main stays in sync with the latest AI summary
create or replace function sync_call_summary_from_ai()
returns trigger
language plpgsql security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  
  update public.calls
  set summary_main = NEW.response_json->>'summary_main',
      updated_at = now()
  where id = NEW.call_id;
  
  return NEW;
end;
$$;

drop trigger if exists trg_sync_call_summary on public.fireflies_ai_outputs;
create trigger trg_sync_call_summary
  after insert or update of response_json
  on public.fireflies_ai_outputs
  for each row
  when (NEW.response_json ? 'summary_main')
  execute function sync_call_summary_from_ai();

-- ============================================================================
-- Table: 8C Qualification Results
-- ============================================================================
-- Stores the results of each 8C qualification attempt for a call
-- Tracks scores, evidence, gaps, and generated tasks
-- One qualification per call per deal (unique constraint)
-- ============================================================================

create table if not exists public.sales_8c_qualifications (
  id uuid primary key default gen_random_uuid(),
  
  -- Foreign keys
  call_id uuid not null references public.calls(id) on delete cascade,
  hubspot_deal_id text not null,
  
  -- Raw scores (0, 3, or 5 per criterion)
  score_compelling_event int check (score_compelling_event in (0, 3, 5)),
  score_stakeholder int check (score_stakeholder in (0, 3, 5)),
  score_funding int check (score_funding in (0, 3, 5)),
  score_challenges int check (score_challenges in (0, 3, 5)),
  score_value_drivers int check (score_value_drivers in (0, 3, 5)),
  score_solution int check (score_solution in (0, 3, 5)),
  score_competitors int check (score_competitors in (0, 3, 5)),
  score_partners int check (score_partners in (0, 3, 5)),
  
  -- Weighted scores (actual points per methodology)
  weighted_compelling_event int check (weighted_compelling_event in (0, 15, 25)),
  weighted_stakeholder int check (weighted_stakeholder in (0, 9, 15)),
  weighted_funding int check (weighted_funding in (0, 9, 15)),
  weighted_challenges int check (weighted_challenges in (0, 15, 25)),
  weighted_value_drivers int check (weighted_value_drivers in (0, 9, 15)),
  weighted_solution int check (weighted_solution in (0, 9, 15)),
  weighted_competitors int check (weighted_competitors in (0, 15, 25)),
  weighted_partners int check (weighted_partners in (0, 3, 5)),
  
  -- Totals
  total_score int not null check (total_score >= 0 and total_score <= 140),
  percentage int not null check (percentage >= 0 and percentage <= 100),
  qualified boolean not null, -- true if percentage >= 64
  
  -- LLM analysis output (full JSON from 8C analyzer)
  analysis_json jsonb not null,
  
  -- RAG context metadata (which calls/transcripts were used)
  rag_context_json jsonb,
  
  -- HubSpot integration
  hubspot_task_id text, -- created task ID
  hubspot_task_url text, -- task URL for Sales
  hubspot_synced_at timestamptz, -- when properties were synced to HubSpot
  
  -- Metadata
  created_by uuid not null, -- user who triggered qualification
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  
  -- Constraint: one qualification per call per deal
  unique (call_id, hubspot_deal_id)
);

-- Index for fast lookup by call
create index if not exists idx_8c_qualifications_call_id 
  on public.sales_8c_qualifications(call_id);

-- Index for fast lookup by deal
create index if not exists idx_8c_qualifications_hubspot_deal_id 
  on public.sales_8c_qualifications(hubspot_deal_id);

-- Index for qualified deals
create index if not exists idx_8c_qualifications_qualified 
  on public.sales_8c_qualifications(qualified, total_score desc);

-- Updated_at trigger
create or replace function update_8c_qualification_updated_at()
returns trigger
language plpgsql
as $$
begin
  NEW.updated_at = now();
  return NEW;
end;
$$;

drop trigger if exists set_8c_qualification_updated_at on public.sales_8c_qualifications;
create trigger set_8c_qualification_updated_at
  before update on public.sales_8c_qualifications
  for each row
  execute function update_8c_qualification_updated_at();

-- RLS: users can read their own qualifications and qualifications for calls they have access to
alter table public.sales_8c_qualifications enable row level security;

create policy "Users can read 8C qualifications for accessible calls"
  on public.sales_8c_qualifications for select
  using (
    exists (
      select 1 from public.calls c
      where c.id = call_id
        and public.can_read_call(c.id)
    )
  );

create policy "Users can insert 8C qualifications for accessible calls"
  on public.sales_8c_qualifications for insert
  with check (
    exists (
      select 1 from public.calls c
      where c.id = call_id
        and public.can_read_call(c.id)
    )
  );

create policy "Users can update their own 8C qualifications"
  on public.sales_8c_qualifications for update
  using (created_by = auth.uid());

-- ============================================================================
-- Table: 8C Qualification Queue
-- ============================================================================
-- Queue for async processing of client calls
-- Background processor polls this table every 5 minutes
-- ============================================================================

create table if not exists public.sales_8c_qualification_queue (
  id uuid primary key default gen_random_uuid(),
  call_id uuid not null unique references public.calls(id) on delete cascade,
  
  -- Processing status
  status text not null default 'pending' check (status in ('pending', 'processing', 'completed', 'failed', 'needs_manual_selection')),
  
  -- Error tracking
  error_message text,
  retry_count int not null default 0,
  max_retries int not null default 3,
  
  -- Timestamps
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  
  -- Priority (lower number = higher priority)
  priority int not null default 100
);

-- Index for background processor query (oldest pending first)
create index if not exists idx_8c_queue_pending 
  on public.sales_8c_qualification_queue(status, priority, created_at)
  where status = 'pending';

-- RLS: only admins and service accounts can manage queue
alter table public.sales_8c_qualification_queue enable row level security;

create policy "Service accounts can manage 8C queue"
  on public.sales_8c_qualification_queue for all
  using (
    exists (
      select 1 from public.user_profiles
      where user_id = auth.uid()
        and role_slug in ('admin', 'service')
    )
  );

-- ============================================================================
-- Trigger: Auto-add client calls to qualification queue
-- ============================================================================
-- Automatically enqueues client calls for 8C qualification
-- Fires when: new call with category='client' and transcript is present
-- ============================================================================

create or replace function trigger_8c_qualification_on_client_call()
returns trigger
language plpgsql security definer
as $$
begin
  -- Only process client calls with transcript
  if NEW.category = 'client' and NEW.transcript_text is not null and length(trim(NEW.transcript_text)) > 100 then
    -- Enqueue for async processing (avoid blocking webhook)
    insert into public.sales_8c_qualification_queue (call_id)
    values (NEW.id)
    on conflict (call_id) do nothing; -- idempotent
  end if;
  
  return NEW;
end;
$$;

-- Attach trigger to calls table
drop trigger if exists auto_8c_qualification_trigger on public.calls;
create trigger auto_8c_qualification_trigger
  after insert or update of transcript_text, category
  on public.calls
  for each row
  execute function trigger_8c_qualification_on_client_call();

-- ============================================================================
-- RPC: Find calls by company domain
-- ============================================================================
-- Returns all calls with participants from a specific company domain
-- Used for RAG context gathering (company history)
-- ============================================================================

create or replace function find_calls_by_company_domain(domain text)
returns table (
  call_id uuid,
  title text,
  occurred_at timestamptz,
  transcript_text text,
  summary_main text
)
language plpgsql security definer
as $$
begin
  return query
  select distinct
    c.id,
    c.title,
    c.occurred_at,
    c.transcript_text,
    c.summary_main
  from public.calls c
  join public.call_participants cp on cp.call_id = c.id
  where cp.email ilike '%@' || domain
    and c.transcript_text is not null
    and public.can_read_call(c.id)
  order by c.occurred_at desc;
end;
$$;

-- ============================================================================
-- RPC: Find calls by participant emails
-- ============================================================================
-- Returns all calls with any of the specified participant emails
-- Used for RAG context gathering (contact history)
-- ============================================================================

create or replace function find_calls_by_participant_emails(emails text[])
returns table (
  call_id uuid,
  title text,
  occurred_at timestamptz,
  transcript_text text,
  summary_main text
)
language plpgsql security definer
as $$
begin
  return query
  select distinct
    c.id,
    c.title,
    c.occurred_at,
    c.transcript_text,
    c.summary_main
  from public.calls c
  join public.call_participants cp on cp.call_id = c.id
  where cp.email = any(emails)
    and c.transcript_text is not null
    and public.can_read_call(c.id)
  order by c.occurred_at desc;
end;
$$;

-- ============================================================================
-- RPC: Get RAG context for 8C qualification
-- ============================================================================
-- Gathers all relevant calls for RAG context with limits
-- Returns current call + deal history + company history + contact history
-- ============================================================================

create or replace function get_8c_rag_context(
  p_call_id uuid,
  p_hubspot_deal_id text default null,
  p_max_deal_calls int default 10,
  p_max_company_calls int default 5,
  p_max_contact_calls int default 5
)
returns jsonb
language plpgsql security definer
as $$
declare
  v_current_call jsonb;
  v_deal_calls jsonb;
  v_company_calls jsonb;
  v_contact_calls jsonb;
  v_participant_emails text[];
  v_company_domains text[];
begin
  -- Get current call
  select jsonb_build_object(
    'call_id', c.id,
    'title', c.title,
    'occurred_at', c.occurred_at,
    'transcript_text', c.transcript_text,
    'summary_main', c.summary_main,
    'category', c.category
  )
  into v_current_call
  from public.calls c
  where c.id = p_call_id;

  -- Extract participant emails and company domains
  select 
    array_agg(distinct cp.email),
    array_agg(distinct substring(cp.email from '@(.*)$'))
  into v_participant_emails, v_company_domains
  from public.call_participants cp
  where cp.call_id = p_call_id
    and cp.email not ilike '%@oversecured.com'; -- exclude internal

  -- Get deal history (if deal ID provided)
  if p_hubspot_deal_id is not null then
    select jsonb_agg(
      jsonb_build_object(
        'call_id', c.id,
        'title', c.title,
        'occurred_at', c.occurred_at,
        'transcript_text', c.transcript_text,
        'summary_main', c.summary_main
      )
      order by c.occurred_at desc
    )
    into v_deal_calls
    from public.calls c
    join public.sales_hypothesis_calls shc on shc.call_id = c.id
    where shc.hubspot_deal_id = p_hubspot_deal_id
      and c.id != p_call_id
      and c.transcript_text is not null
      and public.can_read_call(c.id)
    limit p_max_deal_calls;
  end if;

  -- Get company history
  select jsonb_agg(
    jsonb_build_object(
      'call_id', r.call_id,
      'title', r.title,
      'occurred_at', r.occurred_at,
      'transcript_text', r.transcript_text,
      'summary_main', r.summary_main
    )
  )
  into v_company_calls
  from (
    select distinct on (c.id)
      c.id as call_id,
      c.title,
      c.occurred_at,
      c.transcript_text,
      c.summary_main
    from public.calls c
    join public.call_participants cp on cp.call_id = c.id
    where substring(cp.email from '@(.*)$') = any(v_company_domains)
      and c.id != p_call_id
      and c.transcript_text is not null
      and public.can_read_call(c.id)
    order by c.id, c.occurred_at desc
    limit p_max_company_calls
  ) r;

  -- Get contact history
  select jsonb_agg(
    jsonb_build_object(
      'call_id', r.call_id,
      'title', r.title,
      'occurred_at', r.occurred_at,
      'transcript_text', r.transcript_text,
      'summary_main', r.summary_main
    )
  )
  into v_contact_calls
  from (
    select distinct on (c.id)
      c.id as call_id,
      c.title,
      c.occurred_at,
      c.transcript_text,
      c.summary_main
    from public.calls c
    join public.call_participants cp on cp.call_id = c.id
    where cp.email = any(v_participant_emails)
      and c.id != p_call_id
      and c.transcript_text is not null
      and public.can_read_call(c.id)
    order by c.id, c.occurred_at desc
    limit p_max_contact_calls
  ) r;

  -- Build final RAG context JSON
  return jsonb_build_object(
    'current_call', v_current_call,
    'deal_calls', coalesce(v_deal_calls, '[]'::jsonb),
    'company_calls', coalesce(v_company_calls, '[]'::jsonb),
    'contact_calls', coalesce(v_contact_calls, '[]'::jsonb),
    'participant_emails', v_participant_emails,
    'company_domains', v_company_domains,
    'total_calls', 
      1 + 
      coalesce(jsonb_array_length(v_deal_calls), 0) +
      coalesce(jsonb_array_length(v_company_calls), 0) +
      coalesce(jsonb_array_length(v_contact_calls), 0)
  );
end;
$$;

-- ============================================================================
-- Extension: Add hubspot_deal_id to sales_hypothesis_calls
-- ============================================================================
-- Allows linking calls to HubSpot deals for deal history RAG context
-- ============================================================================

do $$
begin
  if not exists (
    select 1 from information_schema.columns
    where table_schema = 'public'
      and table_name = 'sales_hypothesis_calls'
      and column_name = 'hubspot_deal_id'
  ) then
    alter table public.sales_hypothesis_calls
      add column hubspot_deal_id text;
    
    -- Index for fast deal history lookup
    create index idx_hypothesis_calls_hubspot_deal_id
      on public.sales_hypothesis_calls(hubspot_deal_id)
      where hubspot_deal_id is not null;
  end if;
end $$;

-- ============================================================================
-- Grants
-- ============================================================================

-- Allow authenticated users to use RPC functions
grant execute on function find_calls_by_company_domain to authenticated;
grant execute on function find_calls_by_participant_emails to authenticated;
grant execute on function get_8c_rag_context to authenticated;

-- ============================================================================
-- Verification queries (for testing after deployment)
-- ============================================================================

-- Test 1: Check table exists
-- select * from public.sales_8c_qualifications limit 1;

-- Test 2: Check queue table exists
-- select * from public.sales_8c_qualification_queue limit 1;

-- Test 3: Check RPCs exist
-- select find_calls_by_company_domain('mercadolibre.com');
-- select find_calls_by_participant_emails(array['test@example.com']);

-- Test 4: Check RAG context builder
-- select get_8c_rag_context('36ac4441-f32b-4eb4-a0af-5468ed634f97'::uuid);

-- Test 5: Check trigger is attached
-- select tgname, tgenabled from pg_trigger where tgrelid = 'public.calls'::regclass;

-- ============================================================================
-- End of schema
-- ============================================================================
