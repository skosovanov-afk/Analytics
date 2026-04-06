-- ============================================================================
-- Shared API Tokens - Zero-config setup for MCP users
-- ============================================================================
-- Purpose: Store shared API tokens (HubSpot, Anthropic, Composio) in Supabase
--          so users don't need to configure .env files
-- Date: 2026-01-23
-- Owner: Egor Moskvin
-- ============================================================================

-- Create shared_api_tokens table (if not exists)
create table if not exists public.shared_api_tokens (
  key text primary key,
  value text not null,
  description text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- Add updated_at column if missing (for existing tables)
alter table public.shared_api_tokens 
  add column if not exists updated_at timestamptz default now();

-- Enable RLS
alter table public.shared_api_tokens enable row level security;

-- Write policies intentionally omitted: token management is admin-only via service role key.

-- RLS Policy: Only authorized users can read tokens
-- (founders, sales, admin teams)
drop policy if exists "Allow read for authorized users" on public.shared_api_tokens;
create policy "Allow read for authorized users"
  on public.shared_api_tokens
  for select
  using (
    exists (
      select 1 from public.team_members tm
      join public.teams t on tm.team_id = t.id
      where tm.user_id = auth.uid()
      and t.slug in ('founders', 'sales', 'admin', 'company')
    )
  );

-- ============================================================================
-- RPC: get_shared_api_tokens (SECURITY DEFINER)
-- ============================================================================
-- Purpose: Safely fetch API tokens for authorized users
-- Security: Runs with elevated privileges, checks user's team membership
-- Returns: Array of {key, value} objects

create or replace function public.get_shared_api_tokens(
  p_keys text[] default null
)
returns table (
  key text,
  value text
)
language plpgsql
security definer -- Run with elevated privileges
set search_path = public
as $$
begin
  -- Check if user is authorized (same logic as RLS policy)
  if not exists (
    select 1 from public.team_members tm
    join public.teams t on tm.team_id = t.id
    where tm.user_id = auth.uid()
    and t.slug in ('founders', 'sales', 'admin', 'company')
  ) then
    raise exception 'Not authorized to access shared API tokens';
  end if;

  -- Return requested tokens (or all tokens if p_keys is null)
  return query
    select t.key, t.value
    from public.shared_api_tokens t
    where p_keys is null or t.key = any(p_keys);
end;
$$;

-- Grant execute to authenticated users
grant execute on function public.get_shared_api_tokens(text[]) to authenticated;

-- ============================================================================
-- Sample data (run manually by admin)
-- ============================================================================
-- To populate tokens, admin should run:
-- 
-- insert into public.shared_api_tokens (key, value, description) values
--   ('HUBSPOT_PRIVATE_APP_TOKEN', 'pat-na1-...', 'HubSpot API for 8C qualification'),
--   ('ANTHROPIC_API_KEY', 'sk-ant-...', 'Claude for LLM analysis'),
--   ('COMPOSIO_API_KEY', 'ak_...', 'Composio for Slack notifications'),
--   ('COMPOSIO_USER_ID', 'pg-test-...', 'Composio user ID for Slack')
-- on conflict (key) do update
--   set value = excluded.value, updated_at = now();
