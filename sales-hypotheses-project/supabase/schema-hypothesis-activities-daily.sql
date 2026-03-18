-- RPC to aggregate DAILY activity stats (emails/linkedin) for a hypothesis TAL
-- Grouped by day, filtered by TAL contact association.
--
-- Usage: select * from sales_hypothesis_activity_stats_daily('12345', '2025-01-01', '2025-01-31');

drop function if exists public.sales_hypothesis_activity_stats_daily(text, timestamptz, timestamptz);

create or replace function public.sales_hypothesis_activity_stats_daily(
  p_tal_list_id text,
  p_since timestamptz,
  p_until timestamptz default now()
)
returns table (
  day text,
  emails_sent_count int,
  linkedin_sent_count int,
  replies_count int
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_hyp_id uuid;
begin
  perform set_config('row_security', 'off', true);
  
  if p_tal_list_id is null or p_tal_list_id = '' then
    return;
  end if;

  -- Find the internal hypothesis ID for this TAL list
  select id into v_hyp_id 
  from public.sales_hypotheses 
  where substring(hubspot_tal_url from '/(?:lists|objectLists)/([0-9]+)') = p_tal_list_id
  limit 1;

  if v_hyp_id is null then
    return;
  end if;

  return query
    with daily_agg as (
      select 
        date_trunc('day', occurred_at)::date as d,
        count(*) filter (where activity_type = 'email' and direction = 'outbound')::int as emails_sent_count,
        count(*) filter (where activity_type in ('linkedin', 'linkedin_connection') and direction = 'outbound')::int as linkedin_sent_count,
        count(*) filter (where direction = 'inbound')::int as replies_count
      from public.sales_analytics_activities
      where hypothesis_id = v_hyp_id
        and occurred_at >= p_since 
        and occurred_at <= p_until
      group by 1
    ),
    -- Generate series of days to ensure no gaps
    days as (
      select generate_series(p_since, p_until, '1 day'::interval)::date as d
    )
    select 
      to_char(days.d, 'YYYY-MM-DD') as day,
      coalesce(da.emails_sent_count, 0) as emails_sent_count,
      coalesce(da.linkedin_sent_count, 0) as linkedin_sent_count,
      coalesce(da.replies_count, 0) as replies_count
    from days
    left join daily_agg da on da.d = days.d
    order by days.d;
end;
$$;

grant execute on function public.sales_hypothesis_activity_stats_daily(text, timestamptz, timestamptz) to anon, authenticated;
