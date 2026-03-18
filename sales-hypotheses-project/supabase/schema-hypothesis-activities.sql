-- RPC to aggregate activity stats (emails/linkedin) for a hypothesis TAL
-- Join sales_hubspot_tal_contacts with activity tables.
--
-- Usage: select * from sales_hypothesis_activity_stats('12345', '2025-01-01');

drop function if exists public.sales_hypothesis_activity_stats(uuid, timestamptz);

create or replace function public.sales_hypothesis_activity_stats(p_tal_list_id text, p_since timestamptz)
returns table (emails_sent_count int, linkedin_sent_count int, replies_count int)
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
    return query select 0, 0, 0;
    return;
  end if;

  -- Find the internal hypothesis ID for this TAL list
  select id into v_hyp_id 
  from public.sales_hypotheses 
  where substring(hubspot_tal_url from '/(?:lists|objectLists)/([0-9]+)') = p_tal_list_id
  limit 1;

  if v_hyp_id is null then
    return query select 0, 0, 0;
    return;
  end if;

  return query
    select 
      count(*) filter (where activity_type = 'email' and direction = 'outbound')::int as emails_sent_count,
      count(*) filter (where activity_type in ('linkedin', 'linkedin_connection') and direction = 'outbound')::int as linkedin_sent_count,
      count(*) filter (where direction = 'inbound')::int as replies_count
    from public.sales_analytics_activities
    where hypothesis_id = v_hyp_id
      and occurred_at >= p_since;
end;
$$;

grant execute on function public.sales_hypothesis_activity_stats(text, timestamptz) to anon, authenticated;
