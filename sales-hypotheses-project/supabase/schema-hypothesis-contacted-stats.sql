-- RPC to calculate contacted companies/contacts from SmartLead + GetSales events
-- This replaces sales_hubspot_tal_touch_counts for accurate coverage metrics
--
-- Usage: select * from sales_hypothesis_contacted_stats('12345', '2025-01-01');

drop function if exists public.sales_hypothesis_contacted_stats(text, timestamptz);

create or replace function public.sales_hypothesis_contacted_stats(p_tal_list_id text, p_since timestamptz)
returns table (
  contacted_contacts_count int,
  contacted_companies_count int,
  total_companies_count int,
  total_contacts_count int
)
language plpgsql
stable
security definer
set search_path = public
as $$
begin
  perform set_config('row_security', 'off', true);
  
  if p_tal_list_id is null or p_tal_list_id = '' then
    return query select 0, 0, 0, 0;
    return;
  end if;

  return query
    with tal_companies as (
      select tc.company_id
      from public.sales_hubspot_tal_companies tc
      where tc.tal_list_id = p_tal_list_id
    ),
    tal_contacts as (
      select contact_id from public.sales_hubspot_tal_contacts where tal_list_id = p_tal_list_id
      union
      select contact_id from public.sales_hubspot_company_contacts cc
      join tal_companies tc on tc.company_id = cc.company_id
    ),
    contacted_contacts as (
      -- From SmartLead
      select distinct e.hubspot_contact_id::bigint as contact_id
      from public.sales_smartlead_events e
      where e.occurred_at >= p_since
        and e.event_type = 'sent'
        and exists (
           select 1 from tal_contacts tc 
           where tc.contact_id::text = e.hubspot_contact_id
        )
      union
      -- From GetSales
      select distinct e.hubspot_contact_id::bigint as contact_id
      from public.sales_getsales_events e
      where e.occurred_at >= p_since
        and e.source = 'linkedin'
        and exists (
           select 1 from tal_contacts tc 
           where tc.contact_id::text = e.hubspot_contact_id
        )
    ),
    contacted_companies as (
      select distinct cc.company_id
      from contacted_contacts con
      join public.sales_hubspot_company_contacts cc on cc.contact_id = con.contact_id
      join tal_companies tc on tc.company_id = cc.company_id
    )
    select 
      (select count(*)::int from contacted_contacts),
      (select count(*)::int from contacted_companies),
      (select count(*)::int from tal_companies),
      (select count(distinct contact_id)::int from tal_contacts);
end;
$$;

grant execute on function public.sales_hypothesis_contacted_stats(text, timestamptz) to anon, authenticated;
