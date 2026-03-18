-- "Single Source of Truth" table for hypothesis dashboard stats
create table if not exists public.sales_hypothesis_dashboard_stats (
  id uuid primary key default gen_random_uuid(),
  hypothesis_id text not null,
  day date not null,
  
  -- Activity metrics (filtered by TAL)
  emails_sent_count int default 0,
  linkedin_sent_count int default 0,
  replies_count int default 0,
  
  -- Funnel metrics (Snapshot for that day)
  leads_count int default 0,
  opps_count int default 0,
  
  -- Coverage metrics (Snapshot for that day)
  companies_count int default 0,
  contacts_count int default 0,
  contacted_companies_count int default 0,
  contacted_contacts_count int default 0,

  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  
  unique(hypothesis_id, day)
);

create index if not exists sales_hypothesis_dashboard_stats_day_idx on public.sales_hypothesis_dashboard_stats (hypothesis_id, day desc);

-- RPC to calculate stats for a given hypothesis (replaces on-the-fly calc)
create or replace function public.sales_hypothesis_calc_stats(p_hypothesis_id text)
returns void
language plpgsql
security definer
as $$
declare
  v_tal_list_id text;
  v_owner_id text;
  v_pipeline text;
  v_since date := (now() - interval '90 days')::date; -- Calculate last 90 days
begin
  -- 1. Get configuration
  select 
    (config->>'hubspot_tal_list_id'),
    (config->>'hubspot_owner_id'),
    (config->>'hubspot_pipeline')
  into v_tal_list_id, v_owner_id, v_pipeline
  from sales_hypotheses
  where id = p_hypothesis_id::uuid;

  if v_tal_list_id is null then 
    return; -- Cannot calc without TAL
  end if;

  -- 2. Aggregate Daily Activity (Emails, LinkedIn, Replies) from GetSales events
  -- Linked via sales_getsales_events.hubspot_contact_id -> sales_hubspot_tal_contacts.id
  insert into public.sales_hypothesis_dashboard_stats (hypothesis_id, day, emails_sent_count, linkedin_sent_count, replies_count)
  select
    p_hypothesis_id,
    e.occurred_at::date as day,
    count(*) filter (where e.source = 'email' and (e.payload->>'type') is distinct from 'reply') as emails,
    count(*) filter (where e.source = 'linkedin' and (e.payload->>'type') is distinct from 'inbox') as linkedin,
    count(*) filter (where (e.source = 'email' and (e.payload->>'type') = 'reply') or (e.source = 'linkedin' and (e.payload->>'type') = 'inbox')) as replies
  from sales_getsales_events e
  join sales_hubspot_tal_contacts c on c.id = e.hubspot_contact_id
  where c.tal_list_id = v_tal_list_id
    and e.occurred_at >= v_since
  group by 1, 2
  on conflict (hypothesis_id, day) do update set
    emails_sent_count = excluded.emails_sent_count,
    linkedin_sent_count = excluded.linkedin_sent_count,
    replies_count = excluded.replies_count,
    updated_at = now();

  -- 3. Calculate latest totals (Leads, Opps, Coverage)
  -- For these, we only have "current state" in DB usually, unless we use snapshots.
  -- To keep it simple and accurate for "today", we update today's row with current totals.
  -- Historical tracking of these would require reading from snapshots table, but let's start with current state for today.
  
  update public.sales_hypothesis_dashboard_stats
  set
    companies_count = (select count(*) from sales_hubspot_tal_companies where tal_list_id = v_tal_list_id),
    contacts_count = (select count(*) from sales_hubspot_tal_contacts where tal_list_id = v_tal_list_id),
    leads_count = (
        select count(*) 
        from sales_hubspot_tal_deals d
        join sales_hubspot_tal_companies c on c.id = d.company_id
        where c.tal_list_id = v_tal_list_id
          and d.stage_label ilike '%lead%'
          and (v_owner_id is null or d.owner_id = v_owner_id)
    ),
    opps_count = (
        select count(*) 
        from sales_hubspot_tal_deals d
        join sales_hubspot_tal_companies c on c.id = d.company_id
        where c.tal_list_id = v_tal_list_id
          and d.stage_label ilike '%sql%'
          and (v_owner_id is null or d.owner_id = v_owner_id)
    )
  where hypothesis_id = p_hypothesis_id and day = now()::date;

  -- Ensure row exists for today even if no activity
  if not found then
    insert into public.sales_hypothesis_dashboard_stats (hypothesis_id, day, companies_count, contacts_count, leads_count, opps_count)
    values (
      p_hypothesis_id, 
      now()::date,
      (select count(*) from sales_hubspot_tal_companies where tal_list_id = v_tal_list_id),
      (select count(*) from sales_hubspot_tal_contacts where tal_list_id = v_tal_list_id),
      (
        select count(*) 
        from sales_hubspot_tal_deals d
        join sales_hubspot_tal_companies c on c.id = d.company_id
        where c.tal_list_id = v_tal_list_id
          and d.stage_label ilike '%lead%'
          and (v_owner_id is null or d.owner_id = v_owner_id)
      ),
      (
        select count(*) 
        from sales_hubspot_tal_deals d
        join sales_hubspot_tal_companies c on c.id = d.company_id
        where c.tal_list_id = v_tal_list_id
          and d.stage_label ilike '%sql%'
          and (v_owner_id is null or d.owner_id = v_owner_id)
      )
    )
    on conflict (hypothesis_id, day) do nothing; -- Should be handled by update above but safe fallback
  end if;

end;
$$;
