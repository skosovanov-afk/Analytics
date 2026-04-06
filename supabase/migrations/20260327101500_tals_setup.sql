-- =============================================================================
-- TAL (Territory Account List) setup
-- Date: 2026-03-27
-- Depends on: smartlead_kpi_alltime_v, expandi_kpi_alltime_v
-- =============================================================================

create table if not exists public.tals (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  description text,
  criteria text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.tal_campaigns (
  id uuid primary key default gen_random_uuid(),
  tal_id uuid not null references public.tals(id) on delete cascade,
  channel text not null check (channel in ('smartlead', 'expandi')),
  campaign_id text,
  campaign_name text not null,
  created_at timestamptz default now(),
  unique (tal_id, channel, campaign_name)
);

create index if not exists tal_campaigns_tal_id_idx on public.tal_campaigns(tal_id);

do $$
begin
  if exists (select 1 from pg_proc where pronamespace = 'public'::regnamespace and proname = 'set_updated_at') then
    if not exists (select 1 from pg_trigger where tgname = 'trg_tals_updated_at') then
      create trigger trg_tals_updated_at
      before update on public.tals
      for each row execute function public.set_updated_at();
    end if;
  end if;
end
$$;

create or replace view public.tal_analytics_v as
with sl as (
  select
    tc.tal_id,
    sum(s.sent_count)::int as email_sent,
    sum(s.reply_count)::int as email_replies,
    case
      when sum(s.sent_count) = 0 then null
      else round(sum(s.reply_count)::numeric / sum(s.sent_count) * 100, 2)
    end as email_reply_rate,
    coalesce(sum(s.booked_meetings), 0)::int as email_meetings
  from public.tal_campaigns tc
  join public.smartlead_kpi_alltime_v s on s.campaign_name = tc.campaign_name
  where tc.channel = 'smartlead'
  group by tc.tal_id
),
ex as (
  select
    tc.tal_id,
    sum(e.connection_req)::int as li_invited,
    sum(e.accepted)::int as li_accepted,
    sum(e.replies)::int as li_replies,
    case
      when sum(e.connection_req) = 0 then null
      else round(sum(e.accepted)::numeric / sum(e.connection_req) * 100, 2)
    end as li_accept_rate,
    coalesce(sum(e.booked_meetings), 0)::int as li_meetings
  from public.tal_campaigns tc
  join public.expandi_kpi_alltime_v e on e.campaign_name = tc.campaign_name
  where tc.channel = 'expandi'
  group by tc.tal_id
)
select
  t.id,
  t.name,
  t.description,
  t.criteria,
  t.created_at,
  t.updated_at,
  coalesce(sl.email_sent, 0) as email_sent,
  coalesce(sl.email_replies, 0) as email_replies,
  sl.email_reply_rate,
  coalesce(sl.email_meetings, 0) as email_meetings,
  coalesce(ex.li_invited, 0) as li_invited,
  coalesce(ex.li_accepted, 0) as li_accepted,
  coalesce(ex.li_replies, 0) as li_replies,
  ex.li_accept_rate,
  coalesce(ex.li_meetings, 0) as li_meetings,
  (coalesce(sl.email_meetings, 0) + coalesce(ex.li_meetings, 0)) as total_meetings
from public.tals t
left join sl on sl.tal_id = t.id
left join ex on ex.tal_id = t.id;

alter table public.tals enable row level security;
alter table public.tal_campaigns enable row level security;

drop policy if exists tals_select on public.tals;
create policy tals_select
on public.tals
for select
using (auth.role() = 'authenticated');

drop policy if exists tals_insert on public.tals;
create policy tals_insert
on public.tals
for insert
with check (auth.role() = 'authenticated');

drop policy if exists tals_update on public.tals;
create policy tals_update
on public.tals
for update
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

drop policy if exists tals_delete on public.tals;
create policy tals_delete
on public.tals
for delete
using (auth.role() = 'authenticated');

drop policy if exists tal_campaigns_select on public.tal_campaigns;
create policy tal_campaigns_select
on public.tal_campaigns
for select
using (auth.role() = 'authenticated');

drop policy if exists tal_campaigns_insert on public.tal_campaigns;
create policy tal_campaigns_insert
on public.tal_campaigns
for insert
with check (auth.role() = 'authenticated');

drop policy if exists tal_campaigns_update on public.tal_campaigns;
create policy tal_campaigns_update
on public.tal_campaigns
for update
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

drop policy if exists tal_campaigns_delete on public.tal_campaigns;
create policy tal_campaigns_delete
on public.tal_campaigns
for delete
using (auth.role() = 'authenticated');

grant select, insert, update, delete on public.tals to authenticated;
grant select, insert, update, delete on public.tal_campaigns to authenticated;
grant select on public.tal_analytics_v to authenticated;
