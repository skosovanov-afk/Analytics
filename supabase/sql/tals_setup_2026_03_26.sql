-- =============================================================================
-- TAL (Territory Account List) setup
-- Дата: 2026-03-26
-- Зависит от: smartlead_kpi_alltime_v, expandi_kpi_alltime_v
-- =============================================================================

-- 1. TAL — список компаний, единица учёта outreach
create table if not exists public.tals (
  id          uuid primary key default gen_random_uuid(),
  name        text not null,
  description text,
  criteria    text,  -- что описывает этот список (индустрия, размер, гео и т.д.)
  created_at  timestamptz default now(),
  updated_at  timestamptz default now()
);

-- 2. Привязка кампаний к TAL (junction)
create table if not exists public.tal_campaigns (
  id            uuid primary key default gen_random_uuid(),
  tal_id        uuid not null references public.tals(id) on delete cascade,
  channel       text not null check (channel in ('smartlead', 'expandi', 'app', 'telegram')),
  campaign_id   text,          -- нативный ID в Smartlead / Expandi (для справки)
  source_campaign_key text,    -- стабильный ключ источника для id-first join
  match_group   text,          -- общий cross-channel ключ внутри TAL (ручной matching)
  campaign_name text not null, -- ключ для join с KPI-вьюхами
  created_at    timestamptz default now(),
  unique (tal_id, channel, campaign_name)
);

create index if not exists tal_campaigns_tal_id_idx on public.tal_campaigns(tal_id);
create index if not exists tal_campaigns_source_campaign_key_idx on public.tal_campaigns(source_campaign_key);
create index if not exists tal_campaigns_match_group_idx on public.tal_campaigns(match_group);

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

-- 3. Аналитика TAL — агрегация Email + LinkedIn + App + Telegram по привязанным кампаниям
drop view if exists public.tal_analytics_v;

create view public.tal_analytics_v as
with sl_source as (
  select
    'smartlead:id:' || campaign_id::text as source_campaign_key,
    max(campaign_name) as campaign_name,
    sum(sent_count)::int as sent_count,
    sum(reply_count)::int as reply_count
  from public.smartlead_stats_daily
  where campaign_id is not null
    and campaign_name is not null
  group by campaign_id
),
sl_meetings as (
  select
    campaign_name,
    max(booked_meetings)::int as booked_meetings,
    max(held_meetings)::int as held_meetings
  from public.smartlead_kpi_alltime_v
  group by campaign_name
),
sl as (
  select
    tc.tal_id,
    sum(s.sent_count)::int    as email_sent,
    sum(s.reply_count)::int   as email_replies,
    case
      when sum(s.sent_count) = 0 then null
      else round(sum(s.reply_count)::numeric / sum(s.sent_count) * 100, 2)
    end as email_reply_rate,
    coalesce(sum(m.booked_meetings), 0)::int as email_meetings,
    coalesce(sum(m.held_meetings), 0)::int   as email_held_meetings
  from public.tal_campaigns tc
  join sl_source s
    on (
      tc.source_campaign_key is not null
      and tc.source_campaign_key = s.source_campaign_key
    ) or (
      tc.source_campaign_key is null
      and s.campaign_name = tc.campaign_name
    )
  left join sl_meetings m on m.campaign_name = s.campaign_name
  where tc.channel = 'smartlead'
  group by tc.tal_id
),
ex_source as (
  select
    'expandi:canonical:' || lower(btrim(campaign_name)) as source_campaign_key,
    campaign_name,
    case
      when campaign_missing_in_live_api
        or (
          coalesce(current_instances, 0) = 0
          and coalesce(api_connection_req, 0) = 0
          and coalesce(api_accepted, 0) = 0
          and coalesce(api_replies, 0) = 0
          and (
            coalesce(manual_connection_req, 0) > 0
            or coalesce(manual_accepted, 0) > 0
            or coalesce(manual_sent_messages, 0) > 0
            or coalesce(manual_replies, 0) > 0
          )
        )
      then coalesce(manual_connection_req, 0)
      else coalesce(api_connection_req, 0)
    end::int as li_invited,
    case
      when campaign_missing_in_live_api
        or (
          coalesce(current_instances, 0) = 0
          and coalesce(api_connection_req, 0) = 0
          and coalesce(api_accepted, 0) = 0
          and coalesce(api_replies, 0) = 0
          and (
            coalesce(manual_connection_req, 0) > 0
            or coalesce(manual_accepted, 0) > 0
            or coalesce(manual_sent_messages, 0) > 0
            or coalesce(manual_replies, 0) > 0
          )
        )
      then coalesce(manual_accepted, 0)
      else coalesce(api_accepted, 0)
    end::int as li_accepted,
    case
      when campaign_missing_in_live_api
        or (
          coalesce(current_instances, 0) = 0
          and coalesce(api_connection_req, 0) = 0
          and coalesce(api_accepted, 0) = 0
          and coalesce(api_replies, 0) = 0
          and (
            coalesce(manual_connection_req, 0) > 0
            or coalesce(manual_accepted, 0) > 0
            or coalesce(manual_sent_messages, 0) > 0
            or coalesce(manual_replies, 0) > 0
          )
        )
      then coalesce(manual_replies, 0)
      else coalesce(api_replies, 0)
    end::int as li_replies,
    booked_meetings,
    held_meetings
  from public.linkedin_kpi_alltime_v2
),
ex as (
  select
    tc.tal_id,
    sum(e.li_invited)::int as li_invited,
    sum(e.li_accepted)::int as li_accepted,
    sum(e.li_replies)::int as li_replies,
    case
      when sum(e.li_invited) = 0 then null
      else round(sum(e.li_accepted)::numeric / sum(e.li_invited) * 100, 2)
    end as li_accept_rate,
    coalesce(sum(e.booked_meetings), 0)::int as li_meetings,
    coalesce(sum(e.held_meetings), 0)::int   as li_held_meetings
  from public.tal_campaigns tc
  join ex_source e
    on (
      tc.source_campaign_key is not null
      and tc.source_campaign_key = e.source_campaign_key
    ) or (
      tc.source_campaign_key is null
      and e.campaign_name = tc.campaign_name
    )
  where tc.channel = 'expandi'
  group by tc.tal_id
),
app_source as (
  select
    'app:name:' || lower(btrim(campaign_name)) as source_campaign_key,
    campaign_name,
    total_touches,
    replies,
    booked_meetings,
    held_meetings
  from public.app_kpi_alltime_v
),
app as (
  select
    tc.tal_id,
    sum(a.total_touches)::int as app_touches,
    sum(a.replies)::int       as app_replies,
    case
      when sum(a.total_touches) = 0 then null
      else round(sum(a.replies)::numeric / sum(a.total_touches) * 100, 2)
    end as app_reply_rate,
    coalesce(sum(a.booked_meetings), 0)::int as app_meetings,
    coalesce(sum(a.held_meetings), 0)::int   as app_held_meetings
  from public.tal_campaigns tc
  join app_source a
    on (
      tc.source_campaign_key is not null
      and tc.source_campaign_key = a.source_campaign_key
    ) or (
      tc.source_campaign_key is null
      and a.campaign_name = tc.campaign_name
    )
  where tc.channel = 'app'
  group by tc.tal_id
),
tg_source as (
  select
    'telegram:name:' || lower(btrim(campaign_name)) as source_campaign_key,
    campaign_name,
    total_touches,
    replies,
    booked_meetings,
    held_meetings
  from public.telegram_kpi_alltime_v
),
tg as (
  select
    tc.tal_id,
    sum(g.total_touches)::int as tg_touches,
    sum(g.replies)::int       as tg_replies,
    case
      when sum(g.total_touches) = 0 then null
      else round(sum(g.replies)::numeric / sum(g.total_touches) * 100, 2)
    end as tg_reply_rate,
    coalesce(sum(g.booked_meetings), 0)::int as tg_meetings,
    coalesce(sum(g.held_meetings), 0)::int   as tg_held_meetings
  from public.tal_campaigns tc
  join tg_source g
    on (
      tc.source_campaign_key is not null
      and tc.source_campaign_key = g.source_campaign_key
    ) or (
      tc.source_campaign_key is null
      and g.campaign_name = tc.campaign_name
    )
  where tc.channel = 'telegram'
  group by tc.tal_id
)
select
  t.id,
  t.name,
  t.description,
  t.criteria,
  t.created_at,
  t.updated_at,
  -- Email
  coalesce(sl.email_sent, 0)      as email_sent,
  coalesce(sl.email_replies, 0)   as email_replies,
  sl.email_reply_rate,
  coalesce(sl.email_meetings, 0)  as email_meetings,
  coalesce(sl.email_held_meetings, 0) as email_held_meetings,
  -- LinkedIn
  coalesce(ex.li_invited, 0)      as li_invited,
  coalesce(ex.li_accepted, 0)     as li_accepted,
  coalesce(ex.li_replies, 0)      as li_replies,
  ex.li_accept_rate,
  coalesce(ex.li_meetings, 0)     as li_meetings,
  coalesce(ex.li_held_meetings, 0) as li_held_meetings,
  -- App
  coalesce(app.app_touches, 0)      as app_touches,
  coalesce(app.app_replies, 0)      as app_replies,
  app.app_reply_rate,
  coalesce(app.app_meetings, 0)     as app_meetings,
  coalesce(app.app_held_meetings, 0) as app_held_meetings,
  -- Telegram
  coalesce(tg.tg_touches, 0)      as tg_touches,
  coalesce(tg.tg_replies, 0)      as tg_replies,
  tg.tg_reply_rate,
  coalesce(tg.tg_meetings, 0)     as tg_meetings,
  coalesce(tg.tg_held_meetings, 0) as tg_held_meetings,
  -- Total
  (
    coalesce(sl.email_meetings, 0)
    + coalesce(ex.li_meetings, 0)
    + coalesce(app.app_meetings, 0)
    + coalesce(tg.tg_meetings, 0)
  ) as total_meetings,
  (
    coalesce(sl.email_held_meetings, 0)
    + coalesce(ex.li_held_meetings, 0)
    + coalesce(app.app_held_meetings, 0)
    + coalesce(tg.tg_held_meetings, 0)
  ) as total_held_meetings
from public.tals t
left join sl on sl.tal_id = t.id
left join ex on ex.tal_id = t.id
left join app on app.tal_id = t.id
left join tg on tg.tal_id = t.id;

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
