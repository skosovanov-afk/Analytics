-- =============================================================================
-- Fix: campaign_name_aliases — добавить channel-колонку + LinkedIn маппинги
-- + обновить expandi_kpi_alltime_v чтобы использовал aliases для manual_meetings
--
-- Проблема: manual_stats для LinkedIn хранит короткие имена ("Payroll services",
-- "Freelance platforms" и др.), тогда как expandi_campaign_instances использует
-- длинные/другие имена. Joins не совпадают → booked_meetings = 0 для этих кампаний.
--
-- Применить: вставить в Supabase SQL Editor и запустить целиком
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 1. Добавить channel колонку в campaign_name_aliases
-- ---------------------------------------------------------------------------
alter table public.campaign_name_aliases
  add column if not exists channel text not null default 'email';

-- Обновить unique constraint: теперь уникальность по (alias, channel)
alter table public.campaign_name_aliases
  drop constraint if exists campaign_name_aliases_alias_key;

alter table public.campaign_name_aliases
  add constraint campaign_name_aliases_alias_channel_key unique (alias, channel);

-- Явно проставить channel='email' для существующих записей
update public.campaign_name_aliases set channel = 'email' where channel = 'email';


-- ---------------------------------------------------------------------------
-- 2. LinkedIn маппинги: manual_stats short name → expandi_campaign_instances name
-- ---------------------------------------------------------------------------
insert into public.campaign_name_aliases (alias, canonical, channel) values
  ('FinTech',               'Fintech (Фокус на ускорении переводов и снижении SWIFT/SEPA издержек)',     'linkedin'),
  ('Payroll services',      'Payroll',                                                                    'linkedin'),
  ('Web hosting providers', 'Hosting providers (Высокие PSP комиссии, чарджбэки, задержки платежей от клиентов)', 'linkedin'),
  ('Freelance platforms',   'Freelance Platforms',                                                        'linkedin'),
  ('PG Connects London',    'PG Connects London AFTER',                                                   'linkedin'),
  ('Sigma Dubai',           'Sigma after',                                                                'linkedin')
on conflict (alias, channel) do update set canonical = excluded.canonical;


-- ---------------------------------------------------------------------------
-- 3. Обновить expandi_kpi_alltime_v — manual_meetings использует aliases
-- ---------------------------------------------------------------------------
create or replace view public.expandi_kpi_alltime_v as
with latest_snap as (
  select distinct on (campaign_instance_id)
    campaign_instance_id,
    li_account_id,
    contacted_people,
    connected,
    (coalesce(replied_first_action, 0) + coalesce(replied_other_actions, 0)) as snap_replies
  from public.expandi_campaign_stats_snapshots
  order by campaign_instance_id, snapshot_date desc
),
snap_by_name as (
  select
    s.li_account_id,
    ci.name                           as campaign_name,
    sum(s.contacted_people)::int      as connection_req,
    sum(s.connected)::int             as accepted,
    sum(s.snap_replies)::int          as snap_replies
  from latest_snap s
  join public.expandi_campaign_instances ci on ci.id = s.campaign_instance_id
  group by s.li_account_id, ci.name
),
msg_by_name as (
  select
    d.li_account_id,
    ci.name                               as campaign_name,
    sum(d.sent_messages)::int             as sent_messages,
    sum(d.received_messages)::int         as received_messages,
    sum(d.new_replies)::int               as replies
  from public.expandi_campaign_daily_v d
  join public.expandi_campaign_instances ci on ci.id = d.campaign_instance_id
  group by d.li_account_id, ci.name
),
manual_meetings as (
  -- Booked/held meetings из manual_stats для LinkedIn.
  -- coalesce(campaign_name, account_name): CSV backfill хранит campaign в account_name.
  -- campaign_name_aliases (channel='linkedin'): маппинг коротких имён → expandi имена.
  select
    coalesce(cna.canonical, coalesce(ms.campaign_name, ms.account_name)) as campaign_name,
    sum(case when ms.metric_name = 'booked_meetings' then ms.value else 0 end) as booked_meetings,
    sum(case when ms.metric_name = 'held_meetings'   then ms.value else 0 end) as held_meetings
  from public.manual_stats ms
  left join public.campaign_name_aliases cna
    on cna.alias = coalesce(ms.campaign_name, ms.account_name)
   and cna.channel = 'linkedin'
  where ms.channel = 'linkedin'
  group by coalesce(cna.canonical, coalesce(ms.campaign_name, ms.account_name))
)
select
  sn.li_account_id,
  coalesce(a.name, sn.li_account_id::text)  as account_name,
  sn.campaign_name,
  sn.connection_req,
  sn.accepted,
  coalesce(m.sent_messages, 0)::int         as sent_messages,
  coalesce(m.received_messages, 0)::int     as received_messages,
  coalesce(m.replies, 0)::int               as replies,
  case
    when sn.connection_req = 0 then null
    else least(round(
      (sn.accepted::numeric / sn.connection_req::numeric) * 100.0, 2
    ), 100.00)
  end as cr_to_accept_pct,
  case
    when coalesce(m.sent_messages, 0) = 0 then null
    else round(
      (coalesce(m.replies, 0)::numeric / m.sent_messages::numeric) * 100.0, 2
    )
  end as cr_to_reply_pct,
  -- booked/held показываем только на строке с min(li_account_id) по кампании
  -- чтобы sum() через аккаунты не дублировал значения
  case
    when sn.li_account_id = min(sn.li_account_id) over (partition by sn.campaign_name)
    then coalesce(mm.booked_meetings, 0)::int
    else 0
  end as booked_meetings,
  case
    when sn.li_account_id = min(sn.li_account_id) over (partition by sn.campaign_name)
    then coalesce(mm.held_meetings, 0)::int
    else 0
  end as held_meetings
from snap_by_name sn
left join public.expandi_accounts a on a.id = sn.li_account_id
left join msg_by_name m
  on m.li_account_id = sn.li_account_id
  and m.campaign_name = sn.campaign_name
left join manual_meetings mm on mm.campaign_name = sn.campaign_name
order by sn.li_account_id, sn.campaign_name;


-- =============================================================================
-- ПРОВЕРКА
-- =============================================================================
-- Итого должно быть 17 booked, 13 held (как в CSV):
-- select sum(booked_meetings) as total_booked, sum(held_meetings) as total_held
-- from public.expandi_kpi_alltime_v;
--
-- По кампаниям:
-- select campaign_name, sum(booked_meetings) as booked, sum(held_meetings) as held
-- from public.expandi_kpi_alltime_v
-- where booked_meetings > 0
-- group by campaign_name
-- order by booked desc;
