-- =============================================================================
-- Materialized view: expandi_campaign_daily_mv
--
-- Заменяет тяжёлый view expandi_campaign_daily_v материализованной версией.
-- Downstream views (expandi_kpi_daily_v, weekly, monthly, alltime) не меняются —
-- expandi_campaign_daily_v остаётся как простой passthrough к mat view.
--
-- Применять в Supabase SQL Editor. Порядок важен.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Индексы на исходные таблицы (ускоряют REFRESH)
-- -----------------------------------------------------------------------------
create index if not exists expandi_messages_campaign_instance_id_idx
  on public.expandi_messages (campaign_instance_id);

create index if not exists expandi_messages_event_datetime_idx
  on public.expandi_messages (event_datetime)
  where event_datetime is not null;

create index if not exists expandi_messages_messenger_id_idx
  on public.expandi_messages (messenger_id)
  where messenger_id is not null;

create index if not exists expandi_messengers_campaign_instance_id_idx
  on public.expandi_messengers (campaign_instance_id)
  where campaign_instance_id is not null;

create index if not exists expandi_messengers_invited_at_idx
  on public.expandi_messengers (invited_at)
  where invited_at is not null;

create index if not exists expandi_messengers_connected_at_idx
  on public.expandi_messengers (connected_at)
  where connected_at is not null;


-- -----------------------------------------------------------------------------
-- 2. Создаём materialized view
--    Логика: та же что в expandi_campaign_daily_v,
--    но EXISTS заменены на JOIN (убирает correlated subquery)
-- -----------------------------------------------------------------------------
create materialized view if not exists public.expandi_campaign_daily_mv as
with msg as (
  select
    m.messenger_id,
    coalesce(m.li_account_id, em.li_account_id)                               as li_account_id,
    coalesce(m.campaign_instance_id, em.campaign_instance_id)                 as campaign_instance_id,
    coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source) as event_at,
    case
      when lower(nullif(btrim(coalesce(m.direction, '')), '')) = 'outbound'   then true
      when lower(nullif(btrim(coalesce(m.direction, '')), '')) = 'inbound'    then false
      when coalesce(m.is_outbound, false) and not coalesce(m.is_inbound, false) then true
      when coalesce(m.is_inbound,  false) and not coalesce(m.is_outbound, false) then false
      when m.send_datetime is not null and m.received_datetime is null         then true
      when m.received_datetime is not null and m.send_datetime is null         then false
      when coalesce(m.is_outbound, false) and coalesce(m.is_inbound, false)   then true
      else null
    end as is_outbound_norm,
    case
      when lower(nullif(btrim(coalesce(m.direction, '')), '')) = 'inbound'    then true
      when lower(nullif(btrim(coalesce(m.direction, '')), '')) = 'outbound'   then false
      when coalesce(m.is_inbound,  false) and not coalesce(m.is_outbound, false) then true
      when coalesce(m.is_outbound, false) and not coalesce(m.is_inbound, false) then false
      when m.received_datetime is not null and m.send_datetime is null         then true
      when m.send_datetime is not null and m.received_datetime is null         then false
      when coalesce(m.is_outbound, false) and coalesce(m.is_inbound, false)   then false
      else null
    end as is_inbound_norm,
    nullif(btrim(coalesce(m.body, '')), '') as body_clean
  from public.expandi_messages m
  left join public.expandi_messengers em on em.id = m.messenger_id
  -- JOIN вместо EXISTS — убирает correlated subquery
  join public.expandi_campaign_instances ci_check
    on ci_check.id = coalesce(m.campaign_instance_id, em.campaign_instance_id)
  where coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source) is not null
    and coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source) >= '2025-01-01'::timestamptz
    and m.messenger_id is not null
    and coalesce(m.campaign_instance_id, em.campaign_instance_id) is not null
),
first_outbound as (
  select
    messenger_id,
    min(event_at) as first_outbound_at
  from msg
  where is_outbound_norm = true
    and body_clean is not null
  group by 1
),
first_reply as (
  select
    m.messenger_id,
    min(m.event_at) as replied_at
  from msg m
  join first_outbound o on o.messenger_id = m.messenger_id
  where m.is_inbound_norm = true
    and m.body_clean is not null
    and m.event_at >= o.first_outbound_at
  group by 1
),
facts as (
  select
    m.messenger_id,
    max(m.li_account_id)          as li_account_id,
    max(m.campaign_instance_id)   as campaign_instance_id,
    o.first_outbound_at,
    r.replied_at
  from msg m
  left join first_outbound o on o.messenger_id = m.messenger_id
  left join first_reply r    on r.messenger_id = m.messenger_id
  group by 1, 4, 5
),
message_rows as (
  select
    date_trunc('day', m.event_at)::date as day,
    m.li_account_id,
    m.campaign_instance_id,
    count(*) filter (
      where m.is_outbound_norm = true
        and m.body_clean is not null
        and m.event_at >= coalesce(f.first_outbound_at, '-infinity'::timestamptz)
        and m.event_at <= coalesce(f.replied_at,        'infinity'::timestamptz)
    )::int as sent_messages,
    count(*) filter (
      where m.is_inbound_norm = true
        and m.body_clean is not null
    )::int as received_messages,
    0::int as sent_invitations,
    0::int as new_connections,
    0::int as new_replies
  from msg m
  left join facts f on f.messenger_id = m.messenger_id
  group by 1, 2, 3
),
invitation_rows as (
  select
    date_trunc('day', em.invited_at)::date as day,
    em.li_account_id,
    em.campaign_instance_id,
    0::int as sent_messages,
    0::int as received_messages,
    count(*)::int as sent_invitations,
    0::int as new_connections,
    0::int as new_replies
  from public.expandi_messengers em
  -- JOIN вместо EXISTS
  join public.expandi_campaign_instances ci_check on ci_check.id = em.campaign_instance_id
  where em.campaign_instance_id is not null
    and em.invited_at is not null
    and em.invited_at >= '2025-01-01'::timestamptz
  group by 1, 2, 3
),
connection_rows as (
  select
    date_trunc('day', em.connected_at)::date as day,
    em.li_account_id,
    em.campaign_instance_id,
    0::int as sent_messages,
    0::int as received_messages,
    0::int as sent_invitations,
    count(*)::int as new_connections,
    0::int as new_replies
  from public.expandi_messengers em
  -- JOIN вместо EXISTS
  join public.expandi_campaign_instances ci_check on ci_check.id = em.campaign_instance_id
  where em.campaign_instance_id is not null
    and em.connected_at is not null
    and em.connected_at >= '2025-01-01'::timestamptz
  group by 1, 2, 3
),
reply_rows as (
  select
    date_trunc('day', fa.replied_at)::date as day,
    fa.li_account_id,
    fa.campaign_instance_id,
    0::int as sent_messages,
    0::int as received_messages,
    0::int as sent_invitations,
    0::int as new_connections,
    count(*)::int as new_replies
  from facts fa
  -- JOIN вместо EXISTS
  join public.expandi_campaign_instances ci_check on ci_check.id = fa.campaign_instance_id
  where fa.campaign_instance_id is not null
    and fa.replied_at is not null
    and fa.replied_at >= '2025-01-01'::timestamptz
    and fa.first_outbound_at is not null
    and fa.replied_at >= fa.first_outbound_at
  group by 1, 2, 3
),
unioned as (
  select * from message_rows
  union all
  select * from invitation_rows
  union all
  select * from connection_rows
  union all
  select * from reply_rows
)
select
  u.day,
  u.li_account_id,
  u.campaign_instance_id,
  max(ci.name)                     as campaign_name,
  sum(u.sent_messages)::int        as sent_messages,
  sum(u.received_messages)::int    as received_messages,
  sum(u.new_connections)::int      as new_connections,
  sum(u.new_replies)::int          as new_replies,
  sum(u.sent_invitations)::int     as sent_invitations
from unioned u
join public.expandi_campaign_instances ci on ci.id = u.campaign_instance_id
group by 1, 2, 3
with no data;  -- не заполняем сразу, первый refresh сделает cron


-- -----------------------------------------------------------------------------
-- 3. Уникальный индекс для CONCURRENT refresh + ускорение JOIN downstream
-- -----------------------------------------------------------------------------
create unique index if not exists expandi_campaign_daily_mv_pk
  on public.expandi_campaign_daily_mv (day, li_account_id, campaign_instance_id);

create index if not exists expandi_campaign_daily_mv_account_idx
  on public.expandi_campaign_daily_mv (li_account_id, day);


-- -----------------------------------------------------------------------------
-- 4. Заменяем тяжёлый view лёгким passthrough
--    Downstream views (kpi_daily_v, weekly, monthly, alltime) не меняются.
-- -----------------------------------------------------------------------------
create or replace view public.expandi_campaign_daily_v as
  select * from public.expandi_campaign_daily_mv;


-- -----------------------------------------------------------------------------
-- 5. pg_cron: refresh каждые 30 минут
--    Если есть старый job — удаляем сначала.
-- -----------------------------------------------------------------------------
do $$
declare
  r record;
begin
  for r in
    select jobid from cron.job
    where jobname = 'expandi-campaign-daily-mv-refresh'
  loop
    perform cron.unschedule(r.jobid);
  end loop;
end $$;

select cron.schedule(
  'expandi-campaign-daily-mv-refresh',
  '*/30 * * * *',
  $$ refresh materialized view concurrently public.expandi_campaign_daily_mv; $$
);


-- -----------------------------------------------------------------------------
-- Проверка после применения:
--   select count(*) from public.expandi_campaign_daily_mv;
--   select * from public.expandi_kpi_alltime_v limit 5;
--   select jobid, jobname, schedule, active from cron.job where jobname like 'expandi%';
-- -----------------------------------------------------------------------------
