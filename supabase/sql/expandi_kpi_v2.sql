-- =============================================================================
-- Expandi KPI Analytics v2
-- Purpose: полная аналитика LinkedIn outreach с дедупликацией кампаний по имени,
--          разбивкой по аккаунтам, daily/weekly/monthly/alltime срезами.
--
-- Зависит от:
--   expandi_campaign_daily_v  (обновлён с direction fix в expandi_kpi_hardening.sql)
--   expandi_accounts
--   expandi_campaign_instances
--
-- Как применить:
--   1) Сначала применить обновлённый expandi_kpi_hardening.sql (с direction fix)
--   2) Запустить этот файл целиком в Supabase SQL Editor
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. expandi_kpi_daily_v
--    Гранулярность: day × li_account_id × campaign_name
--    Дедупликация: несколько campaign_instance одной кампании на одном аккаунте
--                  сворачиваются в одну строку (SUM по instance_id)
-- ---------------------------------------------------------------------------
create or replace view public.expandi_kpi_daily_v as
select
  d.day,
  d.li_account_id,
  coalesce(a.name, d.li_account_id::text) as account_name,
  -- Канонизируем имя кампании (берём MAX — все instance одной кампании имеют одно имя)
  max(d.campaign_name) as campaign_name,
  sum(d.sent_invitations)::int      as connection_req,
  sum(d.new_connections)::int       as accepted,
  sum(d.sent_messages)::int         as sent_messages,
  sum(d.received_messages)::int     as received_messages,
  sum(d.new_replies)::int           as replies,
  case
    when sum(d.sent_invitations) = 0 then null
    else least(round(
      (sum(d.new_connections)::numeric / sum(d.sent_invitations)::numeric) * 100.0, 2
    ), 100.00)
  end as cr_to_accept_pct,
  case
    when sum(d.sent_messages) = 0 then null
    else round(
      (sum(d.new_replies)::numeric / sum(d.sent_messages)::numeric) * 100.0, 2
    )
  end as cr_to_reply_pct
from public.expandi_campaign_daily_v d
left join public.expandi_accounts a on a.id = d.li_account_id
-- JOIN с campaign_instances чтобы получить канонное имя и сгруппировать по нему
join public.expandi_campaign_instances ci on ci.id = d.campaign_instance_id
group by
  d.day,
  d.li_account_id,
  coalesce(a.name, d.li_account_id::text),
  ci.name;  -- группируем по имени из catalog (а не d.campaign_name, который может быть пустым)


-- ---------------------------------------------------------------------------
-- 2. expandi_kpi_weekly_v
--    Гранулярность: week_start (понедельник) × li_account_id × campaign_name
-- ---------------------------------------------------------------------------
create or replace view public.expandi_kpi_weekly_v as
select
  date_trunc('week', d.day)::date   as week_start,
  d.li_account_id,
  d.account_name,
  d.campaign_name,
  sum(d.connection_req)::int        as connection_req,
  sum(d.accepted)::int              as accepted,
  sum(d.sent_messages)::int         as sent_messages,
  sum(d.received_messages)::int     as received_messages,
  sum(d.replies)::int               as replies,
  case
    when sum(d.connection_req) = 0 then null
    else least(round(
      (sum(d.accepted)::numeric / sum(d.connection_req)::numeric) * 100.0, 2
    ), 100.00)
  end as cr_to_accept_pct,
  case
    when sum(d.sent_messages) = 0 then null
    else round(
      (sum(d.replies)::numeric / sum(d.sent_messages)::numeric) * 100.0, 2
    )
  end as cr_to_reply_pct
from public.expandi_kpi_daily_v d
group by 1, 2, 3, 4;


-- ---------------------------------------------------------------------------
-- 3. expandi_kpi_monthly_v  (замена старого view)
--    Гранулярность: month × li_account_id × campaign_name
-- ---------------------------------------------------------------------------
create or replace view public.expandi_kpi_monthly_v as
select
  date_trunc('month', d.day)::date  as month,
  d.li_account_id,
  d.account_name,
  d.campaign_name,
  sum(d.connection_req)::int        as connection_req,
  sum(d.accepted)::int              as accepted,
  sum(d.sent_messages)::int         as sent_messages,
  sum(d.received_messages)::int     as received_messages,
  sum(d.replies)::int               as replies,
  case
    when sum(d.connection_req) = 0 then null
    else least(round(
      (sum(d.accepted)::numeric / sum(d.connection_req)::numeric) * 100.0, 2
    ), 100.00)
  end as cr_to_accept_pct,
  case
    when sum(d.sent_messages) = 0 then null
    else round(
      (sum(d.replies)::numeric / sum(d.sent_messages)::numeric) * 100.0, 2
    )
  end as cr_to_reply_pct
from public.expandi_kpi_daily_v d
group by 1, 2, 3, 4;


-- ---------------------------------------------------------------------------
-- 4. expandi_kpi_alltime_v
--    Гранулярность: li_account_id × campaign_name (без временного среза)
--
--    connection_req и accepted берутся из campaign_stats_snapshots
--    (contacted_people / connected) — это официальные кумулятивные счётчики
--    из самого Expandi, точные для всех исторических данных.
--
--    sent_messages и replies берутся из expandi_kpi_daily_v, т.к. снапшоты
--    не имеют детализации по сообщениям.
-- ---------------------------------------------------------------------------
create or replace view public.expandi_kpi_alltime_v as
with latest_snap as (
  -- Последний снапшот на каждый campaign_instance
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
  -- Агрегируем по имени кампании (дедупликация instances одной кампании на одном аккаунте)
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
  -- Сообщения и реплаи из KPI daily (точны для периода синка)
  select
    li_account_id,
    campaign_name,
    sum(sent_messages)::int       as sent_messages,
    sum(received_messages)::int   as received_messages,
    sum(replies)::int             as replies
  from public.expandi_kpi_daily_v
  group by li_account_id, campaign_name
),
manual_meetings as (
  -- Booked/held meetings from manual_stats for LinkedIn channel
  select
    campaign_name,
    sum(case when metric_name = 'booked_meetings' then value else 0 end) as booked_meetings,
    sum(case when metric_name = 'held_meetings'   then value else 0 end) as held_meetings
  from public.manual_stats
  where channel = 'linkedin'
  group by campaign_name
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
  coalesce(mm.booked_meetings, 0)::int      as booked_meetings,
  coalesce(mm.held_meetings, 0)::int        as held_meetings
from snap_by_name sn
left join public.expandi_accounts a on a.id = sn.li_account_id
left join msg_by_name m
  on m.li_account_id = sn.li_account_id
  and m.campaign_name = sn.campaign_name
left join manual_meetings mm on mm.campaign_name = sn.campaign_name
order by sn.li_account_id, sn.campaign_name;


-- =============================================================================
-- ПРИМЕРЫ ЗАПРОСОВ
-- =============================================================================

-- Февраль 2026 — по аккаунтам (строка = один аккаунт, все кампании свёрнуты):
-- select account_name,
--   sum(connection_req) as connection_req, sum(accepted) as accepted,
--   sum(sent_messages) as sent_messages, sum(replies) as replies,
--   round(sum(accepted)::numeric / nullif(sum(connection_req),0) * 100, 1) as cr_accept,
--   round(sum(replies)::numeric   / nullif(sum(sent_messages),0) * 100, 1) as cr_reply
-- from public.expandi_kpi_monthly_v
-- where month = date '2026-02-01'
-- group by account_name
-- order by replies desc;

-- Февраль 2026 — по кампаниям (все аккаунты сложены):
-- select campaign_name,
--   sum(connection_req), sum(accepted), sum(sent_messages), sum(replies),
--   round(sum(accepted)::numeric / nullif(sum(connection_req),0) * 100, 1) as cr_accept,
--   round(sum(replies)::numeric   / nullif(sum(sent_messages),0) * 100, 1) as cr_reply
-- from public.expandi_kpi_monthly_v
-- where month = date '2026-02-01'
-- group by campaign_name
-- order by replies desc;

-- Всё время — топ кампаний по репляям:
-- select campaign_name, account_name, connection_req, accepted, sent_messages, replies, cr_to_reply_pct
-- from public.expandi_kpi_alltime_v
-- order by replies desc;

-- Недельная динамика по всем аккаунтам суммарно:
-- select week_start,
--   sum(connection_req), sum(accepted), sum(sent_messages), sum(replies),
--   round(sum(replies)::numeric / nullif(sum(sent_messages),0) * 100, 1) as cr_reply
-- from public.expandi_kpi_weekly_v
-- group by week_start
-- order by week_start desc;
