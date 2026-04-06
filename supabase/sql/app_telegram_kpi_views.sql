-- =============================================================================
-- App + Telegram KPI views (alltime)
-- Источник: manual_stats (все данные вводятся вручную)
-- Метрики: total_touches, replies, booked_meetings, held_meetings
-- =============================================================================


-- ---------------------------------------------------------------------------
-- app_kpi_alltime_v
-- ---------------------------------------------------------------------------
create or replace view public.app_kpi_alltime_v as
select
  coalesce(campaign_name, account_name) as campaign_name,
  sum(case when metric_name = 'total_touches'   then value else 0 end)::int as total_touches,
  sum(case when metric_name = 'replies'         then value else 0 end)::int as replies,
  sum(case when metric_name = 'booked_meetings' then value else 0 end)::int as booked_meetings,
  sum(case when metric_name = 'held_meetings'   then value else 0 end)::int as held_meetings,
  case
    when sum(case when metric_name = 'total_touches' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'replies' then value else 0 end)::numeric
      / sum(case when metric_name = 'total_touches' then value else 0 end)::numeric * 100, 2
    )
  end as cr_to_reply_pct,
  case
    when sum(case when metric_name = 'replies' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'booked_meetings' then value else 0 end)::numeric
      / sum(case when metric_name = 'replies' then value else 0 end)::numeric * 100, 2
    )
  end as cr_to_booked_pct,
  case
    when sum(case when metric_name = 'booked_meetings' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'held_meetings' then value else 0 end)::numeric
      / sum(case when metric_name = 'booked_meetings' then value else 0 end)::numeric * 100, 2
    )
  end as cr_booked_to_held_pct
from public.manual_stats
where channel = 'app'
group by coalesce(campaign_name, account_name)
order by total_touches desc;


-- ---------------------------------------------------------------------------
-- telegram_kpi_alltime_v
-- ---------------------------------------------------------------------------
create or replace view public.telegram_kpi_alltime_v as
select
  coalesce(campaign_name, account_name) as campaign_name,
  sum(case when metric_name = 'total_touches'   then value else 0 end)::int as total_touches,
  sum(case when metric_name = 'replies'         then value else 0 end)::int as replies,
  sum(case when metric_name = 'booked_meetings' then value else 0 end)::int as booked_meetings,
  sum(case when metric_name = 'held_meetings'   then value else 0 end)::int as held_meetings,
  case
    when sum(case when metric_name = 'total_touches' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'replies' then value else 0 end)::numeric
      / sum(case when metric_name = 'total_touches' then value else 0 end)::numeric * 100, 2
    )
  end as cr_to_reply_pct,
  case
    when sum(case when metric_name = 'replies' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'booked_meetings' then value else 0 end)::numeric
      / sum(case when metric_name = 'replies' then value else 0 end)::numeric * 100, 2
    )
  end as cr_to_booked_pct,
  case
    when sum(case when metric_name = 'booked_meetings' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'held_meetings' then value else 0 end)::numeric
      / sum(case when metric_name = 'booked_meetings' then value else 0 end)::numeric * 100, 2
    )
  end as cr_booked_to_held_pct
from public.manual_stats
where channel = 'telegram'
group by coalesce(campaign_name, account_name)
order by total_touches desc;


-- =============================================================================
-- ПРОВЕРКА
-- =============================================================================
-- App: ожидаем booked=58, held=20
-- select sum(booked_meetings) as booked, sum(held_meetings) as held from public.app_kpi_alltime_v;
--
-- Telegram: ожидаем booked=2, held=2
-- select sum(booked_meetings) as booked, sum(held_meetings) as held from public.telegram_kpi_alltime_v;
