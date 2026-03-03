-- =============================================================================
-- Smartlead KPI Views: daily / weekly / monthly / alltime
-- Применить: вставить в Supabase SQL Editor и запустить целиком
-- =============================================================================

-- 1. Daily (день × кампания, без touch_number)
create or replace view public.smartlead_kpi_daily_v as
select
  date,
  campaign_name,
  sum(sent_count)::int    as sent_count,
  sum(reply_count)::int   as reply_count,
  case
    when sum(sent_count) = 0 then null
    else round(sum(reply_count)::numeric / sum(sent_count)::numeric * 100, 2)
  end as reply_rate_pct
from public.smartlead_stats_daily
group by date, campaign_name;


-- 2. Weekly (неделя × кампания)
create or replace view public.smartlead_kpi_weekly_v as
select
  date_trunc('week', date)::date as week_start,
  campaign_name,
  sum(sent_count)::int    as sent_count,
  sum(reply_count)::int   as reply_count,
  case
    when sum(sent_count) = 0 then null
    else round(sum(reply_count)::numeric / sum(sent_count)::numeric * 100, 2)
  end as reply_rate_pct
from public.smartlead_stats_daily
group by date_trunc('week', date)::date, campaign_name
order by week_start;


-- 3. Monthly (месяц × кампания)
create or replace view public.smartlead_kpi_monthly_v as
select
  date_trunc('month', s.date)::date as month,
  s.campaign_name,
  sum(s.sent_count)::int    as sent_count,
  sum(s.reply_count)::int   as reply_count,
  case
    when sum(s.sent_count) = 0 then null
    else round(sum(s.reply_count)::numeric / sum(s.sent_count)::numeric * 100, 2)
  end as reply_rate_pct,
  coalesce(max(m.booked_meetings), 0)::int as booked_meetings,
  coalesce(max(m.held_meetings), 0)::int   as held_meetings
from public.smartlead_stats_daily s
left join (
  select
    date_trunc('month', record_date)::date as month,
    campaign_name,
    sum(case when metric_name = 'booked_meetings' then value else 0 end) as booked_meetings,
    sum(case when metric_name = 'held_meetings'   then value else 0 end) as held_meetings
  from public.manual_stats
  where channel = 'email'
  group by date_trunc('month', record_date)::date, campaign_name
) m on m.month = date_trunc('month', s.date)::date
     and m.campaign_name = s.campaign_name
group by date_trunc('month', s.date)::date, s.campaign_name
order by month;


-- 4. All-time (кампания итого)
create or replace view public.smartlead_kpi_alltime_v as
select
  s.campaign_name,
  sum(s.sent_count)::int    as sent_count,
  sum(s.reply_count)::int   as reply_count,
  case
    when sum(s.sent_count) = 0 then null
    else round(sum(s.reply_count)::numeric / sum(s.sent_count)::numeric * 100, 2)
  end as reply_rate_pct,
  coalesce(max(m.booked_meetings), 0)::int as booked_meetings,
  coalesce(max(m.held_meetings), 0)::int   as held_meetings
from public.smartlead_stats_daily s
left join (
  select
    campaign_name,
    sum(case when metric_name = 'booked_meetings' then value else 0 end) as booked_meetings,
    sum(case when metric_name = 'held_meetings'   then value else 0 end) as held_meetings
  from public.manual_stats
  where channel = 'email'
  group by campaign_name
) m on m.campaign_name = s.campaign_name
group by s.campaign_name
order by sum(s.sent_count) desc;


-- 5. By touch (все время — эффективность по номеру письма)
create or replace view public.smartlead_kpi_touch_v as
select
  touch_number,
  sum(sent_count)::int    as sent_count,
  sum(reply_count)::int   as reply_count,
  case
    when sum(sent_count) = 0 then null
    else round(sum(reply_count)::numeric / sum(sent_count)::numeric * 100, 2)
  end as reply_rate_pct
from public.smartlead_stats_daily
group by touch_number
order by touch_number;
