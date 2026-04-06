-- =============================================================================
-- Fix: smartlead email KPI — 3 issues
--
-- Fix 1: campaign_name_aliases join без channel-фильтра
--   → FinTech дублировался (email + linkedin aliases с одним canonical)
--   → booked/held FinTech были 8 вместо 4
--   → добавляем AND cna.channel = 'email'
--
-- Fix 2: Cross-Border Corporate Payments не матчился
--   → в manual_stats: 'Cross-Border Corporate Payments'
--   → в smartlead_stats_daily: 'Cross-Border Corporate Payments (Фокус на ...)'
--   → добавляем алиас
--
-- Fix 3: TES booked = 1 вместо 2 в manual_stats (по данным CSV дашборда)
--
-- Ожидаемый результат после всех фиксов:
--   SELECT sum(booked_meetings), sum(held_meetings) FROM smartlead_kpi_alltime_v
--   → 100, 81
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Fix 2: алиас для Cross-Border
-- ---------------------------------------------------------------------------
insert into public.campaign_name_aliases (alias, canonical, channel) values
  ('Cross-Border Corporate Payments',
   'Cross-Border Corporate Payments (Фокус на единой платформе и снижении затрат при международных операциях)',
   'email')
on conflict (alias, channel) do update set canonical = excluded.canonical;


-- ---------------------------------------------------------------------------
-- Fix 3: TES booked 1 → 2 в manual_stats
-- ---------------------------------------------------------------------------
update public.manual_stats
set value = 2
where channel = 'email'
  and campaign_name = 'TES'
  and metric_name = 'booked_meetings';


-- ---------------------------------------------------------------------------
-- Fix 1 + 2: пересоздать оба smartlead view с AND cna.channel = 'email'
-- ---------------------------------------------------------------------------

-- Monthly
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
    coalesce(cna.canonical, ms.campaign_name) as campaign_name,
    date_trunc('month', ms.record_date)::date  as month,
    sum(case when ms.metric_name = 'booked_meetings' then ms.value else 0 end) as booked_meetings,
    sum(case when ms.metric_name = 'held_meetings'   then ms.value else 0 end) as held_meetings
  from public.manual_stats ms
  left join public.campaign_name_aliases cna
    on cna.alias = ms.campaign_name
   and cna.channel = 'email'
  where ms.channel = 'email'
  group by coalesce(cna.canonical, ms.campaign_name),
           date_trunc('month', ms.record_date)::date
) m on m.campaign_name = s.campaign_name
     and m.month = date_trunc('month', s.date)::date
group by date_trunc('month', s.date)::date, s.campaign_name
order by month;


-- Alltime
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
    coalesce(cna.canonical, ms.campaign_name) as campaign_name,
    sum(case when ms.metric_name = 'booked_meetings' then ms.value else 0 end) as booked_meetings,
    sum(case when ms.metric_name = 'held_meetings'   then ms.value else 0 end) as held_meetings
  from public.manual_stats ms
  left join public.campaign_name_aliases cna
    on cna.alias = ms.campaign_name
   and cna.channel = 'email'
  where ms.channel = 'email'
  group by coalesce(cna.canonical, ms.campaign_name)
) m on m.campaign_name = s.campaign_name
group by s.campaign_name
order by sum(s.sent_count) desc;


-- =============================================================================
-- ПРОВЕРКА
-- =============================================================================
-- select sum(booked_meetings) as booked, sum(held_meetings) as held
-- from public.smartlead_kpi_alltime_v;
-- → ожидаем: booked=100, held=81
