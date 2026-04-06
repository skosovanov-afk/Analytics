-- =============================================================================
-- Smartlead campaign name aliases
-- Purpose: маппинг коротких имён из manual_stats → полных имён из smartlead_stats_daily
-- Применить: вставить в Supabase SQL Editor и запустить целиком
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 1. Таблица маппинга
-- ---------------------------------------------------------------------------
create table if not exists public.campaign_name_aliases (
  id          serial primary key,
  alias       text not null unique,   -- имя в manual_stats
  canonical   text not null           -- имя в smartlead_stats_daily
);

comment on table public.campaign_name_aliases is
  'Maps short campaign names used in manual_stats to exact campaign names in smartlead_stats_daily';

alter table public.campaign_name_aliases enable row level security;

create policy "authenticated can manage campaign_name_aliases"
  on public.campaign_name_aliases for all to authenticated
  using (true) with check (true);


-- ---------------------------------------------------------------------------
-- 2. Известные маппинги
-- ---------------------------------------------------------------------------
insert into public.campaign_name_aliases (alias, canonical) values
  ('Slush',                          'Slush (after the conference)'),
  ('Sigma Rome',                     'Sigma Rome (after the conference)'),
  ('Sigma Dubai',                    'Sigma (after the conference)'),
  ('Future Travel',                  'Future Travel (after the conference)'),
  ('ICE',                            'ICE (after the event) '),
  ('Web Summit',                     'Web Summit (after the conference)'),
  ('Web Summit Qatar',               'Web Summit Qatar(after the conference)'),
  ('Affiliate World',                'Affiliate World Conference'),
  ('FinTech',                        'Fintech (Фокус на ускорении переводов и снижении SWIFT/SEPA издержек)'),
  ('Payroll services',               'Payroll (Дорогие международные переводы, долго, комплаенс-риски)'),
  ('MICE',                           'MICE verticals'),
  ('PG Connects London',             'PG Connects London (After the conference)'),
  ('iFX',                            'iFX EXPO after the conference'),
  ('AdTech',                         'AdTech (Сложные многостраничные расчёты с партнёрами SSP/DSP, много валют)'),
  ('TES',                            'TES before the conference ')
on conflict (alias) do update set canonical = excluded.canonical;


-- ---------------------------------------------------------------------------
-- 3. Обновлённые KPI views — monthly и alltime с поддержкой aliases
-- ---------------------------------------------------------------------------

-- 3a. Monthly
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
  left join public.campaign_name_aliases cna on cna.alias = ms.campaign_name
  where ms.channel = 'email'
  group by coalesce(cna.canonical, ms.campaign_name),
           date_trunc('month', ms.record_date)::date
) m on m.campaign_name = s.campaign_name
     and m.month = date_trunc('month', s.date)::date
group by date_trunc('month', s.date)::date, s.campaign_name
order by month;


-- 3b. Alltime
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
  left join public.campaign_name_aliases cna on cna.alias = ms.campaign_name
  where ms.channel = 'email'
  group by coalesce(cna.canonical, ms.campaign_name)
) m on m.campaign_name = s.campaign_name
group by s.campaign_name
order by sum(s.sent_count) desc;


-- =============================================================================
-- ПРОВЕРКА
-- =============================================================================
-- Должны показать booked_meetings > 0 у большинства кампаний:
-- select campaign_name, sent_count, reply_count, booked_meetings, held_meetings
-- from public.smartlead_kpi_alltime_v
-- where booked_meetings > 0
-- order by booked_meetings desc;
--
-- Добавить новый маппинг (если появится новая кампания):
-- insert into public.campaign_name_aliases (alias, canonical)
-- values ('Новое короткое имя', 'Точное имя в Smartlead')
-- on conflict (alias) do update set canonical = excluded.canonical;
