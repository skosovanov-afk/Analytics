-- =============================================================================
-- App channel: backfill missing total_touches and replies
-- Records missing from original backfill (manual_stats_backfill_inxy_...)
-- Source: Cold Outreach Automation Services _ INXY spreadsheet (Grand Total)
-- Apply: paste into Supabase SQL Editor and run
-- =============================================================================

begin;

with src(record_date, channel, campaign_name, metric_name, value, note) as (values
  -- Affiliate World Bangkok: completely absent from original backfill
  ('2026-03-02'::date, 'app', 'Affiliate World Bangkok', 'total_touches', 633, 'App backfill fix 2026-03-03'),
  ('2026-03-02'::date, 'app', 'Affiliate World Bangkok', 'replies',       117, 'App backfill fix 2026-03-03'),

  -- MWC: total_touches=2000 already in DB, replies missing
  ('2026-03-02'::date, 'app', 'MWC', 'replies', 97, 'App backfill fix 2026-03-03'),

  -- Affiliate World Dubai: total_touches missing; replies=62 already in DB, need +14 = 76
  ('2026-03-02'::date, 'app', 'Affiliate World Dubai', 'total_touches', 935, 'App backfill fix 2026-03-03'),
  ('2026-03-02'::date, 'app', 'Affiliate World Dubai', 'replies',        14, 'App backfill fix 2026-03-03')
)
insert into public.manual_stats (record_date, channel, campaign_name, metric_name, value, note)
select s.record_date, s.channel, s.campaign_name, s.metric_name, s.value, s.note
from src s
where not exists (
  select 1 from public.manual_stats m
  where m.record_date = s.record_date
    and m.channel     = s.channel
    and coalesce(m.campaign_name, '') = coalesce(s.campaign_name, '')
    and m.metric_name = s.metric_name
);

commit;

-- Verify: should show correct totals after applying
-- select campaign_name, metric_name, sum(value)
-- from public.manual_stats
-- where channel = 'app' and metric_name in ('total_touches','replies','booked_meetings','held_meetings')
-- group by 1,2 order by 1,2;
--
-- Expected totals:
-- Affiliate World        | total_touches | 104
-- Affiliate World        | replies       | 3
-- Affiliate World        | booked_meetings | 1
-- Affiliate World        | held_meetings | 1
-- Affiliate World Bangkok | total_touches | 633
-- Affiliate World Bangkok | replies       | 117
-- Affiliate World Bangkok | booked_meetings | 21
-- Affiliate World Bangkok | held_meetings | 12
-- Affiliate World Dubai  | total_touches | 935
-- Affiliate World Dubai  | replies       | 76  (62 existing + 14 new)
-- Affiliate World Dubai  | booked_meetings | 14
-- Future Travel          | total_touches | 18
-- MWC                   | total_touches | 2000
-- MWC                   | replies       | 97
-- MWC                   | booked_meetings | 15
