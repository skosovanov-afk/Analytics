-- Generated on 2026-03-02 from Cold Outreach Automation Services _ INXY - Stats (2).csv
-- Scope: channels linkedin/app/telegram; metrics booked_meetings/held_meetings only
-- record_date = '2026-03-02' for all rows (aggregate totals backfill)
-- Excludes rows with value = 0
-- Excludes linkedin/Payroll services rows already inserted in
--   manual_stats_backfill_inxy_linkedin_telegram_app_2026_03_02.sql
--   (1 booked on 2025-08-22, 1 held on 2025-08-28)
begin;

with src(record_date, channel, account_name, campaign_name, metric_name, value, note) as (
  values
    -- linkedin
    ('2026-03-02'::date, 'linkedin', null, 'Sigma Dubai',             'booked_meetings', 1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Sigma Dubai',             'held_meetings',   1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'WHMCS',                   'booked_meetings', 2, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'WHMCS',                   'held_meetings',   1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Affiliate World',         'booked_meetings', 2, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Affiliate World',         'held_meetings',   1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Cryptwerk',               'booked_meetings', 1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Cryptwerk',               'held_meetings',   1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Freelance platforms',     'booked_meetings', 1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Telecom',                 'booked_meetings', 2, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Telecom',                 'held_meetings',   2, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'FinTech',                 'booked_meetings', 1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'FinTech',                 'held_meetings',   1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    -- Payroll services: CSV Grand Total booked=2, held=1.
    -- Existing backfill has 1 booked (2025-08-22) + 1 held (2025-08-28).
    -- Inserting 1 additional booked; held delta = 0 so omitted.
    ('2026-03-02'::date, 'linkedin', null, 'Payroll services',        'booked_meetings', 1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Web hosting providers',   'booked_meetings', 1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Web hosting providers',   'held_meetings',   1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Slush',                   'booked_meetings', 1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Slush',                   'held_meetings',   1, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Affiliate World Bangkok', 'booked_meetings', 2, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'linkedin', null, 'Affiliate World Bangkok', 'held_meetings',   2, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    -- app
    ('2026-03-02'::date, 'app',      null, 'MWC',                     'booked_meetings', 8,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'app',      null, 'Affiliate World Dubai',   'booked_meetings', 13, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'app',      null, 'Affiliate World',         'booked_meetings', 1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'app',      null, 'Affiliate World',         'held_meetings',   1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'app',      null, 'Affiliate World Bangkok', 'booked_meetings', 21, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'app',      null, 'Affiliate World Bangkok', 'held_meetings',   12, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    -- telegram
    ('2026-03-02'::date, 'telegram', null, 'Affiliate World',         'booked_meetings', 1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'telegram', null, 'Affiliate World',         'held_meetings',   1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'telegram', null, 'Coinfest',                'booked_meetings', 1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'telegram', null, 'Coinfest',                'held_meetings',   1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)')
)
insert into public.manual_stats (record_date, channel, account_name, campaign_name, metric_name, value, note)
select s.record_date, s.channel, s.account_name, s.campaign_name, s.metric_name, s.value, s.note
from src as s
where not exists (
  select 1
  from public.manual_stats as m
  where m.record_date = s.record_date
    and m.channel = s.channel
    and coalesce(m.account_name, '') = coalesce(s.account_name, '')
    and coalesce(m.campaign_name, '') = coalesce(s.campaign_name, '')
    and m.metric_name = s.metric_name
);

commit;

-- Validation query:
-- select channel, campaign_name, metric_name, sum(value)
-- from public.manual_stats
-- where record_date = '2026-03-02'
--   and channel in ('linkedin', 'app', 'telegram')
--   and metric_name in ('booked_meetings', 'held_meetings')
-- group by 1, 2, 3
-- order by 1, 2, 3;
