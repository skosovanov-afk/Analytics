-- Generated on 2026-03-02 from Cold Outreach Automation Services _ INXY - Stats (2).csv
-- Scope: channel email; metrics booked_meetings / held_meetings (Grand Total Fact, value > 0 only)
-- record_date = '2026-03-02' for all rows (aggregate totals backfill)
-- Excludes rows with value = 0 (TES held_meetings = 0 omitted)
begin;

with src(record_date, channel, account_name, campaign_name, metric_name, value, note) as (
  values
    ('2026-03-02'::date, 'email', null, 'TES',                             'booked_meetings', 1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'iFX',                             'booked_meetings', 3,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'iFX',                             'held_meetings',   3,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'MICE',                            'booked_meetings', 1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'MICE',                            'held_meetings',   1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Sigma Dubai',                     'booked_meetings', 3,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Sigma Dubai',                     'held_meetings',   3,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'SBC Conference',                  'booked_meetings', 19, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'SBC Conference',                  'held_meetings',   15, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Travel Tech Asia',                'booked_meetings', 4,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Travel Tech Asia',                'held_meetings',   3,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Affiliate World',                 'booked_meetings', 9,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Affiliate World',                 'held_meetings',   8,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Sigma Rome',                      'booked_meetings', 21, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Sigma Rome',                      'held_meetings',   18, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Slush',                           'booked_meetings', 10, 'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Slush',                           'held_meetings',   8,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'ICE',                             'booked_meetings', 3,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'ICE',                             'held_meetings',   2,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Web Summit',                      'booked_meetings', 9,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Web Summit',                      'held_meetings',   4,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Cross-Border Corporate Payments', 'booked_meetings', 3,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Cross-Border Corporate Payments', 'held_meetings',   2,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'FinTech',                         'booked_meetings', 4,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'FinTech',                         'held_meetings',   4,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'AdTech',                          'booked_meetings', 1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'AdTech',                          'held_meetings',   1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Payroll services',                'booked_meetings', 1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Payroll services',                'held_meetings',   1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Future Travel',                   'booked_meetings', 2,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Future Travel',                   'held_meetings',   2,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'PG Connects London',              'booked_meetings', 1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'PG Connects London',              'held_meetings',   1,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Web Summit Qatar',                'booked_meetings', 4,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)'),
    ('2026-03-02'::date, 'email', null, 'Web Summit Qatar',                'held_meetings',   4,  'INXY CSV backfill (source: Cold Outreach Automation Services _ INXY - Stats (2).csv)')
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

-- Validation queries:
-- select campaign_name, metric_name, value from public.manual_stats where channel = 'email' and record_date = '2026-03-02' order by campaign_name, metric_name;
-- select metric_name, sum(value) from public.manual_stats where channel = 'email' and record_date = '2026-03-02' group by 1;
-- Expected: booked_meetings = 99, held_meetings = 80
