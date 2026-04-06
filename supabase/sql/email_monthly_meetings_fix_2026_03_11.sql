-- =============================================================================
-- Fix: Email monthly meetings distribution
--
-- Problem: all per-campaign records dated 2026-03-02 → all meetings show in March
-- Solution: insert monthly aggregate records (campaign_name = NULL) from PDF source
--           and update smartlead_kpi_monthly_v to join on month-level NULL records
--
-- Alltime view: unchanged (joins on campaign_name, NULL records excluded naturally)
-- Monthly view: joins on month only using NULL-campaign records → correct monthly totals
-- =============================================================================

-- Step 1: Insert monthly aggregate records from PDF data
-- campaign_name = NULL marks these as monthly totals (not per-campaign)
INSERT INTO public.manual_stats (record_date, channel, campaign_name, metric_name, value, note)
VALUES
  ('2025-08-01', 'email', NULL, 'booked_meetings', 1,  'PDF monthly total Aug 2025'),
  ('2025-08-01', 'email', NULL, 'held_meetings',   1,  'PDF monthly total Aug 2025'),
  ('2025-09-01', 'email', NULL, 'booked_meetings', 9,  'PDF monthly total Sep 2025'),
  ('2025-09-01', 'email', NULL, 'held_meetings',   7,  'PDF monthly total Sep 2025'),
  ('2025-10-01', 'email', NULL, 'booked_meetings', 17, 'PDF monthly total Oct 2025'),
  ('2025-10-01', 'email', NULL, 'held_meetings',   15, 'PDF monthly total Oct 2025'),
  ('2025-11-01', 'email', NULL, 'booked_meetings', 24, 'PDF monthly total Nov 2025'),
  ('2025-11-01', 'email', NULL, 'held_meetings',   18, 'PDF monthly total Nov 2025'),
  ('2025-12-01', 'email', NULL, 'booked_meetings', 8,  'PDF monthly total Dec 2025'),
  ('2025-12-01', 'email', NULL, 'held_meetings',   6,  'PDF monthly total Dec 2025'),
  ('2026-01-01', 'email', NULL, 'booked_meetings', 19, 'PDF monthly total Jan 2026'),
  ('2026-01-01', 'email', NULL, 'held_meetings',   17, 'PDF monthly total Jan 2026'),
  ('2026-02-01', 'email', NULL, 'booked_meetings', 21, 'PDF monthly total Feb 2026'),
  ('2026-02-01', 'email', NULL, 'held_meetings',   16, 'PDF monthly total Feb 2026'),
  ('2026-03-01', 'email', NULL, 'booked_meetings', 5,  'PDF monthly total Mar 2026'),
  ('2026-03-01', 'email', NULL, 'held_meetings',   4,  'PDF monthly total Mar 2026');


-- Step 2: Update smartlead_kpi_monthly_v to use month-level join (NULL-campaign records only)
-- Each campaign row in a given month gets the same booked/held value (monthly total)
-- Frontend uses MAX per month to avoid multiplying by campaign count
CREATE OR REPLACE VIEW public.smartlead_kpi_monthly_v AS
SELECT
  date_trunc('month', s.date)::date AS month,
  s.campaign_name,
  SUM(s.sent_count)::int    AS sent_count,
  SUM(s.reply_count)::int   AS reply_count,
  CASE
    WHEN SUM(s.sent_count) = 0 THEN NULL
    ELSE ROUND(SUM(s.reply_count)::numeric / SUM(s.sent_count)::numeric * 100, 2)
  END AS reply_rate_pct,
  COALESCE(mt.booked_meetings, 0)::int AS booked_meetings,
  COALESCE(mt.held_meetings, 0)::int   AS held_meetings
FROM public.smartlead_stats_daily s
LEFT JOIN (
  SELECT
    date_trunc('month', record_date)::date AS month,
    SUM(CASE WHEN metric_name = 'booked_meetings' THEN value ELSE 0 END) AS booked_meetings,
    SUM(CASE WHEN metric_name = 'held_meetings'   THEN value ELSE 0 END) AS held_meetings
  FROM public.manual_stats
  WHERE channel = 'email'
    AND campaign_name IS NULL
  GROUP BY date_trunc('month', record_date)::date
) mt ON mt.month = date_trunc('month', s.date)::date
GROUP BY date_trunc('month', s.date)::date, s.campaign_name, mt.booked_meetings, mt.held_meetings
ORDER BY month;


-- =============================================================================
-- VERIFICATION (run after applying)
-- =============================================================================
-- Monthly totals (should match PDF):
-- SELECT month, SUM(sent_count) as sent, MAX(booked_meetings) as booked, MAX(held_meetings) as held
-- FROM smartlead_kpi_monthly_v
-- GROUP BY month ORDER BY month;
--
-- Expected: Aug:1/1, Sep:9/7, Oct:17/15, Nov:24/18, Dec:8/6, Jan:19/17, Feb:21/16, Mar:5/4
--
-- Alltime totals (unchanged):
-- SELECT SUM(booked_meetings), SUM(held_meetings) FROM smartlead_kpi_alltime_v;
-- Expected: 100, 81
