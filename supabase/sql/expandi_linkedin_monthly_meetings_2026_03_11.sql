-- =============================================================================
-- LinkedIn KPI monthly: booked/held meetings + updated monthly view
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Step 1: Insert monthly aggregate records into manual_stats
--         channel='linkedin', campaign_name=NULL, account_name=NULL
--         Data source: ПДФ по встречам (LinkedIn section)
-- ---------------------------------------------------------------------------

-- Удаляем старые записи (если есть), затем вставляем свежие
DELETE FROM public.manual_stats
WHERE channel = 'linkedin'
  AND account_name IS NULL
  AND campaign_name IS NULL
  AND record_date IN (
    '2025-08-01','2025-09-01','2025-10-01','2025-11-01',
    '2025-12-01','2026-01-01','2026-02-01','2026-03-01'
  )
  AND metric_name IN ('booked_meetings', 'held_meetings');

INSERT INTO public.manual_stats (channel, account_name, campaign_name, record_date, metric_name, value, note)
VALUES
  ('linkedin', NULL, NULL, '2025-08-01', 'booked_meetings', 2, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2025-08-01', 'held_meetings',   2, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2025-09-01', 'booked_meetings', 2, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2025-09-01', 'held_meetings',   1, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2025-10-01', 'booked_meetings', 2, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2025-10-01', 'held_meetings',   1, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2025-11-01', 'booked_meetings', 2, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2025-11-01', 'held_meetings',   1, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2025-12-01', 'booked_meetings', 1, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2025-12-01', 'held_meetings',   2, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2026-01-01', 'booked_meetings', 2, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2026-01-01', 'held_meetings',   2, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2026-02-01', 'booked_meetings', 5, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2026-02-01', 'held_meetings',   4, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2026-03-01', 'booked_meetings', 2, 'Monthly aggregate from PDF 2026-03-11'),
  ('linkedin', NULL, NULL, '2026-03-01', 'held_meetings',   1, 'Monthly aggregate from PDF 2026-03-11');


-- ---------------------------------------------------------------------------
-- Step 2: Update expandi_kpi_monthly_v to include booked/held
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW public.expandi_kpi_monthly_v AS
SELECT
  date_trunc('month', d.day)::date  AS month,
  d.li_account_id,
  d.account_name,
  d.campaign_name,
  SUM(d.connection_req)::int        AS connection_req,
  SUM(d.accepted)::int              AS accepted,
  SUM(d.sent_messages)::int         AS sent_messages,
  SUM(d.received_messages)::int     AS received_messages,
  SUM(d.replies)::int               AS replies,
  CASE
    WHEN SUM(d.connection_req) = 0 THEN NULL
    ELSE LEAST(ROUND(
      (SUM(d.accepted)::numeric / SUM(d.connection_req)::numeric) * 100.0, 2
    ), 100.00)
  END AS cr_to_accept_pct,
  CASE
    WHEN SUM(d.sent_messages) = 0 THEN NULL
    ELSE ROUND(
      (SUM(d.replies)::numeric / SUM(d.sent_messages)::numeric) * 100.0, 2
    )
  END AS cr_to_reply_pct,
  COALESCE(mt.booked_meetings, 0)::int AS booked_meetings,
  COALESCE(mt.held_meetings,   0)::int AS held_meetings
FROM public.expandi_kpi_daily_v d
LEFT JOIN (
  SELECT
    date_trunc('month', record_date)::date AS month,
    SUM(CASE WHEN metric_name = 'booked_meetings' THEN value ELSE 0 END) AS booked_meetings,
    SUM(CASE WHEN metric_name = 'held_meetings'   THEN value ELSE 0 END) AS held_meetings
  FROM public.manual_stats
  WHERE channel = 'linkedin'
    AND campaign_name IS NULL
    AND account_name IS NULL
  GROUP BY date_trunc('month', record_date)::date
) mt ON mt.month = date_trunc('month', d.day)::date
GROUP BY
  date_trunc('month', d.day)::date,
  d.li_account_id,
  d.account_name,
  d.campaign_name,
  mt.booked_meetings,
  mt.held_meetings
ORDER BY month;


-- =============================================================================
-- ПРОВЕРКА
-- =============================================================================
-- SELECT month,
--   SUM(connection_req)  AS connections,
--   SUM(replies)         AS replies,
--   MAX(booked_meetings) AS booked,
--   MAX(held_meetings)   AS held
-- FROM public.expandi_kpi_monthly_v
-- GROUP BY month
-- ORDER BY month;
-- Ожидаем: сумма booked по всем месяцам = 18, held = 14
