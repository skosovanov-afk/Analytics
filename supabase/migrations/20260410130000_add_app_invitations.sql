-- =============================================================================
-- Add app_invitations metric to app KPI view and TAL analytics
-- Regression fix: client code (/api/tals, /tals, /hypotheses, /dashboard)
-- references `tal_analytics_v.app_invitations`, but the column was never
-- propagated through `app_kpi_alltime_v` → `tal_analytics_v`.
-- Source: manual_stats.metric_name = 'invitations' where channel = 'app'
-- =============================================================================

BEGIN;

-- Drop dependent view first (TAL depends on app KPI view)
DROP VIEW IF EXISTS public.tal_analytics_v;
DROP VIEW IF EXISTS public.app_kpi_alltime_v;

-- ---------------------------------------------------------------------------
-- 1. app_kpi_alltime_v — add invitations column
-- ---------------------------------------------------------------------------
CREATE VIEW public.app_kpi_alltime_v AS
SELECT
  coalesce(campaign_name, account_name) AS campaign_name,
  sum(case when metric_name = 'invitations'     then value else 0 end)::int AS invitations,
  sum(case when metric_name = 'total_touches'   then value else 0 end)::int AS total_touches,
  sum(case when metric_name = 'replies'         then value else 0 end)::int AS replies,
  sum(case when metric_name = 'booked_meetings' then value else 0 end)::int AS booked_meetings,
  sum(case when metric_name = 'held_meetings'   then value else 0 end)::int AS held_meetings,
  sum(case when metric_name = 'qualified_leads' then value else 0 end)::int AS qualified_leads,
  case
    when sum(case when metric_name = 'total_touches' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'replies' then value else 0 end)::numeric
      / sum(case when metric_name = 'total_touches' then value else 0 end)::numeric * 100, 2
    )
  end AS cr_to_reply_pct,
  case
    when sum(case when metric_name = 'replies' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'booked_meetings' then value else 0 end)::numeric
      / sum(case when metric_name = 'replies' then value else 0 end)::numeric * 100, 2
    )
  end AS cr_to_booked_pct,
  case
    when sum(case when metric_name = 'booked_meetings' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'held_meetings' then value else 0 end)::numeric
      / sum(case when metric_name = 'booked_meetings' then value else 0 end)::numeric * 100, 2
    )
  end AS cr_booked_to_held_pct
FROM public.manual_stats
WHERE channel = 'app'
GROUP BY coalesce(campaign_name, account_name)
ORDER BY total_touches DESC;

GRANT SELECT ON public.app_kpi_alltime_v TO authenticated;

-- ---------------------------------------------------------------------------
-- 2. tal_analytics_v — add app_invitations
-- (full recreate; unchanged except for the `app_source` / `app` CTEs and final SELECT)
-- ---------------------------------------------------------------------------
CREATE VIEW public.tal_analytics_v AS
WITH sl_source AS (
  SELECT
    'smartlead:id:' || campaign_id::text AS source_campaign_key,
    max(campaign_name) AS campaign_name,
    sum(sent_count)::int AS sent_count,
    sum(reply_count)::int AS reply_count
  FROM public.smartlead_stats_daily
  WHERE campaign_id IS NOT NULL
    AND campaign_name IS NOT NULL
  GROUP BY campaign_id
),
sl_meetings AS (
  SELECT
    campaign_name,
    max(booked_meetings)::int AS booked_meetings,
    max(held_meetings)::int AS held_meetings,
    max(qualified_leads)::int AS qualified_leads
  FROM public.smartlead_kpi_alltime_v
  GROUP BY campaign_name
),
sl AS (
  SELECT
    tc.tal_id,
    sum(s.sent_count)::int    AS email_sent,
    sum(s.reply_count)::int   AS email_replies,
    CASE
      WHEN sum(s.sent_count) = 0 THEN NULL
      ELSE round(sum(s.reply_count)::numeric / sum(s.sent_count) * 100, 2)
    END AS email_reply_rate,
    coalesce(sum(m.booked_meetings), 0)::int AS email_meetings,
    coalesce(sum(m.held_meetings), 0)::int   AS email_held_meetings,
    coalesce(sum(m.qualified_leads), 0)::int AS email_qualified_leads
  FROM public.tal_campaigns tc
  JOIN sl_source s
    ON (
      tc.source_campaign_key IS NOT NULL
      AND tc.source_campaign_key = s.source_campaign_key
    ) OR (
      tc.source_campaign_key IS NULL
      AND s.campaign_name = tc.campaign_name
    )
  LEFT JOIN sl_meetings m ON m.campaign_name = s.campaign_name
  WHERE tc.channel = 'smartlead'
  GROUP BY tc.tal_id
),
ex_source AS (
  SELECT
    'expandi:canonical:' || lower(btrim(campaign_name)) AS source_campaign_key,
    campaign_name,
    CASE
      WHEN campaign_missing_in_live_api
        OR (
          coalesce(current_instances, 0) = 0
          AND coalesce(api_connection_req, 0) = 0
          AND coalesce(api_accepted, 0) = 0
          AND coalesce(api_replies, 0) = 0
          AND (
            coalesce(manual_connection_req, 0) > 0
            OR coalesce(manual_accepted, 0) > 0
            OR coalesce(manual_sent_messages, 0) > 0
            OR coalesce(manual_replies, 0) > 0
          )
        )
      THEN coalesce(manual_connection_req, 0)
      ELSE coalesce(api_connection_req, 0)
    END::int AS li_invited,
    CASE
      WHEN campaign_missing_in_live_api
        OR (
          coalesce(current_instances, 0) = 0
          AND coalesce(api_connection_req, 0) = 0
          AND coalesce(api_accepted, 0) = 0
          AND coalesce(api_replies, 0) = 0
          AND (
            coalesce(manual_connection_req, 0) > 0
            OR coalesce(manual_accepted, 0) > 0
            OR coalesce(manual_sent_messages, 0) > 0
            OR coalesce(manual_replies, 0) > 0
          )
        )
      THEN coalesce(manual_accepted, 0)
      ELSE coalesce(api_accepted, 0)
    END::int AS li_accepted,
    CASE
      WHEN campaign_missing_in_live_api
        OR (
          coalesce(current_instances, 0) = 0
          AND coalesce(api_connection_req, 0) = 0
          AND coalesce(api_accepted, 0) = 0
          AND coalesce(api_replies, 0) = 0
          AND (
            coalesce(manual_connection_req, 0) > 0
            OR coalesce(manual_accepted, 0) > 0
            OR coalesce(manual_sent_messages, 0) > 0
            OR coalesce(manual_replies, 0) > 0
          )
        )
      THEN coalesce(manual_replies, 0)
      ELSE coalesce(api_replies, 0)
    END::int AS li_replies,
    booked_meetings,
    held_meetings,
    qualified_leads
  FROM public.linkedin_kpi_alltime_v2
),
ex AS (
  SELECT
    tc.tal_id,
    sum(e.li_invited)::int AS li_invited,
    sum(e.li_accepted)::int AS li_accepted,
    sum(e.li_replies)::int AS li_replies,
    CASE
      WHEN sum(e.li_invited) = 0 THEN NULL
      ELSE round(sum(e.li_accepted)::numeric / sum(e.li_invited) * 100, 2)
    END AS li_accept_rate,
    coalesce(sum(e.booked_meetings), 0)::int AS li_meetings,
    coalesce(sum(e.held_meetings), 0)::int   AS li_held_meetings,
    coalesce(sum(e.qualified_leads), 0)::int AS li_qualified_leads
  FROM public.tal_campaigns tc
  JOIN ex_source e
    ON (
      tc.source_campaign_key IS NOT NULL
      AND tc.source_campaign_key = e.source_campaign_key
    ) OR (
      tc.source_campaign_key IS NULL
      AND e.campaign_name = tc.campaign_name
    )
  WHERE tc.channel = 'expandi'
  GROUP BY tc.tal_id
),
app_source AS (
  SELECT
    'app:name:' || lower(btrim(campaign_name)) AS source_campaign_key,
    campaign_name,
    invitations,
    total_touches,
    replies,
    booked_meetings,
    held_meetings,
    qualified_leads
  FROM public.app_kpi_alltime_v
),
app AS (
  SELECT
    tc.tal_id,
    sum(a.invitations)::int    AS app_invitations,
    sum(a.total_touches)::int  AS app_touches,
    sum(a.replies)::int        AS app_replies,
    CASE
      WHEN sum(a.total_touches) = 0 THEN NULL
      ELSE round(sum(a.replies)::numeric / sum(a.total_touches) * 100, 2)
    END AS app_reply_rate,
    coalesce(sum(a.booked_meetings), 0)::int AS app_meetings,
    coalesce(sum(a.held_meetings), 0)::int   AS app_held_meetings,
    coalesce(sum(a.qualified_leads), 0)::int AS app_qualified_leads
  FROM public.tal_campaigns tc
  JOIN app_source a
    ON (
      tc.source_campaign_key IS NOT NULL
      AND tc.source_campaign_key = a.source_campaign_key
    ) OR (
      tc.source_campaign_key IS NULL
      AND a.campaign_name = tc.campaign_name
    )
  WHERE tc.channel = 'app'
  GROUP BY tc.tal_id
),
tg_source AS (
  SELECT
    'telegram:name:' || lower(btrim(campaign_name)) AS source_campaign_key,
    campaign_name,
    total_touches,
    replies,
    booked_meetings,
    held_meetings,
    qualified_leads
  FROM public.telegram_kpi_alltime_v
),
tg AS (
  SELECT
    tc.tal_id,
    sum(g.total_touches)::int AS tg_touches,
    sum(g.replies)::int       AS tg_replies,
    CASE
      WHEN sum(g.total_touches) = 0 THEN NULL
      ELSE round(sum(g.replies)::numeric / sum(g.total_touches) * 100, 2)
    END AS tg_reply_rate,
    coalesce(sum(g.booked_meetings), 0)::int AS tg_meetings,
    coalesce(sum(g.held_meetings), 0)::int   AS tg_held_meetings,
    coalesce(sum(g.qualified_leads), 0)::int AS tg_qualified_leads
  FROM public.tal_campaigns tc
  JOIN tg_source g
    ON (
      tc.source_campaign_key IS NOT NULL
      AND tc.source_campaign_key = g.source_campaign_key
    ) OR (
      tc.source_campaign_key IS NULL
      AND g.campaign_name = tc.campaign_name
    )
  WHERE tc.channel = 'telegram'
  GROUP BY tc.tal_id
)
SELECT
  t.id,
  t.name,
  t.description,
  t.criteria,
  t.created_at,
  t.updated_at,
  -- Email
  coalesce(sl.email_sent, 0)      AS email_sent,
  coalesce(sl.email_replies, 0)   AS email_replies,
  sl.email_reply_rate,
  coalesce(sl.email_meetings, 0)  AS email_meetings,
  coalesce(sl.email_held_meetings, 0) AS email_held_meetings,
  coalesce(sl.email_qualified_leads, 0) AS email_qualified_leads,
  -- LinkedIn
  coalesce(ex.li_invited, 0)      AS li_invited,
  coalesce(ex.li_accepted, 0)     AS li_accepted,
  coalesce(ex.li_replies, 0)      AS li_replies,
  ex.li_accept_rate,
  coalesce(ex.li_meetings, 0)     AS li_meetings,
  coalesce(ex.li_held_meetings, 0) AS li_held_meetings,
  coalesce(ex.li_qualified_leads, 0) AS li_qualified_leads,
  -- App
  coalesce(app.app_invitations, 0)  AS app_invitations,
  coalesce(app.app_touches, 0)      AS app_touches,
  coalesce(app.app_replies, 0)      AS app_replies,
  app.app_reply_rate,
  coalesce(app.app_meetings, 0)     AS app_meetings,
  coalesce(app.app_held_meetings, 0) AS app_held_meetings,
  coalesce(app.app_qualified_leads, 0) AS app_qualified_leads,
  -- Telegram
  coalesce(tg.tg_touches, 0)      AS tg_touches,
  coalesce(tg.tg_replies, 0)      AS tg_replies,
  tg.tg_reply_rate,
  coalesce(tg.tg_meetings, 0)     AS tg_meetings,
  coalesce(tg.tg_held_meetings, 0) AS tg_held_meetings,
  coalesce(tg.tg_qualified_leads, 0) AS tg_qualified_leads,
  -- Total
  (
    coalesce(sl.email_meetings, 0)
    + coalesce(ex.li_meetings, 0)
    + coalesce(app.app_meetings, 0)
    + coalesce(tg.tg_meetings, 0)
  ) AS total_meetings,
  (
    coalesce(sl.email_held_meetings, 0)
    + coalesce(ex.li_held_meetings, 0)
    + coalesce(app.app_held_meetings, 0)
    + coalesce(tg.tg_held_meetings, 0)
  ) AS total_held_meetings,
  (
    coalesce(sl.email_qualified_leads, 0)
    + coalesce(ex.li_qualified_leads, 0)
    + coalesce(app.app_qualified_leads, 0)
    + coalesce(tg.tg_qualified_leads, 0)
  ) AS total_qualified_leads
FROM public.tals t
LEFT JOIN sl ON sl.tal_id = t.id
LEFT JOIN ex ON ex.tal_id = t.id
LEFT JOIN app ON app.tal_id = t.id
LEFT JOIN tg ON tg.tal_id = t.id;

GRANT SELECT ON public.tal_analytics_v TO authenticated;

COMMIT;
