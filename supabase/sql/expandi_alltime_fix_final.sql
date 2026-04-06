-- =============================================================================
-- expandi_alltime_fix_final.sql  (v3)
-- Purpose: финальный фикс expandi_kpi_alltime_v
--
-- Что исправлено:
--   1) booked/held: window function MIN(li_account_id) устраняет дублирование
--   2) booked/held: campaign_name_aliases для маппинга коротких имён
--   3) connections/messages/replies: delta-строки (manual - expandi) вместо GREATEST
--      чтобы не двоить данные (GREATEST применялся per-account → N×manual total)
--   4) Кампании только в manual_stats добавляются через UNION ALL
--   5) Добавлены недостающие LinkedIn aliases
--   6) [v3] Alias-маппинг применяется к snap_by_name и msg_by_name, чтобы
--      JOIN в manual_delta и WHERE NOT EXISTS в Блоке 3 корректно матчились
--      (было: RAW expandi name vs CANONICAL manual name → двойной счёт)
--
-- IDEMPOTENT: безопасно запускать повторно.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Дополнительные LinkedIn aliases
-- ---------------------------------------------------------------------------
INSERT INTO public.campaign_name_aliases (alias, canonical, channel)
VALUES
  ('Creator payouts',
   'Creator payouts (Ручные выплаты сотням инфлюенсеров, задержки, недовольные креаторы)',
   'linkedin'),
  ('VPN',
   'VPN (Блокировки карт, отказы платежей, потеря подписчиков)',
   'linkedin'),
  ('Digital Banking Infrastructure',
   'Digital Banking Infrastructure (Дорогие международные переводы, долго, комплаенс-риски)',
   'linkedin'),
  ('ICE',
   'ICE after the conf',
   'linkedin'),
  ('Web Summit',
   'WebSummit (after the conf)',
   'linkedin'),
  ('Web Summit Qatar',
   'WebSummit Qatar after',
   'linkedin'),
  ('iFX',
   'iFX EXPO After',
   'linkedin'),
  ('FinTech',
   'Fintech (Фокус на ускорении переводов и снижении SWIFT/SEPA издержек)',
   'linkedin'),
  -- Payroll: обе Expandi-кампании маппятся на canonical 'Payroll services'
  ('Payroll services',
   'Payroll services',
   'linkedin'),
  ('Payroll',
   'Payroll services',
   'linkedin'),
  ('Payroll (Дорогие международные переводы, долго, комплаенс-риски)',
   'Payroll services',
   'linkedin'),
  ('Web hosting providers',
   'Hosting providers (Высокие PSP комиссии, чарджбэки, задержки платежей от клиентов)',
   'linkedin'),
  ('Freelance platforms',
   'Freelance Platforms',
   'linkedin'),
  ('PG Connects London',
   'PG Connects London AFTER',
   'linkedin'),
  -- PG Connects: before conf маппится на ту же canonical
  ('PG Connects London (before the conf)',
   'PG Connects London AFTER',
   'linkedin'),
  ('Sigma Dubai',
   'Sigma after',
   'linkedin'),
  -- ICE: все варианты маппятся на одну canonical
  ('ICE before conf',
   'ICE after the conf',
   'linkedin'),
  ('ICE After the conf',
   'ICE after the conf',
   'linkedin')
ON CONFLICT (alias, channel) DO UPDATE
  SET canonical = EXCLUDED.canonical;


-- ---------------------------------------------------------------------------
-- 2. VIEW
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.expandi_kpi_alltime_v AS
WITH

latest_snap AS (
  SELECT DISTINCT ON (campaign_instance_id)
    campaign_instance_id,
    li_account_id,
    contacted_people,
    connected,
    (COALESCE(replied_first_action, 0) + COALESCE(replied_other_actions, 0)) AS snap_replies
  FROM public.expandi_campaign_stats_snapshots
  ORDER BY campaign_instance_id, snapshot_date DESC
),

snap_by_name AS (
  SELECT
    s.li_account_id,
    COALESCE(cna.canonical, ci.name) AS campaign_name,
    SUM(s.contacted_people)::int AS connection_req,
    SUM(s.connected)::int        AS accepted,
    SUM(s.snap_replies)::int     AS snap_replies
  FROM latest_snap s
  JOIN public.expandi_campaign_instances ci ON ci.id = s.campaign_instance_id
  LEFT JOIN public.campaign_name_aliases cna
    ON cna.alias = ci.name AND cna.channel = 'linkedin'
  GROUP BY s.li_account_id, COALESCE(cna.canonical, ci.name)
),

msg_by_name AS (
  SELECT
    d.li_account_id,
    COALESCE(cna.canonical, ci.name) AS campaign_name,
    SUM(d.sent_messages)::int       AS sent_messages,
    SUM(d.received_messages)::int   AS received_messages,
    SUM(d.new_replies)::int         AS replies
  FROM public.expandi_campaign_daily_v d
  JOIN public.expandi_campaign_instances ci ON ci.id = d.campaign_instance_id
  LEFT JOIN public.campaign_name_aliases cna
    ON cna.alias = ci.name AND cna.channel = 'linkedin'
  GROUP BY d.li_account_id, COALESCE(cna.canonical, ci.name)
),

-- Суммарные Expandi-данные по кампании (агрегат по всем аккаунтам)
expandi_campaign_totals AS (
  SELECT
    sn.campaign_name,
    SUM(sn.connection_req)::int                    AS total_conn,
    SUM(sn.accepted)::int                          AS total_acc,
    SUM(COALESCE(m.sent_messages, 0))::int         AS total_msg,
    SUM(COALESCE(m.replies, 0))::int               AS total_rep
  FROM snap_by_name sn
  LEFT JOIN msg_by_name m
    ON m.li_account_id = sn.li_account_id
   AND m.campaign_name = sn.campaign_name
  GROUP BY sn.campaign_name
),

-- manual_stats: activity с alias-маппингом (агрегат по кампании)
manual_activity AS (
  SELECT
    COALESCE(cna.canonical,
             COALESCE(ms.campaign_name, ms.account_name)) AS campaign_name,
    SUM(CASE WHEN ms.metric_name = 'connection_req'  THEN ms.value ELSE 0 END)::int AS connection_req,
    SUM(CASE WHEN ms.metric_name = 'accepted'        THEN ms.value ELSE 0 END)::int AS accepted,
    SUM(CASE WHEN ms.metric_name = 'sent_messages'   THEN ms.value ELSE 0 END)::int AS sent_messages,
    SUM(CASE WHEN ms.metric_name = 'replies'         THEN ms.value ELSE 0 END)::int AS replies
  FROM public.manual_stats ms
  LEFT JOIN public.campaign_name_aliases cna
    ON cna.alias   = COALESCE(ms.campaign_name, ms.account_name)
   AND cna.channel = 'linkedin'
  WHERE ms.channel = 'linkedin'
    AND ms.metric_name IN ('connection_req', 'accepted', 'sent_messages', 'replies')
  GROUP BY COALESCE(cna.canonical, COALESCE(ms.campaign_name, ms.account_name))
),


-- Дельта: насколько manual_stats превышает Expandi по каждой кампании
-- Эта разница будет добавлена одной строкой-корректировкой (account='Manual adjustment')
manual_delta AS (
  SELECT
    ma.campaign_name,
    GREATEST(ma.connection_req - COALESCE(et.total_conn, 0), 0) AS delta_conn,
    GREATEST(ma.accepted       - COALESCE(et.total_acc,  0), 0) AS delta_acc,
    -- Для messages/replies используем ПОЛНОЕ manual значение (не дельту),
    -- т.к. Block 1 показывает 0 для кампаний с manual данными
    ma.sent_messages AS delta_msg,
    ma.replies       AS delta_rep
  FROM manual_activity ma
  JOIN expandi_campaign_totals et ON et.campaign_name = ma.campaign_name
  WHERE
    GREATEST(ma.connection_req - COALESCE(et.total_conn, 0), 0) > 0
    OR GREATEST(ma.accepted    - COALESCE(et.total_acc,  0), 0) > 0
    OR ma.sent_messages > 0
    OR ma.replies > 0
),

-- manual_stats: booked/held с alias-маппингом
manual_meetings AS (
  SELECT
    COALESCE(cna.canonical,
             COALESCE(ms.campaign_name, ms.account_name)) AS campaign_name,
    SUM(CASE WHEN ms.metric_name = 'booked_meetings' THEN ms.value ELSE 0 END) AS booked_meetings,
    SUM(CASE WHEN ms.metric_name = 'held_meetings'   THEN ms.value ELSE 0 END) AS held_meetings
  FROM public.manual_stats ms
  LEFT JOIN public.campaign_name_aliases cna
    ON cna.alias   = COALESCE(ms.campaign_name, ms.account_name)
   AND cna.channel = 'linkedin'
  WHERE ms.channel = 'linkedin'
  GROUP BY COALESCE(cna.canonical, COALESCE(ms.campaign_name, ms.account_name))
)

-- ── Блок 1: Expandi-кампании (per account) ──────────────────────────────────
SELECT
  sn.li_account_id,
  COALESCE(a.name, sn.li_account_id::text) AS account_name,
  sn.campaign_name,
  sn.connection_req,
  sn.accepted,
  -- messages/replies: если есть manual данные — показываем 0 (придут через Block 2)
  -- Проверяем по-отдельности: кампания может иметь manual replies но не manual messages
  CASE WHEN ma_check.sent_messages > 0 THEN 0
       ELSE COALESCE(m.sent_messages, 0)::int END AS sent_messages,
  COALESCE(m.received_messages, 0)::int AS received_messages,
  CASE WHEN ma_check.replies > 0 THEN 0
       ELSE COALESCE(m.replies, 0)::int END AS replies,
  CASE
    WHEN sn.connection_req = 0 THEN NULL
    ELSE LEAST(ROUND(sn.accepted::numeric / sn.connection_req::numeric * 100.0, 2), 100.00)
  END AS cr_to_accept_pct,
  CASE
    WHEN ma_check.sent_messages > 0 OR ma_check.replies > 0 THEN NULL
    WHEN COALESCE(m.sent_messages, 0) = 0 THEN NULL
    ELSE ROUND(COALESCE(m.replies, 0)::numeric / m.sent_messages::numeric * 100.0, 2)
  END AS cr_to_reply_pct,
  -- booked/held только на строке min(li_account_id) по кампании
  CASE
    WHEN sn.li_account_id = MIN(sn.li_account_id) OVER (PARTITION BY sn.campaign_name)
    THEN COALESCE(mm.booked_meetings, 0)::int ELSE 0
  END AS booked_meetings,
  CASE
    WHEN sn.li_account_id = MIN(sn.li_account_id) OVER (PARTITION BY sn.campaign_name)
    THEN COALESCE(mm.held_meetings, 0)::int ELSE 0
  END AS held_meetings
FROM snap_by_name sn
LEFT JOIN public.expandi_accounts a ON a.id = sn.li_account_id
LEFT JOIN msg_by_name m
  ON m.li_account_id = sn.li_account_id AND m.campaign_name = sn.campaign_name
LEFT JOIN manual_meetings mm ON mm.campaign_name = sn.campaign_name
LEFT JOIN manual_activity ma_check ON ma_check.campaign_name = sn.campaign_name

UNION ALL

-- ── Блок 2: Дельта-строки (manual > expandi) ────────────────────────────────
SELECT
  NULL::bigint        AS li_account_id,
  'Manual adjustment' AS account_name,
  d.campaign_name,
  d.delta_conn        AS connection_req,
  d.delta_acc         AS accepted,
  d.delta_msg         AS sent_messages,
  0                   AS received_messages,
  d.delta_rep         AS replies,
  CASE WHEN d.delta_conn = 0 THEN NULL
    ELSE LEAST(ROUND(d.delta_acc::numeric / d.delta_conn::numeric * 100.0, 2), 100.00)
  END AS cr_to_accept_pct,
  CASE WHEN d.delta_msg = 0 THEN NULL
    ELSE ROUND(d.delta_rep::numeric / d.delta_msg::numeric * 100.0, 2)
  END AS cr_to_reply_pct,
  0 AS booked_meetings,
  0 AS held_meetings
FROM manual_delta d

UNION ALL

-- ── Блок 3: Кампании только в manual_stats (нет в Expandi снапшотах) ─────────
SELECT
  NULL::bigint      AS li_account_id,
  'Manual import'   AS account_name,
  ma.campaign_name,
  ma.connection_req,
  ma.accepted,
  ma.sent_messages,
  0                 AS received_messages,
  ma.replies,
  CASE WHEN ma.connection_req = 0 THEN NULL
    ELSE LEAST(ROUND(ma.accepted::numeric / ma.connection_req::numeric * 100.0, 2), 100.00)
  END AS cr_to_accept_pct,
  CASE WHEN ma.sent_messages = 0 THEN NULL
    ELSE ROUND(ma.replies::numeric / ma.sent_messages::numeric * 100.0, 2)
  END AS cr_to_reply_pct,
  COALESCE(mm.booked_meetings, 0)::int AS booked_meetings,
  COALESCE(mm.held_meetings,   0)::int AS held_meetings
FROM manual_activity ma
LEFT JOIN manual_meetings mm ON mm.campaign_name = ma.campaign_name
WHERE NOT EXISTS (
  SELECT 1 FROM snap_by_name s WHERE s.campaign_name = ma.campaign_name
)

ORDER BY li_account_id NULLS LAST, campaign_name;


-- =============================================================================
-- ВАЛИДАЦИЯ (запускать вручную)
-- =============================================================================
-- SELECT
--   SUM(connection_req) AS conn, SUM(accepted) AS accepted,
--   SUM(sent_messages)  AS messages, SUM(replies) AS replies,
--   SUM(booked_meetings) AS booked, SUM(held_meetings) AS held
-- FROM public.expandi_kpi_alltime_v;
-- Ожидаем: conn≈6390, accepted≈1755, messages≈3739, replies≈434, booked≈18, held≈13
-- Логика messages: manual как приоритет (Block2), Expandi только если нет manual (Block1)
