-- Add reply_body column to persist actual reply text from SmartLead message-history API.
-- Currently message_body stores the outbound email text; reply_body stores the lead's response.

ALTER TABLE public.smartlead_events
  ADD COLUMN IF NOT EXISTS reply_body TEXT NULL;

-- Partial index for efficient backfill: find replies missing body text
CREATE INDEX IF NOT EXISTS idx_smartlead_events_reply_no_body
  ON public.smartlead_events (campaign_id, lead_id)
  WHERE event_type = 'reply' AND reply_body IS NULL AND lead_id IS NOT NULL;

-- Must DROP + CREATE because adding a new column in the middle changes column order
-- (CREATE OR REPLACE cannot rename/reorder existing view columns)
DROP VIEW IF EXISTS public.smartlead_replies_v;
CREATE VIEW public.smartlead_replies_v AS
SELECT
  e.id AS event_id,
  e.occurred_at,
  e.occurred_at::date AS reply_date,
  e.occurred_at::time AS reply_time,
  e.campaign_id,
  e.campaign_name,
  coalesce(e.lead_id, l.lead_id) AS lead_id,
  lower(nullif(btrim(coalesce(e.email, l.email)), '')) AS email,
  l.first_name AS lead_first_name,
  l.last_name AS lead_last_name,
  l.company AS lead_company,
  l.linkedin AS lead_linkedin,
  e.subject,
  e.sequence_number,
  e.from_email,
  e.to_email,
  e.reply_body,
  coalesce(
    l.lead_category_id,
    CASE
      WHEN coalesce(e.raw_payload->>'lead_category_id', '') ~ '^[0-9]+$'
        THEN (e.raw_payload->>'lead_category_id')::int
      ELSE NULL
    END
  ) AS lead_category_id,
  CASE coalesce(
    l.lead_category_id,
    CASE
      WHEN coalesce(e.raw_payload->>'lead_category_id', '') ~ '^[0-9]+$'
        THEN (e.raw_payload->>'lead_category_id')::int
      ELSE NULL
    END
  )
    WHEN 1 THEN 'Interested'
    WHEN 2 THEN 'Meeting Request'
    WHEN 3 THEN 'Not Interested'
    WHEN 4 THEN 'Do Not Contact'
    WHEN 5 THEN 'Information Request'
    WHEN 6 THEN 'Out Of Office'
    WHEN 7 THEN 'Wrong Person'
    WHEN 8 THEN 'Uncategorizable by AI'
    WHEN 9 THEN 'Sender Originated Bounce'
    WHEN 121483 THEN 'Ask for Referral'
    ELSE 'Uncategorized'
  END AS sentiment,
  coalesce(
    l.lead_category_id,
    CASE
      WHEN coalesce(e.raw_payload->>'lead_category_id', '') ~ '^[0-9]+$'
        THEN (e.raw_payload->>'lead_category_id')::int
      ELSE NULL
    END
  ) IN (1, 2, 5, 121483) AS is_positive,
  tc.tal_id,
  t.name AS tal_name,
  e.synced_at
FROM public.smartlead_events e
LEFT JOIN public.smartlead_leads l
  ON l.campaign_id = e.campaign_id
  AND l.lead_id = e.lead_id
LEFT JOIN public.tal_campaigns tc
  ON tc.channel = 'smartlead'
  AND tc.source_campaign_key = 'smartlead:id:' || e.campaign_id::text
LEFT JOIN public.tals t ON t.id = tc.tal_id
WHERE e.event_type = 'reply'
  AND e.occurred_at IS NOT NULL;

GRANT SELECT ON public.smartlead_replies_v TO anon, authenticated, service_role;
