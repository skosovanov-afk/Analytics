begin;

-- Add index on smartlead_leads(campaign_id, lead_id) for fast join
create index if not exists smartlead_leads_campaign_lead_id_idx
  on public.smartlead_leads (campaign_id, lead_id);

-- Rewrite smartlead_replies_v: replace lateral joins with regular LEFT JOINs
create or replace view public.smartlead_replies_v as
select
  e.id as event_id,
  e.occurred_at,
  e.occurred_at::date as reply_date,
  e.occurred_at::time as reply_time,
  e.campaign_id,
  e.campaign_name,
  coalesce(e.lead_id, l.lead_id) as lead_id,
  lower(nullif(btrim(coalesce(e.email, l.email)), '')) as email,
  l.first_name as lead_first_name,
  l.last_name as lead_last_name,
  l.company as lead_company,
  l.linkedin as lead_linkedin,
  e.subject,
  e.sequence_number,
  e.from_email,
  e.to_email,
  coalesce(
    l.lead_category_id,
    case
      when coalesce(e.raw_payload->>'lead_category_id', '') ~ '^[0-9]+$'
        then (e.raw_payload->>'lead_category_id')::int
      else null
    end
  ) as lead_category_id,
  case coalesce(
    l.lead_category_id,
    case
      when coalesce(e.raw_payload->>'lead_category_id', '') ~ '^[0-9]+$'
        then (e.raw_payload->>'lead_category_id')::int
      else null
    end
  )
    when 1 then 'Interested'
    when 2 then 'Meeting Request'
    when 3 then 'Not Interested'
    when 4 then 'Do Not Contact'
    when 5 then 'Information Request'
    when 6 then 'Out Of Office'
    when 7 then 'Wrong Person'
    when 8 then 'Uncategorizable by AI'
    when 9 then 'Sender Originated Bounce'
    when 121483 then 'Ask for Referral'
    else 'Uncategorized'
  end as sentiment,
  coalesce(
    l.lead_category_id,
    case
      when coalesce(e.raw_payload->>'lead_category_id', '') ~ '^[0-9]+$'
        then (e.raw_payload->>'lead_category_id')::int
      else null
    end
  ) in (1, 2, 5, 121483) as is_positive,
  tc.tal_id,
  t.name as tal_name,
  e.synced_at
from public.smartlead_events e
left join public.smartlead_leads l
  on l.campaign_id = e.campaign_id
  and l.lead_id = e.lead_id
left join public.tal_campaigns tc
  on tc.channel = 'smartlead'
  and lower(btrim(tc.campaign_name)) = lower(btrim(e.campaign_name))
left join public.tals t on t.id = tc.tal_id
where lower(coalesce(e.event_type, '')) = 'reply'
  and e.occurred_at is not null;

commit;
