begin;

create index if not exists smartlead_leads_campaign_email_lower_idx
  on public.smartlead_leads (campaign_id, lower(email));

create or replace view public.smartlead_reply_events_categorized_v as
with reply_events as (
  select
    e.id::text as event_id,
    e.occurred_at::date as date,
    e.campaign_id,
    e.campaign_name,
    coalesce(e.lead_id, lead_by_id.lead_id, lead_by_email.lead_id) as lead_id,
    lower(
      nullif(
        btrim(coalesce(e.email, lead_by_id.email, lead_by_email.email)),
        ''
      )
    ) as email,
    coalesce(
      lead_by_id.lead_category_id,
      lead_by_email.lead_category_id,
      case
        when coalesce(e.raw_payload->>'lead_category_id', '') ~ '^[0-9]+$'
          then (e.raw_payload->>'lead_category_id')::int
        else null
      end
    ) as lead_category_id
  from public.smartlead_events e
  left join lateral (
    select l.lead_id, l.email, l.lead_category_id
    from public.smartlead_leads l
    where l.campaign_id = e.campaign_id
      and e.lead_id is not null
      and l.lead_id = e.lead_id
    limit 1
  ) lead_by_id on true
  left join lateral (
    select l.lead_id, l.email, l.lead_category_id
    from public.smartlead_leads l
    where l.campaign_id = e.campaign_id
      and lower(coalesce(l.email, '')) = lower(coalesce(e.email, ''))
    order by
      l.updated_at_source desc nulls last,
      l.synced_at desc nulls last,
      l.created_at_source desc nulls last,
      l.lead_id desc
    limit 1
  ) lead_by_email on true
  where lower(coalesce(e.event_type, '')) = 'reply'
    and e.occurred_at is not null
)
select
  re.event_id,
  re.date,
  re.campaign_id,
  re.campaign_name,
  re.lead_id,
  re.email,
  coalesce(
    re.email,
    case
      when re.lead_id is not null then 'lead:' || re.lead_id::text
      else 'event:' || re.event_id
    end
  ) as lead_key,
  re.lead_category_id,
  case re.lead_category_id
    when 1 then 'Interested'
    when 2 then 'Meeting Request'
    when 3 then 'Not Interested'
    when 4 then 'Do Not Contact'
    when 5 then 'Information Request'
    when 6 then 'Out Of Office'
    when 7 then 'Wrong Person'
    when 8 then 'Uncategorizable by AI'
    when 9 then 'Sender Originated Bounce'
    when 121483 then 'Didn''t Attend (Ask for Referral)'
    else 'Uncategorized'
  end as lead_category_name,
  (re.lead_category_id in (1, 2, 5, 121483)) as is_positive_intent,
  (re.lead_category_id = 6) as is_out_of_office,
  (re.lead_category_id = 7) as is_wrong_person,
  (re.lead_category_id = 8) as is_uncategorizable,
  (re.lead_category_id = 9) as is_sender_bounce
from reply_events re;

create or replace view public.smartlead_reply_category_daily_v as
select
  date,
  campaign_id,
  campaign_name,
  lead_category_id,
  lead_category_name,
  count(*)::int as reply_event_count,
  count(distinct lead_key)::int as replied_leads_count,
  count(distinct lead_key) filter (where is_positive_intent)::int as positive_intent_count,
  count(distinct lead_key) filter (where is_out_of_office)::int as out_of_office_count,
  count(distinct lead_key) filter (where is_wrong_person)::int as wrong_person_count,
  count(distinct lead_key) filter (where is_uncategorizable)::int as uncategorizable_count,
  count(distinct lead_key) filter (where is_sender_bounce)::int as sender_bounce_count
from public.smartlead_reply_events_categorized_v
group by
  date,
  campaign_id,
  campaign_name,
  lead_category_id,
  lead_category_name;

grant select on public.smartlead_reply_events_categorized_v to anon, authenticated, service_role;
grant select on public.smartlead_reply_category_daily_v to anon, authenticated, service_role;

commit;
