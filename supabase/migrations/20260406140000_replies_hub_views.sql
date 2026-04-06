begin;

-- ── Indexes for view performance ─────────────────────────────────────────────

create index if not exists smartlead_events_reply_occurred_idx
  on public.smartlead_events (occurred_at desc)
  where lower(event_type) = 'reply';

create index if not exists expandi_messages_inbound_received_idx
  on public.expandi_messages (received_datetime desc)
  where is_inbound = true;

create index if not exists smartlead_leads_campaign_email_lower_idx
  on public.smartlead_leads (campaign_id, lower(email));

-- ── Smartlead Email Replies View ─────────────────────────────────────────────

create or replace view public.smartlead_replies_v as
with reply_events as (
  select
    e.id as event_id,
    e.occurred_at,
    e.occurred_at::date as reply_date,
    e.occurred_at::time as reply_time,
    e.campaign_id,
    e.campaign_name,
    e.subject,
    e.sequence_number,
    e.from_email,
    e.to_email,
    coalesce(e.lead_id, lead_by_id.lead_id, lead_by_email.lead_id) as lead_id,
    lower(nullif(btrim(coalesce(e.email, lead_by_id.email, lead_by_email.email)), '')) as email,
    coalesce(lead_by_id.first_name, lead_by_email.first_name) as lead_first_name,
    coalesce(lead_by_id.last_name, lead_by_email.last_name) as lead_last_name,
    coalesce(lead_by_id.company, lead_by_email.company) as lead_company,
    coalesce(lead_by_id.linkedin, lead_by_email.linkedin) as lead_linkedin,
    coalesce(
      lead_by_id.lead_category_id,
      lead_by_email.lead_category_id,
      case
        when coalesce(e.raw_payload->>'lead_category_id', '') ~ '^[0-9]+$'
          then (e.raw_payload->>'lead_category_id')::int
        else null
      end
    ) as lead_category_id,
    e.synced_at
  from public.smartlead_events e
  left join lateral (
    select l.lead_id, l.email, l.first_name, l.last_name, l.company, l.linkedin, l.lead_category_id
    from public.smartlead_leads l
    where l.campaign_id = e.campaign_id
      and e.lead_id is not null
      and l.lead_id = e.lead_id
    limit 1
  ) lead_by_id on true
  left join lateral (
    select l.lead_id, l.email, l.first_name, l.last_name, l.company, l.linkedin, l.lead_category_id
    from public.smartlead_leads l
    where l.campaign_id = e.campaign_id
      and lower(coalesce(l.email, '')) = lower(coalesce(e.email, ''))
    order by l.updated_at_source desc nulls last, l.synced_at desc nulls last
    limit 1
  ) lead_by_email on true
  where lower(coalesce(e.event_type, '')) = 'reply'
    and e.occurred_at is not null
)
select
  re.event_id,
  re.occurred_at,
  re.reply_date,
  re.reply_time,
  re.campaign_id,
  re.campaign_name,
  re.lead_id,
  re.email,
  re.lead_first_name,
  re.lead_last_name,
  re.lead_company,
  re.lead_linkedin,
  re.subject,
  re.sequence_number,
  re.from_email,
  re.to_email,
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
    when 121483 then 'Ask for Referral'
    else 'Uncategorized'
  end as sentiment,
  (re.lead_category_id in (1, 2, 5, 121483)) as is_positive,
  tc.tal_id,
  t.name as tal_name,
  re.synced_at
from reply_events re
left join public.tal_campaigns tc
  on tc.channel = 'smartlead'
  and lower(btrim(tc.campaign_name)) = lower(btrim(re.campaign_name))
left join public.tals t on t.id = tc.tal_id;

-- ── Expandi LinkedIn Replies View ────────────────────────────────────────────

create or replace view public.expandi_replies_v as
select
  m.id as message_id,
  coalesce(m.received_datetime, m.event_datetime, m.send_datetime) as occurred_at,
  coalesce(m.received_datetime, m.event_datetime, m.send_datetime)::date as reply_date,
  coalesce(m.received_datetime, m.event_datetime, m.send_datetime)::time as reply_time,
  a.id as li_account_id,
  coalesce(a.name, a.login) as account_name,
  ci.id as campaign_instance_id,
  ci.name as campaign_name,
  mr.id as messenger_id,
  mr.contact_name,
  mr.contact_email,
  mr.contact_job_title,
  mr.contact_company_name,
  mr.contact_profile_link,
  m.body as reply_body,
  mr.is_replied,
  mr.replied_at,
  mr.nr_steps_before_responding,
  tc.tal_id,
  t.name as tal_name,
  m.synced_at
from public.expandi_messages m
join public.expandi_messengers mr on mr.id = m.messenger_id
left join public.expandi_campaign_instances ci on ci.id = m.campaign_instance_id
left join public.expandi_accounts a on a.id = coalesce(m.li_account_id, ci.li_account_id)
left join public.tal_campaigns tc
  on tc.channel = 'expandi'
  and lower(btrim(tc.campaign_name)) = lower(btrim(ci.name))
left join public.tals t on t.id = tc.tal_id
where m.is_inbound = true;

-- ── Grants ───────────────────────────────────────────────────────────────────

grant select on public.smartlead_replies_v to anon, authenticated, service_role;
grant select on public.expandi_replies_v to anon, authenticated, service_role;

commit;
