-- Fix: expandi_replies_v uses is_inbound=true but Expandi API sets both
-- is_inbound=true AND is_outbound=true for all messages.
-- Use normalized direction logic from expandi_campaign_daily_mv instead.

create or replace view public.expandi_replies_v as
with msg_norm as (
  select
    m.*,
    case
      when lower(nullif(btrim(coalesce(m.direction, '')), '')) = 'inbound'    then true
      when lower(nullif(btrim(coalesce(m.direction, '')), '')) = 'outbound'   then false
      when coalesce(m.is_inbound, false) and not coalesce(m.is_outbound, false) then true
      when coalesce(m.is_outbound, false) and not coalesce(m.is_inbound, false) then false
      when m.received_datetime is not null and m.send_datetime is null         then true
      when m.send_datetime is not null and m.received_datetime is null         then false
      -- both true = outbound (our message), so inbound_norm = false
      when coalesce(m.is_outbound, false) and coalesce(m.is_inbound, false)   then false
      else null
    end as is_inbound_norm
  from public.expandi_messages m
  where m.messenger_id is not null
),
-- Find first outbound message per messenger (to identify replies as messages AFTER first outbound)
first_outbound as (
  select messenger_id, min(coalesce(send_datetime, received_datetime, event_datetime)) as first_out_at
  from msg_norm
  where is_inbound_norm = false
    and nullif(btrim(coalesce(body, '')), '') is not null
  group by 1
)
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
from msg_norm m
join first_outbound fo on fo.messenger_id = m.messenger_id
join public.expandi_messengers mr on mr.id = m.messenger_id
left join public.expandi_campaign_instances ci on ci.id = m.campaign_instance_id
left join public.expandi_accounts a on a.id = coalesce(m.li_account_id, ci.li_account_id)
left join public.tal_campaigns tc
  on tc.channel = 'expandi'
  and lower(btrim(tc.campaign_name)) = lower(btrim(ci.name))
left join public.tals t on t.id = tc.tal_id
where m.is_inbound_norm = true
  and nullif(btrim(coalesce(m.body, '')), '') is not null
  and coalesce(m.received_datetime, m.event_datetime, m.send_datetime) >= fo.first_out_at;

grant select on public.expandi_replies_v to anon, authenticated, service_role;
