-- Expandi analytics upgrade
-- Adds campaign/message attribution columns and analytical views.
-- Run after expandi_schema_mvp.sql

begin;

alter table if exists public.expandi_messengers
  add column if not exists campaign_instance_id bigint null,
  add column if not exists campaign_id bigint null,
  add column if not exists campaign_name text null,
  add column if not exists contact_profile_link_sn text null,
  add column if not exists contact_public_identifier text null,
  add column if not exists contact_entity_urn text null,
  add column if not exists contact_job_title text null,
  add column if not exists contact_company_name text null,
  add column if not exists contact_phone text null,
  add column if not exists contact_address text null,
  add column if not exists campaign_contact_status int null,
  add column if not exists campaign_running_status int null,
  add column if not exists last_action_id bigint null,
  add column if not exists nr_steps_before_responding int null,
  add column if not exists first_outbound_at timestamptz null,
  add column if not exists first_inbound_at timestamptz null,
  add column if not exists replied_at timestamptz null,
  add column if not exists is_replied boolean null;

alter table if exists public.expandi_messages
  add column if not exists campaign_instance_id bigint null,
  add column if not exists campaign_id bigint null,
  add column if not exists campaign_step_id bigint null,
  add column if not exists direction text null,
  add column if not exists is_outbound boolean null,
  add column if not exists is_inbound boolean null,
  add column if not exists event_datetime timestamptz null,
  add column if not exists has_attachment boolean null,
  add column if not exists extracted_urls jsonb null,
  add column if not exists extracted_domains jsonb null;

create index if not exists expandi_messengers_campaign_instance_idx
  on public.expandi_messengers (campaign_instance_id);

create index if not exists expandi_messengers_campaign_id_idx
  on public.expandi_messengers (campaign_id);

create index if not exists expandi_messages_campaign_instance_idx
  on public.expandi_messages (campaign_instance_id);

create index if not exists expandi_messages_campaign_id_idx
  on public.expandi_messages (campaign_id);

create index if not exists expandi_messages_direction_idx
  on public.expandi_messages (direction);

create index if not exists expandi_messages_event_datetime_idx
  on public.expandi_messages (event_datetime desc);

create index if not exists expandi_messengers_campaign_running_status_idx
  on public.expandi_messengers (campaign_running_status);

create index if not exists expandi_messages_has_attachment_idx
  on public.expandi_messages (has_attachment);

-- Backfill attribution from timestamps and JSON payload where possible.
update public.expandi_messages
set
  is_outbound = coalesce(is_outbound, send_datetime is not null),
  is_inbound = coalesce(is_inbound, received_datetime is not null),
  direction = coalesce(
    direction,
    case
      when received_datetime is not null and send_datetime is null then 'inbound'
      when send_datetime is not null and received_datetime is null then 'outbound'
      else null
    end
  ),
  event_datetime = coalesce(event_datetime, received_datetime, send_datetime, created_at_source),
  has_attachment = coalesce(
    has_attachment,
    nullif(attachment, '') is not null
  ),
  extracted_urls = coalesce(
    extracted_urls,
    case
      when body is null then '[]'::jsonb
      else (
        select coalesce(jsonb_agg(distinct m[1]), '[]'::jsonb)
        from regexp_matches(body, '(https?://[^[:space:]]+)', 'g') as m
      )
    end
  ),
  extracted_domains = coalesce(
    extracted_domains,
    case
      when body is null then '[]'::jsonb
      else (
        select coalesce(
          jsonb_agg(distinct lower(substring(u from 'https?://([^/\\?]+)'))),
          '[]'::jsonb
        )
        from (
          select m[1] as u
          from regexp_matches(body, '(https?://[^[:space:]]+)', 'g') as m
        ) s
      )
    end
  ),
  campaign_instance_id = coalesce(
    campaign_instance_id,
    case when (raw_payload->>'campaign_instance_id') ~ '^-?[0-9]+$' then (raw_payload->>'campaign_instance_id')::bigint end,
    case when (raw_payload->>'campaign_instance') ~ '^-?[0-9]+$' then (raw_payload->>'campaign_instance')::bigint end,
    case when (raw_payload->'campaign_instance'->>'id') ~ '^-?[0-9]+$' then (raw_payload->'campaign_instance'->>'id')::bigint end
  ),
  campaign_id = coalesce(
    campaign_id,
    case when (raw_payload->>'campaign_id') ~ '^-?[0-9]+$' then (raw_payload->>'campaign_id')::bigint end,
    case when (raw_payload->>'campaign') ~ '^-?[0-9]+$' then (raw_payload->>'campaign')::bigint end,
    case when (raw_payload->'campaign'->>'id') ~ '^-?[0-9]+$' then (raw_payload->'campaign'->>'id')::bigint end
  );

update public.expandi_messengers
set
  campaign_instance_id = coalesce(
    campaign_instance_id,
    case when (raw_payload->>'campaign_instance_id') ~ '^-?[0-9]+$' then (raw_payload->>'campaign_instance_id')::bigint end,
    case when (raw_payload->>'campaign_instance') ~ '^-?[0-9]+$' then (raw_payload->>'campaign_instance')::bigint end,
    case when (raw_payload->'campaign_instance'->>'id') ~ '^-?[0-9]+$' then (raw_payload->'campaign_instance'->>'id')::bigint end
  ),
  campaign_id = coalesce(
    campaign_id,
    case when (raw_payload->>'campaign_id') ~ '^-?[0-9]+$' then (raw_payload->>'campaign_id')::bigint end,
    case when (raw_payload->>'campaign') ~ '^-?[0-9]+$' then (raw_payload->>'campaign')::bigint end,
    case when (raw_payload->'campaign'->>'id') ~ '^-?[0-9]+$' then (raw_payload->'campaign'->>'id')::bigint end
  ),
  campaign_name = coalesce(
    campaign_name,
    nullif(raw_payload->>'campaign_name', ''),
    nullif(raw_payload->'contact'->'campaigninstancecontacts_set'->0->>'campaign_instance_name', ''),
    nullif(raw_payload->'campaign_instance'->>'name', ''),
    nullif(raw_payload->'campaign'->>'name', '')
  ),
  contact_profile_link_sn = coalesce(
    contact_profile_link_sn,
    nullif(raw_payload->'contact'->>'profile_link_sn', ''),
    nullif(raw_payload->'contact'->'campaigninstancecontacts_set'->0->'contact_information'->>'profile_link_sn', '')
  ),
  contact_public_identifier = coalesce(
    contact_public_identifier,
    nullif(raw_payload->'contact'->>'public_identifier', ''),
    nullif(raw_payload->'contact'->'campaigninstancecontacts_set'->0->'contact_information'->>'public_identifier', '')
  ),
  contact_entity_urn = coalesce(
    contact_entity_urn,
    nullif(raw_payload->'contact'->>'entity_urn', ''),
    nullif(raw_payload->'contact'->'campaigninstancecontacts_set'->0->'contact_information'->>'entity_urn', '')
  ),
  contact_job_title = coalesce(
    contact_job_title,
    nullif(raw_payload->'contact'->>'job_title', ''),
    nullif(raw_payload->'contact'->'campaigninstancecontacts_set'->0->'contact_information'->>'job_title', '')
  ),
  contact_company_name = coalesce(
    contact_company_name,
    nullif(raw_payload->'contact'->>'company_name', ''),
    nullif(raw_payload->'contact'->'campaigninstancecontacts_set'->0->'contact_information'->>'company_name', '')
  ),
  contact_phone = coalesce(
    contact_phone,
    nullif(raw_payload->'contact'->>'phone', ''),
    nullif(raw_payload->'contact'->'campaigninstancecontacts_set'->0->'contact_information'->>'phone', '')
  ),
  contact_address = coalesce(
    contact_address,
    nullif(raw_payload->'contact'->>'address', ''),
    nullif(raw_payload->'contact'->'campaigninstancecontacts_set'->0->'contact_information'->>'address', '')
  ),
  campaign_contact_status = coalesce(
    campaign_contact_status,
    case
      when (raw_payload->'contact'->'campaigninstancecontacts_set'->0->>'status') ~ '^-?[0-9]+$'
        then (raw_payload->'contact'->'campaigninstancecontacts_set'->0->>'status')::int
      else null
    end
  ),
  campaign_running_status = coalesce(
    campaign_running_status,
    case
      when (raw_payload->'contact'->'campaigninstancecontacts_set'->0->>'campaign_running_status') ~ '^-?[0-9]+$'
        then (raw_payload->'contact'->'campaigninstancecontacts_set'->0->>'campaign_running_status')::int
      else null
    end
  ),
  last_action_id = coalesce(
    last_action_id,
    case
      when (raw_payload->'contact'->'campaigninstancecontacts_set'->0->>'last_action') ~ '^-?[0-9]+$'
        then (raw_payload->'contact'->'campaigninstancecontacts_set'->0->>'last_action')::bigint
      else null
    end
  ),
  nr_steps_before_responding = coalesce(
    nr_steps_before_responding,
    case
      when (raw_payload->'contact'->'campaigninstancecontacts_set'->0->>'nr_steps_before_responding') ~ '^-?[0-9]+$'
        then (raw_payload->'contact'->'campaigninstancecontacts_set'->0->>'nr_steps_before_responding')::int
      else null
    end
  );

create or replace view public.expandi_conversation_facts as
with msg as (
  select
    m.messenger_id,
    coalesce(m.campaign_instance_id, em.campaign_instance_id) as campaign_instance_id,
    coalesce(m.campaign_id, em.campaign_id) as campaign_id,
    coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source) as event_at,
    coalesce(m.is_outbound, m.direction = 'outbound', m.send_datetime is not null) as is_outbound,
    coalesce(m.is_inbound, m.direction = 'inbound', m.received_datetime is not null) as is_inbound
  from public.expandi_messages m
  left join public.expandi_messengers em on em.id = m.messenger_id
),
agg as (
  select
    messenger_id,
    campaign_instance_id,
    campaign_id,
    min(event_at) filter (where is_outbound) as first_outbound_at,
    min(event_at) filter (where is_inbound) as first_inbound_at,
    count(*) filter (where is_outbound)::int as sent_messages,
    count(*) filter (where is_inbound)::int as received_messages
  from msg
  group by 1, 2, 3
),
reply as (
  select
    a.messenger_id,
    min(m.event_at) as replied_at
  from agg a
  join msg m
    on m.messenger_id = a.messenger_id
   and m.is_inbound
   and a.first_outbound_at is not null
   and m.event_at >= a.first_outbound_at
  group by 1
)
select
  em.id as messenger_id,
  em.li_account_id,
  coalesce(a.campaign_instance_id, em.campaign_instance_id) as campaign_instance_id,
  coalesce(a.campaign_id, em.campaign_id) as campaign_id,
  em.contact_id,
  em.contact_name,
  em.contact_email,
  em.invited_at,
  em.connected_at,
  a.first_outbound_at,
  a.first_inbound_at,
  r.replied_at,
  (r.replied_at is not null) as is_replied,
  case
    when r.replied_at is not null and a.first_outbound_at is not null
      then extract(epoch from (r.replied_at - a.first_outbound_at))::bigint
    else null
  end as reply_latency_seconds,
  coalesce(a.sent_messages, 0) as sent_messages,
  coalesce(a.received_messages, 0) as received_messages,
  em.last_datetime,
  em.updated_at as messenger_updated_at
from public.expandi_messengers em
left join agg a on a.messenger_id = em.id
left join reply r on r.messenger_id = em.id;

update public.expandi_messengers em
set
  first_outbound_at = f.first_outbound_at,
  first_inbound_at = f.first_inbound_at,
  replied_at = f.replied_at,
  is_replied = f.is_replied
from public.expandi_conversation_facts f
where f.messenger_id = em.id
  and (
    em.first_outbound_at is distinct from f.first_outbound_at
    or em.first_inbound_at is distinct from f.first_inbound_at
    or em.replied_at is distinct from f.replied_at
    or em.is_replied is distinct from f.is_replied
  );

create or replace view public.expandi_campaign_daily_v as
with message_rows as (
  select
    date_trunc('day', coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source))::date as day,
    m.li_account_id,
    coalesce(m.campaign_instance_id, em.campaign_instance_id) as campaign_instance_id,
    count(*) filter (
      where coalesce(m.is_outbound, m.direction = 'outbound', m.send_datetime is not null)
    )::int as sent_messages,
    count(*) filter (
      where coalesce(m.is_inbound, m.direction = 'inbound', m.received_datetime is not null)
    )::int as received_messages,
    0::int as sent_invitations,
    0::int as new_connections,
    0::int as new_replies
  from public.expandi_messages m
  left join public.expandi_messengers em on em.id = m.messenger_id
  where coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source) is not null
    and coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source) >= '2025-01-01'::timestamptz
  group by 1, 2, 3
),
invitation_rows as (
  select
    date_trunc('day', invited_at)::date as day,
    li_account_id,
    campaign_instance_id,
    0::int as sent_messages,
    0::int as received_messages,
    count(*)::int as sent_invitations,
    0::int as new_connections,
    0::int as new_replies
  from public.expandi_messengers
  where invited_at is not null
    and invited_at >= '2025-01-01'::timestamptz
  group by 1, 2, 3
),
connection_rows as (
  select
    date_trunc('day', connected_at)::date as day,
    li_account_id,
    campaign_instance_id,
    0::int as sent_messages,
    0::int as received_messages,
    0::int as sent_invitations,
    count(*)::int as new_connections,
    0::int as new_replies
  from public.expandi_messengers
  where connected_at is not null
    and connected_at >= '2025-01-01'::timestamptz
  group by 1, 2, 3
),
reply_rows as (
  select
    date_trunc('day', replied_at)::date as day,
    li_account_id,
    campaign_instance_id,
    0::int as sent_messages,
    0::int as received_messages,
    0::int as sent_invitations,
    0::int as new_connections,
    count(*)::int as new_replies
  from public.expandi_conversation_facts
  where replied_at is not null
    and replied_at >= '2025-01-01'::timestamptz
  group by 1, 2, 3
),
unioned as (
  select * from message_rows
  union all
  select * from invitation_rows
  union all
  select * from connection_rows
  union all
  select * from reply_rows
)
select
  u.day,
  u.li_account_id,
  u.campaign_instance_id,
  max(ci.name) as campaign_name,
  sum(u.sent_messages)::int as sent_messages,
  sum(u.received_messages)::int as received_messages,
  sum(u.new_connections)::int as new_connections,
  sum(u.new_replies)::int as new_replies,
  sum(u.sent_invitations)::int as sent_invitations
from unioned u
join public.expandi_campaign_instances ci on ci.id = u.campaign_instance_id
group by 1, 2, 3;

create or replace view public.expandi_campaign_performance_v as
with daily as (
  select
    campaign_instance_id,
    sum(sent_messages)::int as sent_messages_total,
    sum(received_messages)::int as received_messages_total,
    sum(sent_invitations)::int as sent_invitations_total,
    sum(new_connections)::int as connected_total,
    sum(new_replies)::int as replied_total
  from public.expandi_campaign_daily_v
  group by 1
),
facts as (
  select
    campaign_instance_id,
    count(distinct messenger_id)::int as conversations_total,
    percentile_cont(0.5) within group (order by reply_latency_seconds) as median_reply_latency_seconds,
    avg(reply_latency_seconds)::bigint as avg_reply_latency_seconds
  from public.expandi_conversation_facts
  group by 1
)
select
  c.id as campaign_instance_id,
  c.li_account_id,
  c.name as campaign_name,
  c.active,
  c.archived,
  c.campaign_status,
  coalesce(f.conversations_total, 0) as conversations_total,
  coalesce(d.connected_total, 0) as connected_total,
  coalesce(d.replied_total, 0) as replied_total,
  coalesce(d.sent_invitations_total, 0) as sent_invitations_total,
  coalesce(d.sent_messages_total, 0) as sent_messages_total,
  coalesce(d.received_messages_total, 0) as received_messages_total,
  case
    when coalesce(d.sent_invitations_total, 0) = 0 then 0
    else round((coalesce(d.connected_total, 0)::numeric / d.sent_invitations_total::numeric) * 100.0, 2)
  end as connect_rate_pct,
  case
    when coalesce(d.sent_messages_total, 0) = 0 then 0
    else round((coalesce(d.replied_total, 0)::numeric / d.sent_messages_total::numeric) * 100.0, 2)
  end as reply_rate_pct,
  f.median_reply_latency_seconds,
  f.avg_reply_latency_seconds
from public.expandi_campaign_instances c
left join daily d on d.campaign_instance_id = c.id
left join facts f on f.campaign_instance_id = c.id;

commit;
