-- LinkedIn v2 shadow layer
-- Purpose:
-- - keep current UI untouched
-- - introduce additive reporting views for safe comparison
-- - fix LinkedIn scope to records since 2025-08-31 only
-- - keep manual_stats only as meetings layer in the primary KPI path
-- - expose legacy manual activity as a separate supplement, not mixed silently

create or replace view public.linkedin_kpi_daily_v2 as
with aliases as (
  select lower(btrim(alias)) as alias_key, canonical
  from public.campaign_name_aliases
  where channel = 'linkedin'
),
history as (
  select
    d.day,
    d.li_account_id,
    d.account_name,
    coalesce(a.canonical, d.campaign_name) as campaign_name,
    sum(d.connection_req)::int as connection_req,
    sum(d.accepted)::int as accepted,
    sum(d.sent_messages)::int as sent_messages,
    sum(d.replies)::int as replies
  from public.expandi_kpi_daily_v d
  left join aliases a
    on a.alias_key = lower(btrim(d.campaign_name))
  where d.day >= date '2025-08-31'
  group by 1, 2, 3, 4
),
manual_campaign_meetings as (
  select
    ms.record_date as day,
    coalesce(a.canonical, coalesce(ms.campaign_name, ms.account_name)) as campaign_name,
    sum(case when ms.metric_name = 'booked_meetings' then ms.value else 0 end)::int as booked_meetings,
    sum(case when ms.metric_name = 'held_meetings' then ms.value else 0 end)::int as held_meetings
  from public.manual_stats ms
  left join aliases a
    on a.alias_key = lower(btrim(coalesce(ms.campaign_name, ms.account_name)))
  where ms.channel = 'linkedin'
    and ms.metric_name in ('booked_meetings', 'held_meetings')
    and ms.record_date >= date '2025-08-31'
    and coalesce(ms.campaign_name, ms.account_name) is not null
  group by 1, 2
)
select
  h.day,
  h.li_account_id,
  h.account_name,
  h.campaign_name,
  h.connection_req,
  h.accepted,
  h.sent_messages,
  h.replies,
  coalesce(m.booked_meetings, 0)::int as booked_meetings,
  coalesce(m.held_meetings, 0)::int as held_meetings
from history h
left join manual_campaign_meetings m
  on m.day = h.day
 and m.campaign_name = h.campaign_name;


create or replace view public.linkedin_kpi_monthly_v2 as
with daily_rollup as (
  select
    date_trunc('month', d.day)::date as month,
    d.li_account_id,
    d.account_name,
    d.campaign_name,
    sum(d.connection_req)::int as connection_req,
    sum(d.accepted)::int as accepted,
    sum(d.sent_messages)::int as sent_messages,
    sum(d.replies)::int as replies,
    sum(d.booked_meetings)::int as booked_meetings,
    sum(d.held_meetings)::int as held_meetings
  from public.linkedin_kpi_daily_v2 d
  group by 1, 2, 3, 4
),
manual_month_totals as (
  select
    date_trunc('month', ms.record_date)::date as month,
    sum(case when ms.metric_name = 'booked_meetings' then ms.value else 0 end)::int as month_total_booked_meetings,
    sum(case when ms.metric_name = 'held_meetings' then ms.value else 0 end)::int as month_total_held_meetings
  from public.manual_stats ms
  where ms.channel = 'linkedin'
    and ms.metric_name in ('booked_meetings', 'held_meetings')
    and ms.record_date >= date '2025-08-31'
    and ms.campaign_name is null
    and ms.account_name is null
  group by 1
)
select
  d.month,
  d.li_account_id,
  d.account_name,
  d.campaign_name,
  d.connection_req,
  d.accepted,
  d.sent_messages,
  d.replies,
  d.booked_meetings,
  d.held_meetings,
  coalesce(mt.month_total_booked_meetings, 0)::int as month_total_booked_meetings,
  coalesce(mt.month_total_held_meetings, 0)::int as month_total_held_meetings
from daily_rollup d
left join manual_month_totals mt
  on mt.month = d.month;


create or replace view public.linkedin_live_snapshot_v2 as
with aliases as (
  select lower(btrim(alias)) as alias_key, canonical
  from public.campaign_name_aliases
  where channel = 'linkedin'
),
current_campaigns as (
  select
    ci.id as campaign_instance_id,
    ci.li_account_id,
    coalesce(acc.name, ci.li_account_id::text) as account_name,
    coalesce(a.canonical, ci.name) as campaign_name,
    ci.activated,
    ci.archived
  from public.expandi_campaign_instances ci
  left join public.expandi_accounts acc on acc.id = ci.li_account_id
  left join aliases a
    on a.alias_key = lower(btrim(ci.name))
),
latest_snap as (
  select distinct on (campaign_instance_id)
    campaign_instance_id,
    snapshot_date,
    contacted_people,
    connected,
    replied_first_action,
    replied_other_actions
  from public.expandi_campaign_stats_snapshots
  order by campaign_instance_id, snapshot_date desc
)
select
  c.li_account_id,
  c.account_name,
  c.campaign_name,
  min(c.activated) as first_activated_at,
  min(c.activated)::date < date '2025-08-31' as starts_before_cutoff,
  count(*)::int as current_instances,
  bool_or(coalesce(c.archived, false)) as has_archived_instance,
  max(ls.snapshot_date) as latest_snapshot_date,
  sum(coalesce(ls.contacted_people, 0))::int as api_connection_req,
  sum(coalesce(ls.connected, 0))::int as api_accepted,
  sum(coalesce(ls.replied_first_action, 0) + coalesce(ls.replied_other_actions, 0))::int as api_replies
from current_campaigns c
left join latest_snap ls
  on ls.campaign_instance_id = c.campaign_instance_id
group by 1, 2, 3;


create or replace view public.linkedin_legacy_manual_activity_v2 as
with aliases as (
  select lower(btrim(alias)) as alias_key, canonical
  from public.campaign_name_aliases
  where channel = 'linkedin'
),
live_campaigns as (
  select distinct campaign_name
  from public.linkedin_live_snapshot_v2
),
manual_activity as (
  select
    coalesce(a.canonical, coalesce(ms.campaign_name, ms.account_name)) as campaign_name,
    sum(case when ms.metric_name = 'connection_req' then ms.value else 0 end)::int as manual_connection_req,
    sum(case when ms.metric_name = 'accepted' then ms.value else 0 end)::int as manual_accepted,
    sum(case when ms.metric_name = 'sent_messages' then ms.value else 0 end)::int as manual_sent_messages,
    sum(case when ms.metric_name = 'replies' then ms.value else 0 end)::int as manual_replies,
    min(ms.record_date) as min_record_date,
    max(ms.record_date) as max_record_date,
    count(*)::int as manual_rows
  from public.manual_stats ms
  left join aliases a
    on a.alias_key = lower(btrim(coalesce(ms.campaign_name, ms.account_name)))
  where ms.channel = 'linkedin'
    and ms.metric_name in ('connection_req', 'accepted', 'sent_messages', 'replies')
    and ms.record_date >= date '2025-08-31'
    and coalesce(ms.campaign_name, ms.account_name) is not null
  group by 1
)
select
  m.*,
  not exists (
    select 1
    from live_campaigns lc
    where lc.campaign_name = m.campaign_name
  ) as campaign_missing_in_live_api
from manual_activity m;


create or replace view public.linkedin_kpi_alltime_v2 as
with history as (
  select
    d.li_account_id,
    d.account_name,
    d.campaign_name,
    sum(d.connection_req)::int as history_connection_req,
    sum(d.accepted)::int as history_accepted,
    sum(d.sent_messages)::int as history_sent_messages,
    sum(d.replies)::int as history_replies,
    sum(d.booked_meetings)::int as history_booked_meetings,
    sum(d.held_meetings)::int as history_held_meetings
  from public.linkedin_kpi_daily_v2 d
  group by 1, 2, 3
),
manual_meetings as (
  select
    coalesce(a.canonical, coalesce(ms.campaign_name, ms.account_name)) as campaign_name,
    sum(case when ms.metric_name = 'booked_meetings' then ms.value else 0 end)::int as booked_meetings,
    sum(case when ms.metric_name = 'held_meetings' then ms.value else 0 end)::int as held_meetings
  from public.manual_stats ms
  left join (
    select lower(btrim(alias)) as alias_key, canonical
    from public.campaign_name_aliases
    where channel = 'linkedin'
  ) a
    on a.alias_key = lower(btrim(coalesce(ms.campaign_name, ms.account_name)))
  where ms.channel = 'linkedin'
    and ms.metric_name in ('booked_meetings', 'held_meetings')
    and ms.record_date >= date '2025-08-31'
    and coalesce(ms.campaign_name, ms.account_name) is not null
  group by 1
),
base_rows as (
  select
    coalesce(h.account_name, s.account_name, 'Legacy / manual supplement') as account_name,
    coalesce(h.campaign_name, s.campaign_name) as campaign_name,
    coalesce(h.li_account_id, s.li_account_id) as li_account_id,
    s.first_activated_at,
    coalesce(s.starts_before_cutoff, false) as starts_before_cutoff,
    coalesce(s.current_instances, 0)::int as current_instances,
    coalesce(s.has_archived_instance, false) as has_archived_instance,
    s.latest_snapshot_date,
    coalesce(s.api_connection_req, 0)::int as api_connection_req,
    coalesce(s.api_accepted, 0)::int as api_accepted,
    coalesce(s.api_replies, 0)::int as api_replies,
    coalesce(h.history_connection_req, 0)::int as history_connection_req,
    coalesce(h.history_accepted, 0)::int as history_accepted,
    coalesce(h.history_sent_messages, 0)::int as history_sent_messages,
    coalesce(h.history_replies, 0)::int as history_replies
  from history h
  full outer join public.linkedin_live_snapshot_v2 s
    on s.campaign_name = h.campaign_name
   and s.li_account_id = h.li_account_id
),
ranked_rows as (
  select
    b.*,
    row_number() over (
      partition by b.campaign_name
      order by
        case when b.li_account_id is null then 1 else 0 end,
        b.li_account_id nulls last,
        b.account_name
    ) as campaign_row_num
  from base_rows b
)
select
  coalesce(r.account_name, 'Legacy / manual supplement') as account_name,
  coalesce(r.campaign_name, l.campaign_name) as campaign_name,
  r.li_account_id,
  r.first_activated_at,
  r.starts_before_cutoff,
  r.current_instances,
  r.has_archived_instance,
  r.latest_snapshot_date,
  r.api_connection_req,
  r.api_accepted,
  r.api_replies,
  r.history_connection_req,
  r.history_accepted,
  r.history_sent_messages,
  r.history_replies,
  case when coalesce(r.campaign_row_num, 1) = 1 then coalesce(l.manual_connection_req, 0)::int else 0 end as manual_connection_req,
  case when coalesce(r.campaign_row_num, 1) = 1 then coalesce(l.manual_accepted, 0)::int else 0 end as manual_accepted,
  case when coalesce(r.campaign_row_num, 1) = 1 then coalesce(l.manual_sent_messages, 0)::int else 0 end as manual_sent_messages,
  case when coalesce(r.campaign_row_num, 1) = 1 then coalesce(l.manual_replies, 0)::int else 0 end as manual_replies,
  case when coalesce(r.campaign_row_num, 1) = 1 then coalesce(l.campaign_missing_in_live_api, false) else false end as campaign_missing_in_live_api,
  case when coalesce(r.campaign_row_num, 1) = 1 then coalesce(m.booked_meetings, 0)::int else 0 end as booked_meetings,
  case when coalesce(r.campaign_row_num, 1) = 1 then coalesce(m.held_meetings, 0)::int else 0 end as held_meetings
from ranked_rows r
full outer join public.linkedin_legacy_manual_activity_v2 l
  on l.campaign_name = r.campaign_name
left join manual_meetings m
  on m.campaign_name = coalesce(r.campaign_name, l.campaign_name);
