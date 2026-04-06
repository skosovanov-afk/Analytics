-- =============================================================================
-- Fix: expandi_kpi_alltime_v — three join bugs fixed
--
-- Fix 1 (meetings): manual_stats CSV backfill stores campaign identifier in
-- account_name (campaign_name = null). Original CTE joined on campaign_name alone
-- → booked_meetings/held_meetings = 0. Fixed with coalesce(campaign_name, account_name).
--
-- Fix 2 (messages): expandi_kpi_daily_v exposes max(d.campaign_name) which can be
-- NULL when raw events have no campaign_name. The view groups internally by ci.name,
-- but the exposed column differs → msg_by_name join never matched snap_by_name
-- → sent_messages/received_messages/replies = 0. Fixed by pulling msg_by_name from
-- expandi_campaign_daily_v directly and grouping by ci.name (same key as snap_by_name).
--
-- Fix 3 (booked duplication): snap_by_name has one row per (li_account_id, campaign_name).
-- manual_meetings has one row per campaign_name. The join attached the same meetings
-- value to every account row → sum(booked_meetings) was N× inflated (N = account count).
-- Fixed: booked_meetings shown only on the primary account row (min li_account_id per
-- campaign), so sum() across accounts always yields the correct total.
--
-- Apply: paste into Supabase SQL Editor and run.
-- =============================================================================

create or replace view public.expandi_kpi_alltime_v as
with latest_snap as (
  select distinct on (campaign_instance_id)
    campaign_instance_id,
    li_account_id,
    contacted_people,
    connected,
    (coalesce(replied_first_action, 0) + coalesce(replied_other_actions, 0)) as snap_replies
  from public.expandi_campaign_stats_snapshots
  order by campaign_instance_id, snapshot_date desc
),
snap_by_name as (
  select
    s.li_account_id,
    ci.name                           as campaign_name,
    sum(s.contacted_people)::int      as connection_req,
    sum(s.connected)::int             as accepted,
    sum(s.snap_replies)::int          as snap_replies
  from latest_snap s
  join public.expandi_campaign_instances ci on ci.id = s.campaign_instance_id
  group by s.li_account_id, ci.name
),
msg_by_name as (
  select
    d.li_account_id,
    ci.name                               as campaign_name,
    sum(d.sent_messages)::int             as sent_messages,
    sum(d.received_messages)::int         as received_messages,
    sum(d.new_replies)::int               as replies
  from public.expandi_campaign_daily_v d
  join public.expandi_campaign_instances ci on ci.id = d.campaign_instance_id
  group by d.li_account_id, ci.name
),
manual_meetings as (
  select
    coalesce(campaign_name, account_name) as campaign_name,
    sum(case when metric_name = 'booked_meetings' then value else 0 end) as booked_meetings,
    sum(case when metric_name = 'held_meetings'   then value else 0 end) as held_meetings
  from public.manual_stats
  where channel = 'linkedin'
  group by coalesce(campaign_name, account_name)
)
select
  sn.li_account_id,
  coalesce(a.name, sn.li_account_id::text)  as account_name,
  sn.campaign_name,
  sn.connection_req,
  sn.accepted,
  coalesce(m.sent_messages, 0)::int         as sent_messages,
  coalesce(m.received_messages, 0)::int     as received_messages,
  coalesce(m.replies, 0)::int               as replies,
  case
    when sn.connection_req = 0 then null
    else least(round(
      (sn.accepted::numeric / sn.connection_req::numeric) * 100.0, 2
    ), 100.00)
  end as cr_to_accept_pct,
  case
    when coalesce(m.sent_messages, 0) = 0 then null
    else round(
      (coalesce(m.replies, 0)::numeric / m.sent_messages::numeric) * 100.0, 2
    )
  end as cr_to_reply_pct,
  -- Fix 3: показываем booked/held только на строке с минимальным li_account_id
  -- по кампании, чтобы sum() через аккаунты не дублировал значения.
  case
    when sn.li_account_id = min(sn.li_account_id) over (partition by sn.campaign_name)
    then coalesce(mm.booked_meetings, 0)::int
    else 0
  end as booked_meetings,
  case
    when sn.li_account_id = min(sn.li_account_id) over (partition by sn.campaign_name)
    then coalesce(mm.held_meetings, 0)::int
    else 0
  end as held_meetings
from snap_by_name sn
left join public.expandi_accounts a on a.id = sn.li_account_id
left join msg_by_name m
  on m.li_account_id = sn.li_account_id
  and m.campaign_name = sn.campaign_name
left join manual_meetings mm on mm.campaign_name = sn.campaign_name
order by sn.li_account_id, sn.campaign_name;
