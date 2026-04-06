-- =============================================================================
-- Fix: expandi_kpi_alltime_v — switch to daily data as primary source
--
-- Problem: old view was driven by expandi_campaign_stats_snapshots (snap_by_name).
-- Campaigns that have daily activity but NO snapshot were excluded entirely,
-- causing under-count in connection_req, sent_messages, and replies.
--
-- Fix: drive from expandi_kpi_daily_v (all campaigns with any daily data),
-- keep snapshot values for connection_req/accepted where available
-- (more accurate cumulative), fall back to daily sum where snapshot is missing.
--
-- Run in Supabase SQL Editor.
-- =============================================================================

create or replace view public.expandi_kpi_alltime_v as
with

-- Latest snapshot per campaign_instance (cumulative counters from Expandi API)
latest_snap as (
  select distinct on (campaign_instance_id)
    campaign_instance_id,
    li_account_id,
    contacted_people,
    connected,
    (coalesce(replied_first_action, 0) + coalesce(replied_other_actions, 0)) as snap_replies
  from public.expandi_campaign_stats_snapshots
  order by campaign_instance_id, snapshot_date desc
),

-- Aggregate snapshots by account × campaign name
snap_by_name as (
  select
    s.li_account_id,
    ci.name                           as campaign_name,
    sum(s.contacted_people)::int      as connection_req,
    sum(s.connected)::int             as accepted
  from latest_snap s
  join public.expandi_campaign_instances ci on ci.id = s.campaign_instance_id
  group by s.li_account_id, ci.name
),

-- All daily metrics aggregated all-time, per account × campaign name
-- This is the COMPLETE source — includes campaigns with or without snapshots
daily_by_name as (
  select
    li_account_id,
    account_name,
    campaign_name,
    sum(connection_req)::int          as daily_connection_req,
    sum(accepted)::int                as daily_accepted,
    sum(sent_messages)::int           as sent_messages,
    sum(received_messages)::int       as received_messages,
    sum(replies)::int                 as replies
  from public.expandi_kpi_daily_v
  group by li_account_id, account_name, campaign_name
),

-- Meetings from manual_stats (LinkedIn channel)
manual_meetings as (
  select
    coalesce(campaign_name, account_name)                                  as campaign_name,
    sum(case when metric_name = 'booked_meetings' then value else 0 end)   as booked_meetings,
    sum(case when metric_name = 'held_meetings'   then value else 0 end)   as held_meetings
  from public.manual_stats
  where channel = 'linkedin'
  group by coalesce(campaign_name, account_name)
)

select
  d.li_account_id,
  d.account_name,
  d.campaign_name,
  -- Prefer snapshot (cumulative, from Expandi API) where available;
  -- fall back to daily sum for campaigns with no snapshot
  coalesce(sn.connection_req, d.daily_connection_req)   as connection_req,
  coalesce(sn.accepted,       d.daily_accepted)         as accepted,
  d.sent_messages,
  d.received_messages,
  d.replies,
  case
    when coalesce(sn.connection_req, d.daily_connection_req) = 0 then null
    else least(round(
      (coalesce(sn.accepted, d.daily_accepted)::numeric
       / coalesce(sn.connection_req, d.daily_connection_req)::numeric) * 100.0, 2
    ), 100.00)
  end                                                   as cr_to_accept_pct,
  case
    when d.sent_messages = 0 then null
    else round(
      (d.replies::numeric / d.sent_messages::numeric) * 100.0, 2
    )
  end                                                   as cr_to_reply_pct,
  coalesce(mm.booked_meetings, 0)::int                  as booked_meetings,
  coalesce(mm.held_meetings,   0)::int                  as held_meetings
from daily_by_name d
left join snap_by_name sn
  on  sn.li_account_id  = d.li_account_id
  and sn.campaign_name  = d.campaign_name
left join manual_meetings mm
  on  mm.campaign_name  = d.campaign_name
order by d.li_account_id, d.campaign_name;


-- =============================================================================
-- Diagnostic: compare old (snapshot-driven) vs new (daily-driven) totals
-- Run this BEFORE applying the view change to understand the delta.
-- =============================================================================

/*
-- Old totals (snapshot-driven — current state):
select
  sum(connection_req)  as conn_req,
  sum(accepted)        as accepted,
  sum(sent_messages)   as messages,
  sum(replies)         as replies,
  sum(booked_meetings) as booked,
  sum(held_meetings)   as held
from public.expandi_kpi_alltime_v;

-- New totals (daily-driven — after applying this fix):
-- Use this query to preview before recreating the view:
with
latest_snap as (
  select distinct on (campaign_instance_id)
    campaign_instance_id, li_account_id, contacted_people, connected
  from public.expandi_campaign_stats_snapshots
  order by campaign_instance_id, snapshot_date desc
),
snap_by_name as (
  select s.li_account_id, ci.name as campaign_name,
    sum(s.contacted_people)::int as connection_req,
    sum(s.connected)::int as accepted
  from latest_snap s
  join public.expandi_campaign_instances ci on ci.id = s.campaign_instance_id
  group by s.li_account_id, ci.name
),
daily_by_name as (
  select li_account_id, campaign_name,
    sum(connection_req)::int as daily_conn_req,
    sum(accepted)::int       as daily_accepted,
    sum(sent_messages)::int  as sent_messages,
    sum(replies)::int        as replies
  from public.expandi_kpi_daily_v
  group by li_account_id, campaign_name
)
select
  sum(coalesce(sn.connection_req, d.daily_conn_req))  as conn_req,
  sum(coalesce(sn.accepted, d.daily_accepted))        as accepted,
  sum(d.sent_messages)                                as messages,
  sum(d.replies)                                      as replies
from daily_by_name d
left join snap_by_name sn
  on sn.li_account_id = d.li_account_id and sn.campaign_name = d.campaign_name;
*/
