-- LinkedIn all-time cleanup:
-- - stop injecting manual activity rows as fake accounts
-- - use expandi_kpi_daily_v as the activity source of truth for sent/replies
-- - preserve manual booked/held meetings once per campaign
-- - normalize campaign names through campaign_name_aliases(channel='linkedin')

create or replace view public.expandi_kpi_alltime_v as
with latest_snap as (
  select distinct on (campaign_instance_id)
    campaign_instance_id,
    li_account_id,
    contacted_people,
    connected
  from public.expandi_campaign_stats_snapshots
  order by campaign_instance_id, snapshot_date desc
),
snap_by_name as (
  select
    s.li_account_id,
    coalesce(cna.canonical, ci.name) as campaign_name,
    sum(s.contacted_people)::int as connection_req,
    sum(s.connected)::int as accepted
  from latest_snap s
  join public.expandi_campaign_instances ci on ci.id = s.campaign_instance_id
  left join public.campaign_name_aliases cna
    on cna.alias = ci.name
   and cna.channel = 'linkedin'
  group by s.li_account_id, coalesce(cna.canonical, ci.name)
),
daily_by_name as (
  select
    d.li_account_id,
    d.account_name,
    coalesce(cna.canonical, d.campaign_name) as campaign_name,
    sum(d.connection_req)::int as daily_connection_req,
    sum(d.accepted)::int as daily_accepted,
    sum(d.sent_messages)::int as sent_messages,
    sum(d.received_messages)::int as received_messages,
    sum(d.replies)::int as replies
  from public.expandi_kpi_daily_v d
  left join public.campaign_name_aliases cna
    on cna.alias = d.campaign_name
   and cna.channel = 'linkedin'
  group by d.li_account_id, d.account_name, coalesce(cna.canonical, d.campaign_name)
),
manual_meetings as (
  select
    coalesce(cna.canonical, coalesce(ms.campaign_name, ms.account_name)) as campaign_name,
    sum(case when ms.metric_name = 'booked_meetings' then ms.value else 0 end)::int as booked_meetings,
    sum(case when ms.metric_name = 'held_meetings' then ms.value else 0 end)::int as held_meetings
  from public.manual_stats ms
  left join public.campaign_name_aliases cna
    on cna.alias = coalesce(ms.campaign_name, ms.account_name)
   and cna.channel = 'linkedin'
  where ms.channel = 'linkedin'
    and ms.metric_name in ('booked_meetings', 'held_meetings')
  group by coalesce(cna.canonical, coalesce(ms.campaign_name, ms.account_name))
)
select
  coalesce(d.li_account_id, s.li_account_id) as li_account_id,
  coalesce(d.account_name, a.name, coalesce(d.li_account_id, s.li_account_id)::text) as account_name,
  coalesce(d.campaign_name, s.campaign_name) as campaign_name,
  coalesce(s.connection_req, d.daily_connection_req, 0)::int as connection_req,
  coalesce(s.accepted, d.daily_accepted, 0)::int as accepted,
  coalesce(d.sent_messages, 0)::int as sent_messages,
  coalesce(d.received_messages, 0)::int as received_messages,
  coalesce(d.replies, 0)::int as replies,
  case
    when coalesce(s.connection_req, d.daily_connection_req, 0) = 0 then null
    else least(round(
      (coalesce(s.accepted, d.daily_accepted, 0)::numeric
       / coalesce(s.connection_req, d.daily_connection_req)::numeric) * 100.0, 2
    ), 100.00)
  end as cr_to_accept_pct,
  case
    when coalesce(d.sent_messages, 0) = 0 then null
    else round((coalesce(d.replies, 0)::numeric / d.sent_messages::numeric) * 100.0, 2)
  end as cr_to_reply_pct,
  case
    when coalesce(d.li_account_id, s.li_account_id) =
         min(coalesce(d.li_account_id, s.li_account_id)) over (
           partition by coalesce(d.campaign_name, s.campaign_name)
         )
    then coalesce(mm.booked_meetings, 0)::int
    else 0
  end as booked_meetings,
  case
    when coalesce(d.li_account_id, s.li_account_id) =
         min(coalesce(d.li_account_id, s.li_account_id)) over (
           partition by coalesce(d.campaign_name, s.campaign_name)
         )
    then coalesce(mm.held_meetings, 0)::int
    else 0
  end as held_meetings
from daily_by_name d
full outer join snap_by_name s
  on s.li_account_id = d.li_account_id
 and s.campaign_name = d.campaign_name
left join public.expandi_accounts a
  on a.id = coalesce(d.li_account_id, s.li_account_id)
left join manual_meetings mm
  on mm.campaign_name = coalesce(d.campaign_name, s.campaign_name)
order by li_account_id, campaign_name;
