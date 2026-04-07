-- =============================================================================
-- Add qualified_leads metric to KPI views and TAL analytics
-- Source: manual_stats.metric_name = 'qualified_leads'
-- =============================================================================

-- Drop dependent views first (TAL depends on KPI views)
drop view if exists public.tal_analytics_v;
drop view if exists public.app_kpi_alltime_v;
drop view if exists public.telegram_kpi_alltime_v;
drop view if exists public.smartlead_kpi_monthly_v;
drop view if exists public.smartlead_kpi_alltime_v;
drop view if exists public.linkedin_kpi_alltime_v2;

-- ---------------------------------------------------------------------------
-- 1. app_kpi_alltime_v
-- ---------------------------------------------------------------------------
create view public.app_kpi_alltime_v as
select
  coalesce(campaign_name, account_name) as campaign_name,
  sum(case when metric_name = 'total_touches'    then value else 0 end)::int as total_touches,
  sum(case when metric_name = 'replies'          then value else 0 end)::int as replies,
  sum(case when metric_name = 'booked_meetings'  then value else 0 end)::int as booked_meetings,
  sum(case when metric_name = 'held_meetings'    then value else 0 end)::int as held_meetings,
  sum(case when metric_name = 'qualified_leads'  then value else 0 end)::int as qualified_leads,
  case
    when sum(case when metric_name = 'total_touches' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'replies' then value else 0 end)::numeric
      / sum(case when metric_name = 'total_touches' then value else 0 end)::numeric * 100, 2
    )
  end as cr_to_reply_pct,
  case
    when sum(case when metric_name = 'replies' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'booked_meetings' then value else 0 end)::numeric
      / sum(case when metric_name = 'replies' then value else 0 end)::numeric * 100, 2
    )
  end as cr_to_booked_pct,
  case
    when sum(case when metric_name = 'booked_meetings' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'held_meetings' then value else 0 end)::numeric
      / sum(case when metric_name = 'booked_meetings' then value else 0 end)::numeric * 100, 2
    )
  end as cr_booked_to_held_pct
from public.manual_stats
where channel = 'app'
group by coalesce(campaign_name, account_name)
order by total_touches desc;


-- ---------------------------------------------------------------------------
-- 2. telegram_kpi_alltime_v
-- ---------------------------------------------------------------------------
create view public.telegram_kpi_alltime_v as
select
  coalesce(campaign_name, account_name) as campaign_name,
  sum(case when metric_name = 'total_touches'    then value else 0 end)::int as total_touches,
  sum(case when metric_name = 'replies'          then value else 0 end)::int as replies,
  sum(case when metric_name = 'booked_meetings'  then value else 0 end)::int as booked_meetings,
  sum(case when metric_name = 'held_meetings'    then value else 0 end)::int as held_meetings,
  sum(case when metric_name = 'qualified_leads'  then value else 0 end)::int as qualified_leads,
  case
    when sum(case when metric_name = 'total_touches' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'replies' then value else 0 end)::numeric
      / sum(case when metric_name = 'total_touches' then value else 0 end)::numeric * 100, 2
    )
  end as cr_to_reply_pct,
  case
    when sum(case when metric_name = 'replies' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'booked_meetings' then value else 0 end)::numeric
      / sum(case when metric_name = 'replies' then value else 0 end)::numeric * 100, 2
    )
  end as cr_to_booked_pct,
  case
    when sum(case when metric_name = 'booked_meetings' then value else 0 end) = 0 then null
    else round(
      sum(case when metric_name = 'held_meetings' then value else 0 end)::numeric
      / sum(case when metric_name = 'booked_meetings' then value else 0 end)::numeric * 100, 2
    )
  end as cr_booked_to_held_pct
from public.manual_stats
where channel = 'telegram'
group by coalesce(campaign_name, account_name)
order by total_touches desc;


-- ---------------------------------------------------------------------------
-- 3. smartlead_kpi_monthly_v - add qualified_leads
-- ---------------------------------------------------------------------------
create view public.smartlead_kpi_monthly_v as
select
  date_trunc('month', s.date)::date as month,
  s.campaign_name,
  sum(s.sent_count)::int    as sent_count,
  sum(s.reply_count)::int   as reply_count,
  case
    when sum(s.sent_count) = 0 then null
    else round(sum(s.reply_count)::numeric / sum(s.sent_count)::numeric * 100, 2)
  end as reply_rate_pct,
  coalesce(max(m.booked_meetings), 0)::int as booked_meetings,
  coalesce(max(m.held_meetings), 0)::int   as held_meetings,
  coalesce(max(m.qualified_leads), 0)::int as qualified_leads
from public.smartlead_stats_daily s
left join (
  select
    date_trunc('month', record_date)::date as month,
    campaign_name,
    sum(case when metric_name = 'booked_meetings' then value else 0 end) as booked_meetings,
    sum(case when metric_name = 'held_meetings'   then value else 0 end) as held_meetings,
    sum(case when metric_name = 'qualified_leads'  then value else 0 end) as qualified_leads
  from public.manual_stats
  where channel = 'email'
  group by date_trunc('month', record_date)::date, campaign_name
) m on m.month = date_trunc('month', s.date)::date
     and m.campaign_name = s.campaign_name
group by date_trunc('month', s.date)::date, s.campaign_name
order by month;


-- ---------------------------------------------------------------------------
-- 4. smartlead_kpi_alltime_v - add qualified_leads
-- ---------------------------------------------------------------------------
create view public.smartlead_kpi_alltime_v as
select
  s.campaign_name,
  sum(s.sent_count)::int    as sent_count,
  sum(s.reply_count)::int   as reply_count,
  case
    when sum(s.sent_count) = 0 then null
    else round(sum(s.reply_count)::numeric / sum(s.sent_count)::numeric * 100, 2)
  end as reply_rate_pct,
  coalesce(max(m.booked_meetings), 0)::int as booked_meetings,
  coalesce(max(m.held_meetings), 0)::int   as held_meetings,
  coalesce(max(m.qualified_leads), 0)::int as qualified_leads
from public.smartlead_stats_daily s
left join (
  select
    campaign_name,
    sum(case when metric_name = 'booked_meetings' then value else 0 end) as booked_meetings,
    sum(case when metric_name = 'held_meetings'   then value else 0 end) as held_meetings,
    sum(case when metric_name = 'qualified_leads'  then value else 0 end) as qualified_leads
  from public.manual_stats
  where channel = 'email'
  group by campaign_name
) m on m.campaign_name = s.campaign_name
group by s.campaign_name
order by sum(s.sent_count) desc;


-- ---------------------------------------------------------------------------
-- 5. linkedin_kpi_alltime_v2 - add qualified_leads to manual_meetings CTE
-- ---------------------------------------------------------------------------
create view public.linkedin_kpi_alltime_v2 as
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
    sum(case when ms.metric_name = 'held_meetings' then ms.value else 0 end)::int as held_meetings,
    sum(case when ms.metric_name = 'qualified_leads' then ms.value else 0 end)::int as qualified_leads
  from public.manual_stats ms
  left join (
    select lower(btrim(alias)) as alias_key, canonical
    from public.campaign_name_aliases
    where channel = 'linkedin'
  ) a
    on a.alias_key = lower(btrim(coalesce(ms.campaign_name, ms.account_name)))
  where ms.channel = 'linkedin'
    and ms.metric_name in ('booked_meetings', 'held_meetings', 'qualified_leads')
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
  case when coalesce(r.campaign_row_num, 1) = 1 then coalesce(m.held_meetings, 0)::int else 0 end as held_meetings,
  case when coalesce(r.campaign_row_num, 1) = 1 then coalesce(m.qualified_leads, 0)::int else 0 end as qualified_leads
from ranked_rows r
full outer join public.linkedin_legacy_manual_activity_v2 l
  on l.campaign_name = r.campaign_name
left join manual_meetings m
  on m.campaign_name = coalesce(r.campaign_name, l.campaign_name);


-- ---------------------------------------------------------------------------
-- 6. tal_analytics_v - add qualified_leads per channel
-- ---------------------------------------------------------------------------
create view public.tal_analytics_v as
with sl_source as (
  select
    'smartlead:id:' || campaign_id::text as source_campaign_key,
    max(campaign_name) as campaign_name,
    sum(sent_count)::int as sent_count,
    sum(reply_count)::int as reply_count
  from public.smartlead_stats_daily
  where campaign_id is not null
    and campaign_name is not null
  group by campaign_id
),
sl_meetings as (
  select
    campaign_name,
    max(booked_meetings)::int as booked_meetings,
    max(held_meetings)::int as held_meetings,
    max(qualified_leads)::int as qualified_leads
  from public.smartlead_kpi_alltime_v
  group by campaign_name
),
sl as (
  select
    tc.tal_id,
    sum(s.sent_count)::int    as email_sent,
    sum(s.reply_count)::int   as email_replies,
    case
      when sum(s.sent_count) = 0 then null
      else round(sum(s.reply_count)::numeric / sum(s.sent_count) * 100, 2)
    end as email_reply_rate,
    coalesce(sum(m.booked_meetings), 0)::int as email_meetings,
    coalesce(sum(m.held_meetings), 0)::int   as email_held_meetings,
    coalesce(sum(m.qualified_leads), 0)::int as email_qualified_leads
  from public.tal_campaigns tc
  join sl_source s
    on (
      tc.source_campaign_key is not null
      and tc.source_campaign_key = s.source_campaign_key
    ) or (
      tc.source_campaign_key is null
      and s.campaign_name = tc.campaign_name
    )
  left join sl_meetings m on m.campaign_name = s.campaign_name
  where tc.channel = 'smartlead'
  group by tc.tal_id
),
ex_source as (
  select
    'expandi:canonical:' || lower(btrim(campaign_name)) as source_campaign_key,
    campaign_name,
    case
      when campaign_missing_in_live_api
        or (
          coalesce(current_instances, 0) = 0
          and coalesce(api_connection_req, 0) = 0
          and coalesce(api_accepted, 0) = 0
          and coalesce(api_replies, 0) = 0
          and (
            coalesce(manual_connection_req, 0) > 0
            or coalesce(manual_accepted, 0) > 0
            or coalesce(manual_sent_messages, 0) > 0
            or coalesce(manual_replies, 0) > 0
          )
        )
      then coalesce(manual_connection_req, 0)
      else coalesce(api_connection_req, 0)
    end::int as li_invited,
    case
      when campaign_missing_in_live_api
        or (
          coalesce(current_instances, 0) = 0
          and coalesce(api_connection_req, 0) = 0
          and coalesce(api_accepted, 0) = 0
          and coalesce(api_replies, 0) = 0
          and (
            coalesce(manual_connection_req, 0) > 0
            or coalesce(manual_accepted, 0) > 0
            or coalesce(manual_sent_messages, 0) > 0
            or coalesce(manual_replies, 0) > 0
          )
        )
      then coalesce(manual_accepted, 0)
      else coalesce(api_accepted, 0)
    end::int as li_accepted,
    case
      when campaign_missing_in_live_api
        or (
          coalesce(current_instances, 0) = 0
          and coalesce(api_connection_req, 0) = 0
          and coalesce(api_accepted, 0) = 0
          and coalesce(api_replies, 0) = 0
          and (
            coalesce(manual_connection_req, 0) > 0
            or coalesce(manual_accepted, 0) > 0
            or coalesce(manual_sent_messages, 0) > 0
            or coalesce(manual_replies, 0) > 0
          )
        )
      then coalesce(manual_replies, 0)
      else coalesce(api_replies, 0)
    end::int as li_replies,
    booked_meetings,
    held_meetings,
    qualified_leads
  from public.linkedin_kpi_alltime_v2
),
ex as (
  select
    tc.tal_id,
    sum(e.li_invited)::int as li_invited,
    sum(e.li_accepted)::int as li_accepted,
    sum(e.li_replies)::int as li_replies,
    case
      when sum(e.li_invited) = 0 then null
      else round(sum(e.li_accepted)::numeric / sum(e.li_invited) * 100, 2)
    end as li_accept_rate,
    coalesce(sum(e.booked_meetings), 0)::int as li_meetings,
    coalesce(sum(e.held_meetings), 0)::int   as li_held_meetings,
    coalesce(sum(e.qualified_leads), 0)::int as li_qualified_leads
  from public.tal_campaigns tc
  join ex_source e
    on (
      tc.source_campaign_key is not null
      and tc.source_campaign_key = e.source_campaign_key
    ) or (
      tc.source_campaign_key is null
      and e.campaign_name = tc.campaign_name
    )
  where tc.channel = 'expandi'
  group by tc.tal_id
),
app_source as (
  select
    'app:name:' || lower(btrim(campaign_name)) as source_campaign_key,
    campaign_name,
    total_touches,
    replies,
    booked_meetings,
    held_meetings,
    qualified_leads
  from public.app_kpi_alltime_v
),
app as (
  select
    tc.tal_id,
    sum(a.total_touches)::int as app_touches,
    sum(a.replies)::int       as app_replies,
    case
      when sum(a.total_touches) = 0 then null
      else round(sum(a.replies)::numeric / sum(a.total_touches) * 100, 2)
    end as app_reply_rate,
    coalesce(sum(a.booked_meetings), 0)::int as app_meetings,
    coalesce(sum(a.held_meetings), 0)::int   as app_held_meetings,
    coalesce(sum(a.qualified_leads), 0)::int as app_qualified_leads
  from public.tal_campaigns tc
  join app_source a
    on (
      tc.source_campaign_key is not null
      and tc.source_campaign_key = a.source_campaign_key
    ) or (
      tc.source_campaign_key is null
      and a.campaign_name = tc.campaign_name
    )
  where tc.channel = 'app'
  group by tc.tal_id
),
tg_source as (
  select
    'telegram:name:' || lower(btrim(campaign_name)) as source_campaign_key,
    campaign_name,
    total_touches,
    replies,
    booked_meetings,
    held_meetings,
    qualified_leads
  from public.telegram_kpi_alltime_v
),
tg as (
  select
    tc.tal_id,
    sum(g.total_touches)::int as tg_touches,
    sum(g.replies)::int       as tg_replies,
    case
      when sum(g.total_touches) = 0 then null
      else round(sum(g.replies)::numeric / sum(g.total_touches) * 100, 2)
    end as tg_reply_rate,
    coalesce(sum(g.booked_meetings), 0)::int as tg_meetings,
    coalesce(sum(g.held_meetings), 0)::int   as tg_held_meetings,
    coalesce(sum(g.qualified_leads), 0)::int as tg_qualified_leads
  from public.tal_campaigns tc
  join tg_source g
    on (
      tc.source_campaign_key is not null
      and tc.source_campaign_key = g.source_campaign_key
    ) or (
      tc.source_campaign_key is null
      and g.campaign_name = tc.campaign_name
    )
  where tc.channel = 'telegram'
  group by tc.tal_id
)
select
  t.id,
  t.name,
  t.description,
  t.criteria,
  t.created_at,
  t.updated_at,
  -- Email
  coalesce(sl.email_sent, 0)      as email_sent,
  coalesce(sl.email_replies, 0)   as email_replies,
  sl.email_reply_rate,
  coalesce(sl.email_meetings, 0)  as email_meetings,
  coalesce(sl.email_held_meetings, 0) as email_held_meetings,
  coalesce(sl.email_qualified_leads, 0) as email_qualified_leads,
  -- LinkedIn
  coalesce(ex.li_invited, 0)      as li_invited,
  coalesce(ex.li_accepted, 0)     as li_accepted,
  coalesce(ex.li_replies, 0)      as li_replies,
  ex.li_accept_rate,
  coalesce(ex.li_meetings, 0)     as li_meetings,
  coalesce(ex.li_held_meetings, 0) as li_held_meetings,
  coalesce(ex.li_qualified_leads, 0) as li_qualified_leads,
  -- App
  coalesce(app.app_touches, 0)      as app_touches,
  coalesce(app.app_replies, 0)      as app_replies,
  app.app_reply_rate,
  coalesce(app.app_meetings, 0)     as app_meetings,
  coalesce(app.app_held_meetings, 0) as app_held_meetings,
  coalesce(app.app_qualified_leads, 0) as app_qualified_leads,
  -- Telegram
  coalesce(tg.tg_touches, 0)      as tg_touches,
  coalesce(tg.tg_replies, 0)      as tg_replies,
  tg.tg_reply_rate,
  coalesce(tg.tg_meetings, 0)     as tg_meetings,
  coalesce(tg.tg_held_meetings, 0) as tg_held_meetings,
  coalesce(tg.tg_qualified_leads, 0) as tg_qualified_leads,
  -- Total
  (
    coalesce(sl.email_meetings, 0)
    + coalesce(ex.li_meetings, 0)
    + coalesce(app.app_meetings, 0)
    + coalesce(tg.tg_meetings, 0)
  ) as total_meetings,
  (
    coalesce(sl.email_held_meetings, 0)
    + coalesce(ex.li_held_meetings, 0)
    + coalesce(app.app_held_meetings, 0)
    + coalesce(tg.tg_held_meetings, 0)
  ) as total_held_meetings,
  (
    coalesce(sl.email_qualified_leads, 0)
    + coalesce(ex.li_qualified_leads, 0)
    + coalesce(app.app_qualified_leads, 0)
    + coalesce(tg.tg_qualified_leads, 0)
  ) as total_qualified_leads
from public.tals t
left join sl on sl.tal_id = t.id
left join ex on ex.tal_id = t.id
left join app on app.tal_id = t.id
left join tg on tg.tal_id = t.id;

grant select on public.tal_analytics_v to authenticated;
