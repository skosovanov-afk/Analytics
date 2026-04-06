alter table public.tal_campaigns
  drop constraint if exists tal_campaigns_channel_check;

alter table public.tal_campaigns
  add constraint tal_campaigns_channel_check
  check (channel in ('smartlead', 'expandi', 'app', 'telegram'));

drop view if exists public.tal_analytics_v;

create view public.tal_analytics_v as
with sl as (
  select
    tc.tal_id,
    sum(s.sent_count)::int    as email_sent,
    sum(s.reply_count)::int   as email_replies,
    case
      when sum(s.sent_count) = 0 then null
      else round(sum(s.reply_count)::numeric / sum(s.sent_count) * 100, 2)
    end as email_reply_rate,
    coalesce(sum(s.booked_meetings), 0)::int as email_meetings,
    coalesce(sum(s.held_meetings), 0)::int   as email_held_meetings
  from public.tal_campaigns tc
  join public.smartlead_kpi_alltime_v s on s.campaign_name = tc.campaign_name
  where tc.channel = 'smartlead'
  group by tc.tal_id
),
ex as (
  select
    tc.tal_id,
    sum(
      case
        when e.campaign_missing_in_live_api
          or (
            coalesce(e.current_instances, 0) = 0
            and coalesce(e.api_connection_req, 0) = 0
            and coalesce(e.api_accepted, 0) = 0
            and coalesce(e.api_replies, 0) = 0
            and (
              coalesce(e.manual_connection_req, 0) > 0
              or coalesce(e.manual_accepted, 0) > 0
              or coalesce(e.manual_sent_messages, 0) > 0
              or coalesce(e.manual_replies, 0) > 0
            )
          )
        then coalesce(e.manual_connection_req, 0)
        else coalesce(e.api_connection_req, 0)
      end
    )::int as li_invited,
    sum(
      case
        when e.campaign_missing_in_live_api
          or (
            coalesce(e.current_instances, 0) = 0
            and coalesce(e.api_connection_req, 0) = 0
            and coalesce(e.api_accepted, 0) = 0
            and coalesce(e.api_replies, 0) = 0
            and (
              coalesce(e.manual_connection_req, 0) > 0
              or coalesce(e.manual_accepted, 0) > 0
              or coalesce(e.manual_sent_messages, 0) > 0
              or coalesce(e.manual_replies, 0) > 0
            )
          )
        then coalesce(e.manual_accepted, 0)
        else coalesce(e.api_accepted, 0)
      end
    )::int as li_accepted,
    sum(
      case
        when e.campaign_missing_in_live_api
          or (
            coalesce(e.current_instances, 0) = 0
            and coalesce(e.api_connection_req, 0) = 0
            and coalesce(e.api_accepted, 0) = 0
            and coalesce(e.api_replies, 0) = 0
            and (
              coalesce(e.manual_connection_req, 0) > 0
              or coalesce(e.manual_accepted, 0) > 0
              or coalesce(e.manual_sent_messages, 0) > 0
              or coalesce(e.manual_replies, 0) > 0
            )
          )
        then coalesce(e.manual_replies, 0)
        else coalesce(e.api_replies, 0)
      end
    )::int as li_replies,
    case
      when sum(
        case
          when e.campaign_missing_in_live_api
            or (
              coalesce(e.current_instances, 0) = 0
              and coalesce(e.api_connection_req, 0) = 0
              and coalesce(e.api_accepted, 0) = 0
              and coalesce(e.api_replies, 0) = 0
              and (
                coalesce(e.manual_connection_req, 0) > 0
                or coalesce(e.manual_accepted, 0) > 0
                or coalesce(e.manual_sent_messages, 0) > 0
                or coalesce(e.manual_replies, 0) > 0
              )
            )
          then coalesce(e.manual_connection_req, 0)
          else coalesce(e.api_connection_req, 0)
        end
      ) = 0 then null
      else round(
        sum(
          case
            when e.campaign_missing_in_live_api
              or (
                coalesce(e.current_instances, 0) = 0
                and coalesce(e.api_connection_req, 0) = 0
                and coalesce(e.api_accepted, 0) = 0
                and coalesce(e.api_replies, 0) = 0
                and (
                  coalesce(e.manual_connection_req, 0) > 0
                  or coalesce(e.manual_accepted, 0) > 0
                  or coalesce(e.manual_sent_messages, 0) > 0
                  or coalesce(e.manual_replies, 0) > 0
                )
              )
            then coalesce(e.manual_accepted, 0)
            else coalesce(e.api_accepted, 0)
          end
        )::numeric
        / sum(
          case
            when e.campaign_missing_in_live_api
              or (
                coalesce(e.current_instances, 0) = 0
                and coalesce(e.api_connection_req, 0) = 0
                and coalesce(e.api_accepted, 0) = 0
                and coalesce(e.api_replies, 0) = 0
                and (
                  coalesce(e.manual_connection_req, 0) > 0
                  or coalesce(e.manual_accepted, 0) > 0
                  or coalesce(e.manual_sent_messages, 0) > 0
                  or coalesce(e.manual_replies, 0) > 0
                )
              )
            then coalesce(e.manual_connection_req, 0)
            else coalesce(e.api_connection_req, 0)
          end
        ) * 100, 2
      )
    end as li_accept_rate,
    coalesce(sum(e.booked_meetings), 0)::int as li_meetings,
    coalesce(sum(e.held_meetings), 0)::int   as li_held_meetings
  from public.tal_campaigns tc
  join public.linkedin_kpi_alltime_v2 e on e.campaign_name = tc.campaign_name
  where tc.channel = 'expandi'
  group by tc.tal_id
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
    coalesce(sum(a.held_meetings), 0)::int   as app_held_meetings
  from public.tal_campaigns tc
  join public.app_kpi_alltime_v a on a.campaign_name = tc.campaign_name
  where tc.channel = 'app'
  group by tc.tal_id
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
    coalesce(sum(g.held_meetings), 0)::int   as tg_held_meetings
  from public.tal_campaigns tc
  join public.telegram_kpi_alltime_v g on g.campaign_name = tc.campaign_name
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
  coalesce(sl.email_sent, 0)       as email_sent,
  coalesce(sl.email_replies, 0)    as email_replies,
  sl.email_reply_rate,
  coalesce(sl.email_meetings, 0)   as email_meetings,
  coalesce(sl.email_held_meetings, 0) as email_held_meetings,
  coalesce(ex.li_invited, 0)       as li_invited,
  coalesce(ex.li_accepted, 0)      as li_accepted,
  coalesce(ex.li_replies, 0)       as li_replies,
  ex.li_accept_rate,
  coalesce(ex.li_meetings, 0)      as li_meetings,
  coalesce(ex.li_held_meetings, 0) as li_held_meetings,
  coalesce(app.app_touches, 0)     as app_touches,
  coalesce(app.app_replies, 0)     as app_replies,
  app.app_reply_rate,
  coalesce(app.app_meetings, 0)    as app_meetings,
  coalesce(app.app_held_meetings, 0) as app_held_meetings,
  coalesce(tg.tg_touches, 0)       as tg_touches,
  coalesce(tg.tg_replies, 0)       as tg_replies,
  tg.tg_reply_rate,
  coalesce(tg.tg_meetings, 0)      as tg_meetings,
  coalesce(tg.tg_held_meetings, 0) as tg_held_meetings,
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
  ) as total_held_meetings
from public.tals t
left join sl on sl.tal_id = t.id
left join ex on ex.tal_id = t.id
left join app on app.tal_id = t.id
left join tg on tg.tal_id = t.id;
