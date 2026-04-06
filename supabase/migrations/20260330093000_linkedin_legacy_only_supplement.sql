create or replace view public.linkedin_legacy_manual_activity_v2 as
with aliases as (
  select lower(btrim(alias)) as alias_key, canonical
  from public.campaign_name_aliases
  where channel = 'linkedin'
),
legacy_only_canonicals as (
  select distinct lower(btrim(canonical_name)) as canonical_key
  from public.linkedin_campaign_mapping
  where is_active = true
    and match_status = 'legacy_only'
    and canonical_name is not null
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
from manual_activity m
join legacy_only_canonicals loc
  on loc.canonical_key = lower(btrim(m.campaign_name));
