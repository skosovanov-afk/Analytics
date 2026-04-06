with smartlead_exact as (
  select
    campaign_name,
    min(campaign_id)::text as campaign_id
  from public.smartlead_stats_daily
  where campaign_id is not null
    and campaign_name is not null
  group by campaign_name
  having count(distinct campaign_id) = 1
)
update public.tal_campaigns tc
set
  campaign_id = coalesce(tc.campaign_id, s.campaign_id),
  source_campaign_key = coalesce(tc.source_campaign_key, 'smartlead:id:' || s.campaign_id)
from smartlead_exact s
where tc.channel = 'smartlead'
  and tc.campaign_name = s.campaign_name
  and (tc.campaign_id is null or tc.source_campaign_key is null);
