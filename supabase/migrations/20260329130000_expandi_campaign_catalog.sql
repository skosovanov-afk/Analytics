create table if not exists public.expandi_campaign_catalog (
  id bigint primary key,
  li_account_id bigint not null,
  campaign_id bigint null,
  name text null,
  campaign_type int null,
  active boolean null,
  archived boolean null,
  step_count int null,
  first_action_action_type int null,
  nr_contacts_total int null,
  campaign_status text null,
  limit_requests_daily int null,
  limit_follow_up_messages_daily int null,
  stats_datetime timestamptz null,
  activated timestamptz null,
  deactivated timestamptz null,
  stats jsonb null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  is_live boolean not null default true,
  catalog_source text not null default 'campaign_instances',
  raw_payload jsonb not null default '{}'::jsonb,
  synced_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists expandi_campaign_catalog_li_account_idx
  on public.expandi_campaign_catalog (li_account_id);

create index if not exists expandi_campaign_catalog_campaign_id_idx
  on public.expandi_campaign_catalog (campaign_id);

create index if not exists expandi_campaign_catalog_is_live_idx
  on public.expandi_campaign_catalog (is_live);

create index if not exists expandi_campaign_catalog_last_seen_idx
  on public.expandi_campaign_catalog (last_seen_at desc);

insert into public.expandi_campaign_catalog (
  id,
  li_account_id,
  campaign_id,
  name,
  campaign_type,
  active,
  archived,
  step_count,
  first_action_action_type,
  nr_contacts_total,
  campaign_status,
  limit_requests_daily,
  limit_follow_up_messages_daily,
  stats_datetime,
  activated,
  deactivated,
  stats,
  first_seen_at,
  last_seen_at,
  is_live,
  catalog_source,
  raw_payload,
  synced_at,
  updated_at
)
select
  ci.id,
  ci.li_account_id,
  ci.campaign_id,
  ci.name,
  ci.campaign_type,
  ci.active,
  ci.archived,
  ci.step_count,
  ci.first_action_action_type,
  ci.nr_contacts_total,
  ci.campaign_status,
  ci.limit_requests_daily,
  ci.limit_follow_up_messages_daily,
  ci.stats_datetime,
  ci.activated,
  ci.deactivated,
  ci.stats,
  coalesce(ci.synced_at, now()),
  coalesce(ci.synced_at, now()),
  true,
  'campaign_instances',
  coalesce(ci.raw_payload, '{}'::jsonb),
  coalesce(ci.synced_at, now()),
  coalesce(ci.updated_at, ci.synced_at, now())
from public.expandi_campaign_instances ci
on conflict (id) do update
set
  li_account_id = excluded.li_account_id,
  campaign_id = excluded.campaign_id,
  name = excluded.name,
  campaign_type = excluded.campaign_type,
  active = excluded.active,
  archived = excluded.archived,
  step_count = excluded.step_count,
  first_action_action_type = excluded.first_action_action_type,
  nr_contacts_total = excluded.nr_contacts_total,
  campaign_status = excluded.campaign_status,
  limit_requests_daily = excluded.limit_requests_daily,
  limit_follow_up_messages_daily = excluded.limit_follow_up_messages_daily,
  stats_datetime = excluded.stats_datetime,
  activated = excluded.activated,
  deactivated = excluded.deactivated,
  stats = excluded.stats,
  last_seen_at = greatest(public.expandi_campaign_catalog.last_seen_at, excluded.last_seen_at),
  is_live = true,
  raw_payload = excluded.raw_payload,
  synced_at = excluded.synced_at,
  updated_at = excluded.updated_at;

insert into public.expandi_campaign_catalog (
  id,
  li_account_id,
  campaign_id,
  name,
  first_seen_at,
  last_seen_at,
  is_live,
  catalog_source,
  raw_payload,
  synced_at,
  updated_at
)
select
  em.campaign_instance_id as id,
  max(em.li_account_id) as li_account_id,
  max(em.campaign_id) as campaign_id,
  max(em.campaign_name) as name,
  min(coalesce(em.synced_at, now())) as first_seen_at,
  max(coalesce(em.synced_at, now())) as last_seen_at,
  false as is_live,
  'messengers_backfill' as catalog_source,
  '{}'::jsonb as raw_payload,
  max(coalesce(em.synced_at, now())) as synced_at,
  max(coalesce(em.synced_at, now())) as updated_at
from public.expandi_messengers em
where em.campaign_instance_id is not null
group by em.campaign_instance_id
on conflict (id) do update
set
  li_account_id = coalesce(public.expandi_campaign_catalog.li_account_id, excluded.li_account_id),
  campaign_id = coalesce(public.expandi_campaign_catalog.campaign_id, excluded.campaign_id),
  name = coalesce(public.expandi_campaign_catalog.name, excluded.name),
  first_seen_at = least(public.expandi_campaign_catalog.first_seen_at, excluded.first_seen_at),
  last_seen_at = greatest(public.expandi_campaign_catalog.last_seen_at, excluded.last_seen_at),
  updated_at = greatest(public.expandi_campaign_catalog.updated_at, excluded.updated_at);

insert into public.expandi_campaign_catalog (
  id,
  li_account_id,
  campaign_id,
  first_seen_at,
  last_seen_at,
  is_live,
  catalog_source,
  raw_payload,
  synced_at,
  updated_at
)
select
  em.campaign_instance_id as id,
  max(em.li_account_id) as li_account_id,
  max(em.campaign_id) as campaign_id,
  min(coalesce(em.synced_at, now())) as first_seen_at,
  max(coalesce(em.synced_at, now())) as last_seen_at,
  false as is_live,
  'messages_backfill' as catalog_source,
  '{}'::jsonb as raw_payload,
  max(coalesce(em.synced_at, now())) as synced_at,
  max(coalesce(em.synced_at, now())) as updated_at
from public.expandi_messages em
where em.campaign_instance_id is not null
group by em.campaign_instance_id
on conflict (id) do update
set
  li_account_id = coalesce(public.expandi_campaign_catalog.li_account_id, excluded.li_account_id),
  campaign_id = coalesce(public.expandi_campaign_catalog.campaign_id, excluded.campaign_id),
  first_seen_at = least(public.expandi_campaign_catalog.first_seen_at, excluded.first_seen_at),
  last_seen_at = greatest(public.expandi_campaign_catalog.last_seen_at, excluded.last_seen_at),
  updated_at = greatest(public.expandi_campaign_catalog.updated_at, excluded.updated_at);
