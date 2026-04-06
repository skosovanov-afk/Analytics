-- Expandi campaigns-only upgrade
-- Adds campaign-level fields and snapshot table for autonomous campaign stats sync.
-- Run once in Supabase SQL Editor.

begin;

alter table if exists public.expandi_campaign_instances
  add column if not exists limit_requests_daily int null,
  add column if not exists limit_follow_up_messages_daily int null,
  add column if not exists stats_datetime timestamptz null,
  add column if not exists activated timestamptz null,
  add column if not exists deactivated timestamptz null;

create table if not exists public.expandi_campaign_stats_snapshots (
  snapshot_date date not null,
  li_account_id bigint not null,
  campaign_instance_id bigint not null,
  campaign_name text null,
  connected int not null default 0,
  contacted_people int not null default 0,
  replied_first_action int not null default 0,
  replied_other_actions int not null default 0,
  people_in_campaign int not null default 0,
  step_count int not null default 0,
  raw_stats jsonb not null default '{}'::jsonb,
  synced_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, li_account_id, campaign_instance_id)
);

create index if not exists expandi_campaign_stats_snapshots_day_idx
  on public.expandi_campaign_stats_snapshots (snapshot_date desc);

create index if not exists expandi_campaign_stats_snapshots_campaign_idx
  on public.expandi_campaign_stats_snapshots (campaign_instance_id, snapshot_date desc);

create index if not exists expandi_campaign_instances_stats_datetime_idx
  on public.expandi_campaign_instances (stats_datetime desc);

commit;

