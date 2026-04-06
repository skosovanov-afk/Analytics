-- Expandi hardening pack
-- Purpose:
-- 1) stable campaign-only analytics
-- 2) monthly KPI layer in business terms
-- 3) ingestion observability (runs + quarantine)
-- 4) hourly hygiene + data quality snapshot

begin;

create extension if not exists pgcrypto;
create extension if not exists pg_cron;

create table if not exists public.expandi_ingest_runs (
  run_id uuid primary key default gen_random_uuid(),
  mode text not null,
  ok boolean not null,
  requests_total int not null default 0,
  pages_fetched_total int not null default 0,
  requests_failed int not null default 0,
  normalized jsonb not null default '{}'::jsonb,
  errors jsonb not null default '[]'::jsonb,
  started_at timestamptz not null default now(),
  finished_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create index if not exists expandi_ingest_runs_created_at_idx
  on public.expandi_ingest_runs (created_at desc);

create index if not exists expandi_ingest_runs_mode_idx
  on public.expandi_ingest_runs (mode);

create index if not exists expandi_ingest_runs_ok_idx
  on public.expandi_ingest_runs (ok);

create table if not exists public.expandi_ingest_quarantine (
  id bigint generated always as identity primary key,
  entity text not null,
  endpoint_path text null,
  reason text not null,
  record_id bigint null,
  li_account_id bigint null,
  campaign_instance_id bigint null,
  messenger_id bigint null,
  event_datetime timestamptz null,
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists expandi_ingest_quarantine_created_at_idx
  on public.expandi_ingest_quarantine (created_at desc);

create index if not exists expandi_ingest_quarantine_reason_idx
  on public.expandi_ingest_quarantine (reason);

create index if not exists expandi_ingest_quarantine_campaign_idx
  on public.expandi_ingest_quarantine (campaign_instance_id, created_at desc);

create table if not exists public.expandi_data_quality_daily (
  snapshot_day date primary key,
  messages_total int not null default 0,
  messages_null_campaign int not null default 0,
  messages_before_cutoff int not null default 0,
  messages_direction_null int not null default 0,
  messages_ambiguous int not null default 0,
  messengers_total int not null default 0,
  messengers_null_campaign int not null default 0,
  campaigns_total int not null default 0,
  snapshots_days_last_14 int not null default 0,
  updated_at timestamptz not null default now()
);

commit;

create or replace view public.expandi_messages_clean_v as
with base as (
  select
    m.id,
    m.messenger_id,
    coalesce(m.li_account_id, em.li_account_id) as li_account_id,
    coalesce(m.campaign_instance_id, em.campaign_instance_id) as campaign_instance_id,
    coalesce(m.campaign_id, em.campaign_id) as campaign_id,
    m.campaign_step_id,
    coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source) as event_at,
    date_trunc('day', coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source))::date as event_day,
    lower(nullif(btrim(m.direction), '')) as direction,
    coalesce(m.is_outbound, false) as is_outbound,
    coalesce(m.is_inbound, false) as is_inbound,
    lower(coalesce(m.send_by, '')) as send_by,
    nullif(btrim(coalesce(m.body, '')), '') as body_clean,
    coalesce(m.has_attachment, false) as has_attachment,
    coalesce(m.extracted_urls, '[]'::jsonb) as extracted_urls,
    coalesce(m.extracted_domains, '[]'::jsonb) as extracted_domains,
    m.raw_payload
  from public.expandi_messages m
  left join public.expandi_messengers em on em.id = m.messenger_id
)
select
  b.*,
  case
    when b.direction = 'outbound' then 'outbound'
    when b.direction = 'inbound' then 'inbound'
    when b.is_outbound and not b.is_inbound then 'outbound'
    when b.is_inbound and not b.is_outbound then 'inbound'
    when b.is_outbound and b.is_inbound and b.send_by ~* '(lead|contact|prospect|recipient)' then 'inbound'
    when b.is_outbound and b.is_inbound and b.send_by ~* '(me|user|owner|admin|account|sender)' then 'outbound'
    when b.is_outbound and b.is_inbound then 'ambiguous'
    else 'system'
  end as event_kind,
  exists (
    select 1
    from public.expandi_campaign_instances ci
    where ci.id = b.campaign_instance_id
  ) as is_campaign_scoped,
  (b.event_day >= date '2025-01-01') as is_after_cutoff
from base b
where b.event_at is not null;

create or replace view public.expandi_campaign_daily_v as
with msg as (
  select
    m.messenger_id,
    coalesce(m.li_account_id, em.li_account_id) as li_account_id,
    coalesce(m.campaign_instance_id, em.campaign_instance_id) as campaign_instance_id,
    coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source) as event_at,
    case
      when lower(nullif(btrim(coalesce(m.direction, '')), '')) = 'outbound' then true
      when lower(nullif(btrim(coalesce(m.direction, '')), '')) = 'inbound' then false
      when coalesce(m.is_outbound, false) and not coalesce(m.is_inbound, false) then true
      when coalesce(m.is_inbound, false) and not coalesce(m.is_outbound, false) then false
      when m.send_datetime is not null and m.received_datetime is null then true
      when m.received_datetime is not null and m.send_datetime is null then false
      when coalesce(m.is_outbound, false) and coalesce(m.is_inbound, false) then true  -- ambiguous → outbound
      else null
    end as is_outbound_norm,
    case
      when lower(nullif(btrim(coalesce(m.direction, '')), '')) = 'inbound' then true
      when lower(nullif(btrim(coalesce(m.direction, '')), '')) = 'outbound' then false
      when coalesce(m.is_inbound, false) and not coalesce(m.is_outbound, false) then true
      when coalesce(m.is_outbound, false) and not coalesce(m.is_inbound, false) then false
      when m.received_datetime is not null and m.send_datetime is null then true
      when m.send_datetime is not null and m.received_datetime is null then false
      when coalesce(m.is_outbound, false) and coalesce(m.is_inbound, false) then false  -- ambiguous → not inbound
      else null
    end as is_inbound_norm,
    nullif(btrim(coalesce(m.body, '')), '') as body_clean
  from public.expandi_messages m
  left join public.expandi_messengers em on em.id = m.messenger_id
  where coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source) is not null
    and coalesce(m.event_datetime, m.received_datetime, m.send_datetime, m.created_at_source) >= '2025-01-01'::timestamptz
    and m.messenger_id is not null
    and coalesce(m.campaign_instance_id, em.campaign_instance_id) is not null
    and exists (
      select 1
      from public.expandi_campaign_instances ci
      where ci.id = coalesce(m.campaign_instance_id, em.campaign_instance_id)
    )
),
first_outbound as (
  select
    messenger_id,
    min(event_at) as first_outbound_at
  from msg
  where is_outbound_norm = true
    and body_clean is not null
  group by 1
),
first_reply as (
  select
    m.messenger_id,
    min(m.event_at) as replied_at
  from msg m
  join first_outbound o on o.messenger_id = m.messenger_id
  where m.is_inbound_norm = true
    and m.body_clean is not null
    and m.event_at >= o.first_outbound_at
  group by 1
),
facts as (
  select
    m.messenger_id,
    max(m.li_account_id) as li_account_id,
    max(m.campaign_instance_id) as campaign_instance_id,
    o.first_outbound_at,
    r.replied_at
  from msg m
  left join first_outbound o on o.messenger_id = m.messenger_id
  left join first_reply r on r.messenger_id = m.messenger_id
  group by 1, 4, 5
),
message_rows as (
  select
    date_trunc('day', m.event_at)::date as day,
    m.li_account_id,
    m.campaign_instance_id,
    count(*) filter (
      where m.is_outbound_norm = true
        and m.body_clean is not null
        and m.event_at >= coalesce(f.first_outbound_at, '-infinity'::timestamptz)
        and m.event_at <= coalesce(f.replied_at, 'infinity'::timestamptz)
    )::int as sent_messages,
    count(*) filter (
      where m.is_inbound_norm = true
        and m.body_clean is not null
    )::int as received_messages,
    0::int as sent_invitations,
    0::int as new_connections,
    0::int as new_replies
  from msg m
  left join facts f on f.messenger_id = m.messenger_id
  group by 1, 2, 3
),
invitation_rows as (
  select
    date_trunc('day', em.invited_at)::date as day,
    em.li_account_id,
    em.campaign_instance_id,
    0::int as sent_messages,
    0::int as received_messages,
    count(*)::int as sent_invitations,
    0::int as new_connections,
    0::int as new_replies
  from public.expandi_messengers em
  where em.campaign_instance_id is not null
    and exists (
      select 1
      from public.expandi_campaign_instances ci
      where ci.id = em.campaign_instance_id
    )
    and em.invited_at is not null
    and em.invited_at >= '2025-01-01'::timestamptz
  group by 1, 2, 3
),
connection_rows as (
  select
    date_trunc('day', em.connected_at)::date as day,
    em.li_account_id,
    em.campaign_instance_id,
    0::int as sent_messages,
    0::int as received_messages,
    0::int as sent_invitations,
    count(*)::int as new_connections,
    0::int as new_replies
  from public.expandi_messengers em
  where em.campaign_instance_id is not null
    and exists (
      select 1
      from public.expandi_campaign_instances ci
      where ci.id = em.campaign_instance_id
    )
    and em.connected_at is not null
    and em.connected_at >= '2025-01-01'::timestamptz
  group by 1, 2, 3
),
reply_rows as (
  select
    date_trunc('day', fa.replied_at)::date as day,
    fa.li_account_id,
    fa.campaign_instance_id,
    0::int as sent_messages,
    0::int as received_messages,
    0::int as sent_invitations,
    0::int as new_connections,
    count(*)::int as new_replies
  from facts fa
  where fa.campaign_instance_id is not null
    and exists (
      select 1
      from public.expandi_campaign_instances ci
      where ci.id = fa.campaign_instance_id
    )
    and fa.replied_at is not null
    and fa.replied_at >= '2025-01-01'::timestamptz
    and fa.first_outbound_at is not null
    and fa.replied_at >= fa.first_outbound_at
  group by 1, 2, 3
),
unioned as (
  select * from message_rows
  union all
  select * from invitation_rows
  union all
  select * from connection_rows
  union all
  select * from reply_rows
)
select
  u.day,
  u.li_account_id,
  u.campaign_instance_id,
  max(ci.name) as campaign_name,
  sum(u.sent_messages)::int as sent_messages,
  sum(u.received_messages)::int as received_messages,
  sum(u.new_connections)::int as new_connections,
  sum(u.new_replies)::int as new_replies,
  sum(u.sent_invitations)::int as sent_invitations
from unioned u
join public.expandi_campaign_instances ci on ci.id = u.campaign_instance_id
group by 1, 2, 3;

create or replace view public.expandi_kpi_monthly_v as
select
  date_trunc('month', d.day)::date as month,
  d.li_account_id,
  d.campaign_instance_id,
  max(d.campaign_name) as campaign_name,
  sum(d.sent_invitations)::int as connection_req,
  sum(d.new_connections)::int as accepted,
  sum(d.sent_messages)::int as total_touches,
  sum(d.received_messages)::int as received_messages,
  sum(d.new_replies)::int as replies,
  case
    when sum(d.sent_invitations) = 0 then 0
    else round((sum(d.new_connections)::numeric / sum(d.sent_invitations)::numeric) * 100.0, 2)
  end as cr_to_accept_pct,
  case
    when sum(d.sent_messages) = 0 then 0
    else round((sum(d.new_replies)::numeric / sum(d.sent_messages)::numeric) * 100.0, 2)
  end as cr_to_reply_pct
from public.expandi_campaign_daily_v d
group by 1, 2, 3;

create or replace function public.expandi_capture_data_quality(
  p_cutoff date default date '2025-01-01'
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_messages_total int := 0;
  v_messages_null_campaign int := 0;
  v_messages_before_cutoff int := 0;
  v_messages_direction_null int := 0;
  v_messages_ambiguous int := 0;
  v_messengers_total int := 0;
  v_messengers_null_campaign int := 0;
  v_campaigns_total int := 0;
  v_snapshots_days_14 int := 0;
  v_today date := current_date;
begin
  select
    count(*)::int,
    count(*) filter (where campaign_instance_id is null)::int,
    count(*) filter (
      where coalesce(event_datetime, received_datetime, send_datetime, created_at_source)::date < p_cutoff
    )::int,
    count(*) filter (where direction is null)::int
  into
    v_messages_total,
    v_messages_null_campaign,
    v_messages_before_cutoff,
    v_messages_direction_null
  from public.expandi_messages;

  select count(*)::int
  into v_messages_ambiguous
  from public.expandi_messages_clean_v
  where event_kind = 'ambiguous'
    and is_after_cutoff
    and is_campaign_scoped;

  select
    count(*)::int,
    count(*) filter (where campaign_instance_id is null)::int
  into
    v_messengers_total,
    v_messengers_null_campaign
  from public.expandi_messengers;

  select count(*)::int
  into v_campaigns_total
  from public.expandi_campaign_instances;

  select count(distinct snapshot_date)::int
  into v_snapshots_days_14
  from public.expandi_campaign_stats_snapshots
  where snapshot_date >= (v_today - 13);

  insert into public.expandi_data_quality_daily (
    snapshot_day,
    messages_total,
    messages_null_campaign,
    messages_before_cutoff,
    messages_direction_null,
    messages_ambiguous,
    messengers_total,
    messengers_null_campaign,
    campaigns_total,
    snapshots_days_last_14,
    updated_at
  ) values (
    v_today,
    v_messages_total,
    v_messages_null_campaign,
    v_messages_before_cutoff,
    v_messages_direction_null,
    v_messages_ambiguous,
    v_messengers_total,
    v_messengers_null_campaign,
    v_campaigns_total,
    v_snapshots_days_14,
    now()
  )
  on conflict (snapshot_day) do update
  set
    messages_total = excluded.messages_total,
    messages_null_campaign = excluded.messages_null_campaign,
    messages_before_cutoff = excluded.messages_before_cutoff,
    messages_direction_null = excluded.messages_direction_null,
    messages_ambiguous = excluded.messages_ambiguous,
    messengers_total = excluded.messengers_total,
    messengers_null_campaign = excluded.messengers_null_campaign,
    campaigns_total = excluded.campaigns_total,
    snapshots_days_last_14 = excluded.snapshots_days_last_14,
    updated_at = now();

  return jsonb_build_object(
    'ok', true,
    'snapshot_day', v_today,
    'messages_total', v_messages_total,
    'messages_null_campaign', v_messages_null_campaign,
    'messages_before_cutoff', v_messages_before_cutoff,
    'messages_direction_null', v_messages_direction_null,
    'messages_ambiguous', v_messages_ambiguous,
    'messengers_total', v_messengers_total,
    'messengers_null_campaign', v_messengers_null_campaign,
    'campaigns_total', v_campaigns_total,
    'snapshots_days_last_14', v_snapshots_days_14
  );
end;
$$;

create or replace function public.expandi_hygiene_hourly(
  p_cutoff date default date '2025-01-01',
  p_keep_raw_days int default 14
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_cleanup jsonb := '{}'::jsonb;
  v_quality jsonb := '{}'::jsonb;
  v_raw_deleted int := 0;
begin
  if to_regprocedure('public.expandi_enforce_catalog_cutoff(date)') is not null then
    execute 'select public.expandi_enforce_catalog_cutoff($1)'
      into v_cleanup
      using p_cutoff;
  else
    v_cleanup := jsonb_build_object('ok', false, 'note', 'expandi_enforce_catalog_cutoff(date) is missing');
  end if;

  delete from public.expandi_raw
  where synced_at < now() - make_interval(days => greatest(1, p_keep_raw_days));
  get diagnostics v_raw_deleted = row_count;

  v_quality := public.expandi_capture_data_quality(p_cutoff);

  return jsonb_build_object(
    'ok', true,
    'cleanup', v_cleanup,
    'quality', v_quality,
    'raw_deleted', v_raw_deleted
  );
end;
$$;

do $$
declare
  r record;
begin
  for r in
    select jobid
    from cron.job
    where jobname in ('expandi-hygiene-daily', 'expandi-hygiene-hourly')
  loop
    perform cron.unschedule(r.jobid);
  end loop;
end $$;

select cron.schedule(
  'expandi-hygiene-hourly',
  '7 * * * *',
  $$
  select public.expandi_hygiene_hourly('2025-01-01'::date, 14);
  $$
);

-- Quick checks:
-- select * from public.expandi_kpi_monthly_v where month = date '2026-02-01' order by total_touches desc limit 20;
-- select public.expandi_capture_data_quality('2025-01-01'::date);
-- select jobid, jobname, schedule, active from cron.job where jobname like 'expandi-hygiene%' order by jobname;
