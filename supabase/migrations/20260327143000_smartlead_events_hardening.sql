-- Phase: harden smartlead_events against duplicate sent/reply rows.
-- Strategy:
-- 1. create a rollback-safe backup of current rows
-- 2. add a generated event_identity_key column
-- 3. dedupe existing rows by that key, keeping the newest row
-- 4. enforce uniqueness for rows with a non-null identity key

do $$
begin
  if to_regclass('public.smartlead_events_backup_20260327_phase1') is null then
    execute $backup$
      create table public.smartlead_events_backup_20260327_phase1 as
      select *
      from public.smartlead_events
    $backup$;
  end if;
end $$;

alter table public.smartlead_events
  drop column if exists event_identity_key;

alter table public.smartlead_events
  add column event_identity_key text;

update public.smartlead_events
set event_identity_key = (
  case
    when campaign_id is null or nullif(btrim(coalesce(event_type, '')), '') is null then null
    when nullif(btrim(coalesce(stats_id, '')), '') is not null then
      concat_ws(
        '|',
        campaign_id::text,
        lower(btrim(event_type)),
        btrim(stats_id)
      )
    when nullif(btrim(coalesce(email, '')), '') is not null and occurred_at is not null then
      concat_ws(
        '|',
        campaign_id::text,
        lower(btrim(event_type)),
        lower(btrim(email)),
        coalesce(sequence_number, 0)::text,
        occurred_at::text
      )
    else null
  end
);

with ranked as (
  select
    id,
    row_number() over (
      partition by event_identity_key
      order by occurred_at desc nulls last, synced_at desc nulls last, id desc
    ) as rn
  from public.smartlead_events
  where event_identity_key is not null
)
delete from public.smartlead_events e
using ranked r
where e.id = r.id
  and r.rn > 1;

create unique index if not exists smartlead_events_identity_uidx
  on public.smartlead_events (event_identity_key)
  where event_identity_key is not null;

create index if not exists smartlead_events_campaign_occ_idx
  on public.smartlead_events (campaign_id, occurred_at desc);
