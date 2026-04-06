alter table public.linkedin_campaign_mapping
  drop constraint if exists linkedin_campaign_mapping_match_status_check;

alter table public.linkedin_campaign_mapping
  add constraint linkedin_campaign_mapping_match_status_check
  check (match_status in ('approved', 'needs_review', 'group_only', 'ignored'));

create or replace view public.linkedin_campaign_mapping_conflicts_v as
with live_names as (
  select distinct btrim(name) as live_name
  from public.expandi_campaign_catalog
  where name is not null
),
mapping as (
  select
    id,
    btrim(raw_name) as raw_name,
    btrim(canonical_name) as canonical_name,
    btrim(campaign_group) as campaign_group,
    phase,
    source,
    match_status
  from public.linkedin_campaign_mapping
  where is_active = true
),
raw_live_conflict as (
  select
    m.id,
    m.raw_name,
    m.canonical_name,
    m.campaign_group,
    m.phase,
    m.source,
    m.match_status,
    'raw_name_matches_live_name_but_points_elsewhere'::text as conflict_type
  from mapping m
  join live_names l
    on lower(l.live_name) = lower(m.raw_name)
  where lower(m.canonical_name) <> lower(m.raw_name)
    and m.match_status not in ('ignored', 'group_only')
),
canonical_missing as (
  select
    m.id,
    m.raw_name,
    m.canonical_name,
    m.campaign_group,
    m.phase,
    m.source,
    m.match_status,
    'canonical_name_missing_in_catalog'::text as conflict_type
  from mapping m
  left join live_names l
    on lower(l.live_name) = lower(m.canonical_name)
  where l.live_name is null
    and m.match_status not in ('ignored', 'group_only')
),
group_mixed as (
  select
    min(m.id) as id,
    null::text as raw_name,
    null::text as canonical_name,
    m.campaign_group,
    null::text as phase,
    'derived'::text as source,
    'needs_review'::text as match_status,
    'campaign_group_has_multiple_canonical_names'::text as conflict_type
  from mapping m
  where m.match_status not in ('ignored', 'group_only')
  group by m.campaign_group
  having count(distinct lower(m.canonical_name)) > 1
)
select * from raw_live_conflict
union all
select * from canonical_missing
union all
select * from group_mixed;

update public.linkedin_campaign_mapping
set
  canonical_name = raw_name,
  campaign_group = 'START Summit',
  match_status = 'group_only',
  notes = 'Group-only: use for TAL grouping across before/after live campaigns, not for KPI supplement'
where raw_name = 'START Summit';

update public.linkedin_campaign_mapping
set
  canonical_name = raw_name,
  campaign_group = 'Esports',
  match_status = 'group_only',
  notes = 'Group-only: use for TAL grouping across Esports SegA/B/C, not for KPI supplement'
where raw_name = 'Esports';

update public.linkedin_campaign_mapping
set
  canonical_name = raw_name,
  campaign_group = 'Payroll',
  match_status = 'group_only',
  notes = 'Group-only: generic payroll label spans multiple live payroll campaigns; do not use as KPI supplement'
where raw_name = 'Payroll services';
