update public.linkedin_campaign_mapping
set
  canonical_name = raw_name,
  campaign_group = raw_name,
  phase = 'before',
  match_status = 'approved',
  notes = 'Cleaned: before-phase campaign must not map to AFTER canonical'
where raw_name = 'PG Connects London (before the conf)';

update public.linkedin_campaign_mapping
set
  canonical_name = raw_name,
  campaign_group = raw_name,
  phase = 'before',
  match_status = 'approved',
  notes = 'Cleaned: before-phase campaign must not map to AFTER canonical'
where raw_name = 'ICE before conf';

update public.linkedin_campaign_mapping
set
  canonical_name = raw_name,
  campaign_group = raw_name,
  phase = null,
  match_status = 'approved',
  notes = 'Cleaned: live Expandi name should self-map, not point to generic legacy alias'
where raw_name in (
  'Payroll',
  'Payroll (Дорогие международные переводы, долго, комплаенс-риски)'
);

update public.linkedin_campaign_mapping
set
  match_status = 'needs_review',
  notes = 'Generic legacy name: manual review required before recovery/TAL grouping'
where raw_name = 'Payroll services';
