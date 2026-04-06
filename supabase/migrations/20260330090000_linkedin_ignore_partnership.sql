update public.linkedin_campaign_mapping
set
  match_status = 'ignored',
  notes = coalesce(notes || E'\n', '') || 'Excluded from LinkedIn product totals by product decision on 2026-03-30.'
where raw_name = 'Partnership';
