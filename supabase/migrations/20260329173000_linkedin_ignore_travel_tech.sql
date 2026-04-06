update public.linkedin_campaign_mapping
set
  match_status = 'ignored',
  notes = coalesce(notes || E'\n', '') || 'Excluded from LinkedIn product totals by product decision on 2026-03-29.'
where raw_name = 'Travel Tech';
