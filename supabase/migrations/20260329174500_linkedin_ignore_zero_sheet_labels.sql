update public.linkedin_campaign_mapping
set
  match_status = 'ignored',
  notes = coalesce(notes || E'\n', '') || 'Ignored because Google Sheet row has zero activity across LinkedIn metrics.'
where raw_name in (
  'Coinfest',
  'Future Travel',
  'MWC',
  'SBC Conference',
  'Sigma Rome',
  'Ticketing Platforms',
  'Travel Tech Asia',
  'Warsaw AI'
);
