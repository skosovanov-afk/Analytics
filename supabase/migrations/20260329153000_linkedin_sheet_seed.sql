insert into public.linkedin_campaign_mapping (
  raw_name,
  canonical_name,
  campaign_group,
  phase,
  source,
  confidence,
  match_status,
  notes
)
values
  ('AdTech', 'AdTech', 'AdTech', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('SBC Conference', 'SBC Conference', 'SBC Conference', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Coinfest', 'Coinfest', 'Coinfest', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Travel Tech', 'Travel Tech', 'Travel Tech', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Travel Tech Asia', 'Travel Tech Asia', 'Travel Tech Asia', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Ticketing Platforms', 'Ticketing Platforms', 'Ticketing Platforms', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Future Travel', 'Future Travel', 'Future Travel', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Sigma Rome', 'Sigma Rome', 'Sigma Rome', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('ITB Berlin 2026', 'ITB Berlin 2026', 'ITB Berlin 2026', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Affiliate World Dubai', 'Affiliate World Dubai', 'Affiliate World Dubai', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Robert', 'Robert', 'Robert', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('MWC', 'MWC', 'MWC', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Partnership', 'Partnership', 'Partnership', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Conf Re-eng (ICE, WebSummit, Sigma, iFX)', 'Conf Re-eng (ICE, WebSummit, Sigma, iFX)', 'Conf Re-eng (ICE, WebSummit, Sigma, iFX)', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('START Summit', 'START Summit', 'START Summit', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Warsaw AI', 'Warsaw AI', 'Warsaw AI', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Money Motion', 'Money Motion', 'Money Motion', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery'),
  ('Esports', 'Esports', 'Esports', null, 'google_sheet_seed', 'manual', 'needs_review', 'Seeded from LinkedIn Google Sheet; requires review before recovery')
on conflict (raw_name) do nothing;
