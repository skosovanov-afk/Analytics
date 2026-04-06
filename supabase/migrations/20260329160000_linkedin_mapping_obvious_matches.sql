update public.linkedin_campaign_mapping
set
  canonical_name = 'AW Dubai',
  campaign_group = 'Affiliate World Dubai',
  match_status = 'approved',
  notes = 'Resolved obvious live match: Affiliate World Dubai -> AW Dubai'
where raw_name = 'Affiliate World Dubai';

update public.linkedin_campaign_mapping
set
  canonical_name = 'ITB',
  campaign_group = 'ITB Berlin 2026',
  match_status = 'approved',
  notes = 'Resolved obvious live match: ITB Berlin 2026 -> ITB'
where raw_name = 'ITB Berlin 2026';

update public.linkedin_campaign_mapping
set
  canonical_name = 'Robert Companies',
  campaign_group = 'Robert',
  match_status = 'approved',
  notes = 'Resolved obvious live match: Robert -> Robert Companies'
where raw_name = 'Robert';

update public.linkedin_campaign_mapping
set
  canonical_name = 'Money Motion After',
  campaign_group = 'Money Motion',
  match_status = 'approved',
  notes = 'Resolved obvious live match: Money Motion -> Money Motion After'
where raw_name = 'Money Motion';

update public.linkedin_campaign_mapping
set
  canonical_name = 'Conferences Re-eng (ICE,WebSummit, Sigma, iFX, ITB)',
  campaign_group = 'Conf Re-eng (ICE, WebSummit, Sigma, iFX)',
  match_status = 'approved',
  notes = 'Resolved obvious live match: generic re-eng sheet label -> live conference re-eng campaign'
where raw_name = 'Conf Re-eng (ICE, WebSummit, Sigma, iFX)';
