alter table public.tal_campaigns
add column if not exists match_group text;

create index if not exists tal_campaigns_match_group_idx
on public.tal_campaigns(match_group);
