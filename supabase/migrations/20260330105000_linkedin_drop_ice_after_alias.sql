begin;

delete from public.campaign_name_aliases
where channel = 'linkedin'
  and alias = 'ICE After the conf';

commit;
