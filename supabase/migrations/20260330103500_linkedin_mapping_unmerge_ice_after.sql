begin;

update public.linkedin_campaign_mapping
set canonical_name = 'ICE After the conf',
    campaign_group = 'ICE After the conf',
    notes = 'Preserve distinct live Expandi campaign casing/name; do not merge into ICE after the conf'
where raw_name = 'ICE After the conf';

commit;
