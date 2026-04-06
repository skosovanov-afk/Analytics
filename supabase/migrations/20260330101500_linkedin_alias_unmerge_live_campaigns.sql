begin;

update public.campaign_name_aliases
set canonical = alias
where channel = 'linkedin'
  and alias in (
    'ICE After the conf',
    'ICE before conf',
    'Payroll',
    'Payroll (Дорогие международные переводы, долго, комплаенс-риски)',
    'PG Connects London (before the conf)'
  );

commit;
