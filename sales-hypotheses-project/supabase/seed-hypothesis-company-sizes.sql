with seed(name, sort_order) as (
  values
    ('Self-Employed', 10),
    ('2-10 employees', 20),
    ('11-50 employees', 30),
    ('51-200 employees', 40),
    ('201-500 employees', 50),
    ('501-1,000 employees', 60),
    ('1,001-5,000 employees', 70),
    ('5,001-10,000 employees', 80),
    ('10,001+ employees', 90)
),
updated as (
  update public.sales_company_scales as target
  set
    name = seed.name,
    sort_order = seed.sort_order,
    is_active = true,
    updated_at = now()
  from seed
  where lower(target.name) = lower(seed.name)
  returning target.id
)
insert into public.sales_company_scales (name, sort_order, is_active)
select seed.name, seed.sort_order, true
from seed
where not exists (
  select 1
  from public.sales_company_scales existing
  where lower(existing.name) = lower(seed.name)
);
