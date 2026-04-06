with seed(name, sort_order) as (
  values
    ('Payroll services', 10),
    ('Ad Tech', 20),
    ('Web Hosting', 30),
    ('Creator payments', 40),
    ('VPN', 50),
    ('Fintech', 60),
    ('Telecom', 70),
    ('Digital Banking Infrastructure', 80),
    ('Cross-Border Corporate Payments', 90),
    ('Game Publisher', 100),
    ('Recruitment Agency', 110),
    ('SBC', 120),
    ('Affiliate World', 130),
    ('Affiliate Networks', 140),
    ('Freelance Platform', 150),
    ('Travel Tech', 160),
    ('Travel Tech Asia', 170),
    ('Ticketing Platforms', 180),
    ('Sigma Rome', 190),
    ('Web Summit', 200),
    ('Future Travel', 210),
    ('Slush', 220),
    ('Cryptwerk', 230),
    ('Affiliate World Bangkok', 240)
),
updated as (
  update public.sales_verticals as target
  set
    name = seed.name,
    sort_order = seed.sort_order,
    is_active = true,
    updated_at = now()
  from seed
  where lower(target.name) = lower(seed.name)
  returning target.id
)
insert into public.sales_verticals (name, sort_order, is_active)
select seed.name, seed.sort_order, true
from seed
where not exists (
  select 1
  from public.sales_verticals existing
  where lower(existing.name) = lower(seed.name)
);
