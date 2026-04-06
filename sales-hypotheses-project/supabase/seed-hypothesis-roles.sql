with seed(name) as (
  values
    ('Chief Financial Officer'),
    ('Finance Director'),
    ('Financial Controller'),
    ('Vice President of Finance'),
    ('Head of Finance'),
    ('Head of Financial Planning & Analysis'),
    ('Corporate Finance Manager'),
    ('Head of Treasury'),
    ('Director of Treasury'),
    ('Treasury Manager'),
    ('Liquidity Manager'),
    ('Cash Management Director'),
    ('Head of Capital Management'),
    ('Head of Payments'),
    ('Director of Payment Solutions'),
    ('Head of Transaction Services'),
    ('Payment Operations Manager'),
    ('Payment Systems Manager'),
    ('Head of Merchant Services'),
    ('Payment Service Provider Manager'),
    ('Chief Executive Officer'),
    ('Managing Director'),
    ('President'),
    ('General Manager'),
    ('Head of Business Development'),
    ('Business Development Director'),
    ('Vice President of Partnerships'),
    ('Commercial Director'),
    ('Strategic Alliances Manager'),
    ('Ecommerce Director'),
    ('Head of Digital'),
    ('Director of Online Sales'),
    ('Head of Omnichannel'),
    ('Vice President of Ecommerce'),
    ('Payroll Manager'),
    ('Compensation & Benefits Manager'),
    ('Payroll & Compliance Lead')
)
insert into public.sales_icp_roles (name)
select seed.name
from seed
where not exists (
  select 1
  from public.sales_icp_roles existing
  where lower(existing.name) = lower(seed.name)
);
