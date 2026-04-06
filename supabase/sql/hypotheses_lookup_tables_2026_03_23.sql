-- Migration: add lookup tables for hypotheses
-- Safe to run multiple times (all IF NOT EXISTS / IF EXISTS)

-- 1. Create lookup tables
create table if not exists public.sales_verticals (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  sort_order int not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create unique index if not exists sales_verticals_name_uidx on public.sales_verticals(lower(name));
create index if not exists sales_verticals_active_idx on public.sales_verticals(is_active, sort_order, lower(name));

create table if not exists public.sales_subverticals (
  id uuid primary key default gen_random_uuid(),
  vertical_id uuid not null references public.sales_verticals(id) on delete cascade,
  name text not null,
  sort_order int not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create unique index if not exists sales_subverticals_vertical_name_uidx
  on public.sales_subverticals(vertical_id, lower(name));
create index if not exists sales_subverticals_active_idx on public.sales_subverticals(vertical_id, is_active, sort_order, lower(name));

create table if not exists public.sales_company_scales (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  sort_order int not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create unique index if not exists sales_company_scales_name_uidx on public.sales_company_scales(lower(name));
create index if not exists sales_company_scales_active_idx on public.sales_company_scales(is_active, sort_order, lower(name));

-- 2. Add FK columns to sales_hypothesis_rows
alter table if exists public.sales_hypothesis_rows
  add column if not exists vertical_id uuid references public.sales_verticals(id) on delete set null;
alter table if exists public.sales_hypothesis_rows
  add column if not exists subvertical_id uuid references public.sales_subverticals(id) on delete set null;
alter table if exists public.sales_hypothesis_rows
  add column if not exists company_scale_id uuid references public.sales_company_scales(id) on delete set null;

create index if not exists sales_hypothesis_rows_vertical_idx on public.sales_hypothesis_rows(vertical_id);
create index if not exists sales_hypothesis_rows_subvertical_idx on public.sales_hypothesis_rows(subvertical_id);
create index if not exists sales_hypothesis_rows_company_scale_idx on public.sales_hypothesis_rows(company_scale_id);

-- 3. Triggers (updated_at)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_verticals_updated_at') then
    create trigger trg_sales_verticals_updated_at
    before update on public.sales_verticals
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_subverticals_updated_at') then
    create trigger trg_sales_subverticals_updated_at
    before update on public.sales_subverticals
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_company_scales_updated_at') then
    create trigger trg_sales_company_scales_updated_at
    before update on public.sales_company_scales
    for each row execute function public.set_updated_at();
  end if;
end $$;

-- 4. Enable RLS
alter table public.sales_verticals enable row level security;
alter table public.sales_subverticals enable row level security;
alter table public.sales_company_scales enable row level security;

-- 5. RLS policies
drop policy if exists sales_verticals_select on public.sales_verticals;
create policy sales_verticals_select on public.sales_verticals
for select using (auth.role() = 'authenticated');
drop policy if exists sales_verticals_write on public.sales_verticals;
create policy sales_verticals_write on public.sales_verticals
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_subverticals_select on public.sales_subverticals;
create policy sales_subverticals_select on public.sales_subverticals
for select using (auth.role() = 'authenticated');
drop policy if exists sales_subverticals_write on public.sales_subverticals;
create policy sales_subverticals_write on public.sales_subverticals
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_company_scales_select on public.sales_company_scales;
create policy sales_company_scales_select on public.sales_company_scales
for select using (auth.role() = 'authenticated');
drop policy if exists sales_company_scales_write on public.sales_company_scales;
create policy sales_company_scales_write on public.sales_company_scales
for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');
