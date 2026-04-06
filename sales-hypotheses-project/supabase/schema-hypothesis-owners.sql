-- Hypothesis Owners - lookup table for responsible persons
-- Run in Supabase SQL Editor

-- 1. Owners lookup table
create table if not exists public.hypothesis_owners (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  email text,
  created_at timestamptz not null default now()
);

-- RLS
alter table public.hypothesis_owners enable row level security;

create policy "hypothesis_owners_read" on public.hypothesis_owners
  for select to authenticated using (true);

-- 2. Add owner_id to sales_hypothesis_rows
alter table public.sales_hypothesis_rows
  add column if not exists owner_id uuid references public.hypothesis_owners(id) on delete set null;

create index if not exists sales_hyp_rows_owner_idx
  on public.sales_hypothesis_rows(owner_id);

-- 3. Seed owners
insert into public.hypothesis_owners (name, email) values
  ('Elmira', 'e.chubarova@inxy.io'),
  ('Dmitro', 'd.spichek@inxy.io'),
  ('Radmila', 'r.rastogi@inxy.io')
on conflict do nothing;
