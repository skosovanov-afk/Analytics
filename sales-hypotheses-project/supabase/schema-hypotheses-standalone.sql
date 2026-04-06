-- Standalone Sales Hypotheses schema
-- Safe to run in a plain Supabase project without dependencies on Calls/user_profiles/is_admin.

create extension if not exists pgcrypto;

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

-- Shared lookup/library tables
create table if not exists public.sales_icp_roles (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  decision_roles text[] not null default '{}'::text[],
  seniority text,
  titles text[] not null default '{}'::text[],
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists sales_icp_roles_name_uidx on public.sales_icp_roles(lower(name));

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
  updated_at timestamptz not null default now(),
  unique (vertical_id, name)
);

create unique index if not exists sales_subverticals_vertical_name_uidx
  on public.sales_subverticals(vertical_id, lower(name));
create index if not exists sales_subverticals_active_idx
  on public.sales_subverticals(vertical_id, is_active, sort_order, lower(name));

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

create table if not exists public.sales_icp_company_profiles (
  id uuid primary key default gen_random_uuid(),
  vertical_name text,
  sub_vertical text,
  region text,
  size_bucket text,
  tech_stack text[] not null default '{}'::text[],
  constraints_json jsonb not null default '{}'::jsonb,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists sales_icp_company_profiles_vertical_idx
  on public.sales_icp_company_profiles(lower(coalesce(vertical_name,'')), lower(coalesce(sub_vertical,'')));

create table if not exists public.sales_channels (
  id uuid primary key default gen_random_uuid(),
  slug text not null unique,
  name text not null,
  sort_order int not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.sales_metrics (
  id uuid primary key default gen_random_uuid(),
  slug text not null unique,
  name text not null,
  input_type text not null default 'number',
  unit text,
  sort_order int not null default 0,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Workspace container
create table if not exists public.sales_hypotheses (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  status text not null default 'draft',
  priority int not null default 0,
  owner_user_id uuid not null default auth.uid(),
  owner_email text,
  vertical_name text,
  pricing_model text,
  opps_in_progress_count int not null default 0,
  timebox_days int not null default 28,
  win_criteria text,
  kill_criteria text,
  tal_companies_count_baseline int,
  contacts_count_baseline int,
  one_sentence_pitch text,
  product_description text,
  company_profile_text text,
  cjm_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists sales_hypotheses_updated_idx on public.sales_hypotheses(updated_at desc);
create index if not exists sales_hypotheses_owner_idx on public.sales_hypotheses(owner_user_id, updated_at desc);

-- Row-level hypothesis sheet
create table if not exists public.sales_hypothesis_rows (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  row_code text,
  title text,
  tal_id uuid default null, -- FK to tals(id) managed via migration, not in standalone
  role_id uuid references public.sales_icp_roles(id) on delete set null,
  role_label text,
  company_profile_id uuid references public.sales_icp_company_profiles(id) on delete set null,
  vertical_id uuid references public.sales_verticals(id) on delete set null,
  subvertical_id uuid references public.sales_subverticals(id) on delete set null,
  company_scale_id uuid references public.sales_company_scales(id) on delete set null,
  vertical_name text,
  sub_vertical text,
  company_scale text,
  decision_context text,
  vp_point text not null,
  pain text,
  expected_signal text,
  disqualifiers text,
  calls_count int not null default 0,
  pain_confirmed_rate numeric(5,2),
  severity_rate numeric(5,2),
  interest_rate numeric(5,2),
  opportunities_count int not null default 0,
  signal_speed text,
  decision text,
  status text not null default 'new',
  priority int not null default 0,
  owner_user_id uuid,
  notes text,
  source text not null default 'manual',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists sales_hypothesis_rows_workspace_idx on public.sales_hypothesis_rows(workspace_id, updated_at desc);
create index if not exists sales_hypothesis_rows_status_idx on public.sales_hypothesis_rows(status, updated_at desc);
create index if not exists sales_hypothesis_rows_decision_idx on public.sales_hypothesis_rows(decision, updated_at desc);
create index if not exists sales_hypothesis_rows_tal_id_idx on public.sales_hypothesis_rows(tal_id);
create index if not exists sales_hypothesis_rows_role_idx on public.sales_hypothesis_rows(role_id);
create index if not exists sales_hypothesis_rows_vertical_idx on public.sales_hypothesis_rows(vertical_id);
create index if not exists sales_hypothesis_rows_subvertical_idx on public.sales_hypothesis_rows(subvertical_id);
create index if not exists sales_hypothesis_rows_company_scale_idx on public.sales_hypothesis_rows(company_scale_id);
create unique index if not exists sales_hypothesis_rows_workspace_code_uidx
  on public.sales_hypothesis_rows(workspace_id, lower(row_code))
  where row_code is not null and btrim(row_code) <> '';

-- Optional joins used by the current UI
create table if not exists public.sales_hypothesis_roles (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  role_id uuid not null references public.sales_icp_roles(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, role_id)
);

create table if not exists public.sales_hypothesis_company_profiles (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  company_profile_id uuid not null references public.sales_icp_company_profiles(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, company_profile_id)
);

create table if not exists public.sales_hypothesis_metrics (
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  metric_id uuid not null references public.sales_metrics(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (hypothesis_id, metric_id)
);

-- updated_at triggers
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_icp_roles_updated_at') then
    create trigger trg_sales_icp_roles_updated_at before update on public.sales_icp_roles
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_verticals_updated_at') then
    create trigger trg_sales_verticals_updated_at before update on public.sales_verticals
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_subverticals_updated_at') then
    create trigger trg_sales_subverticals_updated_at before update on public.sales_subverticals
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_company_scales_updated_at') then
    create trigger trg_sales_company_scales_updated_at before update on public.sales_company_scales
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_icp_company_profiles_updated_at') then
    create trigger trg_sales_icp_company_profiles_updated_at before update on public.sales_icp_company_profiles
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_channels_updated_at') then
    create trigger trg_sales_channels_updated_at before update on public.sales_channels
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_metrics_updated_at') then
    create trigger trg_sales_metrics_updated_at before update on public.sales_metrics
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_hypotheses_updated_at') then
    create trigger trg_sales_hypotheses_updated_at before update on public.sales_hypotheses
    for each row execute function public.set_updated_at();
  end if;
  if not exists (select 1 from pg_trigger where tgname = 'trg_sales_hypothesis_rows_updated_at') then
    create trigger trg_sales_hypothesis_rows_updated_at before update on public.sales_hypothesis_rows
    for each row execute function public.set_updated_at();
  end if;
end $$;

-- RLS
alter table public.sales_icp_roles enable row level security;
alter table public.sales_verticals enable row level security;
alter table public.sales_subverticals enable row level security;
alter table public.sales_company_scales enable row level security;
alter table public.sales_icp_company_profiles enable row level security;
alter table public.sales_channels enable row level security;
alter table public.sales_metrics enable row level security;
alter table public.sales_hypotheses enable row level security;
alter table public.sales_hypothesis_rows enable row level security;
alter table public.sales_hypothesis_roles enable row level security;
alter table public.sales_hypothesis_company_profiles enable row level security;
alter table public.sales_hypothesis_metrics enable row level security;

-- Simple MVP policies: authenticated users can collaborate.
drop policy if exists sales_icp_roles_select on public.sales_icp_roles;
create policy sales_icp_roles_select on public.sales_icp_roles for select using (auth.role() = 'authenticated');
drop policy if exists sales_icp_roles_write on public.sales_icp_roles;
create policy sales_icp_roles_write on public.sales_icp_roles for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_verticals_select on public.sales_verticals;
create policy sales_verticals_select on public.sales_verticals for select using (auth.role() = 'authenticated');
drop policy if exists sales_verticals_write on public.sales_verticals;
create policy sales_verticals_write on public.sales_verticals for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_subverticals_select on public.sales_subverticals;
create policy sales_subverticals_select on public.sales_subverticals for select using (auth.role() = 'authenticated');
drop policy if exists sales_subverticals_write on public.sales_subverticals;
create policy sales_subverticals_write on public.sales_subverticals for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_company_scales_select on public.sales_company_scales;
create policy sales_company_scales_select on public.sales_company_scales for select using (auth.role() = 'authenticated');
drop policy if exists sales_company_scales_write on public.sales_company_scales;
create policy sales_company_scales_write on public.sales_company_scales for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_icp_company_profiles_select on public.sales_icp_company_profiles;
create policy sales_icp_company_profiles_select on public.sales_icp_company_profiles for select using (auth.role() = 'authenticated');
drop policy if exists sales_icp_company_profiles_write on public.sales_icp_company_profiles;
create policy sales_icp_company_profiles_write on public.sales_icp_company_profiles for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_channels_select on public.sales_channels;
create policy sales_channels_select on public.sales_channels for select using (auth.role() = 'authenticated');
drop policy if exists sales_channels_write on public.sales_channels;
create policy sales_channels_write on public.sales_channels for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_metrics_select on public.sales_metrics;
create policy sales_metrics_select on public.sales_metrics for select using (auth.role() = 'authenticated');
drop policy if exists sales_metrics_write on public.sales_metrics;
create policy sales_metrics_write on public.sales_metrics for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_hypotheses_select on public.sales_hypotheses;
create policy sales_hypotheses_select on public.sales_hypotheses for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypotheses_insert on public.sales_hypotheses;
create policy sales_hypotheses_insert on public.sales_hypotheses for insert with check (auth.role() = 'authenticated');
drop policy if exists sales_hypotheses_update on public.sales_hypotheses;
create policy sales_hypotheses_update on public.sales_hypotheses for update using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');
drop policy if exists sales_hypotheses_delete on public.sales_hypotheses;
create policy sales_hypotheses_delete on public.sales_hypotheses for delete using (auth.role() = 'authenticated');

drop policy if exists sales_hypothesis_rows_select on public.sales_hypothesis_rows;
create policy sales_hypothesis_rows_select on public.sales_hypothesis_rows for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_rows_insert on public.sales_hypothesis_rows;
create policy sales_hypothesis_rows_insert on public.sales_hypothesis_rows for insert with check (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_rows_update on public.sales_hypothesis_rows;
create policy sales_hypothesis_rows_update on public.sales_hypothesis_rows for update using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_rows_delete on public.sales_hypothesis_rows;
create policy sales_hypothesis_rows_delete on public.sales_hypothesis_rows for delete using (auth.role() = 'authenticated');

drop policy if exists sales_hypothesis_roles_select on public.sales_hypothesis_roles;
create policy sales_hypothesis_roles_select on public.sales_hypothesis_roles for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_roles_write on public.sales_hypothesis_roles;
create policy sales_hypothesis_roles_write on public.sales_hypothesis_roles for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_hypothesis_company_profiles_select on public.sales_hypothesis_company_profiles;
create policy sales_hypothesis_company_profiles_select on public.sales_hypothesis_company_profiles for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_company_profiles_write on public.sales_hypothesis_company_profiles;
create policy sales_hypothesis_company_profiles_write on public.sales_hypothesis_company_profiles for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');

drop policy if exists sales_hypothesis_metrics_select on public.sales_hypothesis_metrics;
create policy sales_hypothesis_metrics_select on public.sales_hypothesis_metrics for select using (auth.role() = 'authenticated');
drop policy if exists sales_hypothesis_metrics_write on public.sales_hypothesis_metrics;
create policy sales_hypothesis_metrics_write on public.sales_hypothesis_metrics for all using (auth.role() = 'authenticated') with check (auth.role() = 'authenticated');
