create or replace function public.can_delete_sales_hypothesis(p_hypothesis_id uuid)
returns boolean
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  ok boolean;
begin
  perform set_config('row_security', 'off', true);
  select true into ok
  from public.sales_hypotheses h
  where h.id = p_hypothesis_id
    and (h.owner_user_id = auth.uid() or public.is_admin())
  limit 1;
  return coalesce(ok, false);
end;
$$;

grant execute on function public.can_delete_sales_hypothesis(uuid) to anon, authenticated;

create or replace function public.can_submit_sales_checkin(p_hypothesis_id uuid)
returns boolean
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  ok boolean;
  email text;
begin
  perform set_config('row_security', 'off', true);
  email := lower(coalesce(auth.jwt() ->> 'email', ''));
  if email = '' then
    return false;
  end if;

  if public.can_edit_sales_hypothesis(p_hypothesis_id) then
    return true;
  end if;

  select true into ok
  from public.sales_hypothesis_channel_owners o
  where o.hypothesis_id = p_hypothesis_id
    and lower(o.owner_email) = email
  limit 1;
  if coalesce(ok, false) then
    return true;
  end if;

  select true into ok
  from public.sales_hypothesis_channel_metric_owners mo
  where mo.hypothesis_id = p_hypothesis_id
    and lower(mo.owner_email) = email
  limit 1;

  return coalesce(ok, false);
end;
$$;

grant execute on function public.can_submit_sales_checkin(uuid) to anon, authenticated;

create table if not exists public.sales_hypothesis_checkins (
  id uuid primary key default gen_random_uuid(),
  hypothesis_id uuid not null references public.sales_hypotheses(id) on delete cascade,
  week_start date not null,
  opps_in_progress_count int,
  tal_companies_count int,
  contacts_count int,
  notes text,
  blockers text,
  next_steps text,
  channel_activity_json jsonb not null default '{}'::jsonb,
  metrics_snapshot_json jsonb not null default '{}'::jsonb,
  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (hypothesis_id, week_start)
);

create index if not exists sales_checkins_hypothesis_idx
on public.sales_hypothesis_checkins(hypothesis_id, week_start desc);

alter table public.sales_hypothesis_checkins enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_trigger
    where tgname = 'trg_sales_checkins_updated_at'
  ) then
    create trigger trg_sales_checkins_updated_at
    before update on public.sales_hypothesis_checkins
    for each row execute function public.set_updated_at();
  end if;
end $$;

drop policy if exists sales_checkins_select on public.sales_hypothesis_checkins;
create policy sales_checkins_select
on public.sales_hypothesis_checkins
for select
using (auth.role() = 'authenticated');

drop policy if exists sales_checkins_insert on public.sales_hypothesis_checkins;
create policy sales_checkins_insert
on public.sales_hypothesis_checkins
for insert
with check (auth.role() = 'authenticated' and public.can_submit_sales_checkin(hypothesis_id));

drop policy if exists sales_checkins_update on public.sales_hypothesis_checkins;
create policy sales_checkins_update
on public.sales_hypothesis_checkins
for update
using (public.can_submit_sales_checkin(hypothesis_id))
with check (public.can_submit_sales_checkin(hypothesis_id));

drop policy if exists sales_checkins_delete on public.sales_hypothesis_checkins;
create policy sales_checkins_delete
on public.sales_hypothesis_checkins
for delete
using (public.can_delete_sales_hypothesis(hypothesis_id));
