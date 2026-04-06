alter table if exists public.sales_hypothesis_rows
  add column if not exists tal_id uuid references public.tals(id) on delete set null;

create index if not exists sales_hypothesis_rows_tal_id_idx
  on public.sales_hypothesis_rows(tal_id);
