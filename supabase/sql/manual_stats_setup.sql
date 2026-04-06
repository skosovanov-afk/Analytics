-- =============================================================================
-- Manual Stats: таблица для ручного ввода данных по каналам
-- Применить: вставить в Supabase SQL Editor и запустить целиком
-- =============================================================================

create table if not exists public.manual_stats (
  id            uuid        default gen_random_uuid() primary key,
  created_at    timestamptz default now(),
  record_date   date        not null,
  channel       text        not null,   -- 'linkedin' | 'email' | 'telegram' | 'app'
  account_name  text,
  campaign_name text,
  metric_name   text        not null,   -- 'connection_req' | 'accepted' | 'sent_messages' | 'replies' | 'sent_count' | 'reply_count' | 'open_count' | custom
  value         int         not null,
  note          text
);

alter table public.manual_stats enable row level security;

create policy "authenticated can manage manual_stats"
  on public.manual_stats for all to authenticated
  using (true) with check (true);
