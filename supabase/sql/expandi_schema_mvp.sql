-- Expandi MVP schema (separate from smartlead_* tables)
-- Run in Supabase SQL Editor.

begin;

create extension if not exists pgcrypto;

create table if not exists public.expandi_accounts (
  id bigint primary key,
  workspace_id bigint null,
  name text null,
  login text null,
  headline text null,
  job_title text null,
  image_base64 text null,
  li_account_user_id bigint null,
  li_account_user_role_id int null,
  li_account_user_role_name text null,
  raw_payload jsonb not null default '{}'::jsonb,
  synced_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists expandi_accounts_workspace_idx
  on public.expandi_accounts (workspace_id);

create index if not exists expandi_accounts_synced_at_idx
  on public.expandi_accounts (synced_at desc);

create table if not exists public.expandi_campaign_instances (
  id bigint primary key,
  li_account_id bigint not null,
  campaign_id bigint null,
  name text null,
  campaign_type int null,
  active boolean null,
  archived boolean null,
  step_count int null,
  first_action_action_type int null,
  nr_contacts_total int null,
  campaign_status text null,
  stats jsonb null,
  raw_payload jsonb not null default '{}'::jsonb,
  synced_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists expandi_campaign_instances_li_account_idx
  on public.expandi_campaign_instances (li_account_id);

create index if not exists expandi_campaign_instances_active_idx
  on public.expandi_campaign_instances (active);

create index if not exists expandi_campaign_instances_synced_at_idx
  on public.expandi_campaign_instances (synced_at desc);

create table if not exists public.expandi_messengers (
  id bigint primary key,
  li_account_id bigint not null,
  contact_id bigint null,
  campaign_instance_id bigint null,
  campaign_id bigint null,
  campaign_name text null,
  contact_profile_link_sn text null,
  contact_public_identifier text null,
  contact_entity_urn text null,
  contact_job_title text null,
  contact_company_name text null,
  contact_phone text null,
  contact_address text null,
  campaign_contact_status int null,
  campaign_running_status int null,
  last_action_id bigint null,
  nr_steps_before_responding int null,
  contact_profile_link text null,
  contact_email text null,
  contact_name text null,
  contact_status int null,
  conversation_status int null,
  last_message_id bigint null,
  has_new_messages boolean null,
  last_datetime timestamptz null,
  connected_at timestamptz null,
  invited_at timestamptz null,
  first_outbound_at timestamptz null,
  first_inbound_at timestamptz null,
  replied_at timestamptz null,
  is_replied boolean null,
  is_blacklisted boolean null,
  reason_failed int null,
  raw_payload jsonb not null default '{}'::jsonb,
  synced_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists expandi_messengers_li_account_idx
  on public.expandi_messengers (li_account_id);

create index if not exists expandi_messengers_contact_email_idx
  on public.expandi_messengers (contact_email);

create index if not exists expandi_messengers_campaign_instance_idx
  on public.expandi_messengers (campaign_instance_id);

create index if not exists expandi_messengers_campaign_id_idx
  on public.expandi_messengers (campaign_id);

create index if not exists expandi_messengers_last_datetime_idx
  on public.expandi_messengers (last_datetime desc);

create index if not exists expandi_messengers_synced_at_idx
  on public.expandi_messengers (synced_at desc);

create table if not exists public.expandi_messages (
  id bigint primary key,
  messenger_id bigint not null,
  li_account_id bigint null,
  campaign_instance_id bigint null,
  campaign_id bigint null,
  campaign_step_id bigint null,
  created_at_source timestamptz null,
  updated_at_source timestamptz null,
  send_datetime timestamptz null,
  received_datetime timestamptz null,
  event_datetime timestamptz null,
  body text null,
  status int null,
  send_by text null,
  send_by_id bigint null,
  direction text null,
  is_outbound boolean null,
  is_inbound boolean null,
  flag_direct boolean null,
  flag_mobile boolean null,
  flag_open_inmail boolean null,
  inmail boolean null,
  inmail_type int null,
  inmail_accepted boolean null,
  reason_failed int null,
  attachment text null,
  attachment_size int null,
  has_attachment boolean null,
  extracted_urls jsonb null,
  extracted_domains jsonb null,
  raw_payload jsonb not null default '{}'::jsonb,
  synced_at timestamptz not null default now()
);

create index if not exists expandi_messages_messenger_idx
  on public.expandi_messages (messenger_id);

create index if not exists expandi_messages_campaign_instance_idx
  on public.expandi_messages (campaign_instance_id);

create index if not exists expandi_messages_campaign_id_idx
  on public.expandi_messages (campaign_id);

create index if not exists expandi_messages_direction_idx
  on public.expandi_messages (direction);

create index if not exists expandi_messages_send_datetime_idx
  on public.expandi_messages (send_datetime desc);

create index if not exists expandi_messages_received_datetime_idx
  on public.expandi_messages (received_datetime desc);

create index if not exists expandi_messages_event_datetime_idx
  on public.expandi_messages (event_datetime desc);

create index if not exists expandi_messages_synced_at_idx
  on public.expandi_messages (synced_at desc);

create table if not exists public.expandi_sync_state (
  key text primary key,
  value text null,
  value_int bigint null,
  updated_at timestamptz not null default now()
);

create table if not exists public.expandi_stats_daily (
  date date not null,
  li_account_id bigint not null,
  campaign_instance_id bigint null,
  sent_messages int not null default 0,
  received_messages int not null default 0,
  new_conversations int not null default 0,
  failed_actions int not null default 0,
  updated_at timestamptz not null default now(),
  primary key (date, li_account_id, campaign_instance_id)
);

create index if not exists expandi_stats_daily_date_idx
  on public.expandi_stats_daily (date desc);

create index if not exists expandi_stats_daily_li_account_idx
  on public.expandi_stats_daily (li_account_id, date desc);

commit;
