# Sales Hypotheses Portal (Vercel)

Portal for managing PMF hypothesis experiments (create hypotheses, weekly check-ins, compare, link Calls).

UX notes:
- On `/hypotheses/[id]`, **owner pickers auto-save on selection** (no extra "Add" button click required).

## Deploy on Vercel (subfolder only)

In Vercel project settings:

- Root Directory: `99-applications/sales/portal`

## Environment variables

Set these in Vercel (Project Settings -> Environment Variables):

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- `NEXT_PUBLIC_ALLOWED_EMAIL_DOMAIN` (set to `@oversecured.com`)

## HubSpot integration (MVP)

This portal supports a lightweight HubSpot integration to **load deals for a hypothesis** directly from HubSpot (without relying on saved views or custom Deal properties).

### Required HubSpot env vars (server-side)

Set these in Vercel (Project Settings -> Environment Variables):

- `HUBSPOT_PRIVATE_APP_TOKEN`
  - HubSpot Private App token (server-side only, never `NEXT_PUBLIC_...`)
- `HUBSPOT_CRON_USER_ID`
  - Supabase user id used as `created_by` for HubSpot cron sync (required for cron).
- `HUBSPOT_PORTAL_ID` (optional)
  - Used to generate "Open deal" links in the UI
- `HUBSPOT_FUNNEL_PIPELINE_IDS` (optional)
  - Comma-separated HubSpot Deal pipeline IDs to treat as the "funnel pipeline".
  - When set, the home dashboard funnel metrics and per-metric tables count **only** deals from these pipelines.

### HubSpot object model requirement

In HubSpot, create (or reuse) a **TAL list** (target accounts list) of Companies for the hypothesis.

Then in the portal:

- store the TAL link on the hypothesis (field: **HubSpot TAL link**)

Notes:
- You can paste a HubSpot Company list URL in either format:
  - `/lists/<id>` URLs
  - `/objectLists/<id>/filters` URLs (used by HubSpot UI for object lists)
- On `/hypotheses/new` and `/hypotheses/[id]`, use **Sync TAL cache (exact)** to auto-fill:
  - Companies in TAL (exact, supports 1000s of companies)
  - Deals in TAL (exact, derived from company→deal associations)
  - Contacts in TAL (exact, from Contacts list when provided; otherwise from company→contact associations)
- Paste the HubSpot **Contacts list** for the same segment into **Contacts list** (required).
  - Contacts in TAL and contact touch coverage use this list as the source of truth to match HubSpot UI counts exactly.
  - Company touch coverage is derived from contact touches (email/meeting/call) using contact→company associations.
  - For contact lists, the cache sync still builds contact→company associations via batch associations.
  - If email/meeting/call fields are missing, contact touch falls back to HubSpot sales activity date.
  - Meeting activity also uses HubSpot hs_latest_meeting_activity when present.
- If **Deals view** is provided, Leads/Opps are computed from the HubSpot view filters (via API).
  - If the view cannot be read, it falls back to TAL company deals.
  - If `HUBSPOT_WEBSITE_PIPELINE_ID` or `HUBSPOT_FUNNEL_PIPELINE_IDS` is set, Opps/Leads are filtered to those pipelines.
- Deal TAL category is auto-derived from **Vertical name** (kept in sync).
- open a hypothesis page and use the section:

- **HubSpot TAL analytics (API)** -> **Load weekly summary**
- (optional) **Load deals** (raw list of deals for TAL)

It derives analytics via:

- Company list membership (TAL) -> companies -> company->deal associations
- Deal properties and (best-effort) deal stage history
- Deal->activity associations (emails/meetings/notes/tasks)

### Exact TAL cache (recommended for large TALs)

If your TAL has thousands of companies (e.g. 6000+), you must cache it in Supabase.
The portal provides a job-style sync endpoint and stores the results in these tables:

- `sales_hubspot_tal_companies` (tal_list_id → company_id)
- `sales_hubspot_company_deals` (company_id → deal_id)
- `sales_hubspot_deal_contacts` (deal_id → contact_id)
- `sales_hubspot_tal_cache_jobs` (progress + cursors)

UI button:
- `/hypotheses/new` and `/hypotheses/[id]` → **Actions → HubSpot → Sync TAL cache (exact)**

Notes:
- The sync runs in batches and may take minutes for large TALs (it is rate-limit aware).
- You can close the tab and run it again to resume.

Requirements:
- Apply schema update: `99-applications/sales/supabase/schema-hypotheses.sql`
- Set `SUPABASE_SERVICE_ROLE_KEY` in Vercel env (server-side writer).

### HubSpot history (all-time)

By default the portal can compute a weekly summary for the last 7 days on demand.  
To keep **full history since hypothesis creation**, apply the schema update (table `sales_hubspot_tal_snapshots`) and use:

- `/hypotheses/[id]` -> **HubSpot TAL analytics (API)** -> **Sync history**

This will backfill weekly snapshots and store them in Supabase, enabling a full timeline without re-querying HubSpot for past weeks.

### Dashboard (HubSpot metrics)

On `/dashboard` you can select **Metric source: HubSpot (TAL snapshots)** and chart metrics across all hypotheses:

- new deals
- stage moves
- emails (total/sent/received)
- meetings / notes / tasks

Note: the dashboard reads from stored snapshots, so run **Sync history** in each hypothesis at least once.

### Main dashboard (home page)

The home page (`/`) shows a simplified “executive” dashboard:

- KPI "stock" counts (current state) for the selected HubSpot pipeline:
  - Leads
  - SQL
  - Opportunity (Evaluate + Select + Negotiate + Purchase)
  - Clients (Integration + Active)
  - Lost (Lost + Dormant)
  - Each KPI shows a delta vs week start (best-effort).

- One main chart (toggle "Show charts"):
  - Stacked bars: **New deals created** per period (day/week/month/quarter/year)
  - Colors: by channel or by hypothesis (unassigned deals are shown in grey)
  - Line: **Active deals in funnel** (A: excludes Lost/Dormant; can go up and down; aligned to current active total)
  - Table view: click a bar or press "Table view" to see the underlying deals list for that period
- Optional UI filters on the home page:
  - Pipelines (multi-select) and Stages (multi-select)
  - Filters affect KPI stock counts immediately; the new-deals chart uses the selected pipeline(s).

### Debug / audit (local)

If numbers look suspicious, you can run a local audit against HubSpot without touching the database:

- `node 99-applications/sales/tools/audit-hubspot-funnel.mjs --pipeline-id <PIPELINE_ID> --max 5000`

Important: provide the token via env var (do not paste tokens into git or commit history).
- Color mode toggle:
  - Colors by channel (best-effort from HubSpot deal source props)
  - Colors by hypothesis

Requirements:
- Apply schema update (table `sales_hubspot_tal_snapshots`).
- Run **Sync history** in hypotheses so snapshots exist for the period.

Global sync:
- Home page includes a single **Sync** button.
- Background syncing is handled by Vercel Cron (recommended).

### Unified sync orchestration (cron + manual)

The portal uses a single orchestrator endpoint:

- `POST /api/sync/all` (manual)
  - Triggered by the **Sync** button in the top bar
  - Requires user `Authorization: Bearer <SUPABASE_JWT>`
- `GET /api/sync/all` (cron)
  - Requires `Authorization: Bearer <CRON_SECRET>`

It runs:

- HubSpot daily snapshots: `/api/hubspot/global/daily/sync`
- HubSpot weekly snapshots: `/api/hubspot/global/sync`
- GetSales activities -> HubSpot: `/api/getsales/sync` (cron requires `GETSALES_CRON_USER_ID`)

Cron schedule (Vercel):

- `99-applications/sales/portal/vercel.json`
- Default:
  - `/api/sync/all` every 30 minutes (`*/30 * * * *`)
  - `/api/hubspot/contact-form/sync` every 5 minutes (`*/5 * * * *`)

## GetSales integration (LinkedIn + email activities -> HubSpot)

This portal can also sync outreach activity events from GetSales into HubSpot Contact timeline.

### Required GetSales env vars (server-side)

Set these in Vercel (Project Settings -> Environment Variables):

- `GETSALES_API_TOKEN`
  - GetSales API token (server-side only)
- `GETSALES_BASE_URL` (optional)
  - Defaults to `https://amazing.getsales.io`
- `GETSALES_TEAM_ID` (required for GetSales "Reports metrics" API)
  - Example: `8783`
- `GETSALES_REPORTS_TOKEN` (optional, recommended if you want exact Reports counters)
  - This is a web UI token (the same auth used by `app.voitechsales.com`).
  - If not set, the portal falls back to best-effort heuristics from Public API events (Connections may be 0).
  - Treat it like a password; store only in Vercel env. It may expire and require refresh.
- `GETSALES_LOGIN_EMAIL` + `GETSALES_LOGIN_PASSWORD` (optional, recommended if you want auto-refresh)
  - If set and `GETSALES_REPORTS_TOKEN` is NOT set, the portal will auto-login to GetSales and cache a JWT until it expires.
  - This avoids manual token refresh, but storing a password is higher risk: keep it in Vercel env only and rotate if leaked.
- `GETSALES_SYNC_SECRET` (optional)
  - If set, API routes under `/api/getsales/*` can be called from CLI/cron with either:
    - header `x-sync-secret: <secret>`, or
    - `Authorization: Bearer <secret>`
  - This avoids relying on a short-lived Supabase session JWT for terminal scripts.

### Cron requirements for GetSales

If you want `GET /api/sync/all` to include GetSales:

- `SUPABASE_SERVICE_ROLE_KEY` (required for cron writers)
- `CRON_SECRET`
- `GETSALES_CRON_USER_ID` (required)
  - Supabase user UUID used as `created_by` for `sales_getsales_*` rows in cron mode.
  - Without it, GetSales is skipped in cron (manual sync still works from the UI).

## Home page: Deals vs Activities

The home page (`/`) has a mode toggle:

- Deals: funnel KPIs + "New deals (created)" chart + table view.
- Activities: a GetSales report-style view (LinkedIn / Email) built from GetSales Public API events.

This is designed to be extended with more activity sources (e.g. LinkedIn Ads impressions/clicks) as additional series on the same chart.

Activities visibility:
- Activities are meant to be **company-wide** (CEO-friendly): any authenticated portal user can see all activity rows.
- Data is written by the cron sync using `GETSALES_CRON_USER_ID` (recommended) and/or by server-side sync using `SUPABASE_SERVICE_ROLE_KEY`.
- `GETSALES_CREATE_CONTACTS` (optional)
  - Defaults to `true`
  - When a GetSales activity cannot be matched to an existing HubSpot Contact, the sync will create a new HubSpot Contact (best-effort) so the activity can be attached.
- `GETSALES_ASSOCIATE_COMPANIES` (optional)
  - Defaults to `true`
  - Best-effort: associate the matched/created HubSpot Contact with a HubSpot Company.
- `GETSALES_CREATE_COMPANIES` (optional)
  - Defaults to `true`
  - When associating a company and none is found by domain/name, create a new HubSpot Company (best-effort).
- `HUBSPOT_CONTACT_LINKEDIN_PROPERTY` (optional)
  - Defaults to `hs_linkedin_url`
  - HubSpot Contact property used to match by LinkedIn when email is missing.
- `HUBSPOT_COMPANY_LINKEDIN_PROPERTY` (optional)
  - Defaults to `linkedin_company_page`
  - HubSpot Company property used to match/create company by LinkedIn URL.

### What is synced (MVP)

- LinkedIn messages: `GET /flows/api/linkedin-messages`
- Emails: `GET /emails/api/emails`

Each event is mapped to a HubSpot Contact by email (work_email/personal_email on the GetSales lead) and written as a HubSpot NOTE engagement.
If email is missing, it tries to match by LinkedIn. If still not found, it can create a contact (configurable) and attach the activity there.

### Activities: GetSales report (LinkedIn / Email)

The portal renders a GetSales-like "Reports" graph:

- LinkedIn: Connections sent/accepted, Messages sent/opened/replied, InMails sent/replied
- Email: Emails sent/replied (MVP)

Important:

- The GetSales Public API does not expose a dedicated "Reports" endpoint; the portal derives these counts from:
  - `GET /flows/api/linkedin-messages`
  - `GET /emails/api/emails`
- Some metrics (especially "Connections" vs "Messages") are classified best-effort from message `type/status/text` fields and may need tuning to match the GetSales UI 1:1.

Influence overlay (best-effort):

- The Activities view also overlays **HubSpot deals created (influenced)** as additional KPI tiles and chart lines.
- Influence is computed by:
  - taking HubSpot deals **created in the selected date range** (within selected pipeline),
  - fetching associated HubSpot contacts and their `email` + `hs_additional_emails`,
  - matching these emails to `public.sales_getsales_events.contact_email` within **60 days before** deal creation.
- This is **not exclusive attribution** (a deal can be influenced by multiple channels).
- For large ranges, influence computation may be **truncated** to stay within HubSpot API limits; narrow the date range for full accuracy.

Troubleshooting (connections):

- If **Connections Sent/Accepted** stay at 0, your GetSales account may track them via a private/internal endpoint not included in the public OpenAPI.
- Helper endpoint: `GET /api/getsales/discover`
  - It probes a small list of likely report/connection endpoints and shows which ones exist (status 200/404).

GetSales "Reports UI metrics" (fix for Connections Sent/Accepted):

- The portal can fetch the exact counters from the same internal endpoint the GetSales Reports page uses:
  - `POST /api/getsales/reports/metrics` (portal API)
- For chart accuracy, the portal also tries to fetch a day-by-day timeseries:
  - `POST /api/getsales/reports/timeseries` (portal API)
  - If the GetSales deployment does not support `group_by=days`, the portal falls back to event-derived heuristics for the chart.
- It uses official GetSales domain (`GETSALES_BASE_URL`, default `https://amazing.getsales.io`) + `GETSALES_TEAM_ID` + either:
  - `GETSALES_REPORTS_TOKEN`, or
  - `GETSALES_LOGIN_EMAIL` + `GETSALES_LOGIN_PASSWORD` (auto-refresh)
- Do NOT paste browser cookie tokens into chat; treat them like passwords. Store secrets only in Vercel env.

GetSales backfill & repair (when Activities/Influence show 0):

Sometimes the Activities dashboard shows zeros (especially influenced deals). Common root causes:

- old GetSales activities were never imported (default sync is last 7 days)
- older `sales_getsales_events` rows have `contact_email = null` (so influence matching by email cannot work)

Backfill last 6 months (does NOT touch sync cursor):

- Endpoint: `POST /api/getsales/backfill`
- Defaults:
  - `since = now - 6 months`
  - `skip_state_update = true` (does not overwrite `sales_getsales_sync_state`)
  - returns `cursor.max_seen` (for looping)
- Example body: `{ "months": 6, "max": 200 }`
- Repeat until `stats.inserted` becomes ~0.

Repair missing event emails (enables influence matching):

- Endpoint: `POST /api/getsales/repair-event-emails`
- What it does:
  - selects `sales_getsales_events` rows with `contact_email IS NULL`
  - refetches GetSales lead by `lead_uuid`
  - fills `contact_email` when possible
- Example body: `{ "limit": 200 }`
- Repeat until `stats.updated` becomes 0.

## SmartLead integration (HubSpot SQL -> SmartLead enroll -> HubSpot notes)

This portal can run a cron sync that:

- detects HubSpot **Deal stage transitions into SQL** (within selected pipelines)
- enrolls the associated HubSpot Contact(s) into a SmartLead campaign
- when SmartLead lead status becomes **COMPLETED**, writes a HubSpot NOTE attached to the Deal + Contact with the raw SmartLead sequence details

## SmartLead email activities (sent/open/reply) for "Email" tab

Problem:
- The standard SmartLead -> HubSpot connector is great for CRM hygiene, but it does NOT guarantee that you can query accurate per-email **sent/open/reply** counters from HubSpot API (depends on what objects/properties the connector writes and on HubSpot Email Events availability).

Solution (this repo):
- The home page Activities view has an **Email** tab powered by **direct SmartLead events ingest** into Supabase:
  - Table: `public.sales_smartlead_events`
  - Sync endpoint: `POST /api/smartlead/activities/sync`
  - Reports endpoints used by the UI:
    - `POST /api/smartlead/reports/metrics`
    - `POST /api/smartlead/reports/timeseries`
  - Campaigns list endpoint used by the UI (status labels are best-effort):
    - `POST /api/smartlead/campaigns`

What the UI shows:
- KPI tiles: Emails Sent / Emails Opened / Emails Replied
- SmartLead-like campaign banner (best-effort fields from SmartLead campaign details):
  - Sent / Opened / Replied
  - Replied w/OOO
  - Positive reply
  - Bounced
- Time series graph (supports 1y with bucketing)
- Events table (audit) from `sales_smartlead_events`
- Influence overlay: HubSpot deals created that had a matching SmartLead event within a configurable lookback window
  - Campaign filter: you can optionally select one or more SmartLead campaigns; KPIs/graph/events/influence will be filtered to those campaigns

How to sync:
- Use the main dashboard (`/`) **Sync** button (runs `POST /api/sync/all`)
  - it includes SmartLead activities when SmartLead is enabled
- Or call `POST /api/smartlead/activities/sync` directly (authorized portal session)

Campaigns discovery:
- By default, the portal will try to list campaigns from SmartLead API (no need to maintain an env list).
- You can still set `SMARTLEAD_CAMPAIGN_IDS` to restrict scanning (faster + cheaper), especially if you have many campaigns.

Limitations:
- The activity parser is intentionally conservative (only sent/open/reply right now).
- We treat "opened" as at least "replied" in the UI (reply implies open).

### Required SmartLead env vars (server-side)

Set these in Vercel (Project Settings -> Environment Variables):

- `SMARTLEAD_API_KEY` (required)
- `SMARTLEAD_BASE_URL` (optional)
  - Defaults to `https://server.smartlead.ai`
- `SMARTLEAD_DEFAULT_CAMPAIGN_ID` (recommended)
  - Used when a HubSpot Deal does not specify a per-deal campaign id
- `SMARTLEAD_CAMPAIGN_IDS` (optional)
  - Comma-separated campaign IDs to scan for COMPLETED leads (fallbacks to default + recent enrollments)
- `SMARTLEAD_IGNORE_DUPLICATE_LEADS_IN_OTHER_CAMPAIGN` (optional)
  - Defaults to `true`
  - Passed to SmartLead "Add leads to a campaign" API as `ignore_duplicate_leads_in_other_campaign`
- `SMARTLEAD_SYNC_SECRET` (optional)
  - If set, `/api/smartlead/sync` can be triggered from CLI/cron via `x-sync-secret` or `Authorization: Bearer <secret>`

### HubSpot config for SQL trigger

- `HUBSPOT_FUNNEL_PIPELINE_IDS` (required)
  - Comma-separated HubSpot pipeline IDs to treat as the funnel pipeline(s)
- `HUBSPOT_SQL_STAGE_IDS` (optional)
  - Comma-separated HubSpot stage IDs considered "SQL"
  - If not set, the integration falls back to matching stage label containing "sql"
- `HUBSPOT_DEAL_SMARTLEAD_CAMPAIGN_PROPERTY` (optional)
  - Defaults to `smartlead_campaign_id`
  - If set on a deal, that campaign id overrides `SMARTLEAD_DEFAULT_CAMPAIGN_ID`

### Cron requirements

If you want `GET /api/sync/all` (Vercel cron) to include SmartLead:

- `SUPABASE_SERVICE_ROLE_KEY` (required for cron writers)
- `CRON_SECRET`
- `SMARTLEAD_CRON_USER_ID` (required)
  - Supabase user UUID used as `created_by` for `sales_smartlead_*` rows in cron mode.
  - Without it, SmartLead is skipped in cron (manual sync still works if authorized).

### Supabase schema (required)

Apply the schema update:

- `99-applications/sales/supabase/schema-hypotheses.sql`

It creates:

- `public.sales_smartlead_sync_state` (incremental cursors)
- `public.sales_smartlead_enrollments` (idempotency + notes linkage)

### How to run

- Main dashboard (`/`) -> button **Sync**
  - It runs `POST /api/sync/all` which includes GetSales (if configured).
- Direct API route: `POST /api/getsales/sync`
  - Requires Supabase Bearer token (from portal session) OR `GETSALES_SYNC_SECRET` for CLI/cron usage.

### Import contacts from a GetSales list (CSV-free)

If you have a GetSales `list_uuid` where leads already have email, you can bulk upsert them into HubSpot Contacts:

- API route: `POST /api/getsales/import-contacts`
  - body: `{ "list_uuid": "...", "max": 6000, "offset": 0, "batch_limit": 50 }`
  - Run it in pages to avoid serverless timeouts. Response contains `stats.next_offset` and `stats.has_more`.
  - It upserts contacts by email (fast path: batch upsert; fallback: search+create/update).

### Repair placeholder HubSpot contacts created without email

If you previously synced GetSales activities while emails were missing, the portal could create placeholder HubSpot contacts (so notes have a record to attach to).
Once your GetSales list has emails, you can patch those placeholder contacts by matching on LinkedIn URL and filling email/name.

- API route: `POST /api/getsales/repair-placeholders`
  - body: `{ "list_uuid": "...", "offset": 0, "batch_limit": 20, "max": 6000 }`
  - Run it in pages; response contains `stats.next_offset` and `stats.has_more`.

### Associate contacts with HubSpot Companies (mode B: create missing companies)

After importing contacts, you can create/link HubSpot Company objects for them:

- API route: `POST /api/getsales/associate-companies`
  - body: `{ "list_uuid": "...", "offset": 0, "batch_limit": 20, "max": 6000 }`
  - Matching order:
    - company by email domain (skip personal domains)
    - company by LinkedIn company URL (from GetSales lead experience/company_ln_id)
    - company by company_name
  - If not found, it creates a company (config: `GETSALES_CREATE_COMPANIES`, default true) and associates Contact -> Company.

### Optional LLM weekly summary

If you want a natural-language weekly summary, set:

- `TAL_SUMMARY_LLM_PROVIDER=anthropic`
- `ANTHROPIC_API_KEY`
- `TAL_SUMMARY_LLM_MODEL` (optional)

### Saved view links (optional)

Each hypothesis can still store a **HubSpot Saved View URL** filtered by that property, so the pipeline can be audited in HubSpot UI.

## Supabase schema (required)

Before using the portal, apply the hypotheses tables/RLS in Supabase:

- Open Supabase SQL Editor for the Calls project
- Paste and run: `99-applications/sales/supabase/schema-hypotheses.sql`

PostgREST schema cache can take a minute to refresh; if you see “table not found”, wait ~60s and retry.

## Core UI structure (v1)

- Library (shared building blocks):
  - /icp (Sales Library)
  - /icp/roles (Roles)
  - /icp/companies (Company Profiles)
  - /icp/matrix (VP Matrix: rows=companies, cols=roles)
  - /icp/channels (Channels)

- Hypotheses:
  - /hypotheses (list)
  - /hypotheses/new (create)
  - /hypotheses/[id] (edit + weekly check-ins + call linking)
  - /checkins/new (submit weekly report across multiple hypotheses you own channels for)
  - /dashboard (analytics)
  - /compare (deprecated redirect to /dashboard)

- Home:
  - / (main dashboard: HubSpot TAL snapshots + funnel metrics)

## Demo / test data (seed)

To populate the portal with safe synthetic data (2 demo hypotheses + demo channels/metrics + 3 weekly check-ins each):

- Run: `node 99-applications/sales/tools/seed-demo.mjs`

This creates rows prefixed with `demo_` and hypotheses titled `[DEMO] ...`.

Key behavior:

- Hypotheses select Roles + Company Profiles + Channels from the Library.
- VP is stored per hypothesis intersection (Role x CompanyProfile) as a single statement and edited inside the hypothesis page as a matrix.
- /icp/matrix shows an aggregated, read-only VP view across hypotheses.
- Weekly check-ins prompt per selected channel; channel-level inputs show ONLY metrics that were linked to that channel (no implicit calls/opps fields).
- Calls can be linked to hypotheses with tag + notes; the UI shows a picker of your calls from last 7 days.
- Hypotheses can be created via a guided questionnaire on /hypotheses/new.
- Hypothesis page has 3 collapsible sections (Description / Weekly check-ins / Calls) collapsed by default.
- Hypotheses can be deleted from /hypotheses list and from the hypothesis page (owner/admin).

## Hypothesis page: Client profile + pain grids

On `/hypotheses/[id]` the Description section includes:

- Client profile: a long text block to paste a detailed TAL/segment description (company/app profile, constraints, buying context, etc.).
- Pain points grid: matrix (rows=company profiles, cols=roles) with multiline pain points per segment.
- How product closes pains grid: matrix (rows=company profiles, cols=roles) with multiline product mapping per segment.

## Hypothesis page: HubSpot links in Actions

To keep the main form clean, HubSpot links are edited in:

- `/hypotheses/[id]` -> `Actions` -> `Links (edit here)`

It contains:

- TAL link
- Contacts list
- Deals view

Last updated: December 2025


