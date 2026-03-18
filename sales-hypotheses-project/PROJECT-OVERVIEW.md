# Sales Hypotheses System

System for managing PMF (Product-Market Fit) hypothesis testing with integrated analytics.

Built with Cursor AI as the primary development tool.

## What this project does

- Manage sales hypotheses (create, track, compare PMF experiments)
- ICP Library (roles, company profiles, value propositions, channels, metrics)
- Weekly check-ins per hypothesis with per-channel activity tracking
- HubSpot integration (deal tracking, TAL analytics, funnel metrics)
- Auto-qualification of sales calls using 8C methodology (LLM + RAG)
- Dashboard with KPIs, stacked bar charts, conversion tracking
- Markdown report export for hypothesis snapshots

## Architecture

### Tech stack

- **Frontend/Backend:** Next.js 14 (App Router), deployed on Vercel
- **Database:** Supabase (PostgreSQL + Row Level Security + Edge Functions)
- **CRM:** HubSpot API (deals, contacts, pipelines, custom properties)
- **Outreach analytics:** SmartLead / GetSales API integration
- **AI:** Anthropic Claude (8C call qualification, hypothesis content generation)
- **Auth:** Supabase Auth (email/password, shared auth.json tokens)

### Folder structure

```
portal/              # Next.js app (Vercel deployment)
  app/
    page.tsx         # Home dashboard (KPIs, new deals chart, active funnel)
    layout.tsx       # Root layout with Supabase auth provider
    dashboard/       # Cross-hypothesis analytics dashboard
    hypotheses/      # Hypothesis CRUD: list, create (questionnaire), detail view
    icp/             # ICP Library: roles, companies, channels, metrics, VP matrix
    compare/         # Side-by-side hypothesis comparison
    checkins/        # Weekly check-in form
    calls/           # Call detail view (linked to hypotheses)
    components/      # Shared UI: topbar, charts (StackedBars, ActivityLines)
    lib/             # Supabase client, utils
    api/             # API routes:
      hubspot/       #   HubSpot deal sync, TAL analytics, global snapshots
      smartlead/     #   SmartLead campaign sync, events, reports
      getsales/      #   GetSales CRM sync, contact import, reports
      analytics/     #   Daily stats aggregation
      sync/          #   Unified sync-all endpoint

supabase/            # Database schema (SQL migrations)
  schema-hypotheses.sql              # Core tables: hypotheses, library, check-ins
  schema-8c-qualification.sql        # 8C auto-qualification queue and results
  schema-analytics.sql               # Analytics and daily stats
  schema-hypothesis-activities.sql   # Hypothesis activity tracking
  schema-shared-tokens.sql           # Shared API tokens (HubSpot, Anthropic, etc.)

tools/               # CLI tools (Node.js scripts)
  sync-hypotheses.mjs                # Export hypotheses to markdown reports
  create-hypothesis.mjs              # Create hypothesis from JSON payload
  fill-hypothesis.mjs                # AI-fill hypothesis content (VP, messaging)
  qualify-call-8c.mjs                # 8C qualification for a single call
  process-8c-queue.mjs               # Background processor for 8C queue
  export-tal-companies.mjs           # Export HubSpot TAL company list
  upsert-channels-metrics.mjs        # Bootstrap library data
  lib/
    8c-analyzer.mjs                  # LLM-based 8C scoring engine
    8c-prompt-template.txt           # Prompt template for 8C analysis
    rag-context-builder.mjs          # RAG context from call transcripts
    hubspot-deal-matcher.mjs         # Match calls to HubSpot deals
```

## Key concepts

### Hypotheses

A hypothesis = a PMF experiment targeting a specific segment.
Each hypothesis selects from the ICP Library (roles + company profiles + channels)
and has its own Value Proposition matrix (role x company profile intersections).

### ICP Library (shared across hypotheses)

- **Roles** — buyer personas (e.g., CISO, Mobile Security Engineer)
- **Company Profiles** — target company segments (e.g., "Enterprise bank, 10K+ employees")
- **Channels** — outreach channels (LinkedIn, email, conferences, partners)
- **Metrics** — KPIs to track per hypothesis (reply rate, meetings booked, etc.)
- **VP Matrix** — read-only aggregated view of all hypothesis value propositions

### Weekly check-ins

Periodic snapshots of hypothesis progress. Each check-in captures per-channel
activity and metric values for the hypothesis.

### 8C Auto-Qualification

Automated scoring of sales calls against 8 criteria (Compelling Event, Stakeholder
Strategy, Funding, Challenges, Business Value, Solution, Competitors, Partners).
Uses LLM with RAG context from current + historical calls. Results sync to HubSpot.

## How it was built

This entire project was built iteratively using Cursor AI:

1. Started with Supabase schema design (tables, RLS policies, RPCs)
2. Built Next.js portal page by page, using Cursor to generate components
3. Added API routes for HubSpot/SmartLead/GetSales integrations
4. Created CLI tools for batch operations and report generation
5. Added 8C qualification system (LLM prompt engineering + RAG pipeline)

Cursor rules (`.cursor/rules/`) drive consistent behavior across chat sessions.
The project heavily uses Supabase RPCs and Row Level Security for data access control.

## To adapt for your own use

1. Set up a Supabase project and apply the SQL schemas from `supabase/`
2. Deploy `portal/` to Vercel (or run locally with `npm run dev`)
3. Configure environment variables (see `tools/.env.example`)
4. Customize the ICP Library for your market
5. Create your first hypothesis and start testing
