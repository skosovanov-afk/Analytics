# Sales hypotheses system (PMF experiments)

This folder contains the Sales hypotheses system used to test PMF hypotheses in a structured way.

Components:

- Portal UI (Next.js, deployed as a Vercel subfolder):
  - 99-applications/sales/portal
- Supabase schema (tables + RLS) to apply in the Calls Supabase project:
  - 99-applications/sales/supabase/schema-hypotheses.sql
- Markdown export tool (reports):
  - 99-applications/sales/tools/sync-hypotheses.mjs
  - output folder: 01-workspace/sales/hypotheses/

Key ideas:

- Sales Library (shared building blocks):
  - Roles
  - Company Profiles
  - VP Matrix (rows=companies, cols=roles, cell=VP)
  - Channels
- Metrics
- Hypotheses only select from the Library:
  - Roles + Company Profiles (VP is filled per hypothesis intersection)
  - Channels
- Metrics
- Weekly check-ins prompt per selected channel.
- Weekly check-ins collect values for selected metrics.
- Calls can be linked to hypotheses with tag + notes.

Last updated: December 2025

Note (Jan 2026):
- The portal home page Activities view includes an **Email** tab powered by direct SmartLead events ingest (see `99-applications/sales/portal/README.md` section "SmartLead email activities").

## Tools (Node scripts)

**Hypotheses & Library:**
- sync-hypotheses.mjs (export hypotheses to markdown)
- create-hypothesis.mjs (create via CLI)
- fill-hypothesis.mjs (auto-fill messaging/VP/pains)
- export-tal-companies.mjs (export HubSpot TAL companies list)
- upsert-channels-metrics.mjs (bootstrap library + hypothesis mapping)
- seed-demo.mjs, purge-demo.mjs (demo data management)

**8C Auto-Qualification (NEW - Jan 2026):**
- qualify-call-8c.mjs (qualify a call using 8C methodology with LLM + RAG)
- process-8c-queue.mjs (background processor for automatic qualification)
- create-hubspot-8c-properties.mjs (setup HubSpot custom properties)
- lib/rag-context-builder.mjs (gather RAG context from call history)
- lib/8c-analyzer.mjs (LLM analysis with conservative scoring)
- lib/8c-prompt-template.txt (8C methodology prompt for LLM)
- lib/hubspot-deal-matcher.mjs (find deals by participants/domains)

See: tools/README-8C-QUALIFICATION.md for full 8C documentation


