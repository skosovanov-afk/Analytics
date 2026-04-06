# INXY CSV Analysis (2026-03-02)

Source file: `Cold Outreach Automation Services _ INXY - Stats (2).csv`

## Data shape

- Rows: `836`
- Columns: `1131`
- Header rows: `3`
- Date columns detected (daily): `735`
- Date columns range: `2024-12-30` to `2027-01-03`
- Non-zero factual activity range in file: `2025-08-07` to `2026-02-28`

## Full factual totals (all channels, integer metrics only)

- `email.sent_count`: `61,333`
- `email.reply_count`: `892`
- `email.booked_meetings`: `99`
- `email.held_meetings`: `80`
- `email.clients`: `1`
- `linkedin.connection_req`: `6,131`
- `linkedin.accepted`: `1,662`
- `linkedin.sent_messages`: `3,547`
- `linkedin.replies`: `405`
- `linkedin.booked_meetings`: `16`
- `linkedin.held_meetings`: `12`
- `telegram.total_touches`: `98`
- `telegram.replies`: `28`
- `telegram.booked_meetings`: `2`
- `telegram.held_meetings`: `2`
- `app.invitation`: `3,322`
- `app.total_touches`: `2,122`
- `app.replies`: `65`
- `app.booked_meetings`: `22`
- `app.held_meetings`: `1`

## Notes

- Percentage rows (`CR ...`) were intentionally excluded from import because `manual_stats.value` is integer.
- Channel label `LinkedIN` was normalized to `linkedin`.
- For `linkedin`, CSV metric `Total touches` was mapped to `sent_messages`.
- For `app`, metric `Invitation` was kept as custom metric `invitation`.

## Backfill scope executed

- Target channels: `linkedin`, `telegram`, `app`
- Insert/update source rows prepared: `2937`
- Imported date range: `2025-08-07` to `2026-02-27`
- Imported totals:
  - `linkedin.connection_req`: `6,131`
  - `linkedin.accepted`: `1,662`
  - `linkedin.sent_messages`: `3,547`
  - `linkedin.replies`: `405`
  - `linkedin.booked_meetings`: `16`
  - `linkedin.held_meetings`: `12`
  - `telegram.total_touches`: `98`
  - `telegram.replies`: `28`
  - `telegram.booked_meetings`: `2`
  - `telegram.held_meetings`: `2`
  - `app.invitation`: `3,322`
  - `app.total_touches`: `2,122`
  - `app.replies`: `65`
  - `app.booked_meetings`: `22`
  - `app.held_meetings`: `1`

Backfill SQL file:
- `supabase/sql/manual_stats_backfill_inxy_linkedin_telegram_app_2026_03_02.sql`
