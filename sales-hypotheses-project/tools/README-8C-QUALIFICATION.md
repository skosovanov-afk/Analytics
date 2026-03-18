# 8C Auto-Qualification System

Automatic 8C qualification for client calls with RAG context, HubSpot integration, and Slack notifications.

## Architecture

```
Call with category='client' 
  ↓
Supabase trigger → adds to queue
  ↓
Background processor (every 5 min)
  ↓
Find HubSpot deal (by participant emails + company domain)
  ↓
If deal found:          If deal NOT found:
  ↓                       ↓
Gather RAG context      Send Slack to call owner
  ↓                       ↓
LLM analysis (8C)       Skip qualification
  ↓
Update HubSpot properties
  ↓
Create HubSpot task
  ↓
Save to Supabase
  ↓
Done
```

## Setup (One-time)

### 1. Install dependencies

```bash
cd 99-applications/sales/tools
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

Required variables:
- `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`
- `HUBSPOT_PRIVATE_APP_TOKEN` (needs deals read/write, tasks create, contacts/companies search)
- `ANTHROPIC_API_KEY` (Claude API for LLM analysis)

### 3. Create HubSpot custom properties

```bash
node create-hubspot-8c-properties.mjs
```

This creates 25 custom properties for 8C scoring:
- 8 raw score fields (0/3/5)
- 8 weighted score fields (actual points)
- Total score, percentage, qualified decision
- Evidence field (multi-line text)

Verify in HubSpot: Settings → Data Management → Properties → Deals

### 4. Apply Supabase schema

Upload `../supabase/schema-8c-qualification.sql` to your Supabase project:

```bash
# Option 1: Supabase Dashboard
# SQL Editor → New query → paste schema → Run

# Option 2: Supabase CLI (if installed)
supabase db push
```

This creates:
- `sales_8c_qualifications` table (results)
- `sales_8c_qualification_queue` table (processing queue)
- Auto-trigger for client calls
- RAG context RPC functions

### 5. Start background processor

#### Option A: Node.js Cron (Recommended)

```bash
# One-time manual processing
node process-8c-queue.mjs

# Continuous processing (every 5 minutes)
node process-8c-queue.mjs --continuous

# Or with PM2 (persistent background service)
pm2 start process-8c-queue.mjs --cron "*/5 * * * *" --name 8c-queue-processor
```

#### Option B: System Crontab

```bash
# Edit crontab
crontab -e

# Add line (adjust path):
*/5 * * * * cd /path/to/Oversecured && node 99-applications/sales/tools/process-8c-queue.mjs >> /tmp/8c-queue.log 2>&1
```

#### Option C: GitHub Actions

Create `.github/workflows/8c-queue-processor.yml`:

```yaml
name: 8C Queue Processor
on:
  schedule:
    - cron: '*/5 * * * *'
  workflow_dispatch:

jobs:
  process:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '20'
      - run: cd 99-applications/sales/tools && npm install
      - run: node 99-applications/sales/tools/process-8c-queue.mjs
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
          HUBSPOT_PRIVATE_APP_TOKEN: ${{ secrets.HUBSPOT_PRIVATE_APP_TOKEN }}
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
```

### 6. Rebuild MCP server (for manual trigger from chat)

```bash
cd 99-applications/calls/mcp-server
npm run build
```

Then restart Cursor or reload MCP servers.

## Usage

### Automatic (Default)

**No action needed!** When a call with `category='client'` is added to Supabase:
1. Auto-added to queue via database trigger
2. GitHub Actions workflow runs every 5 minutes
3. Processes up to 5 calls per run
4. For each call:
   - If **1 active deal found** → full auto-qualification → HubSpot update → task created ✅
   - If **multiple deals found** → Slack message to owners → wait for manual selection 🔄
   - If **no deal found** → Slack notification to call owner ❌

### Manual Queue Management

#### Add call to queue (SQL)

```sql
-- Add single call
INSERT INTO public.sales_8c_qualification_queue (call_id, priority)
VALUES ('36ac4441-f32b-4eb4-a0af-5468ed634f97', 1);

-- Add multiple calls
INSERT INTO public.sales_8c_qualification_queue (call_id, priority)
VALUES 
  ('call-uuid-1', 1),
  ('call-uuid-2', 2),
  ('call-uuid-3', 1);

-- Find recent external calls to queue
SELECT c.id, c.title, c.occurred_at
FROM calls c
JOIN call_participants cp ON c.id = cp.call_id
JOIN user_profiles up ON cp.user_id = up.id
WHERE EXISTS (
  SELECT 1 FROM call_participants cp2
  JOIN user_profiles up2 ON cp2.user_id = up2.id
  WHERE cp2.call_id = c.id 
  AND up2.email NOT LIKE '%@oversecured.com'
)
AND c.occurred_at > NOW() - INTERVAL '30 days'
ORDER BY c.occurred_at DESC
LIMIT 10;
```

#### Add call to queue (Node.js script)

```bash
cd 99-applications/sales/tools

# Basic usage
CALLS_AUTH_FILE=/path/to/auth.json \
node enqueue-call-8c.mjs --call-id <uuid>

# With priority
CALLS_AUTH_FILE=/path/to/auth.json \
node enqueue-call-8c.mjs --call-id <uuid> --priority 2
```

**Note:** Requires valid JWT token in `auth.json`. If JWT expired, download fresh `auth.json` from portal or use GitHub Actions (which has pre-configured secrets).

#### Trigger GitHub Actions workflow

**Option 1: Manual trigger in GitHub UI**
1. Go to: https://github.com/emoskvin-ops/Oversecured/actions
2. Select workflow: "8C Queue Processor"
3. Click "Run workflow"
4. Branch: main
5. Limit: 1 (or 5 for batch)
6. Click "Run workflow"

**Option 2: GitHub CLI**
```bash
gh workflow run 8c-queue-processor.yml --ref main -f limit=1
```

**Monitoring workflow:**
- GitHub → Actions → click on running workflow → see real-time logs
- Workflow runs in ~2-3 minutes per call

### Manual (from Cursor chat)

```
проквалифицируй звонок <call_id> по 8C для сделки <deal_id>
```

This bypasses the queue and runs immediately in your local environment.

Or using MCP tool directly:
```javascript
qualifyCall8C({
  call_id: "36ac4441-f32b-4eb4-a0af-5468ed634f97",
  deal_id: "123456789", // optional override
  skip_hubspot_sync: false // optional dry run
})
```

### CLI (for debugging)

```bash
# Qualify a specific call (auto-find deal)
node qualify-call-8c.mjs --call-id <uuid>

# Dry run (no HubSpot updates)
node qualify-call-8c.mjs --call-id <uuid> --dry-run

# Manual deal override (skip deal matching)
node qualify-call-8c.mjs --call-id <uuid> --deal-id <hubspot_deal_id>
```

## How It Works

### 1. Deal Matching

Finds HubSpot deal by:
1. Participant emails → HubSpot contacts → associated deals
2. Company domains → HubSpot companies → associated deals
3. Filters active deals only (excludes closed won/lost)

**Logic:**
- **1 active deal found** → Auto-select → proceed to qualification ✅
- **Multiple active deals found** → Send Slack message with deal picker → exit(0) → wait for manual selection 🔄
- **0 active deals found** → Send Slack notification → exit(0) → skip qualification ❌

**Multiple deals workflow:**
1. Qualification script sends Slack message to #calls channel with:
   - List of matched deals (name, ID, stage, owner)
   - Cursor prompts for each deal (ready to paste in chat)
   - @ mentions for all deal owners
2. Deal owner responds by pasting the Cursor prompt in chat
3. Agent runs `qualify-call-8c.mjs --call-id <uuid> --deal-id <hubspot_id>` manually
4. Call is **removed from queue** after Slack is sent (does not retry automatically)

**Critical for GitHub Actions:**
- When running in GitHub Actions, if multiple deals are found, the call exits successfully (exit 0) and is removed from queue
- This is intentional: we rely on human selection via Slack, not automated retries
- If you want to test the **full automated cycle** (1 deal → end-to-end), you must use a call that matches exactly 1 active deal

### 2. RAG Context Gathering

Collects up to 20 calls for analysis:
- Current call (always full transcript)
- Deal history: max 10 recent calls for same HubSpot deal
- Company history: max 5 recent calls with same company domain
- Contact history: max 5 recent calls with same participant emails

If total context > 500K tokens → applies summarization to older calls.

### 3. LLM Analysis (Conservative Scoring)

Uses Claude Sonnet 4 with strict rules:
- **If any doubt → score = 0**
- **Implicit information → score = 0**
- **Vague evidence ("probably", "maybe") → score = 0**
- **No direct quote → score = 0**

Returns structured JSON with:
- Scores for each of 8 criteria
- Evidence (direct quotes from transcripts)
- Gaps (what we still need to discover)
- Next call questions (targeted per criterion)

### 4. HubSpot Update (Additive Merge)

Updates 25 custom properties:
- 8 raw scores + 8 weighted scores
- Total score, percentage, qualification decision
- Evidence field (structured text with all quotes and gaps)

**Merge strategy:**
- If new score > existing → update score + merge evidence
- If new score = existing → keep score + append new evidence
- If new score < existing → keep existing score + append new evidence

### 5. Task Creation

Creates HubSpot task for deal owner:
- Subject: "8C Qualification: N gaps to address"
- Body: Targeted questions for each criterion with gaps
- Due date: 7 days from call date
- Priority: High if score < 64%, Medium if >= 64%

### 6. Slack Notification (if deal not found)

Sends Slack DM to call owner:
- Call details (title, date, participants)
- Company domains extracted
- Request to create deal or link manually

## Output Examples

### Successful Qualification

```
✅ 8C Auto-Qualification Complete!

📊 Results:
   Total Score: 42/140 (30%)
   Qualification: NOT_QUALIFIED
   Criteria with evidence: 3/8
   Next call questions: 12

🔗 HubSpot deal: https://app.hubspot.com/contacts/12345/deal/67890
📋 Task created: https://app.hubspot.com/contacts/12345/task/11111
```

### Deal Not Found

```
❌ No HubSpot deal found

💬 Slack notification sent to call owner:
   Recipient: oleg@oversecured.com
   Message: Create deal for participants or link manually

⏭️  Skipping qualification - deal required
```

## Monitoring

### Check queue status

```sql
-- View pending queue items
select * from sales_8c_qualification_queue where status = 'pending';

-- View failed items
select * from sales_8c_qualification_queue where status = 'failed';

-- View recent qualifications
select 
  c.title,
  q.total_score,
  q.percentage,
  q.qualified,
  q.created_at
from sales_8c_qualifications q
join calls c on c.id = q.call_id
order by q.created_at desc
limit 10;
```

### Process queue manually

```bash
# Process next pending call
node process-8c-queue.mjs

# Process up to 10 calls
node process-8c-queue.mjs --limit 10

# Process specific call from queue
node process-8c-queue.mjs --call-id <uuid>
```

## Troubleshooting

### JWT expired (local scripts)

**Error:** `JWT expired` when running local Node.js scripts (`enqueue-call-8c.mjs`, `find-single-deal-call.mjs`, etc.)

**Cause:** 
- Supabase JWT tokens expire after 1 hour
- `02-calls/_private_cache/auth.json` contains expired `access_token`

**Fix:**
1. Download fresh `auth.json` from Calls portal (Settings → Developer → Download auth.json)
2. Replace `02-calls/_private_cache/auth.json`
3. Re-run script

**Alternative (no token refresh needed):**
- Use GitHub Actions instead of local scripts
- GitHub Actions uses repository secrets (always fresh)
- Trigger via UI: GitHub → Actions → "8C Queue Processor" → Run workflow

### No deals found for legitimate client calls

**Possible causes:**
1. Participants not in HubSpot contacts yet → create contacts first
2. Contacts not associated with deals → link manually in HubSpot
3. Company domain mismatch → verify domain in HubSpot company record

**Fix:** Create deal manually, then re-run qualification with `--deal-id` override.

### LLM returns all scores = 0

**Expected behavior if:**
- Call is early discovery (no compelling event, budget, etc. discussed yet)
- Conservative scoring rules applied (no clear evidence = 0)

**This is OK!** The generated task will guide Sales on what to ask next.

### HubSpot API rate limit errors

**Mitigation:**
- Queue processor runs with 1 second delay between calls
- Max 1 qualification per second (60/minute, well below HubSpot's 100/10sec limit)

If still hitting limits → increase `8C_QUEUE_PROCESSING_INTERVAL_MINUTES`.

### Qualification takes too long (> 60 seconds)

**Possible causes:**
- Too many calls in RAG context (> 20)
- Very long transcripts (> 50K tokens)

**Fix:** Adjust limits in .env:
```
8C_RAG_MAX_DEAL_CALLS=5
8C_RAG_MAX_COMPANY_CALLS=3
8C_RAG_MAX_CONTACT_CALLS=3
```

## Testing

### Test on a single call (dry run)

```bash
node qualify-call-8c.mjs --call-id <uuid> --dry-run
```

This will:
- Run full analysis
- Print results to console
- Skip HubSpot updates and task creation

### Validate LLM accuracy

```bash
# Qualify 10 test calls
for call_id in call1 call2 call3 ...; do
  node qualify-call-8c.mjs --call-id $call_id
done

# Then manually compare LLM scores vs Sales expert scores
# Target: 80%+ agreement on qualified/not qualified decision
```

## Files

- `qualify-call-8c.mjs` - Main qualification script
- `process-8c-queue.mjs` - Background queue processor
- `create-hubspot-8c-properties.mjs` - HubSpot properties setup
- `lib/rag-context-builder.mjs` - RAG context gathering
- `lib/8c-analyzer.mjs` - LLM analysis engine
- `lib/8c-prompt-template.txt` - LLM prompt with methodology
- `lib/hubspot-deal-matcher.mjs` - Deal matching logic
- `../supabase/schema-8c-qualification.sql` - Database schema
- `../supabase/edge-functions/process-8c-queue/index.ts` - Edge function (alternative to Node cron)

## Maintenance

### Update 8C methodology

Edit `lib/8c-prompt-template.txt` with new scoring rules or examples.

### Add new 8C criteria (future)

1. Update `schema-8c-qualification.sql` (add score columns)
2. Update `create-hubspot-8c-properties.mjs` (add HubSpot properties)
3. Update `lib/8c-prompt-template.txt` (add criterion definition)
4. Update `lib/8c-analyzer.mjs` (add to validation logic)
5. Rebuild and test

### Monitor quality

Weekly spot-check:
1. Query 5 random qualifications from `sales_8c_qualifications`
2. Review evidence in HubSpot `qual8c_evidence_all` field
3. Compare with Sales expert judgment
4. If accuracy < 75% → re-tune prompt with new examples

## Security

- Never commit `.env` file (gitignored)
- Service role key has full database access (use carefully)
- HubSpot token needs minimal scopes: `crm.objects.deals.read`, `crm.objects.deals.write`, `crm.objects.contacts.read`, `crm.objects.companies.read`, `crm.objects.tasks.write`

## Support

For questions or issues, see:
- Full plan: `/Users/yegormoskvin/.cursor/plans/8c_auto-qualification_tool_a30c8012.plan.md`
- Sales playbook: `03-knowledge-base/sales-playbook/sales-methodology-stages.md`
- Calls workflow: `.cursor/rules/calls.mdc`
