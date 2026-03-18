#!/usr/bin/env node

/**
 * 8C Auto-Qualification Tool
 * 
 * Automatically qualifies client calls using 8C methodology:
 * 1. Finds HubSpot deal by participant emails/domains
 * 2. Gathers RAG context (current + historical calls)
 * 3. Analyzes with LLM (Claude Sonnet 4) using conservative scoring
 * 4. Updates HubSpot deal properties (additive merge)
 * 5. Creates HubSpot task for deal owner with next call questions
 * 6. Sends Slack notification if deal not found
 * 
 * ⚡ ZERO SETUP REQUIRED FOR MCP USERS! ⚡
 * 
 * Authentication modes:
 * - MCP/Cursor: Uses CALLS_AUTH_FILE (auth.json) - NO .env NEEDED!
 * - GitHub Actions: Uses SUPABASE_SERVICE_ROLE_KEY from secrets
 * 
 * API tokens (HubSpot, Anthropic, Composio):
 * - Automatically loaded from Supabase table `shared_api_tokens`
 * - Optional .env fallback for custom overrides only
 * - MOST USERS DON'T NEED ANY ENV VARS AT ALL!
 * 
 * Usage:
 *   node qualify-call-8c.mjs --call-id <uuid>
 *   node qualify-call-8c.mjs --call-id <uuid> --deal-id <hubspot_deal_id>  # manual override
 *   node qualify-call-8c.mjs --call-id <uuid> --dry-run  # test without HubSpot updates
 */

import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
import { buildRagContext, formatRagContextForPrompt, getExternalParticipants } from './lib/rag-context-builder.mjs';
import { analyze8C, formatEvidenceForHubSpot, calculateScoreChange } from './lib/8c-analyzer.mjs';
import { findDealForCall, getHubSpotOwner } from './lib/hubspot-deal-matcher.mjs';
import fs from 'fs';

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL 
  || process.env.SUPABASE_URL 
  || 'https://vutdygdrxruzuryuqtgg.supabase.co'; // Hardcoded fallback
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CALLS_AUTH_FILE = process.env.CALLS_AUTH_FILE;

// Initialize Supabase client (two modes: service role or user auth)
let supabase;

if (SUPABASE_SERVICE_KEY) {
  // Service role mode (GitHub Actions)
  console.log('🔑 Using service role key (server mode)');
  supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
} else if (CALLS_AUTH_FILE && fs.existsSync(CALLS_AUTH_FILE)) {
  // User auth mode (Cursor MCP)
  console.log('🔑 Using user auth from CALLS_AUTH_FILE (MCP mode)');
  
  const authData = JSON.parse(fs.readFileSync(CALLS_AUTH_FILE, 'utf-8'));
  
  if (!authData.access_token) {
    console.error('❌ Invalid auth.json: missing access_token');
    process.exit(1);
  }
  
  // Create anon client and set session manually
  supabase = createClient(
    SUPABASE_URL, 
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZ1dGR5Z2RyeHJ1enVyeXVxdGdnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjU1NTYwMjgsImV4cCI6MjA4MTEzMjAyOH0.k2Mlic9slH-j55lHOMwNSWO8XV4ZAY-SqNI9JRBml7M', // anon key (updated Jan 2026)
    {
      global: {
        headers: {
          Authorization: `Bearer ${authData.access_token}`
        }
      }
    }
  );
} else {
  console.error('❌ Missing authentication: provide either SUPABASE_SERVICE_ROLE_KEY or CALLS_AUTH_FILE');
  console.error('   For Cursor MCP: CALLS_AUTH_FILE should point to 02-calls/_private_cache/auth.json');
  console.error('   For GitHub Actions: SUPABASE_SERVICE_ROLE_KEY should be in secrets');
  process.exit(1);
}

// ============================================================================
// Load shared API tokens from Supabase (with fallback to .env)
// ============================================================================

let HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
let ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
let COMPOSIO_API_KEY = process.env.COMPOSIO_API_KEY;
let COMPOSIO_USER_ID = process.env.COMPOSIO_USER_ID;

console.log('🔍 Loading API tokens...');

// If any token is missing in .env, try to fetch from Supabase via RPC
if (!HUBSPOT_TOKEN || !ANTHROPIC_API_KEY || !COMPOSIO_API_KEY) {
  console.log('   Fetching shared tokens from Supabase...');
  
  // Use RPC with SECURITY DEFINER to bypass RLS
  const { data: tokens, error } = await supabase
    .rpc('get_shared_api_tokens', {
      p_keys: ['HUBSPOT_PRIVATE_APP_TOKEN', 'ANTHROPIC_API_KEY', 'COMPOSIO_API_KEY', 'COMPOSIO_USER_ID']
    });
  
  if (error) {
    console.error('⚠️  Failed to fetch shared tokens from Supabase:', error.message);
    console.log('   Falling back to .env only');
    console.log('   (If you see "function does not exist", ask admin to run schema-shared-tokens.sql)');
  } else if (tokens && tokens.length > 0) {
    console.log(`   ✅ Loaded ${tokens.length} shared token(s) from Supabase`);
    
    tokens.forEach(t => {
      if (t.key === 'HUBSPOT_PRIVATE_APP_TOKEN' && !HUBSPOT_TOKEN) {
        HUBSPOT_TOKEN = t.value;
        console.log('      • HUBSPOT_PRIVATE_APP_TOKEN (from Supabase)');
      }
      if (t.key === 'ANTHROPIC_API_KEY' && !ANTHROPIC_API_KEY) {
        ANTHROPIC_API_KEY = t.value;
        console.log('      • ANTHROPIC_API_KEY (from Supabase)');
      }
      if (t.key === 'COMPOSIO_API_KEY' && !COMPOSIO_API_KEY) {
        COMPOSIO_API_KEY = t.value;
        console.log('      • COMPOSIO_API_KEY (from Supabase)');
      }
      if (t.key === 'COMPOSIO_USER_ID' && !COMPOSIO_USER_ID) {
        COMPOSIO_USER_ID = t.value;
        console.log('      • COMPOSIO_USER_ID (from Supabase)');
      }
    });
  } else {
    console.log('   ⚠️  No shared tokens found in Supabase (table empty?)');
  }
}

// Validate required tokens
if (!HUBSPOT_TOKEN) {
  console.error('❌ Missing HUBSPOT_PRIVATE_APP_TOKEN (not in .env or Supabase)');
  process.exit(1);
}

if (!ANTHROPIC_API_KEY) {
  console.error('❌ Missing ANTHROPIC_API_KEY (not in .env or Supabase)');
  process.exit(1);
}

console.log('✅ All required API tokens loaded\n');

// Cache for pipeline stages (ID -> name)
let stageNamesCache = null;

/**
 * Get stage name by stage ID (with caching)
 */
async function getStageName(stageId) {
  if (!stageNamesCache) {
    // Load all pipelines and build cache
    const pipelines = await hubspotFetch('/crm/v3/pipelines/deals');
    stageNamesCache = {};
    
    for (const pipeline of pipelines.results) {
      for (const stage of pipeline.stages) {
        stageNamesCache[stage.id] = stage.label;
      }
    }
  }
  
  return stageNamesCache[stageId] || stageId;
}

/**
 * Execute Composio action via direct API call (v3)
 * 
 * @param {string} action - Composio action/tool name (e.g., 'SLACK_FIND_USERS')
 * @param {object} params - Action parameters
 * @returns {Promise<object>} Action result data
 */
async function composioExecute(action, params) {
  if (!COMPOSIO_API_KEY) {
    throw new Error('Missing COMPOSIO_API_KEY (check Supabase shared_api_tokens or .env)');
  }

  // v3 API: /api/v3/tools/execute/{tool_slug}
  const response = await fetch(`https://backend.composio.dev/api/v3/tools/execute/${action}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': COMPOSIO_API_KEY,
    },
    body: JSON.stringify({
      arguments: params,
      user_id: COMPOSIO_USER_ID || 'pg-test-2d03a61c-2a98-4aaa-b569-f224b5b64b6b',
    }),
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Composio API error (${response.status}): ${error}`);
  }

  const result = await response.json();
  return result.data;
}

// Parse CLI args safely
const args = process.argv.slice(2);

const callIdIdx = args.indexOf('--call-id');
const callId = callIdIdx !== -1 ? args[callIdIdx + 1] : null;

const dealIdIdx = args.indexOf('--deal-id');
const manualDealId = dealIdIdx !== -1 ? args[dealIdIdx + 1] : null;

const dryRun = args.includes('--dry-run');
const debugMode = args.includes('--debug');

if (!callId) {
  console.error('Usage: node qualify-call-8c.mjs --call-id <uuid> [--deal-id <id>] [--dry-run] [--debug]');
  process.exit(1);
}

/**
 * HubSpot API helper
 */
async function hubspotFetch(path, options = {}) {
  const url = `https://api.hubapi.com${path}`;
  const response = await fetch(url, {
    ...options,
    headers: {
      'Authorization': `Bearer ${HUBSPOT_TOKEN}`,
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`HubSpot API error (${response.status}): ${error}`);
  }

  return response.json();
}

/**
 * Extract existing 8C qualification from HubSpot deal properties
 * 
 * @param {object} properties - HubSpot deal properties
 * @returns {object} Existing 8C data formatted for LLM
 */
function extractExisting8C(properties) {
  const existing = {
    total_score: parseInt(properties.qual8c_total_score, 10) || 0,
    percentage: parseFloat(properties.qual8c_percentage) || 0,
    qualified: properties.qual8c_qualified === 'qualified',
    criteria: [],
  };

  // Map HubSpot property names to criterion IDs
  const scoreMapping = [
    { id: 1, scoreKey: 'qual8c_score_compelling_event', evidenceKey: 'x_8c_compelling_event' },
    { id: 2, scoreKey: 'qual8c_score_stakeholder', evidenceKey: 'x_8c_customer_stakeholder_strategy' },
    { id: 3, scoreKey: 'qual8c_score_funding', evidenceKey: 'x_8c_funding' },
    { id: 4, scoreKey: 'qual8c_score_challenges', evidenceKey: 'x_8s_customer_challenges' }, // Note: typo in HubSpot (8s)
    { id: 5, scoreKey: 'qual8c_score_value_drivers', evidenceKey: 'x_8c_business_value_drivers' },
    { id: 6, scoreKey: 'qual8c_score_solution', evidenceKey: 'x_8c_solution_differentiators' },
    { id: 7, scoreKey: 'qual8c_score_competitors', evidenceKey: 'x_8c_competitors' },
    { id: 8, scoreKey: 'qual8c_score_partners', evidenceKey: 'x_8c_ecosystem' },
  ];

  // Extract scores and evidence for each criterion
  for (const mapping of scoreMapping) {
    const score = parseInt(properties[mapping.scoreKey], 10) || 0;
    const evidenceText = properties[mapping.evidenceKey] || '';
    const evidence = evidenceText 
      ? evidenceText.split('\n').filter(e => e.trim() && !e.startsWith('Score:') && !e.startsWith('Confidence:') && !e.startsWith('Evidence:') && !e.startsWith('Gaps:'))
      : [];

    existing.criteria.push({
      id: mapping.id,
      score,
      evidence,
    });
  }

  return existing;
}

/**
 * Get full HubSpot deal data including 8C properties
 * 
 * @param {string} dealId - HubSpot deal ID
 * @returns {Promise<object>} Full deal object with all properties
 */
async function getHubSpotDealWithProperties(dealId) {
  console.log(`🔍 Fetching HubSpot deal properties...`);

  const deal = await hubspotFetch(`/crm/v3/objects/deals/${dealId}?properties=dealname,dealstage,hubspot_owner_id,qual8c_score_compelling_event,qual8c_score_stakeholder,qual8c_score_funding,qual8c_score_challenges,qual8c_score_value_drivers,qual8c_score_solution,qual8c_score_competitors,qual8c_score_partners,x_8c_compelling_event,x_8c_customer_stakeholder_strategy,x_8c_funding,x_8s_customer_challenges,x_8c_business_value_drivers,x_8c_solution_differentiators,x_8c_competitors,x_8c_ecosystem,qual8c_percentage,qual8c_qualified,qual8c_total_score`);

  console.log(`   ✅ Deal: ${deal.properties.dealname}`);
  
  // Check if 8C already populated
  const hasExisting8C = deal.properties.qual8c_percentage !== null && deal.properties.qual8c_percentage !== undefined;
  if (hasExisting8C) {
    console.log(`   📊 Existing 8C score: ${deal.properties.qual8c_percentage}%`);
  } else {
    console.log(`   📊 No existing 8C qualification`);
  }

  return deal;
}

/**
 * Format evidence for a single criterion (for HubSpot individual evidence fields)
 * 
 * Uses LLM-synthesized consolidated evidence + direct quotes from current call
 * 
 * @param {object} criterion - Analysis criterion with consolidated_evidence + evidence from LLM
 * @param {string} existingText - Existing evidence text from HubSpot (fallback if LLM didn't synthesize)
 * @returns {string|null} Consolidated evidence text or null if no evidence
 */
function formatCriterionEvidence(criterion, existingText = '') {
  // Prefer LLM-synthesized consolidated evidence
  if (criterion.consolidated_evidence && criterion.consolidated_evidence.trim()) {
    // Add metadata header for tracking
    let text = `Score: ${criterion.score}/5 (Weighted: ${criterion.weighted_score})\n`;
    text += `Confidence: ${criterion.confidence}\n\n`;
    text += `${criterion.consolidated_evidence}\n`;
    
    // Add direct quotes from CURRENT call (for reference)
    if (criterion.evidence && criterion.evidence.length > 0) {
      text += `\nQuotes from current call:\n`;
      criterion.evidence.forEach(quote => {
        text += `- "${quote}"\n`;
      });
    }
    
    if (criterion.gaps && criterion.gaps.length > 0) {
      text += `\nGaps to address on next call:\n`;
      criterion.gaps.forEach(gap => {
        text += `- ${gap}\n`;
      });
    }
    
    return text;
  }
  
  // Fallback: if LLM didn't provide consolidated evidence, keep existing
  // (shouldn't happen if prompt is followed correctly)
  if (existingText && existingText.trim()) {
    console.warn(`   ⚠️  Criterion ${criterion.id}: No consolidated_evidence from LLM, keeping existing`);
    return existingText;
  }
  
  // No evidence at all - return null (HubSpot will clear the field)
  return null;
}

/**
 * Update HubSpot deal properties with 8C scores
 * 
 * @param {string} dealId - HubSpot deal ID
 * @param {object} analysis - 8C analysis result (with consolidated_evidence per criterion)
 * @param {object} existingProperties - Existing HubSpot deal properties (fallback if LLM synthesis fails)
 */
async function updateHubSpotDeal(dealId, analysis, existingProperties = {}) {
  console.log(`📤 Updating HubSpot deal ${dealId}...`);

  if (dryRun) {
    console.log('   [DRY RUN] Would update properties (skipped)');
    return;
  }

  // Map for existing evidence texts (fallback if LLM didn't synthesize)
  const existingEvidence = [
    existingProperties.x_8c_compelling_event || '',
    existingProperties.x_8c_customer_stakeholder_strategy || '',
    existingProperties.x_8c_funding || '',
    existingProperties.x_8s_customer_challenges || '', // Note: typo in HubSpot (8s)
    existingProperties.x_8c_business_value_drivers || '',
    existingProperties.x_8c_solution_differentiators || '',
    existingProperties.x_8c_competitors || '',
    existingProperties.x_8c_ecosystem || '',
  ];

  // ADDITIVE MERGE: Never downgrade existing scores (safety net for LLM errors)
  const existingScores = [
    parseInt(existingProperties.qual8c_score_compelling_event || '0', 10),
    parseInt(existingProperties.qual8c_score_stakeholder || '0', 10),
    parseInt(existingProperties.qual8c_score_funding || '0', 10),
    parseInt(existingProperties.qual8c_score_challenges || '0', 10),
    parseInt(existingProperties.qual8c_score_value_drivers || '0', 10),
    parseInt(existingProperties.qual8c_score_solution || '0', 10),
    parseInt(existingProperties.qual8c_score_competitors || '0', 10),
    parseInt(existingProperties.qual8c_score_partners || '0', 10),
  ];

  // Log score protection (if LLM tried to downgrade)
  const criteriaNames = ['Compelling Event', 'Stakeholder', 'Funding', 'Challenges', 'Value Drivers', 'Solution', 'Competitors', 'Partners'];
  const downgrades = [];
  analysis.criteria.forEach((criterion, i) => {
    if (criterion.score < existingScores[i]) {
      downgrades.push(`${criteriaNames[i]}: ${existingScores[i]} → ${criterion.score} (PROTECTED)`);
    }
  });
  if (downgrades.length > 0) {
    console.log(`   🛡️  Protected ${downgrades.length} criteria from downgrade:`);
    downgrades.forEach(d => console.log(`      - ${d}`));
  }

  const existingWeightedScores = [
    parseInt(existingProperties.qual8c_weighted_compelling_event || '0', 10),
    parseInt(existingProperties.qual8c_weighted_stakeholder || '0', 10),
    parseInt(existingProperties.qual8c_weighted_funding || '0', 10),
    parseInt(existingProperties.qual8c_weighted_challenges || '0', 10),
    parseInt(existingProperties.qual8c_weighted_value_drivers || '0', 10),
    parseInt(existingProperties.qual8c_weighted_solution || '0', 10),
    parseInt(existingProperties.qual8c_weighted_competitors || '0', 10),
    parseInt(existingProperties.qual8c_weighted_partners || '0', 10),
  ];

  // Precompute merged scores first (do NOT reference `properties` inside its own initializer).
  // This avoids a Temporal Dead Zone bug in Node.js ("Cannot access 'properties' before initialization").
  const mergedRawScores = analysis.criteria.map((criterion, i) =>
    // CRITICAL: Use MAX(existing, new) to prevent downgrades
    Math.max(existingScores[i], criterion.score)
  );

  // Recalculate weighted scores from merged raw scores (also additive-only).
  // We still protect against any external inconsistency by keeping MAX(existingWeighted, recomputed).
  const mergedWeightedScores = [
    Math.max(existingWeightedScores[0], mergedRawScores[0] * 5),
    Math.max(existingWeightedScores[1], mergedRawScores[1] * 3),
    Math.max(existingWeightedScores[2], mergedRawScores[2] * 3),
    Math.max(existingWeightedScores[3], mergedRawScores[3] * 5),
    Math.max(existingWeightedScores[4], mergedRawScores[4] * 3),
    Math.max(existingWeightedScores[5], mergedRawScores[5] * 3),
    Math.max(existingWeightedScores[6], mergedRawScores[6] * 5),
    Math.max(existingWeightedScores[7], mergedRawScores[7] * 1),
  ];

  const mergedTotalScore = mergedWeightedScores.reduce((sum, score) => sum + score, 0);
  const mergedPercentage = Math.round((mergedTotalScore / 140) * 100);
  const mergedQualified = mergedPercentage >= 64 ? 'qualified' : 'not_qualified';

  // Build properties update payload
  const properties = {
    // Raw scores (using actual HubSpot property names: qual8c_score_*)
    'qual8c_score_compelling_event': mergedRawScores[0],
    'qual8c_score_stakeholder': mergedRawScores[1],
    'qual8c_score_funding': mergedRawScores[2],
    'qual8c_score_challenges': mergedRawScores[3],
    'qual8c_score_value_drivers': mergedRawScores[4],
    'qual8c_score_solution': mergedRawScores[5],
    'qual8c_score_competitors': mergedRawScores[6],
    'qual8c_score_partners': mergedRawScores[7],

    // Weighted scores (recalculate from merged raw scores)
    'qual8c_weighted_compelling_event': mergedWeightedScores[0],
    'qual8c_weighted_stakeholder': mergedWeightedScores[1],
    'qual8c_weighted_funding': mergedWeightedScores[2],
    'qual8c_weighted_challenges': mergedWeightedScores[3],
    'qual8c_weighted_value_drivers': mergedWeightedScores[4],
    'qual8c_weighted_solution': mergedWeightedScores[5],
    'qual8c_weighted_competitors': mergedWeightedScores[6],
    'qual8c_weighted_partners': mergedWeightedScores[7],

    // Evidence per criterion (LLM-synthesized, not appended)
    'x_8c_compelling_event': formatCriterionEvidence(analysis.criteria[0], existingEvidence[0]),
    'x_8c_customer_stakeholder_strategy': formatCriterionEvidence(analysis.criteria[1], existingEvidence[1]),
    'x_8c_funding': formatCriterionEvidence(analysis.criteria[2], existingEvidence[2]),
    'x_8s_customer_challenges': formatCriterionEvidence(analysis.criteria[3], existingEvidence[3]), // Note: typo in HubSpot (8s)
    'x_8c_business_value_drivers': formatCriterionEvidence(analysis.criteria[4], existingEvidence[4]),
    'x_8c_solution_differentiators': formatCriterionEvidence(analysis.criteria[5], existingEvidence[5]),
    'x_8c_competitors': formatCriterionEvidence(analysis.criteria[6], existingEvidence[6]),
    'x_8c_ecosystem': formatCriterionEvidence(analysis.criteria[7], existingEvidence[7]),

    // Totals (recalculate from merged weighted scores)
    'qual8c_total_score': mergedTotalScore,
    'qual8c_percentage': mergedPercentage,
    'qual8c_qualified': mergedQualified,

    // Evidence (formatted as structured text)
    'qual8c_evidence_all': formatEvidenceForHubSpot(analysis),
  };

  await hubspotFetch(`/crm/v3/objects/deals/${dealId}`, {
    method: 'PATCH',
    body: JSON.stringify({ properties }),
  });

  console.log('   ✅ HubSpot deal updated');
}

/**
 * Create HubSpot task for deal owner
 */
async function createHubSpotTask(dealId, dealName, ownerId, ownerEmail, analysis, callTitle, callDate) {
  console.log(`📋 Creating HubSpot task for ${ownerEmail}...`);

  if (dryRun) {
    console.log('   [DRY RUN] Would create task (skipped)');
    return null;
  }

  // Build task body with ALL gaps from criteria (HTML formatting - HubSpot strips plain \n)
  const criteriasWithGaps = analysis.criteria.filter(c => c.gaps && c.gaps.length > 0);
  const totalGapsCount = criteriasWithGaps.reduce((sum, c) => sum + c.gaps.length, 0);
  
  let taskBody = `<strong>Based on call "${callTitle}" (${new Date(callDate).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}), please discover on next call:</strong><br><br>`;

  // Add ALL gaps grouped by criterion with single Why per criterion
  criteriasWithGaps.forEach((criterion, idx) => {
    if (idx > 0) taskBody += `<br>`;
    
    taskBody += `<strong>${criterion.name.toUpperCase()}</strong><br>`;
    taskBody += `<em>Why: Need to improve score from ${criterion.score}/5 to qualify deal</em><br><br>`;
    
    criterion.gaps.forEach((gap) => {
      taskBody += `&nbsp;&nbsp;&nbsp;• ${gap}<br>`;
    });
  });

  taskBody += `${'─'.repeat(60)}<br><br>`;
  taskBody += `<strong>Current 8C Score:</strong> ${analysis.total_score}/140 (${analysis.percentage}%)<br>`;
  if (analysis.qualification === 'NOT_QUALIFIED') {
    taskBody += `<strong>Status:</strong> BELOW THRESHOLD (need 90 points / 64% to qualify as SQL)<br>`;
    taskBody += `<strong>Gap:</strong> ${90 - analysis.total_score} points needed<br>`;
  } else {
    taskBody += `<strong>Status:</strong> QUALIFIED as SQL<br>`;
  }

  // Create task
  const taskProperties = {
    hs_task_subject: `8C Qualification: ${totalGapsCount} question${totalGapsCount !== 1 ? 's' : ''} for next call`,
    hs_task_body: taskBody,
    hs_task_status: 'NOT_STARTED',
    hs_task_priority: analysis.percentage < 64 ? 'HIGH' : 'MEDIUM',
    hs_timestamp: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).getTime(), // 7 days from now
  };

  // Assign to deal owner if available
  if (ownerId) {
    taskProperties.hubspot_owner_id = ownerId;
  }

  const task = await hubspotFetch('/crm/v3/objects/tasks', {
    method: 'POST',
    body: JSON.stringify({
      properties: taskProperties,
    }),
  });

  // Associate task with deal
  await hubspotFetch(`/crm/v3/objects/tasks/${task.id}/associations/deals/${dealId}/task_to_deal`, {
    method: 'PUT',
  });

  const taskUrl = `https://app.hubspot.com/contacts/${process.env.HUBSPOT_PORTAL_ID}/task/${task.id}`;

  console.log(`   ✅ Task created: ${task.id}`);
  console.log(`   🔗 ${taskUrl}`);

  return {
    id: task.id,
    url: taskUrl,
  };
}

/**
 * Send Slack notification if deal not found
 */
async function sendSlackNotification(callId, callTitle, callDate, participantEmails, companyDomains) {
  console.log(`💬 Sending Slack notification for missing deal...`);

  // Get call owner from Supabase
  const { data: call } = await supabase
    .from('calls')
    .select('owner_email')
    .eq('id', callId)
    .single();

  const ownerEmail = call?.owner_email;

  if (!ownerEmail) {
    console.warn('   ⚠️  Call owner email not found, skipping Slack notification');
    return;
  }

  // Get all Slack users and find owner by email
  let slackUserId = null;
  try {
    const listUsersResult = await composioExecute('SLACK_LIST_USERS', {});
    const allSlackUsers = listUsersResult?.members || [];
    
    const slackUser = allSlackUsers.find(u => 
      u.profile?.email?.toLowerCase() === ownerEmail.toLowerCase() && 
      !u.deleted && 
      !u.is_bot
    );

    if (slackUser) {
      slackUserId = slackUser.id;
      console.log(`   ✓ Found Slack user for ${ownerEmail}: ${slackUserId}`);
    } else {
      console.warn(`   ⚠️  Slack user not found for ${ownerEmail}`);
    }
  } catch (error) {
    console.error(`   ❌ Failed to list Slack users: ${error.message}`);
  }

  // Build message with owner tag
  const ownerTag = slackUserId ? `<@${slackUserId}>\n\n` : `@${ownerEmail}\n\n`;
  
  const message = `${ownerTag}У тебя прошел звонок с клиентом, но я не нашел сделку в HubSpot:

- **Звонок:** ${callTitle}
- **Дата:** ${callDate}
- **Участники:** ${participantEmails.join(', ')}
- **Домены компаний:** ${companyDomains.join(', ')}

Пожалуйста, создай deal в HubSpot или привяжи звонок к существующей сделке вручную.

**Call ID:** \`${callId}\``;

  if (dryRun) {
    console.log('   [DRY RUN] Would send Slack message to #calls:');
    console.log(message);
    return;
  }

  // Send to #calls channel (use channel ID for reliability)
  try {
    const result = await composioExecute('SLACK_SEND_MESSAGE', {
      channel: 'C0AA1NBG84V', // #calls channel ID
      markdown_text: message
    });

    console.log(`   ✅ Slack message sent to #calls (tagged ${ownerEmail})`);
    
    if (debugMode && result) {
      console.log(`   📊 Slack response:`, JSON.stringify(result, null, 2));
    }
  } catch (error) {
    console.error(`   ❌ Failed to send Slack message: ${error.message}`);
    if (debugMode) {
      console.error('   Stack trace:', error.stack);
    }
  }
}

/**
 * Send Slack message to #calls channel, tagging all deal owners
 * 
 * @param {string} callId - Call UUID
 * @param {string} callTitle - Call title
 * @param {string} callDate - Call occurred_at timestamp
 * @param {array} matchedDeals - All matched deals with owner info
 */
async function sendSlackDealSelection(callId, callTitle, callDate, matchedDeals) {
  console.log(`💬 Sending Slack deal selection to #calls channel...`);

  // Get unique owners from matched deals
  const ownerIds = [...new Set(matchedDeals.map(d => d.owner_id).filter(Boolean))];
  
  if (ownerIds.length === 0) {
    console.warn('   ⚠️  No owners found for matched deals');
    return;
  }

  // Get all Slack users once (more reliable than SLACK_FIND_USERS)
  let allSlackUsers = [];
  try {
    const listUsersResult = await composioExecute('SLACK_LIST_USERS', {});
    allSlackUsers = listUsersResult?.members || [];
    console.log(`   📋 Retrieved ${allSlackUsers.length} Slack users`);
  } catch (error) {
    console.error(`   ❌ Failed to list Slack users: ${error.message}`);
  }

  // Resolve all owners (HubSpot email + Slack user ID)
  const owners = [];
  for (const ownerId of ownerIds) {
    const owner = await getHubSpotOwner(ownerId);
    if (!owner?.email) {
      console.warn(`   ⚠️  Could not resolve owner ${ownerId}`);
      continue;
    }

    // Find Slack user by email (manual search in all users)
    const slackUser = allSlackUsers.find(u => 
      u.profile?.email?.toLowerCase() === owner.email.toLowerCase() && 
      !u.deleted && 
      !u.is_bot
    );

    if (slackUser) {
      owners.push({ hubspotId: ownerId, email: owner.email, slackId: slackUser.id });
      console.log(`   ✓ Found Slack user for ${owner.email}: ${slackUser.id}`);
    } else {
      console.warn(`   ⚠️  Slack user not found for ${owner.email}`);
      owners.push({ hubspotId: ownerId, email: owner.email, slackId: null });
    }
  }

  if (owners.length === 0) {
    console.warn('   ⚠️  No owners resolved, skipping Slack notification');
    return;
  }

  // Build message with owner tags
  const ownerTags = owners
    .filter(o => o.slackId)
    .map(o => `<@${o.slackId}>`)
    .join(' ');

  const ownersNotFoundInSlack = owners
    .filter(o => !o.slackId)
    .map(o => o.email);

  const dealsList = await Promise.all(
    matchedDeals.map(async (d, i) => {
      const owner = owners.find(o => o.hubspotId === d.owner_id);
      const ownerDisplay = owner ? owner.email : d.owner_id || 'unknown';
      const stageName = await getStageName(d.stage);
      
      return `${i + 1}. **${d.name}** (${d.id})
   - Stage: ${stageName}
   - Owner: ${ownerDisplay}
   
   **Cursor prompt:**
   \`\`\`
   Проквалифицируй звонок ${callId} по 8C для сделки ${d.id}
   \`\`\``;
    })
  ).then(items => items.join('\n\n'));

  let message = `${ownerTags ? ownerTags + '\n\n' : ''}После звонка с клиентом я нашел **${matchedDeals.length} активных сделок** в HubSpot:

- **Звонок:** ${callTitle}
- **Дата:** ${callDate}

**Найденные сделки:**

${dealsList}

**Пожалуйста, выбери правильную сделку** и вставь соответствующий Cursor prompt в чат для автоматической квалификации.`;

  if (ownersNotFoundInSlack.length > 0) {
    message += `\n\n_Note: Could not tag owners not found in Slack: ${ownersNotFoundInSlack.join(', ')}_`;
  }

  if (dryRun) {
    console.log('   [DRY RUN] Would send Slack message to #calls:');
    console.log(message);
    console.log(`   👥 Owners: ${owners.map(o => `${o.email} (${o.slackId || 'not found'})`).join(', ')}`);
    return;
  }

  // Send to #calls channel (use channel ID for reliability)
  try {
    const result = await composioExecute('SLACK_SEND_MESSAGE', {
      channel: 'C0AA1NBG84V', // #calls channel ID
      markdown_text: message
    });

    // Log full response for debugging
    console.log(`   ✅ Slack message sent to #calls`);
    console.log(`   👥 Tagged ${owners.filter(o => o.slackId).length}/${owners.length} owners`);
    console.log(`   📊 Response:`, JSON.stringify(result, null, 2));
  } catch (error) {
    console.error(`   ❌ Failed to send Slack message: ${error.message}`);
    throw error; // Re-throw to see full error context
  }
}

/**
 * Save qualification result to Supabase
 */
async function saveQualificationToSupabase(callId, dealId, analysis, ragContext, taskInfo, userId) {
  console.log(`💾 Saving qualification to Supabase...`);

  const { error } = await supabase
    .from('sales_8c_qualifications')
    .upsert({
      call_id: callId,
      hubspot_deal_id: dealId,
      
      // Raw scores
      score_compelling_event: analysis.criteria[0].score,
      score_stakeholder: analysis.criteria[1].score,
      score_funding: analysis.criteria[2].score,
      score_challenges: analysis.criteria[3].score,
      score_value_drivers: analysis.criteria[4].score,
      score_solution: analysis.criteria[5].score,
      score_competitors: analysis.criteria[6].score,
      score_partners: analysis.criteria[7].score,
      
      // Weighted scores
      weighted_compelling_event: analysis.criteria[0].weighted_score,
      weighted_stakeholder: analysis.criteria[1].weighted_score,
      weighted_funding: analysis.criteria[2].weighted_score,
      weighted_challenges: analysis.criteria[3].weighted_score,
      weighted_value_drivers: analysis.criteria[4].weighted_score,
      weighted_solution: analysis.criteria[5].weighted_score,
      weighted_competitors: analysis.criteria[6].weighted_score,
      weighted_partners: analysis.criteria[7].weighted_score,
      
      // Totals
      total_score: analysis.total_score,
      percentage: analysis.percentage,
      qualified: analysis.qualification === 'QUALIFIED',
      
      // Full analysis JSON
      analysis_json: analysis,
      
      // RAG context metadata
      rag_context_json: {
        total_calls: ragContext.total_calls,
        participant_emails: ragContext.participant_emails,
        company_domains: ragContext.company_domains,
      },
      
      // HubSpot task info
      hubspot_task_id: taskInfo?.id,
      hubspot_task_url: taskInfo?.url,
      hubspot_synced_at: new Date().toISOString(),
      
      // Metadata
      created_by: userId,
    }, {
      onConflict: 'call_id,hubspot_deal_id',
    });

  if (error) {
    throw new Error(`Failed to save qualification: ${error.message}`);
  }

  console.log('   ✅ Saved to Supabase');
}

/**
 * Main qualification workflow
 */
async function main() {
  console.log('🚀 Starting 8C Auto-Qualification...\n');
  console.log(`📞 Call ID: ${callId}`);
  if (manualDealId) {
    console.log(`🎯 Manual deal override: ${manualDealId}`);
  }
  if (dryRun) {
    console.log('⚠️  DRY RUN MODE - no HubSpot updates\n');
  }
  console.log('');

  // Step 1: Get external participants (pass supabase client)
  const participantEmails = await getExternalParticipants(supabase, callId);
  
  if (participantEmails.length === 0) {
    console.log('⏭️  No external participants found - skipping (internal call)');
    process.exit(0);
  }

  console.log(`👥 External participants: ${participantEmails.join(', ')}\n`);

  // Step 2: Find HubSpot deal
  let dealId = manualDealId;
  let deal = null;
  let allMatchedDeals = [];

  if (!dealId) {
    const matchResult = await findDealForCall(participantEmails);
    
    // Get call details for potential Slack notifications
    const { data: callData } = await supabase
      .from('calls')
      .select('title, occurred_at')
      .eq('id', callId)
      .single();
    
    if (!matchResult || !matchResult.deal) {
      if (matchResult?.needs_manual_selection) {
        // Multiple deals found - send selection request
        console.log('⚠️  Multiple active deals found - manual selection required\n');
        
        await sendSlackDealSelection(
          callId,
          callData?.title || 'Unknown call',
          callData?.occurred_at || new Date().toISOString(),
          matchResult.all_matched_deals
        );
        
        console.log('⏭️  Skipping qualification - waiting for manual deal selection\n');
        process.exit(0);
      } else {
        // No deals found - send missing deal notification
        console.log('❌ No HubSpot deal found\n');
        
        const companyDomains = [...new Set(participantEmails.map(e => e.split('@')[1]))];
        
        await sendSlackNotification(
          callId,
          callData?.title || 'Unknown call',
          callData?.occurred_at || new Date().toISOString(),
          participantEmails,
          companyDomains
        );
        
        console.log('⏭️  Skipping qualification - deal required\n');
        process.exit(0);
      }
    }

    deal = matchResult.deal;
    dealId = deal.id;
    allMatchedDeals = matchResult.all_matched_deals;
  }

  console.log('');

  // Step 2.5: Get full deal data with 8C properties
  const fullDeal = await getHubSpotDealWithProperties(dealId);
  
  console.log('');

  // Step 3: Get call metadata (needed for evidence appending and task creation)
  const { data: callData } = await supabase
    .from('calls')
    .select('title, occurred_at')
    .eq('id', callId)
    .single();
  
  const callTitle = callData?.title || 'Unknown call';
  const callDate = callData?.occurred_at || new Date().toISOString();

  // Step 4: Build RAG context (pass supabase client)
  const ragContext = await buildRagContext(supabase, callId, dealId);
  const formattedContext = formatRagContextForPrompt(ragContext);
  
  if (debugMode) {
    console.log('\n=== DEBUG: RAG CONTEXT ===');
    console.log(formattedContext.substring(0, 2000) + '\n...(truncated)');
  }
  
  console.log('');

  // Step 5: LLM Analysis (with existing 8C context)
  const { analysis, raw_response } = await analyze8C(formattedContext, {
    existingQualification: extractExisting8C(fullDeal.properties),
    apiKey: ANTHROPIC_API_KEY
  });
  
  if (debugMode) {
    console.log('\n=== DEBUG: LLM RAW RESPONSE ===');
    console.log(raw_response);
    console.log('\n=== DEBUG: PARSED ANALYSIS (first criterion) ===');
    console.log(JSON.stringify(analysis.criteria[0], null, 2));
  }
  
  console.log('');

  // Step 6: Update HubSpot deal properties (LLM synthesizes consolidated evidence)
  await updateHubSpotDeal(dealId, analysis, fullDeal.properties);
  
  console.log('');

  // Step 7: Create HubSpot task for deal owner
  let taskInfo = null;
  if (analysis.next_call_questions && analysis.next_call_questions.length > 0) {
    const ownerId = fullDeal.properties.hubspot_owner_id;
    const owner = await getHubSpotOwner(ownerId);
    const ownerEmail = owner?.email || 'unknown';
    
    taskInfo = await createHubSpotTask(
      dealId,
      fullDeal.properties.dealname || 'Unknown deal',
      ownerId,
      ownerEmail,
      analysis,
      callTitle,
      callDate
    );
  }
  
  console.log('');

  // Step 7: Save to Supabase
  // Get current user ID (for created_by)
  // In production, this would come from the authenticated context
  // For now, use service account or lookup from call owner
  const { data: userData } = await supabase
    .from('user_profiles')
    .select('user_id')
    .eq('email', process.env.USER_EMAIL || 'service@oversecured.com')
    .single();

  await saveQualificationToSupabase(
    callId,
    dealId,
    analysis,
    ragContext,
    taskInfo,
    userData?.user_id || '00000000-0000-0000-0000-000000000000'
  );

  // Print summary
  console.log('');
  console.log('✅ 8C Auto-Qualification Complete!\n');
  console.log('📊 Results:');
  console.log(`   Total Score: ${analysis.total_score}/140 (${analysis.percentage}%)`);
  console.log(`   Qualification: ${analysis.qualification}`);
  console.log(`   Criteria with evidence: ${analysis.criteria.filter(c => c.evidence.length > 0).length}/8`);
  const totalGaps = analysis.criteria.reduce((sum, c) => sum + (c.gaps || []).length, 0);
  console.log(`   Next call questions: ${totalGaps} (from ${analysis.criteria.filter(c => c.gaps && c.gaps.length > 0).length} criteria)`);
  if (taskInfo) {
    console.log(`   HubSpot task: ${taskInfo.url}`);
  }
  console.log('');
  console.log(`🔗 HubSpot deal: https://app.hubspot.com/contacts/${process.env.HUBSPOT_PORTAL_ID}/deal/${dealId}`);
  
  if (allMatchedDeals.length > 1) {
    console.log('');
    console.log(`ℹ️  Note: ${allMatchedDeals.length} deals matched, selected most recent active`);
  }
}

main().catch(error => {
  console.error('\n❌ Fatal error:', error.message);
  console.error(error.stack);
  process.exit(1);
});
