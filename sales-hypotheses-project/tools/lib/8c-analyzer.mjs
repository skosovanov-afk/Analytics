/**
 * 8C LLM Analyzer
 * 
 * Analyzes call transcripts using Claude Sonnet 4 with RAG context
 * to score deals on 8C qualification methodology.
 * 
 * Features:
 * - Conservative scoring (better to under-score than over-score)
 * - Evidence-based (no evidence = score 0)
 * - Chain-of-Thought analysis
 * - Structured JSON output with validation
 */

import Anthropic from '@anthropic-ai/sdk';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load 8C methodology prompt template
const PROMPT_TEMPLATE = readFileSync(
  join(__dirname, '8c-prompt-template.txt'),
  'utf-8'
);

/**
 * Analyze call transcripts using 8C methodology
 * 
 * @param {string} ragContext - Formatted RAG context (current + historical calls)
 * @param {object} options - Analysis options
 * @returns {Promise<object>} 8C analysis results with scores and evidence
 */
export async function analyze8C(ragContext, options = {}) {
  const {
    model = 'claude-sonnet-4-20250514',
    temperature = 0.1, // low temperature for consistent, conservative scoring
    maxTokens = 16000,
    existingQualification = null,
    apiKey = process.env.ANTHROPIC_API_KEY, // Accept API key or fallback to env
  } = options;

  if (!apiKey) {
    throw new Error('Missing ANTHROPIC_API_KEY (pass via options.apiKey or set in environment)');
  }

  // Initialize Anthropic client with provided key
  const anthropic = new Anthropic({ apiKey });

  console.log('🤖 Running 8C analysis with Claude Sonnet 4...');
  console.log(`   Model: ${model}`);
  console.log(`   Temperature: ${temperature} (conservative)`);
  
  if (existingQualification && existingQualification.percentage > 0) {
    console.log(`   📊 Existing qualification: ${existingQualification.percentage}% (${existingQualification.total_score}/140)`);
  }

  // Build existing qualification context if available
  let existingContext = '';
  if (existingQualification && existingQualification.percentage > 0) {
    existingContext = `
================================================================================
EXISTING 8C QUALIFICATION IN HUBSPOT
================================================================================

This deal already has a partial or complete 8C qualification from previous calls.

Current Score: ${existingQualification.total_score}/140 (${existingQualification.percentage}%)
Status: ${existingQualification.qualified ? 'QUALIFIED' : 'NOT QUALIFIED'}

Existing Evidence by Criterion:
${existingQualification.criteria.map(c => {
  const criterionNames = ['Compelling Event', 'Customer Stakeholder Strategy', 'Funding', 
    'Customer Challenges', 'Business Value Drivers', 'Solution & Differentiators', 
    'Competitors', 'Partners & Ecosystem'];
  return `
${c.id}. ${criterionNames[c.id - 1]}: ${c.score} points
Evidence:
${c.evidence.length > 0 ? c.evidence.map(e => `- ${e}`).join('\n') : '- None found yet'}
`;
}).join('\n')}

YOUR TASK: Review the existing qualification and UPDATE it based on new information from the current call.
- If existing evidence is strong and new call confirms it → KEEP the score
- If new call provides BETTER evidence → INCREASE the score
- If new call contradicts existing evidence → REASSESS and explain
- If no new information for a criterion → KEEP existing score and evidence

DO NOT LOWER SCORES unless there is clear contradictory evidence.
DO NOT CREATE GAPS for criteria that are already well-scored (score >= 3).

`;
  }

  // Build the prompt
  const userPrompt = `${PROMPT_TEMPLATE}
${existingContext}
================================================================================
CALL TRANSCRIPTS TO ANALYZE
================================================================================

${ragContext}

================================================================================
YOUR ANALYSIS
================================================================================

Analyze the above transcripts and provide your 8C qualification assessment in JSON format.

${existingQualification && existingQualification.percentage > 0 ? 
  `CRITICAL ADDITIVE-ONLY RULES:
1. NEVER downgrade existing scores (existing evidence remains valid)
2. ONLY add new evidence from the current call
3. ONLY increase scores if current call provides stronger evidence
4. If current call has no new info for a criterion, keep existing score + evidence AS-IS
5. This is CUMULATIVE analysis across multiple calls - scores can only go UP or stay SAME, never DOWN

Previous qualification exists (${existingQualification.total_score}/140). Your job: ADD new evidence, don't re-evaluate old evidence.` : 
  'REMEMBER: Be STRICT and CONSERVATIVE. Only assign scores > 0 when you have clear, explicit evidence.'}
`;

  try {
    // Call Claude API with structured output
    const response = await anthropic.messages.create({
      model,
      max_tokens: maxTokens,
      temperature,
      messages: [
        {
          role: 'user',
          content: userPrompt,
        },
      ],
      // Request JSON output
      system: 'You are a precise sales qualification analyst. Always respond with valid JSON matching the specified schema.',
    });

    // Extract JSON from response
    const content = response.content[0].text;
    
    // Parse JSON (handle potential markdown code blocks)
    let analysisJson;
    try {
      // Try direct parse first
      analysisJson = JSON.parse(content);
    } catch (e) {
      // Try extracting from code block
      const jsonMatch = content.match(/```json\s*([\s\S]*?)\s*```/) || 
                        content.match(/```\s*([\s\S]*?)\s*```/);
      if (jsonMatch) {
        analysisJson = JSON.parse(jsonMatch[1]);
      } else {
        throw new Error('Could not parse JSON from LLM response');
      }
    }

    // Validate the analysis (pass existing qualification to check for unexplained score decreases)
    const validated = validateAnalysis(analysisJson, existingQualification);

    console.log('   ✅ Analysis complete');
    console.log(`   📊 Total Score: ${validated.total_score}/140 (${validated.percentage}%)`);
    console.log(`   🎯 Qualification: ${validated.qualification}`);
    console.log(`   📝 Next call questions: ${validated.next_call_questions.length}`);
    
    // DEBUG: Check if consolidated_evidence is present
    const hasConsolidated = validated.criteria.filter(c => c.consolidated_evidence).length;
    console.log(`   🔍 Criteria with consolidated_evidence: ${hasConsolidated}/8`);
    if (hasConsolidated === 0) {
      console.warn('   ⚠️  WARNING: LLM did not return consolidated_evidence field!');
    }

    return {
      analysis: validated,
      raw_response: content,
      tokens_used: response.usage,
    };
  } catch (error) {
    console.error('❌ LLM analysis failed:', error.message);
    throw error;
  }
}

/**
 * Validate and fix LLM analysis output
 * 
 * Applies conservative scoring rules:
 * - If evidence is empty → score must be 0
 * - If confidence is low → score must be 0
 * - Recalculates totals to ensure correctness
 * - Validates score decreases have explicit reasons
 * 
 * @param {object} analysis - LLM output
 * @param {object} existingQualification - Previous qualification data (if any)
 * @returns {object} Validated and fixed analysis
 */
function validateAnalysis(analysis, existingQualification = null) {
  console.log('🔍 Validating LLM analysis...');

  if (!analysis.criteria || !Array.isArray(analysis.criteria) || analysis.criteria.length !== 8) {
    throw new Error('Invalid analysis: must have exactly 8 criteria');
  }

  // Apply conservative scoring rules
  for (const criterion of analysis.criteria) {
    // Rule 1: No evidence → score must be 0
    if (!criterion.evidence || criterion.evidence.length === 0) {
      if (criterion.score !== 0) {
        console.warn(`   ⚠️  Criterion ${criterion.id}: No evidence but score=${criterion.score}, forcing to 0`);
        criterion.score = 0;
        criterion.weighted_score = 0;
      }
    }

    // Rule 2: Low confidence → score must be 0
    if (criterion.confidence === 'low' && criterion.score !== 0) {
      console.warn(`   ⚠️  Criterion ${criterion.id}: Low confidence but score=${criterion.score}, forcing to 0`);
      criterion.score = 0;
      criterion.weighted_score = 0;
    }

    // Rule 3: Score decreased without explanation → warning
    if (existingQualification && existingQualification.criteria) {
      const existingCriterion = existingQualification.criteria.find(c => c.id === criterion.id);
      if (existingCriterion && criterion.score < existingCriterion.score) {
        const hasExplanation = criterion.consolidated_evidence && 
                               criterion.consolidated_evidence.includes('⬇️ SCORE DECREASED:');
        if (!hasExplanation) {
          console.warn(`   ⚠️  Criterion ${criterion.id}: Score decreased (${existingCriterion.score} → ${criterion.score}) WITHOUT explanation in consolidated_evidence!`);
          console.warn(`   ⚠️  LLM should explain WHY score was lowered (compelling event expired, stakeholder left, etc.)`);
        }
      }
    }

    // Validate weighted score calculation
    const expectedWeighted = calculateWeightedScore(criterion.id, criterion.score);
    if (criterion.weighted_score !== expectedWeighted) {
      console.warn(`   ⚠️  Criterion ${criterion.id}: Weighted score mismatch (${criterion.weighted_score} vs ${expectedWeighted}), fixing`);
      criterion.weighted_score = expectedWeighted;
    }
  }

  // Recalculate totals
  const totalScore = analysis.criteria.reduce((sum, c) => sum + c.weighted_score, 0);
  const percentage = Math.round((totalScore / 140) * 100);
  const qualified = percentage >= 64;

  if (analysis.total_score !== totalScore) {
    console.warn(`   ⚠️  Total score mismatch (${analysis.total_score} vs ${totalScore}), fixing`);
    analysis.total_score = totalScore;
  }

  if (analysis.percentage !== percentage) {
    console.warn(`   ⚠️  Percentage mismatch (${analysis.percentage} vs ${percentage}), fixing`);
    analysis.percentage = percentage;
  }

  const expectedQualification = qualified ? 'QUALIFIED' : 'NOT_QUALIFIED';
  if (analysis.qualification !== expectedQualification) {
    console.warn(`   ⚠️  Qualification mismatch (${analysis.qualification} vs ${expectedQualification}), fixing`);
    analysis.qualification = expectedQualification;
  }

  console.log('   ✅ Validation complete');

  return analysis;
}

/**
 * Calculate weighted score for a criterion
 * 
 * Weights per 8C methodology:
 * 1. Compelling Event: weight 5 (0/15/25)
 * 2. Stakeholder Strategy: weight 3 (0/9/15)
 * 3. Funding: weight 3 (0/9/15)
 * 4. Customer Challenges: weight 5 (0/15/25)
 * 5. Business Value Drivers: weight 3 (0/9/15)
 * 6. Solution & Differentiators: weight 3 (0/9/15)
 * 7. Competitors: weight 5 (0/15/25)
 * 8. Partners & Ecosystem: weight 1 (0/3/5)
 * 
 * @param {number} criterionId - Criterion ID (1-8)
 * @param {number} rawScore - Raw score (0, 3, or 5)
 * @returns {number} Weighted score
 */
function calculateWeightedScore(criterionId, rawScore) {
  const weights = {
    1: 5, // Compelling Event
    2: 3, // Stakeholder Strategy
    3: 3, // Funding
    4: 5, // Customer Challenges
    5: 3, // Business Value Drivers
    6: 3, // Solution & Differentiators
    7: 5, // Competitors
    8: 1, // Partners & Ecosystem
  };

  const weight = weights[criterionId];
  if (!weight) {
    throw new Error(`Invalid criterion ID: ${criterionId}`);
  }

  return rawScore * weight;
}

/**
 * Format evidence for HubSpot multi-line text field
 * 
 * Creates structured text format for qual8c_evidence_all property
 * 
 * @param {object} analysis - Validated 8C analysis
 * @returns {string} Formatted evidence text
 */
export function formatEvidenceForHubSpot(analysis) {
  const timestamp = new Date().toISOString();
  
  let text = `=== 8C QUALIFICATION EVIDENCE ===\n`;
  text += `Last updated: ${timestamp}\n`;
  text += `Total Score: ${analysis.total_score}/140 (${analysis.percentage}%)\n`;
  text += `Qualification: ${analysis.qualification}\n\n`;

  analysis.criteria.forEach(criterion => {
    text += `--- ${criterion.id}. ${criterion.name.toUpperCase()} `;
    text += `(Score: ${criterion.score}/5 → Weighted: ${criterion.weighted_score}) ---\n`;
    
    if (criterion.evidence && criterion.evidence.length > 0) {
      text += `Evidence:\n`;
      criterion.evidence.forEach(quote => {
        text += `- "${quote}"\n`;
      });
    } else {
      text += `Evidence: None found\n`;
    }
    
    if (criterion.gaps && criterion.gaps.length > 0) {
      text += `Gaps:\n`;
      criterion.gaps.forEach(gap => {
        text += `- ${gap}\n`;
      });
    }
    
    text += `\n`;
  });

  if (analysis.next_call_questions && analysis.next_call_questions.length > 0) {
    text += `--- NEXT CALL QUESTIONS ---\n`;
    analysis.next_call_questions.forEach((q, idx) => {
      text += `${idx + 1}. ${q.criterion_name}: ${q.question}\n`;
      if (q.rationale) {
        text += `   Rationale: ${q.rationale}\n`;
      }
    });
    text += `\n`;
  }

  if (analysis.overall_assessment) {
    text += `--- OVERALL ASSESSMENT ---\n`;
    text += `${analysis.overall_assessment}\n`;
  }

  return text;
}

/**
 * Calculate score change from previous qualification
 * 
 * @param {object} newAnalysis - New 8C analysis
 * @param {object|null} previousAnalysis - Previous 8C analysis (if exists)
 * @returns {object} Score changes and recommendations
 */
export function calculateScoreChange(newAnalysis, previousAnalysis) {
  if (!previousAnalysis) {
    return {
      is_first_qualification: true,
      score_delta: newAnalysis.total_score,
      percentage_delta: newAnalysis.percentage,
      status_change: null,
    };
  }

  const scoreDelta = newAnalysis.total_score - previousAnalysis.total_score;
  const percentageDelta = newAnalysis.percentage - previousAnalysis.percentage;
  
  let statusChange = null;
  if (previousAnalysis.qualification === 'NOT_QUALIFIED' && newAnalysis.qualification === 'QUALIFIED') {
    statusChange = 'PROMOTED_TO_QUALIFIED';
  } else if (previousAnalysis.qualification === 'QUALIFIED' && newAnalysis.qualification === 'NOT_QUALIFIED') {
    statusChange = 'DEMOTED_TO_NOT_QUALIFIED';
  }

  return {
    is_first_qualification: false,
    score_delta: scoreDelta,
    percentage_delta: percentageDelta,
    status_change: statusChange,
    improved_criteria: newAnalysis.criteria.filter((c, idx) => 
      c.score > previousAnalysis.criteria[idx].score
    ).map(c => c.name),
    degraded_criteria: newAnalysis.criteria.filter((c, idx) => 
      c.score < previousAnalysis.criteria[idx].score
    ).map(c => c.name),
  };
}
