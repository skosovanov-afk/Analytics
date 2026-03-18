/**
 * RAG Context Builder for 8C Qualification
 * 
 * Gathers all relevant call transcripts for LLM analysis:
 * - Current call (full transcript)
 * - Deal history (max 10 recent calls for same HubSpot deal)
 * - Company history (max 5 recent calls with same company domain)
 * - Contact history (max 5 recent calls with same participant emails)
 * 
 * Uses Supabase RPC get_8c_rag_context for efficient retrieval.
 * 
 * NOTE: Does NOT create its own Supabase client! 
 * Client must be passed as parameter (supports both user auth and service role).
 */

/**
 * Build RAG context for 8C qualification
 * 
 * @param {object} supabase - Initialized Supabase client (user auth or service role)
 * @param {string} callId - UUID of the call to qualify
 * @param {string|null} hubspotDealId - HubSpot deal ID (if found)
 * @param {object} options - RAG context limits
 * @returns {Promise<object>} RAG context with all relevant calls
 */
export async function buildRagContext(supabase, callId, hubspotDealId = null, options = {}) {
  const {
    maxDealCalls = 10,
    maxCompanyCalls = 5,
    maxContactCalls = 5,
    maxContextTokens = 500000, // ~500K tokens limit
  } = options;

  console.log(`📚 Building RAG context for call ${callId}...`);
  console.log(`   Deal ID: ${hubspotDealId || 'not provided'}`);
  console.log(`   Limits: deal=${maxDealCalls}, company=${maxCompanyCalls}, contacts=${maxContactCalls}`);

  // Call Supabase RPC to gather all relevant calls
  const { data: ragContext, error } = await supabase.rpc('get_8c_rag_context', {
    p_call_id: callId,
    p_hubspot_deal_id: hubspotDealId,
    p_max_deal_calls: maxDealCalls,
    p_max_company_calls: maxCompanyCalls,
    p_max_contact_calls: maxContactCalls,
  });

  if (error) {
    throw new Error(`Failed to build RAG context: ${error.message}`);
  }

  if (!ragContext) {
    throw new Error('RAG context is null - call not found or access denied');
  }

  console.log(`   ✅ Gathered ${ragContext.total_calls} calls for context`);
  console.log(`      Current: 1`);
  console.log(`      Deal history: ${ragContext.deal_calls?.length || 0}`);
  console.log(`      Company history: ${ragContext.company_calls?.length || 0}`);
  console.log(`      Contact history: ${ragContext.contact_calls?.length || 0}`);

  // Estimate token count (rough: 1 token ≈ 4 chars)
  const estimatedTokens = estimateTokenCount(ragContext);
  console.log(`   📊 Estimated tokens: ~${estimatedTokens.toLocaleString()}`);

  if (estimatedTokens > maxContextTokens) {
    console.warn(`   ⚠️  Context exceeds limit (${maxContextTokens.toLocaleString()} tokens)`);
    console.warn(`   🔧 Applying summarization for older calls...`);
    
    // Apply summarization for older calls to fit within limit
    ragContext.company_calls = summarizeOlderCalls(ragContext.company_calls, maxContextTokens / 4);
    ragContext.contact_calls = summarizeOlderCalls(ragContext.contact_calls, maxContextTokens / 4);
  }

  return ragContext;
}

/**
 * Estimate token count for RAG context (rough approximation)
 * 
 * @param {object} ragContext - RAG context object
 * @returns {number} Estimated token count
 */
function estimateTokenCount(ragContext) {
  const jsonString = JSON.stringify(ragContext);
  // Rough estimate: 1 token ≈ 4 characters
  return Math.ceil(jsonString.length / 4);
}

/**
 * Summarize older calls to reduce context size
 * 
 * Strategy:
 * - Keep full transcripts for most recent 2 calls
 * - Replace older call transcripts with summary_main only
 * 
 * @param {array} calls - Array of call objects
 * @param {number} targetTokens - Target token budget for this section
 * @returns {array} Summarized calls
 */
function summarizeOlderCalls(calls, targetTokens) {
  if (!calls || calls.length === 0) return calls;

  // Sort by occurred_at desc (most recent first)
  const sorted = [...calls].sort((a, b) => 
    new Date(b.occurred_at) - new Date(a.occurred_at)
  );

  // Keep first 2 calls with full transcripts
  const result = sorted.slice(0, 2);

  // Summarize remaining calls (use summary_main instead of full transcript)
  for (let i = 2; i < sorted.length; i++) {
    const call = sorted[i];
    result.push({
      ...call,
      transcript_text: null, // remove full transcript
      summary_main: call.summary_main || '[Summary not available]',
      _summarized: true, // marker for debugging
    });
  }

  return result;
}

/**
 * Format RAG context for LLM prompt
 * 
 * Converts RAG context into human-readable format for inclusion in prompt
 * 
 * @param {object} ragContext - RAG context from buildRagContext
 * @returns {string} Formatted text for LLM prompt
 */
export function formatRagContextForPrompt(ragContext) {
  let formatted = '';

  // Current call (always included with full transcript)
  formatted += '=== CURRENT CALL ===\n';
  formatted += `Title: ${ragContext.current_call.title}\n`;
  formatted += `Date: ${ragContext.current_call.occurred_at}\n`;
  formatted += `Category: ${ragContext.current_call.category}\n`;
  if (ragContext.current_call.summary_main) {
    formatted += `\nSummary:\n${ragContext.current_call.summary_main}\n`;
  }
  formatted += `\nFull Transcript:\n${ragContext.current_call.transcript_text}\n\n`;

  // Deal history
  if (ragContext.deal_calls && ragContext.deal_calls.length > 0) {
    formatted += '=== DEAL HISTORY (Previous calls for same HubSpot deal) ===\n';
    ragContext.deal_calls.forEach((call, idx) => {
      formatted += `\n--- Call ${idx + 1}: ${call.title} (${call.occurred_at}) ---\n`;
      if (call._summarized) {
        formatted += `Summary: ${call.summary_main}\n`;
      } else {
        formatted += `Transcript:\n${call.transcript_text}\n`;
      }
    });
    formatted += '\n';
  }

  // Company history
  if (ragContext.company_calls && ragContext.company_calls.length > 0) {
    formatted += '=== COMPANY HISTORY (Calls with same company domain) ===\n';
    ragContext.company_calls.forEach((call, idx) => {
      formatted += `\n--- Call ${idx + 1}: ${call.title} (${call.occurred_at}) ---\n`;
      if (call._summarized) {
        formatted += `Summary: ${call.summary_main}\n`;
      } else {
        formatted += `Transcript:\n${call.transcript_text}\n`;
      }
    });
    formatted += '\n';
  }

  // Contact history
  if (ragContext.contact_calls && ragContext.contact_calls.length > 0) {
    formatted += '=== CONTACT HISTORY (Calls with same participant emails) ===\n';
    ragContext.contact_calls.forEach((call, idx) => {
      formatted += `\n--- Call ${idx + 1}: ${call.title} (${call.occurred_at}) ---\n`;
      if (call._summarized) {
        formatted += `Summary: ${call.summary_main}\n`;
      } else {
        formatted += `Transcript:\n${call.transcript_text}\n`;
      }
    });
    formatted += '\n';
  }

  // Context metadata
  formatted += '=== CONTEXT METADATA ===\n';
  formatted += `Total calls analyzed: ${ragContext.total_calls}\n`;
  formatted += `Participant emails: ${ragContext.participant_emails.join(', ')}\n`;
  formatted += `Company domains: ${ragContext.company_domains.join(', ')}\n`;

  return formatted;
}

/**
 * Get external participant emails (exclude @oversecured.com)
 * 
 * @param {object} supabase - Initialized Supabase client
 * @param {string} callId - UUID of the call
 * @returns {Promise<string[]>} Array of external participant emails
 */
export async function getExternalParticipants(supabase, callId) {
  const { data, error } = await supabase
    .from('call_participants')
    .select('email')
    .eq('call_id', callId)
    .not('email', 'ilike', '%@oversecured.com');

  if (error) {
    throw new Error(`Failed to get participants: ${error.message}`);
  }

  return data.map(p => p.email);
}

/**
 * Extract company domains from participant emails
 * 
 * @param {string[]} emails - Array of email addresses
 * @returns {string[]} Array of unique company domains
 */
export function extractCompanyDomains(emails) {
  const domains = emails
    .map(email => email.split('@')[1])
    .filter(Boolean);
  
  return [...new Set(domains)]; // deduplicate
}
