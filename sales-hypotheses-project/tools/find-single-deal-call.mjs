#!/usr/bin/env node
/**
 * Find calls with exactly 1 active HubSpot deal for testing automated qualification
 * 
 * Usage:
 *   CALLS_AUTH_FILE=... node find-single-deal-call.mjs [--limit 10]
 * 
 * Output: call_id, title, date, participant emails, deal info
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

// Parse args
const args = process.argv.slice(2);
const limitArg = args.find(a => a.startsWith('--limit='));
const limit = limitArg ? parseInt(limitArg.split('=')[1], 10) : 10;

// Load auth
const authPath = process.env.CALLS_AUTH_FILE || './../../02-calls/_private_cache/auth.json';
let auth;
try {
  auth = JSON.parse(readFileSync(authPath, 'utf8'));
} catch (err) {
  console.error(`❌ Cannot read auth file: ${authPath}`);
  console.error(`   Set CALLS_AUTH_FILE=/path/to/auth.json`);
  process.exit(1);
}

// Init Supabase
const supabase = createClient(
  auth.supabase_url,
  auth.supabase_anon_key,
  {
    global: {
      headers: {
        Authorization: `Bearer ${auth.access_token}`
      }
    }
  }
);

// Load HubSpot config
const hubspotToken = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!hubspotToken) {
  console.error('❌ HUBSPOT_PRIVATE_APP_TOKEN not set');
  process.exit(1);
}

/**
 * HubSpot API fetch wrapper
 */
async function hubspotFetch(path, options = {}) {
  const url = `https://api.hubapi.com${path}`;
  const response = await fetch(url, {
    ...options,
    headers: {
      'Authorization': `Bearer ${hubspotToken}`,
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HubSpot API error (${response.status}): ${text}`);
  }

  return response.json();
}

/**
 * Find active HubSpot deals for participant emails
 */
async function findDealsForEmails(emails) {
  const companyDomains = [...new Set(emails.map(e => e.split('@')[1]))];
  
  // Search contacts by email
  const contactResults = await Promise.all(
    emails.map(email =>
      hubspotFetch('/crm/v3/objects/contacts/search', {
        method: 'POST',
        body: JSON.stringify({
          filterGroups: [{
            filters: [{ propertyName: 'email', operator: 'EQ', value: email }]
          }],
          properties: ['email', 'firstname', 'lastname'],
          limit: 1,
        }),
      }).catch(() => ({ results: [] }))
    )
  );

  const contactIds = contactResults.flatMap(r => r.results?.map(c => c.id) || []);

  if (contactIds.length === 0) {
    return [];
  }

  // Get associated deals
  const dealResults = await Promise.all(
    contactIds.map(contactId =>
      hubspotFetch(`/crm/v4/objects/contacts/${contactId}/associations/deals`)
        .then(r => r.results?.map(a => a.toObjectId) || [])
        .catch(() => [])
    )
  );

  const uniqueDealIds = [...new Set(dealResults.flat())];

  if (uniqueDealIds.length === 0) {
    return [];
  }

  // Get deal details and filter active
  const dealsData = await hubspotFetch('/crm/v3/objects/deals/batch/read', {
    method: 'POST',
    body: JSON.stringify({
      inputs: uniqueDealIds.map(id => ({ id })),
      properties: ['dealname', 'dealstage', 'hs_is_closed'],
    }),
  });

  const activeDeals = dealsData.results?.filter(d => 
    d.properties.hs_is_closed !== 'true'
  ) || [];

  return activeDeals;
}

/**
 * Main
 */
async function main() {
  console.log(`🔍 Searching for calls with exactly 1 active HubSpot deal...\n`);
  console.log(`   Limit: ${limit} calls\n`);

  // Get recent external calls
  const { data: calls, error } = await supabase
    .from('calls')
    .select(`
      id,
      title,
      occurred_at,
      call_participants!inner(
        user_profiles!inner(email)
      )
    `)
    .order('occurred_at', { ascending: false })
    .limit(limit);

  if (error) {
    console.error('❌ Supabase error:', error.message);
    process.exit(1);
  }

  console.log(`📋 Checking ${calls.length} recent calls...\n`);

  // Check each call
  for (const call of calls) {
    const participants = call.call_participants || [];
    const emails = participants
      .map(p => p.user_profiles?.email)
      .filter(Boolean);

    // Filter external participants
    const externalEmails = emails.filter(e => !e.endsWith('@oversecured.com'));

    if (externalEmails.length === 0) {
      continue; // Skip internal calls
    }

    console.log(`\n📞 Call: ${call.title}`);
    console.log(`   ID: ${call.id}`);
    console.log(`   Date: ${call.occurred_at}`);
    console.log(`   Participants: ${externalEmails.join(', ')}`);

    try {
      const deals = await findDealsForEmails(externalEmails);
      console.log(`   🎯 Active deals: ${deals.length}`);

      if (deals.length === 1) {
        console.log(`\n✅ FOUND! Call with exactly 1 active deal:`);
        console.log(`   Call ID: ${call.id}`);
        console.log(`   Deal: ${deals[0].properties.dealname} (${deals[0].id})`);
        console.log(`\n📥 To enqueue for testing:`);
        console.log(`   INSERT INTO public.sales_8c_qualification_queue (call_id, priority)`);
        console.log(`   VALUES ('${call.id}', 1);`);
        process.exit(0);
      }
    } catch (err) {
      console.warn(`   ⚠️  Error checking deals: ${err.message}`);
    }
  }

  console.log(`\n❌ No calls with exactly 1 active deal found in last ${limit} calls`);
  console.log(`   Try increasing --limit or check HubSpot manually`);
}

main().catch(err => {
  console.error('❌ Fatal error:', err);
  process.exit(1);
});
