#!/usr/bin/env node

/**
 * Enqueue a call for 8C auto-qualification
 * 
 * Usage:
 *   node enqueue-call-8c.mjs --call-id <uuid>
 *   node enqueue-call-8c.mjs --call-id <uuid> --priority 1
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

// Parse args
const args = process.argv.slice(2);
const callIdIdx = args.indexOf('--call-id');
const priorityIdx = args.indexOf('--priority');

if (callIdIdx === -1 || !args[callIdIdx + 1]) {
  console.error('❌ Usage: node enqueue-call-8c.mjs --call-id <uuid> [--priority <number>]');
  process.exit(1);
}

const callId = args[callIdIdx + 1];
const priority = priorityIdx !== -1 && args[priorityIdx + 1] 
  ? parseInt(args[priorityIdx + 1], 10) 
  : 100;

// Load Supabase auth
const authPath = process.env.CALLS_AUTH_FILE || './../../02-calls/_private_cache/auth.json';
let auth;
try {
  auth = JSON.parse(readFileSync(authPath, 'utf8'));
} catch (err) {
  console.error('❌ Cannot read auth file:', authPath);
  console.error('   Set CALLS_AUTH_FILE or create 02-calls/_private_cache/auth.json');
  process.exit(1);
}

const supabase = createClient(
  auth.supabase_url,
  auth.supabase_anon_key,
  { global: { headers: { Authorization: `Bearer ${auth.access_token}` } } }
);

console.log(`📥 Enqueueing call ${callId} for 8C qualification...`);

// Insert into queue
const { data, error } = await supabase
  .from('sales_8c_qualification_queue')
  .insert({ 
    call_id: callId,
    priority: priority 
  })
  .select();

if (error) {
  console.error('❌ Error enqueueing call:', error.message);
  process.exit(1);
}

console.log('✅ Call enqueued successfully!');
console.log(`   Queue ID: ${data[0].id}`);
console.log(`   Priority: ${data[0].priority}`);
console.log(`   Status: ${data[0].status}`);
console.log('');
console.log('🤖 GitHub Actions workflow will process this within 5 minutes');
console.log('   Check workflow runs: https://github.com/emoskvin-ops/Oversecured/actions');
