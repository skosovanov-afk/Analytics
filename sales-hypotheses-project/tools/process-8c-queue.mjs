#!/usr/bin/env node

/**
 * 8C Qualification Queue Processor
 * 
 * Background worker that processes the 8C qualification queue.
 * Runs every 5 minutes (or on-demand) to qualify pending client calls.
 * 
 * Usage:
 *   node process-8c-queue.mjs                    # process one batch
 *   node process-8c-queue.mjs --continuous       # run continuously every 5 min
 *   node process-8c-queue.mjs --limit 10         # process max 10 calls
 *   node process-8c-queue.mjs --call-id <uuid>   # process specific call from queue
 */

import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// Parse CLI args
const args = process.argv.slice(2);
const continuous = args.includes('--continuous');

// Safely parse --limit argument
const limitIndex = args.indexOf('--limit');
const limit = limitIndex >= 0 && args[limitIndex + 1] 
  ? parseInt(args[limitIndex + 1], 10) 
  : 1;

// Safely parse --call-id argument
const callIdIndex = args.indexOf('--call-id');
const specificCallId = callIdIndex >= 0 ? args[callIdIndex + 1] : undefined;

const PROCESSING_INTERVAL = parseInt(process.env['8C_QUEUE_PROCESSING_INTERVAL_MINUTES'] || '5', 10) * 60 * 1000;

/**
 * Get next pending call from queue
 */
async function getNextPendingCall() {
  const query = supabase
    .from('sales_8c_qualification_queue')
    .select('*')
    .eq('status', 'pending')
    .order('priority', { ascending: true })
    .order('created_at', { ascending: true })
    .limit(1);

  if (specificCallId) {
    query.eq('call_id', specificCallId);
  }

  const { data, error } = await query;

  if (error) {
    throw new Error(`Failed to fetch queue: ${error.message}`);
  }

  return data && data.length > 0 ? data[0] : null;
}

/**
 * Process a single call from queue
 */
async function processCall(queueItem) {
  const { id: queueId, call_id: callId, retry_count: retryCount, max_retries: maxRetries } = queueItem;

  console.log(`\n${'='.repeat(80)}`);
  console.log(`🔄 Processing call ${callId} (queue ${queueId})`);
  console.log(`   Retry: ${retryCount}/${maxRetries}`);
  console.log(`${'='.repeat(80)}\n`);

  // Update status to processing
  await supabase
    .from('sales_8c_qualification_queue')
    .update({
      status: 'processing',
      started_at: new Date().toISOString(),
    })
    .eq('id', queueId);

  try {
    // Run the qualification script
    const { stdout, stderr } = await execAsync(
      `node ./qualify-call-8c.mjs --call-id ${callId}`,
      {
        cwd: process.cwd(),
        env: process.env,
        maxBuffer: 10 * 1024 * 1024, // 10MB buffer for large transcripts
      }
    );

    console.log(stdout);
    if (stderr) {
      console.warn('⚠️  Stderr:', stderr);
    }

    // Mark as completed and remove from queue
    await supabase
      .from('sales_8c_qualification_queue')
      .delete()
      .eq('id', queueId);

    console.log(`✅ Completed and removed from queue\n`);
    return { success: true };
  } catch (error) {
    console.error(`❌ Processing failed: ${error.message}\n`);

    // Increment retry count
    const newRetryCount = retryCount + 1;

    if (newRetryCount >= maxRetries) {
      // Max retries reached, mark as failed
      await supabase
        .from('sales_8c_qualification_queue')
        .update({
          status: 'failed',
          error_message: error.message,
          completed_at: new Date().toISOString(),
        })
        .eq('id', queueId);

      console.error(`❌ Max retries reached (${maxRetries}), marked as failed\n`);
    } else {
      // Reset to pending for retry
      await supabase
        .from('sales_8c_qualification_queue')
        .update({
          status: 'pending',
          error_message: error.message,
          retry_count: newRetryCount,
        })
        .eq('id', queueId);

      console.log(`🔄 Retry ${newRetryCount}/${maxRetries}, reset to pending\n`);
    }

    return { success: false, error: error.message };
  }
}

/**
 * Process queue batch
 */
async function processBatch() {
  console.log(`\n🔍 Checking queue for pending calls (limit: ${limit})...`);

  let processed = 0;
  let succeeded = 0;
  let failed = 0;

  for (let i = 0; i < limit; i++) {
    const queueItem = await getNextPendingCall();

    if (!queueItem) {
      console.log('   ✅ Queue is empty\n');
      break;
    }

    processed++;
    const result = await processCall(queueItem);

    if (result.success) {
      succeeded++;
    } else {
      failed++;
    }

    // Rate limit: wait 1 second between calls (avoid HubSpot rate limits)
    if (i < limit - 1) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  console.log(`\n📊 Batch Summary:`);
  console.log(`   Processed: ${processed}`);
  console.log(`   Succeeded: ${succeeded}`);
  console.log(`   Failed: ${failed}`);
  console.log('');

  return { processed, succeeded, failed };
}

/**
 * Continuous processing loop
 */
async function runContinuous() {
  console.log('🔁 Starting continuous queue processing...');
  console.log(`   Interval: ${PROCESSING_INTERVAL / 1000 / 60} minutes`);
  console.log(`   Batch limit: ${limit} calls per run\n`);

  while (true) {
    try {
      await processBatch();
    } catch (error) {
      console.error('❌ Batch processing error:', error.message);
    }

    console.log(`⏰ Waiting ${PROCESSING_INTERVAL / 1000 / 60} minutes until next run...\n`);
    await new Promise(resolve => setTimeout(resolve, PROCESSING_INTERVAL));
  }
}

/**
 * Main execution
 */
if (continuous) {
  runContinuous().catch(error => {
    console.error('❌ Fatal error in continuous mode:', error);
    process.exit(1);
  });
} else {
  processBatch()
    .then(() => {
      console.log('✅ Queue processing complete');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Fatal error:', error);
      process.exit(1);
    });
}
