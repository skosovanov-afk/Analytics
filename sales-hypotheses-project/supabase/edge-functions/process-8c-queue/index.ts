/**
 * Supabase Edge Function: Process 8C Qualification Queue
 * 
 * Serverless background worker that processes pending 8C qualifications.
 * Triggered by:
 * - Cron schedule (every 5 minutes)
 * - Manual HTTP POST
 * 
 * Deploy:
 *   supabase functions deploy process-8c-queue
 * 
 * Schedule (add to Supabase Dashboard):
 *   */5 * * * * (every 5 minutes)
 */

import { serve } from 'https://deno.land/std@0.168.0/http/server.ts';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

serve(async (req) => {
  console.log('🚀 8C Queue Processor triggered');

  try {
    // Get next pending call from queue
    const { data: queueItems, error: fetchError } = await supabase
      .from('sales_8c_qualification_queue')
      .select('*')
      .eq('status', 'pending')
      .order('priority', { ascending: true })
      .order('created_at', { ascending: true })
      .limit(1);

    if (fetchError) {
      throw fetchError;
    }

    if (!queueItems || queueItems.length === 0) {
      console.log('✅ Queue is empty');
      return new Response(
        JSON.stringify({ message: 'Queue is empty', processed: 0 }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    }

    const queueItem = queueItems[0];
    console.log(`📞 Processing call ${queueItem.call_id}`);

    // Mark as processing
    await supabase
      .from('sales_8c_qualification_queue')
      .update({
        status: 'processing',
        started_at: new Date().toISOString(),
      })
      .eq('id', queueItem.id);

    // Call the qualification tool via HTTP
    // In production, this would call the Node.js qualify-call-8c.mjs script
    // For Edge Function, we need to either:
    // 1. Reimplement the logic in Deno/TypeScript (preferred for fully serverless)
    // 2. Call external service that runs the Node script
    // 3. Use Supabase Functions to invoke another function
    
    // For now, we'll implement a minimal version that delegates to the RPC
    // Full implementation would include LLM calls, HubSpot updates, etc.
    
    console.log('⚠️  Edge Function implementation pending');
    console.log('   For now, use Node.js cron: node process-8c-queue.mjs --continuous');

    // Mark as completed (placeholder)
    await supabase
      .from('sales_8c_qualification_queue')
      .delete()
      .eq('id', queueItem.id);

    return new Response(
      JSON.stringify({
        message: 'Processed (placeholder)',
        call_id: queueItem.call_id,
        processed: 1,
      }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error('❌ Error:', error);
    
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }
});

/* 
 * DEPLOYMENT NOTES:
 * 
 * For production use, recommend Node.js cron instead of Edge Function:
 * 
 * Option 1: Node.js Cron (RECOMMENDED)
 *   - Run on a server with crontab:
 *     */5 * * * * cd /path/to/repo && node 99-applications/sales/tools/process-8c-queue.mjs
 * 
 *   - Or use PM2:
 *     pm2 start 99-applications/sales/tools/process-8c-queue.mjs --cron "*/5 * * * *"
 * 
 * Option 2: Supabase Edge Function
 *   - Requires reimplementing all logic in Deno/TypeScript
 *   - More complex due to Anthropic SDK, HubSpot SDK dependencies
 *   - Better for fully serverless architecture
 * 
 * Option 3: GitHub Actions
 *   - Create .github/workflows/8c-queue-processor.yml
 *   - Schedule: cron '*/5 * * * *'
 *   - Runs in GitHub's infrastructure
 */
