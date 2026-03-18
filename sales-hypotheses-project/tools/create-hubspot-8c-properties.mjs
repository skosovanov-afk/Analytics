#!/usr/bin/env node

/**
 * Create HubSpot custom properties for 8C qualification scoring
 * 
 * Creates 25 custom properties:
 * - 8 raw score fields (0/3/5)
 * - 8 weighted score fields (actual points)
 * - 3 totals (total score, percentage, qualified decision)
 * - 1 evidence field (multi-line text)
 * 
 * Usage:
 *   node create-hubspot-8c-properties.mjs
 *   node create-hubspot-8c-properties.mjs --dry-run  # preview only
 */

import 'dotenv/config';

const HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
const DRY_RUN = process.argv.includes('--dry-run');

if (!HUBSPOT_TOKEN) {
  console.error('❌ Missing HUBSPOT_PRIVATE_APP_TOKEN in environment');
  console.error('Set it in 99-applications/sales/portal/.env.local or export it');
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
    throw new Error(`HubSpot API error: ${response.status} ${error}`);
  }

  return response.json();
}

/**
 * Create property group if it doesn't exist
 */
async function createPropertyGroup(groupName, label) {
  console.log(`📁 Creating property group: ${groupName}...`);
  
  if (DRY_RUN) {
    console.log(`  [DRY RUN] Would create group: ${groupName}`);
    return;
  }

  try {
    await hubspotFetch('/crm/v3/properties/deals/groups', {
      method: 'POST',
      body: JSON.stringify({
        name: groupName,
        label: label,
        displayOrder: -1,
      }),
    });
    console.log(`  ✅ Created group: ${groupName}\n`);
  } catch (error) {
    if (error.message.includes('already exists') || error.message.includes('GROUP_ALREADY_EXISTS')) {
      console.log(`  ⚠️  Group already exists: ${groupName}\n`);
    } else {
      console.error(`  ❌ Failed to create group: ${groupName} - ${error.message}`);
      throw error;
    }
  }
}

/**
 * Create a single custom property for deals
 */
async function createProperty(propertyDef) {
  console.log(`  Creating: ${propertyDef.name}...`);
  
  if (DRY_RUN) {
    console.log(`  [DRY RUN] Would create: ${JSON.stringify(propertyDef, null, 2)}`);
    return;
  }

  try {
    await hubspotFetch('/crm/v3/properties/deals', {
      method: 'POST',
      body: JSON.stringify(propertyDef),
    });
    console.log(`  ✅ Created: ${propertyDef.name}`);
  } catch (error) {
    if (error.message.includes('already exists')) {
      console.log(`  ⚠️  Already exists: ${propertyDef.name}`);
    } else {
      console.error(`  ❌ Failed: ${propertyDef.name} - ${error.message}`);
      throw error;
    }
  }
}

/**
 * All 8C custom properties definitions
 */
const PROPERTIES = [
  // ============================================================================
  // RAW SCORES (0/3/5 for each criterion)
  // ============================================================================
  {
    name: 'qual8c_score_compelling_event',
    label: '8C Score: Compelling Event',
    description: 'Raw score (0/3/5) for Compelling Event criterion. 0=Weak, 3=Moderate, 5=Strong.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_score_stakeholder',
    label: '8C Score: Stakeholder Strategy',
    description: 'Raw score (0/3/5) for Customer Stakeholder Strategy criterion.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_score_funding',
    label: '8C Score: Funding',
    description: 'Raw score (0/3/5) for Funding criterion.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_score_challenges',
    label: '8C Score: Customer Challenges',
    description: 'Raw score (0/3/5) for Customer Challenges criterion.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_score_value_drivers',
    label: '8C Score: Business Value Drivers',
    description: 'Raw score (0/3/5) for Business Value Drivers criterion.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_score_solution',
    label: '8C Score: Solution & Differentiators',
    description: 'Raw score (0/3/5) for Solution & Differentiators criterion.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_score_competitors',
    label: '8C Score: Competitors',
    description: 'Raw score (0/3/5) for Competitors criterion.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_score_partners',
    label: '8C Score: Partners & Ecosystem',
    description: 'Raw score (0/3/5) for Partners & Ecosystem criterion.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },

  // ============================================================================
  // WEIGHTED SCORES (actual points per methodology)
  // ============================================================================
  {
    name: 'qual8c_weighted_compelling_event',
    label: '8C Weighted: Compelling Event',
    description: 'Weighted score (0/15/25 points) for Compelling Event.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_weighted_stakeholder',
    label: '8C Weighted: Stakeholder Strategy',
    description: 'Weighted score (0/9/15 points) for Stakeholder Strategy.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_weighted_funding',
    label: '8C Weighted: Funding',
    description: 'Weighted score (0/9/15 points) for Funding.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_weighted_challenges',
    label: '8C Weighted: Customer Challenges',
    description: 'Weighted score (0/15/25 points) for Customer Challenges.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_weighted_value_drivers',
    label: '8C Weighted: Business Value Drivers',
    description: 'Weighted score (0/9/15 points) for Business Value Drivers.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_weighted_solution',
    label: '8C Weighted: Solution & Differentiators',
    description: 'Weighted score (0/9/15 points) for Solution & Differentiators.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_weighted_competitors',
    label: '8C Weighted: Competitors',
    description: 'Weighted score (0/15/25 points) for Competitors.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_weighted_partners',
    label: '8C Weighted: Partners & Ecosystem',
    description: 'Weighted score (0/3/5 points) for Partners & Ecosystem.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },

  // ============================================================================
  // TOTALS
  // ============================================================================
  {
    name: 'qual8c_total_score',
    label: '8C Total Score',
    description: 'Total 8C score (0-140 points). Minimum 90 points (64%) required for SQL qualification.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_percentage',
    label: '8C Percentage',
    description: '8C score as percentage (0-100%). Minimum 64% required for SQL qualification.',
    type: 'number',
    fieldType: 'number',
    groupName: 'qual8c_qualification',
  },
  {
    name: 'qual8c_qualified',
    label: '8C Qualified',
    description: 'Qualification decision based on 8C score. Qualified = 64%+ (90/140 points).',
    type: 'enumeration',
    fieldType: 'select',
    groupName: 'qual8c_qualification',
    options: [
      { label: 'Qualified', value: 'qualified' },
      { label: 'Not Qualified', value: 'not_qualified' },
    ],
  },

  // ============================================================================
  // EVIDENCE
  // ============================================================================
  {
    name: 'qual8c_evidence_all',
    label: '8C Evidence & Gaps',
    description: 'Structured evidence from call transcripts for all 8C criteria, including gaps and next call questions.',
    type: 'string',
    fieldType: 'textarea',
    groupName: 'qual8c_qualification',
  },
];

/**
 * Main execution
 */
async function main() {
  console.log('🚀 Creating HubSpot custom properties for 8C qualification...\n');
  
  if (DRY_RUN) {
    console.log('⚠️  DRY RUN MODE - no actual changes will be made\n');
  }

  // Create property group first
  await createPropertyGroup('qual8c_qualification', '8C Qualification');

  console.log(`📊 Total properties to create: ${PROPERTIES.length}\n`);

  let created = 0;
  let skipped = 0;
  let failed = 0;

  for (const prop of PROPERTIES) {
    try {
      await createProperty(prop);
      created++;
    } catch (error) {
      if (error.message.includes('already exists')) {
        skipped++;
      } else {
        failed++;
      }
    }
  }

  console.log('\n📈 Summary:');
  console.log(`  ✅ Created: ${created}`);
  console.log(`  ⚠️  Already existed: ${skipped}`);
  if (failed > 0) {
    console.log(`  ❌ Failed: ${failed}`);
  }
  console.log('\n✅ Done! HubSpot custom properties are ready for 8C qualification.');
  console.log('\nNext steps:');
  console.log('  1. Verify properties in HubSpot UI: Settings → Data Management → Properties → Deals');
  console.log('  2. Test property update: node 99-applications/sales/tools/qualify-call-8c.mjs --call-id <id>');
}

main().catch(error => {
  console.error('❌ Fatal error:', error);
  process.exit(1);
});
