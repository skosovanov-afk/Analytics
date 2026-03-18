/**
 * HubSpot Deal Matcher
 * 
 * Finds the most relevant HubSpot deal for a call based on participant emails
 * and company domains.
 * 
 * Matching strategy:
 * 1. Search contacts by participant emails → get associated deals
 * 2. Search companies by participant domains → get associated deals
 * 3. Merge and deduplicate results
 * 4. Filter out closed deals
 * 5. Pick most recent active deal
 */

/**
 * HubSpot API helper
 */
async function hubspotFetch(path, options = {}) {
  const HUBSPOT_TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
  
  if (!HUBSPOT_TOKEN) {
    throw new Error('Missing HUBSPOT_PRIVATE_APP_TOKEN');
  }

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
 * Find HubSpot deal for a call
 * 
 * @param {string[]} participantEmails - External participant emails
 * @returns {Promise<object|null>} Matched deal or null if not found
 */
export async function findDealForCall(participantEmails) {
  console.log(`🔍 Finding HubSpot deal for participants: ${participantEmails.join(', ')}`);

  const allDeals = [];

  // Step 1: Search by contact emails
  for (const email of participantEmails) {
    const contactDeals = await findDealsByContactEmail(email);
    allDeals.push(...contactDeals);
  }

  // Step 2: Search by company domains
  const domains = participantEmails.map(email => email.split('@')[1]).filter(Boolean);
  const uniqueDomains = [...new Set(domains)];
  
  for (const domain of uniqueDomains) {
    const companyDeals = await findDealsByCompanyDomain(domain);
    allDeals.push(...companyDeals);
  }

  if (allDeals.length === 0) {
    console.log('   ❌ No deals found');
    return null;
  }

  // Step 3: Deduplicate by deal ID
  const uniqueDeals = deduplicateDeals(allDeals);
  console.log(`   📋 Found ${uniqueDeals.length} unique deal(s)`);

  // Step 4: Filter out closed deals
  const activeDeals = uniqueDeals.filter(deal => 
    !deal.properties.dealstage?.includes('closedwon') &&
    !deal.properties.dealstage?.includes('closedlost')
  );

  if (activeDeals.length === 0) {
    console.log('   ⚠️  All found deals are closed');
    return {
      deal: null,
      all_matched_deals: uniqueDeals.map(d => ({
        id: d.id,
        name: d.properties.dealname,
        stage: d.properties.dealstage,
        status: 'closed',
      })),
    };
  }

  // Step 5: Check if manual selection needed
  if (activeDeals.length > 1) {
    console.log(`   ⚠️  Found ${activeDeals.length} active deals - manual selection required`);
    
    return {
      deal: null,
      needs_manual_selection: true,
      all_matched_deals: uniqueDeals.map(d => ({
        id: d.id,
        name: d.properties.dealname,
        stage: d.properties.dealstage,
        owner_id: d.properties.hubspot_owner_id,
        last_modified: d.properties.hs_lastmodifieddate,
      })),
    };
  }

  // Single deal - auto-select
  const selectedDeal = activeDeals[0];

  console.log(`   ✅ Selected deal: ${selectedDeal.properties.dealname} (${selectedDeal.id})`);
  console.log(`      Stage: ${selectedDeal.properties.dealstage}`);
  console.log(`      Last modified: ${selectedDeal.properties.hs_lastmodifieddate}`);

  return {
    deal: selectedDeal,
    needs_manual_selection: false,
    all_matched_deals: uniqueDeals.map(d => ({
      id: d.id,
      name: d.properties.dealname,
      stage: d.properties.dealstage,
      owner_id: d.properties.hubspot_owner_id,
      last_modified: d.properties.hs_lastmodifieddate,
    })),
  };
}

/**
 * Find deals by contact email
 */
async function findDealsByContactEmail(email) {
  try {
    // Search for contact by email
    const contactSearch = await hubspotFetch('/crm/v3/objects/contacts/search', {
      method: 'POST',
      body: JSON.stringify({
        filterGroups: [
          {
            filters: [
              {
                propertyName: 'email',
                operator: 'EQ',
                value: email,
              },
            ],
          },
        ],
        properties: ['email', 'firstname', 'lastname'],
        limit: 1,
      }),
    });

    if (!contactSearch.results || contactSearch.results.length === 0) {
      return [];
    }

    const contactId = contactSearch.results[0].id;

    // Get associated deals for this contact
    const associations = await hubspotFetch(
      `/crm/v3/objects/contacts/${contactId}/associations/deals`
    );

    if (!associations.results || associations.results.length === 0) {
      return [];
    }

    const dealIds = associations.results.map(a => a.id);

    // Batch get deal details
    const deals = await batchGetDeals(dealIds);
    return deals;
  } catch (error) {
    console.warn(`   ⚠️  Failed to find deals for contact ${email}: ${error.message}`);
    return [];
  }
}

/**
 * Find deals by company domain
 */
async function findDealsByCompanyDomain(domain) {
  try {
    // Search for company by domain
    const companySearch = await hubspotFetch('/crm/v3/objects/companies/search', {
      method: 'POST',
      body: JSON.stringify({
        filterGroups: [
          {
            filters: [
              {
                propertyName: 'domain',
                operator: 'EQ',
                value: domain,
              },
            ],
          },
        ],
        properties: ['domain', 'name'],
        limit: 10,
      }),
    });

    if (!companySearch.results || companySearch.results.length === 0) {
      return [];
    }

    const allDeals = [];

    // Get deals for each matched company
    for (const company of companySearch.results) {
      const associations = await hubspotFetch(
        `/crm/v3/objects/companies/${company.id}/associations/deals`
      );

      if (associations.results && associations.results.length > 0) {
        const dealIds = associations.results.map(a => a.id);
        const deals = await batchGetDeals(dealIds);
        allDeals.push(...deals);
      }
    }

    return allDeals;
  } catch (error) {
    console.warn(`   ⚠️  Failed to find deals for company ${domain}: ${error.message}`);
    return [];
  }
}

/**
 * Batch get deal details
 */
async function batchGetDeals(dealIds) {
  if (dealIds.length === 0) return [];

  try {
    const response = await hubspotFetch('/crm/v3/objects/deals/batch/read', {
      method: 'POST',
      body: JSON.stringify({
        inputs: dealIds.map(id => ({ id })),
        properties: [
          'dealname',
          'dealstage',
          'pipeline',
          'amount',
          'closedate',
          'createdate',
          'hs_lastmodifieddate',
          'hubspot_owner_id',
          // 8C properties (will be null if not set yet)
          'qual8c_total_score',
          'qual8c_percentage',
          'qual8c_qualified',
        ],
      }),
    });

    return response.results || [];
  } catch (error) {
    console.warn(`   ⚠️  Batch get deals failed: ${error.message}`);
    return [];
  }
}

/**
 * Deduplicate deals by ID
 */
function deduplicateDeals(deals) {
  const seen = new Set();
  const unique = [];

  for (const deal of deals) {
    if (!seen.has(deal.id)) {
      seen.add(deal.id);
      unique.push(deal);
    }
  }

  return unique;
}

/**
 * Get HubSpot owner info (for task assignment and Slack lookup)
 */
export async function getHubSpotOwner(ownerId) {
  if (!ownerId) return null;

  try {
    const response = await hubspotFetch(`/crm/v3/owners/${ownerId}`);
    return {
      id: response.id,
      email: response.email,
      firstName: response.firstName,
      lastName: response.lastName,
    };
  } catch (error) {
    console.warn(`   ⚠️  Failed to get owner ${ownerId}: ${error.message}`);
    return null;
  }
}
