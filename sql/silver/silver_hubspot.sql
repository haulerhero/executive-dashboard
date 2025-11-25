-- ============================================================================
-- Silver Layer: HubSpot Data
-- Purpose: Clean and standardize HubSpot deals and companies
-- Source: executive_dash_raw.hubspot_deals, executive_dash_raw.hubspot_companies
-- ============================================================================

-- Silver: HubSpot Deals
CREATE OR REPLACE TABLE `executive_dash_silver.hubspot_deals` AS
SELECT
  id as deal_id,
  company_id as hubspot_company_id,
  dealname as deal_name,
  SAFE_CAST(amount AS FLOAT64) as deal_amount,
  SAFE_CAST(implementation_cost AS FLOAT64) as implementation_cost,
  SAFE_CAST(daily_routes AS INT64) as daily_routes,
  dealstage as deal_stage,
  pipeline,
  closedate as close_date,
  createdate as create_date,
  hs_lastmodifieddate as last_modified_date,
  hubspot_deal_url,
  hubspot_company_url,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.hubspot_deals`
WHERE id IS NOT NULL
  AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
    SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
    FROM `executive_dash_raw.hubspot_deals`
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY hs_lastmodifieddate DESC) = 1;


-- Silver: HubSpot Companies
-- Note: current_software field pending - needs HubSpot API fix to pull properties
CREATE OR REPLACE TABLE `executive_dash_silver.hubspot_companies` AS
SELECT
  id as hubspot_company_id,
  name as company_name,
  domain,
  -- current_business_management_software__cloned_ as current_software,  -- TODO: Fix HubSpot API to pull this
  createdate as create_date,
  hs_lastmodifieddate as last_modified_date,
  hubspot_company_url,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.hubspot_companies`
WHERE id IS NOT NULL
  AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
    SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
    FROM `executive_dash_raw.hubspot_companies`
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY hs_lastmodifieddate DESC) = 1;