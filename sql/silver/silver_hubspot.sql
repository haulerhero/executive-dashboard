-- ============================================================================
-- Silver Layer: HubSpot Data
-- Purpose: Clean and standardize HubSpot deals and companies
-- Source: raw.hubspot_deals, raw.hubspot_companies
-- ============================================================================

-- Silver: HubSpot Deals
-- Tracks all deals closed in the last 18 months (filtered at extraction)
CREATE OR REPLACE TABLE `silver.hubspot_deals` AS
SELECT
  -- IDs and relationships
  id as deal_id,
  company_id as hubspot_company_id,
  
  -- Deal information
  dealname as deal_name,
  CAST(amount AS FLOAT64) as deal_amount,
  CAST(implementation_cost AS FLOAT64) as implementation_cost,
  CAST(daily_routes AS INT64) as daily_routes,
  
  -- Deal classification
  dealstage as deal_stage,
  pipeline,
  
  -- Dates (parse HubSpot ISO formats)
  PARSE_TIMESTAMP('%Y-%m-%d', SUBSTR(closedate, 1, 10)) as close_date,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(createdate, 1, 19)) as create_date,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(hs_lastmodifieddate, 1, 19)) as last_modified_date,
  
  -- Links for reference
  hubspot_deal_url,
  hubspot_company_url,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `raw.hubspot_deals`
WHERE id IS NOT NULL
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `raw.hubspot_deals`) || '%'  -- Latest run only
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY hs_lastmodifieddate DESC) = 1;  -- Dedupe by latest modified


-- Silver: HubSpot Companies
-- Customer companies associated with closed deals (all properties extracted)
CREATE OR REPLACE TABLE `silver.hubspot_companies` AS
SELECT
  -- IDs
  id as hubspot_company_id,
  
  -- Company information
  name as company_name,
  domain,
  
  -- Previous software (critical for implementation planning)
  current_business_management_software__cloned_ as current_software,
  
  -- Dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(createdate, 1, 19)) as create_date,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(hs_lastmodifieddate, 1, 19)) as last_modified_date,
  
  -- Link for reference
  hubspot_company_url,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `raw.hubspot_companies`
WHERE id IS NOT NULL
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `raw.hubspot_companies`) || '%'  -- Latest run only
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY hs_lastmodifieddate DESC) = 1;  -- Dedupe by latest modified
