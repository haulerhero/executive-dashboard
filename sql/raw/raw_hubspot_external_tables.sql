-- ============================================================================
-- Raw Layer: HubSpot External Tables
-- Purpose: Create external tables pointing to GCS JSON data
-- Note: These tables query GCS directly (no data stored in BigQuery)
-- ============================================================================

-- External table for HubSpot Deals
CREATE OR REPLACE EXTERNAL TABLE `executive_dash_raw.hubspot_deals`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/hubspot/deals/*'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for HubSpot Companies
CREATE OR REPLACE EXTERNAL TABLE `executive_dash_raw.hubspot_companies`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/hubspot/companies/*'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);
