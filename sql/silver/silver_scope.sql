-- ============================================================================
-- Silver Layer: Scope Data
-- Purpose: Clean and standardize Scope project management data
-- Source: executive_dash_raw.scope_* (7 active entity types)
-- Note: field_groups, fields, and tags skipped - no API access currently
-- ============================================================================

-- ============================================================================
-- TIER 1: Core Customer and Reference Data
-- ============================================================================

-- Silver: Scope Companies
-- Customer implementations being managed in Scope
-- Links to HubSpot via hubspot_id field
CREATE OR REPLACE TABLE `executive_dash_silver.scope_companies` AS
SELECT
  id as scope_company_id,
  hubspot_id as hubspot_company_id,
  name as company_name,
  description as company_description,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_companies`
WHERE id IS NOT NULL
  AND COALESCE(is_archived, false) = false
  AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
    SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
    FROM `executive_dash_raw.scope_companies`
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;


-- Silver: Scope Users
-- Internal Hauler Hero team members (Implementation Managers, Data Engineers, etc.)
CREATE OR REPLACE TABLE `executive_dash_silver.scope_users` AS
SELECT
  id as user_id,
  email,
  first_name,
  last_name,
  CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')) as full_name,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_users`
WHERE id IS NOT NULL
  AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
    SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
    FROM `executive_dash_raw.scope_users`
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;


-- Silver: Scope Task Statuses
-- Reference data for task status values
CREATE OR REPLACE TABLE `executive_dash_silver.scope_task_statuses` AS
SELECT
  id as status_id,
  name as status_name,
  icon as status_icon,
  state as status_state,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_task_statuses`
WHERE id IS NOT NULL
  AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
    SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
    FROM `executive_dash_raw.scope_task_statuses`
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;


-- Silver: Scope Task Types
-- Reference data for task categories
CREATE OR REPLACE TABLE `executive_dash_silver.scope_task_types` AS
SELECT
  id as type_id,
  name as type_name,
  icon as type_icon,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_task_types`
WHERE id IS NOT NULL
  AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
    SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
    FROM `executive_dash_raw.scope_task_types`
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;


-- ============================================================================
-- TIER 2: Customer Relationship and Structure Data
-- ============================================================================

-- Silver: Scope Company Users
-- External customer contacts linked to companies
CREATE OR REPLACE TABLE `executive_dash_silver.scope_company_users` AS
SELECT
  id as company_user_id,
  company_id as scope_company_id,
  email,
  first_name,
  last_name,
  CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')) as full_name,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_company_users`
WHERE id IS NOT NULL
  AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
    SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
    FROM `executive_dash_raw.scope_company_users`
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;


-- Silver: Scope Lists
-- Project/work organization structure (Lists = Implementation Projects)
CREATE OR REPLACE TABLE `executive_dash_silver.scope_lists` AS
SELECT
  id as list_id,
  subject_id as project_id,
  name as list_name,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_lists`
WHERE id IS NOT NULL
  AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
    SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
    FROM `executive_dash_raw.scope_lists`
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;


-- ============================================================================
-- TIER 3: Activity and Engagement Data (Customer Health Indicators)
-- ============================================================================

-- Silver: Scope Tasks
-- Primary activity/engagement data for milestone tracking
-- These are the key actions (Welcome Call, Data Pull, Training, Go-Live, etc.)
CREATE OR REPLACE TABLE `executive_dash_silver.scope_tasks` AS
SELECT
  id as task_id,
  assigned_company_id as scope_company_id,
  project_id,
  project_stage_id,
  assigned_user_id,
  created_by_user_id,
  closed_by_user_id,
  status_id as task_status_id,
  type_id as task_type_id,
  name as task_title,
  description as task_description,
  number as task_number,
  visibility,
  jira_key,
  timeframe,
  list_ids,
  foreign_list_ids,
  creation_date as created_at,
  close_date as completed_at,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_tasks`
WHERE id IS NOT NULL
  AND COALESCE(is_archived, false) = false
  AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
    SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
    FROM `executive_dash_raw.scope_tasks`
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;


-- ============================================================================
-- TIER 4: Metadata (Skipped - no API access currently)
-- ============================================================================

-- TODO: Uncomment when field_groups, fields, tags API access is available

-- -- Silver: Scope Field Groups
-- CREATE OR REPLACE TABLE `executive_dash_silver.scope_field_groups` AS
-- SELECT
--   id as field_group_id,
--   name as field_group_name,
--   CURRENT_TIMESTAMP() as loaded_at
-- FROM `executive_dash_raw.scope_field_groups`
-- WHERE id IS NOT NULL
--   AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
--     SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
--     FROM `executive_dash_raw.scope_field_groups`
--   )
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;


-- -- Silver: Scope Fields
-- CREATE OR REPLACE TABLE `executive_dash_silver.scope_fields` AS
-- SELECT
--   id as field_id,
--   field_group_id,
--   name as field_name,
--   CURRENT_TIMESTAMP() as loaded_at
-- FROM `executive_dash_raw.scope_fields`
-- WHERE id IS NOT NULL
--   AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
--     SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
--     FROM `executive_dash_raw.scope_fields`
--   )
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;


-- -- Silver: Scope Tags
-- CREATE OR REPLACE TABLE `executive_dash_silver.scope_tags` AS
-- SELECT
--   id as tag_id,
--   field_id,
--   name as tag_name,
--   color as tag_color,
--   CURRENT_TIMESTAMP() as loaded_at
-- FROM `executive_dash_raw.scope_tags`
-- WHERE id IS NOT NULL
--   AND REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)') = (
--     SELECT MAX(REGEXP_EXTRACT(_FILE_NAME, r'run=([^/]+)'))
--     FROM `executive_dash_raw.scope_tags`
--   )
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1;