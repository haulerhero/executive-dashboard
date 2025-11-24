-- ============================================================================
-- Silver Layer: Scope Data
-- Purpose: Clean and standardize Scope project management data
-- Source: raw.scope_* (9 entity types)
-- Note: Scope uses soft deletes (archived? flag), filtered at extraction
-- ============================================================================

-- ============================================================================
-- TIER 1: Core Customer and Reference Data
-- ============================================================================

-- Silver: Scope Companies
-- Customer implementations being managed in Scope
-- Links to HubSpot via hubspot_id field
CREATE OR REPLACE TABLE `executive_dash_silver.scope_companies` AS
SELECT
  -- IDs and relationships
  id as scope_company_id,
  hubspot_id as hubspot_company_id,  -- Link to HubSpot
  
  -- Company information
  name as company_name,
  description as company_description,
  
  -- Dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(created_at, 1, 19)) as created_at,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(updated_at, 1, 19)) as updated_at,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_companies`
WHERE id IS NOT NULL
  AND COALESCE(archived, false) = false  -- Exclude archived
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `executive_dash_raw.scope_companies`) || '%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;


-- Silver: Scope Users
-- Internal Hauler Hero team members (Implementation Managers, Data Engineers, etc.)
CREATE OR REPLACE TABLE `executive_dash_silver.scope_users` AS
SELECT
  -- IDs
  id as user_id,
  
  -- User information
  email,
  first_name,
  last_name,
  CONCAT(first_name, ' ', last_name) as full_name,
  
  -- Status
  COALESCE(active, true) as is_active,
  
  -- Dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(created_at, 1, 19)) as created_at,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(updated_at, 1, 19)) as updated_at,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_users`
WHERE id IS NOT NULL
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `executive_dash_raw.scope_users`) || '%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;


-- Silver: Scope Task Statuses
-- Reference data for task status values
CREATE OR REPLACE TABLE `executive_dash_silver.scope_task_statuses` AS
SELECT
  -- IDs
  id as status_id,
  
  -- Status information
  name as status_name,
  icon as status_icon,
  state as status_state,  -- e.g., "todo", "in_progress", "done"
  
  -- Dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(created_at, 1, 19)) as created_at,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(updated_at, 1, 19)) as updated_at,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_task_statuses`
WHERE id IS NOT NULL
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `executive_dash_raw.scope_task_statuses`) || '%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;


-- Silver: Scope Task Types
-- Reference data for task categories
CREATE OR REPLACE TABLE `executive_dash_silver.scope_task_types` AS
SELECT
  -- IDs
  id as type_id,
  
  -- Type information
  name as type_name,
  icon as type_icon,
  
  -- Dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(created_at, 1, 19)) as created_at,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(updated_at, 1, 19)) as updated_at,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_task_types`
WHERE id IS NOT NULL
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `executive_dash_raw.scope_task_types`) || '%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;


-- ============================================================================
-- TIER 2: Customer Relationship and Structure Data
-- ============================================================================

-- Silver: Scope Company Users
-- External customer contacts linked to companies
CREATE OR REPLACE TABLE `executive_dash_silver.scope_company_users` AS
SELECT
  -- IDs and relationships
  id as company_user_id,
  company_id as scope_company_id,  -- Link to scope_companies
  
  -- Contact information
  email,
  first_name,
  last_name,
  CONCAT(first_name, ' ', last_name) as full_name,
  
  -- Dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(created_at, 1, 19)) as created_at,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(updated_at, 1, 19)) as updated_at,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_company_users`
WHERE id IS NOT NULL
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `executive_dash_raw.scope_company_users`) || '%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;


-- Silver: Scope Lists
-- Project/work organization structure (Lists = Implementation Projects)
CREATE OR REPLACE TABLE `executive_dash_silver.scope_lists` AS
SELECT
  -- IDs
  id as list_id,
  subject_id as project_id,  -- Project identifier (e.g., "SCO")
  
  -- List information
  name as list_name,
  
  -- Ownership (array field - stored as JSON string for now)
  owner_user_ids,
  
  -- Timeframe (array field - [start, end] timestamps)
  timeframe,
  
  -- Parse timeframe start/end if present
  CASE 
    WHEN timeframe IS NOT NULL AND JSON_ARRAY_LENGTH(timeframe) >= 1
    THEN PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(JSON_EXTRACT_SCALAR(timeframe, '$[0]'), 1, 19))
    ELSE NULL
  END as timeframe_start,
  CASE 
    WHEN timeframe IS NOT NULL AND JSON_ARRAY_LENGTH(timeframe) >= 2
    THEN PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(JSON_EXTRACT_SCALAR(timeframe, '$[1]'), 1, 19))
    ELSE NULL
  END as timeframe_end,
  
  -- Status
  COALESCE(published, false) as is_published,
  
  -- Dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(created_at, 1, 19)) as created_at,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(updated_at, 1, 19)) as updated_at,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_lists`
WHERE id IS NOT NULL
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `executive_dash_raw.scope_lists`) || '%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;


-- ============================================================================
-- TIER 3: Activity and Engagement Data (Customer Health Indicators)
-- ============================================================================

-- Silver: Scope Tasks
-- Primary activity/engagement data for milestone tracking
-- These are the key actions (Welcome Call, Data Pull, Training, Go-Live, etc.)
CREATE OR REPLACE TABLE `executive_dash_silver.scope_tasks` AS
SELECT
  -- IDs and relationships
  id as task_id,
  company_id as scope_company_id,  -- Link to scope_companies
  project_id,  -- Project identifier
  user_id as assigned_user_id,  -- Link to scope_users
  status_id as task_status_id,  -- Link to scope_task_statuses
  type_id as task_type_id,  -- Link to scope_task_types
  
  -- Task can be in multiple lists (array field)
  list_ids,
  
  -- Task information
  COALESCE(title, name) as task_title,
  description as task_description,
  
  -- Number (for sorting/display)
  number as task_number,
  
  -- Timeframe (array field - [start, end] timestamps)
  timeframe,
  
  -- Parse timeframe start/end if present
  CASE 
    WHEN timeframe IS NOT NULL AND JSON_ARRAY_LENGTH(timeframe) >= 1
    THEN PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(JSON_EXTRACT_SCALAR(timeframe, '$[0]'), 1, 19))
    ELSE NULL
  END as timeframe_start,
  CASE 
    WHEN timeframe IS NOT NULL AND JSON_ARRAY_LENGTH(timeframe) >= 2
    THEN PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(JSON_EXTRACT_SCALAR(timeframe, '$[1]'), 1, 19))
    ELSE NULL
  END as timeframe_end,
  
  -- Dates (task-specific)
  CASE
    WHEN due_date IS NOT NULL THEN PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(due_date, 1, 19))
    ELSE NULL
  END as due_date,
  CASE
    WHEN completed_at IS NOT NULL THEN PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(completed_at, 1, 19))
    ELSE NULL
  END as completed_at,
  
  -- Standard dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(created_at, 1, 19)) as created_at,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(updated_at, 1, 19)) as updated_at,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_tasks`
WHERE id IS NOT NULL
  AND COALESCE(archived, false) = false
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `executive_dash_raw.scope_tasks`) || '%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;


-- ============================================================================
-- TIER 4: Metadata (Lower priority for dashboard, useful for context)
-- ============================================================================

-- Silver: Scope Field Groups
-- Organization of custom fields
CREATE OR REPLACE TABLE `executive_dash_silver.scope_field_groups` AS
SELECT
  -- IDs
  id as field_group_id,
  
  -- Field group information
  name as field_group_name,
  
  -- Dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(created_at, 1, 19)) as created_at,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(updated_at, 1, 19)) as updated_at,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_field_groups`
WHERE id IS NOT NULL
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `executive_dash_raw.scope_field_groups`) || '%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;


-- Silver: Scope Fields
-- Custom field definitions
CREATE OR REPLACE TABLE `executive_dash_silver.scope_fields` AS
SELECT
  -- IDs and relationships
  id as field_id,
  field_group_id,  -- Link to scope_field_groups
  
  -- Field information
  name as field_name,
  
  -- Dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(created_at, 1, 19)) as created_at,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(updated_at, 1, 19)) as updated_at,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_fields`
WHERE id IS NOT NULL
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `executive_dash_raw.scope_fields`) || '%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;


-- Silver: Scope Tags
-- Tag values for additional customer context
CREATE OR REPLACE TABLE `executive_dash_silver.scope_tags` AS
SELECT
  -- IDs and relationships
  id as tag_id,
  field_id,  -- Link to scope_fields
  
  -- Tag information
  name as tag_name,
  color as tag_color,
  
  -- Dates
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(created_at, 1, 19)) as created_at,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', SUBSTR(updated_at, 1, 19)) as updated_at,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_raw.scope_tags`
WHERE id IS NOT NULL
  AND _FILE_NAME LIKE '%' || (SELECT MAX(run) FROM `executive_dash_raw.scope_tags`) || '%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;
