-- ============================================================================
-- Gold Layer: Star Schema for Executive Dashboard
-- Purpose: Create analytics-ready fact and dimension tables
-- Source: executive_dash_silver.hubspot_*, executive_dash_silver.scope_*
-- ============================================================================

-- ============================================================================
-- DIMENSIONS
-- ============================================================================

-- Dimension: Customers
-- Combines HubSpot and Scope customer records via hubspot_id
CREATE OR REPLACE TABLE `executive_dash_gold.dim_customers` AS
WITH hubspot_base AS (
  SELECT
    CAST(hubspot_company_id AS STRING) as hubspot_company_id,
    company_name as hubspot_company_name,
    domain,
    create_date as hubspot_created_at
  FROM `executive_dash_silver.hubspot_companies`
),
scope_base AS (
  SELECT
    scope_company_id,
    CAST(hubspot_company_id AS STRING) as hubspot_company_id,
    company_name as scope_company_name
  FROM `executive_dash_silver.scope_companies`
)
SELECT
  COALESCE(s.hubspot_company_id, h.hubspot_company_id) as customer_key,
  h.hubspot_company_id,
  s.scope_company_id,
  COALESCE(s.scope_company_name, h.hubspot_company_name) as customer_name,
  h.domain,
  h.hubspot_created_at,
  CASE
    WHEN s.scope_company_id IS NOT NULL THEN 'Implementation'
    WHEN h.hubspot_company_id IS NOT NULL THEN 'Sales Only'
    ELSE 'Unknown'
  END as customer_stage,
  CURRENT_TIMESTAMP() as loaded_at
FROM hubspot_base h
FULL OUTER JOIN scope_base s ON h.hubspot_company_id = s.hubspot_company_id;


-- Dimension: Users (Internal Team Members)
CREATE OR REPLACE TABLE `executive_dash_gold.dim_users` AS
SELECT
  user_id,
  email,
  first_name,
  last_name,
  full_name,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_silver.scope_users`;


-- Dimension: Task Statuses
CREATE OR REPLACE TABLE `executive_dash_gold.dim_task_statuses` AS
SELECT
  status_id,
  status_name,
  status_icon,
  status_state,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_silver.scope_task_statuses`;


-- Dimension: Task Types
CREATE OR REPLACE TABLE `executive_dash_gold.dim_task_types` AS
SELECT
  type_id,
  type_name,
  type_icon,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_silver.scope_task_types`;


-- ============================================================================
-- FACTS
-- ============================================================================

-- Fact: Deals
CREATE OR REPLACE TABLE `executive_dash_gold.fact_deals` AS
SELECT
  d.deal_id,
  d.hubspot_company_id,
  c.customer_name,
  c.customer_stage,
  d.deal_name,
  d.deal_amount,
  d.implementation_cost,
  d.daily_routes,
  d.deal_stage,
  d.pipeline,
  d.close_date,
  d.create_date,
  d.last_modified_date,
  -- Customer size cohort
  CASE
    WHEN d.daily_routes <= 2 THEN '1-2 trucks'
    WHEN d.daily_routes <= 14 THEN '3-14 trucks'
    WHEN d.daily_routes <= 49 THEN '15-49 trucks'
    WHEN d.daily_routes > 49 THEN '50+ trucks'
    ELSE 'Unknown'
  END as customer_size_cohort,
  d.hubspot_deal_url,
  d.hubspot_company_url,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_silver.hubspot_deals` d
LEFT JOIN `executive_dash_gold.dim_customers` c
  ON CAST(d.hubspot_company_id AS STRING) = CAST(c.hubspot_company_id AS STRING);


-- Fact: Implementation Milestones (Tasks)
CREATE OR REPLACE TABLE `executive_dash_gold.fact_implementation_milestones` AS
SELECT
  t.task_id,
  t.scope_company_id,
  c.hubspot_company_id,
  c.customer_name,
  c.customer_stage,
  t.task_title,
  t.task_description,
  t.task_number,
  t.project_id,
  t.project_stage_id,
  -- Status
  ts.status_name as task_status,
  ts.status_state,
  -- Type
  tt.type_name as task_type,
  -- Assignment
  t.assigned_user_id,
  u.full_name as assigned_to,
  u.email as assigned_to_email,
  -- Created by
  t.created_by_user_id,
  cu.full_name as created_by,
  -- Closed by
  t.closed_by_user_id,
  clu.full_name as closed_by,
  -- Dates
  t.timeframe,
  t.created_at,
  t.completed_at,
  -- Calculated fields
  CASE
    WHEN t.completed_at IS NOT NULL THEN 'Completed'
    WHEN ts.status_state = 'done' THEN 'Completed'
    WHEN ts.status_state = 'in_progress' THEN 'In Progress'
    ELSE 'Not Started'
  END as milestone_status,
  -- Milestone type identification (based on task title patterns)
  -- Note: Order matters - more specific patterns must come before general ones
  CASE
    -- CS Handoff (check BEFORE kickoff patterns)
    WHEN LOWER(t.task_title) LIKE '%cs handoff%' OR LOWER(t.task_title) LIKE '%customer success handoff%' THEN 'CS Handoff'
    -- Welcome Call / Kickoff
    WHEN LOWER(t.task_title) LIKE '%welcome%call%' THEN 'Welcome Call'
    WHEN LOWER(t.task_title) LIKE '%discovery call%' THEN 'Welcome Call'
    WHEN LOWER(t.task_title) LIKE '%kickoff%' OR LOWER(t.task_title) LIKE '%kick-off%' THEN 'Welcome Call'
    -- Welcome Email
    WHEN LOWER(t.task_title) LIKE '%welcome%email%' THEN 'Welcome Email'
    -- IM Assignment
    WHEN LOWER(t.task_title) LIKE '%im assignment%' OR LOWER(t.task_title) LIKE '%implementation manager%' THEN 'IM Assignment'
    -- Project Plan
    WHEN LOWER(t.task_title) LIKE '%project plan%' THEN 'Project Plan'
    -- Data Pull / Import
    WHEN LOWER(t.task_title) LIKE '%data import%' THEN 'Data Pull'
    WHEN LOWER(t.task_title) LIKE '%data cut%' THEN 'Data Pull'
    WHEN LOWER(t.task_title) LIKE '%data complete%' THEN 'Data Pull'
    WHEN LOWER(t.task_title) LIKE '%data pull%' OR LOWER(t.task_title) LIKE '%data extract%' THEN 'Data Pull'
    -- Data Sign Off
    WHEN LOWER(t.task_title) LIKE '%data sign%' OR LOWER(t.task_title) LIKE '%data approval%' THEN 'Data Sign Off'
    -- Training
    WHEN LOWER(t.task_title) LIKE '%training%' THEN 'Training'
    -- Go Live
    WHEN LOWER(t.task_title) LIKE '%go live%' OR LOWER(t.task_title) LIKE '%go-live%' OR LOWER(t.task_title) LIKE '%golive%' THEN 'Go Live'
    -- Sales Handoff
    WHEN LOWER(t.task_title) LIKE '%sales handoff%' THEN 'Sales Handoff'
    ELSE 'Other'
  END as milestone_type,
  -- Metadata
  t.visibility,
  t.jira_key,
  t.list_ids,
  t.foreign_list_ids,
  CURRENT_TIMESTAMP() as loaded_at
FROM `executive_dash_silver.scope_tasks` t
LEFT JOIN `executive_dash_gold.dim_customers` c ON t.scope_company_id = c.scope_company_id
LEFT JOIN `executive_dash_gold.dim_task_statuses` ts ON t.task_status_id = ts.status_id
LEFT JOIN `executive_dash_gold.dim_task_types` tt ON t.task_type_id = tt.type_id
LEFT JOIN `executive_dash_gold.dim_users` u ON t.assigned_user_id = u.user_id
LEFT JOIN `executive_dash_gold.dim_users` cu ON t.created_by_user_id = cu.user_id
LEFT JOIN `executive_dash_gold.dim_users` clu ON t.closed_by_user_id = clu.user_id;


-- ============================================================================
-- VIEWS
-- ============================================================================

-- View: Customer Journey Summary
-- Aggregated view showing key milestones per customer for executive reporting
CREATE OR REPLACE VIEW `executive_dash_gold.view_customer_journey_summary` AS
WITH milestone_dates AS (
  SELECT
    scope_company_id,
    hubspot_company_id,
    MAX(CASE WHEN milestone_type = 'Welcome Email' AND completed_at IS NOT NULL THEN completed_at END) as welcome_email_date,
    MAX(CASE WHEN milestone_type = 'Welcome Call' AND completed_at IS NOT NULL THEN completed_at END) as welcome_call_date,
    MAX(CASE WHEN milestone_type = 'IM Assignment' AND completed_at IS NOT NULL THEN completed_at END) as im_assignment_date,
    MAX(CASE WHEN milestone_type = 'Project Plan' AND completed_at IS NOT NULL THEN completed_at END) as project_plan_date,
    MAX(CASE WHEN milestone_type = 'Data Pull' AND completed_at IS NOT NULL THEN completed_at END) as data_pull_date,
    MAX(CASE WHEN milestone_type = 'Training' AND completed_at IS NOT NULL THEN completed_at END) as training_date,
    MAX(CASE WHEN milestone_type = 'Data Sign Off' AND completed_at IS NOT NULL THEN completed_at END) as data_signoff_date,
    MAX(CASE WHEN milestone_type = 'Go Live' AND completed_at IS NOT NULL THEN completed_at END) as golive_date,
    MAX(CASE WHEN milestone_type = 'CS Handoff' AND completed_at IS NOT NULL THEN completed_at END) as cs_handoff_date
  FROM `executive_dash_gold.fact_implementation_milestones`
  WHERE milestone_type != 'Other'
  GROUP BY scope_company_id, hubspot_company_id
)
SELECT
  d.deal_id,
  d.hubspot_company_id,
  c.scope_company_id,
  d.customer_name,
  d.customer_size_cohort,
  d.deal_amount,
  d.implementation_cost,
  d.daily_routes,
  -- Key dates from deal
  d.close_date,
  -- Milestone dates from tasks
  m.welcome_email_date,
  m.im_assignment_date,
  m.welcome_call_date,
  m.project_plan_date,
  m.data_pull_date,
  m.training_date,
  m.data_signoff_date,
  m.golive_date,
  m.cs_handoff_date,
  -- Implementation status
  CASE
    WHEN m.cs_handoff_date IS NOT NULL THEN 'Complete'
    WHEN m.golive_date IS NOT NULL THEN 'Go Live Complete'
    WHEN m.welcome_call_date IS NOT NULL THEN 'In Progress'
    WHEN m.welcome_email_date IS NOT NULL THEN 'Started'
    WHEN d.close_date IS NOT NULL THEN 'Pending Start'
    ELSE 'Unknown'
  END as implementation_status,
  -- Days calculations
  DATE_DIFF(DATE(m.golive_date), DATE(d.close_date), DAY) as days_close_to_golive,
  DATE_DIFF(DATE(m.cs_handoff_date), DATE(m.golive_date), DAY) as days_golive_to_cs,
  -- Links
  d.hubspot_deal_url,
  d.hubspot_company_url
FROM `executive_dash_gold.fact_deals` d
LEFT JOIN `executive_dash_gold.dim_customers` c
  ON CAST(d.hubspot_company_id AS STRING) = CAST(c.hubspot_company_id AS STRING)
LEFT JOIN milestone_dates m
  ON c.scope_company_id = m.scope_company_id;