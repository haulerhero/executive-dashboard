-- ============================================================================
-- Gold Layer: Star Schema for Executive Dashboard
-- Purpose: Create analytics-ready fact and dimension tables
-- Source: silver.hubspot_*, silver.scope_*
-- ============================================================================

-- ============================================================================
-- DIMENSION: Customers
-- ============================================================================
-- Combines HubSpot and Scope customer records
-- Links deals to implementation projects via hubspot_id

CREATE OR REPLACE TABLE `gold.dim_customers` AS
WITH hubspot_base AS (
  SELECT
    hubspot_company_id,
    company_name as hubspot_company_name,
    domain,
    current_software,
    create_date as hubspot_created_at
  FROM `silver.hubspot_companies`
),
scope_base AS (
  SELECT
    scope_company_id,
    hubspot_company_id,
    company_name as scope_company_name,
    created_at as scope_created_at
  FROM `silver.scope_companies`
  WHERE hubspot_company_id IS NOT NULL
)
SELECT
  -- Primary key (use HubSpot ID as single source of truth)
  COALESCE(s.hubspot_company_id, h.hubspot_company_id) as customer_key,
  
  -- IDs for joins
  h.hubspot_company_id,
  s.scope_company_id,
  
  -- Customer information (prefer Scope name if exists, else HubSpot)
  COALESCE(s.scope_company_name, h.hubspot_company_name) as customer_name,
  h.domain,
  h.current_software,
  
  -- Dates
  h.hubspot_created_at,
  s.scope_created_at,
  
  -- Customer stage
  CASE
    WHEN s.scope_company_id IS NOT NULL THEN 'Implementation'
    WHEN h.hubspot_company_id IS NOT NULL THEN 'Sales Only'
    ELSE 'Unknown'
  END as customer_stage,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM hubspot_base h
FULL OUTER JOIN scope_base s ON h.hubspot_company_id = s.hubspot_company_id;


-- ============================================================================
-- DIMENSION: Users (Internal Team Members)
-- ============================================================================
-- Implementation Managers, Data Engineers, Sales Reps, etc.

CREATE OR REPLACE TABLE `gold.dim_users` AS
SELECT
  user_id,
  email,
  first_name,
  last_name,
  full_name,
  is_active,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as loaded_at
FROM `silver.scope_users`;


-- ============================================================================
-- DIMENSION: Task Metadata
-- ============================================================================

-- Task Statuses
CREATE OR REPLACE TABLE `gold.dim_task_statuses` AS
SELECT
  status_id,
  status_name,
  status_icon,
  status_state,  -- todo, in_progress, done
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as loaded_at
FROM `silver.scope_task_statuses`;

-- Task Types
CREATE OR REPLACE TABLE `gold.dim_task_types` AS
SELECT
  type_id,
  type_name,
  type_icon,
  created_at,
  updated_at,
  CURRENT_TIMESTAMP() as loaded_at
FROM `silver.scope_task_types`;


-- ============================================================================
-- FACT: Deals
-- ============================================================================
-- All closed deals from HubSpot (last 18 months)
-- This is the starting point for customer journey tracking

CREATE OR REPLACE TABLE `gold.fact_deals` AS
SELECT
  -- IDs
  d.deal_id,
  d.hubspot_company_id,
  c.customer_name,
  c.customer_stage,
  
  -- Deal information
  d.deal_name,
  d.deal_amount,
  d.implementation_cost,
  d.daily_routes,
  d.deal_stage,
  d.pipeline,
  
  -- Dates
  d.close_date,
  d.create_date,
  d.last_modified_date,
  
  -- Customer size cohort (for go-live expectations)
  CASE
    WHEN d.daily_routes <= 2 THEN '1-2 trucks'
    WHEN d.daily_routes <= 14 THEN '3-14 trucks'
    WHEN d.daily_routes <= 49 THEN '15-49 trucks'
    ELSE '50+ trucks'
  END as customer_size_cohort,
  
  -- Data requirements (based on current_software)
  CASE
    WHEN c.current_software IS NULL THEN 'Manual'
    WHEN c.current_software IN ('RouteWare', 'Soft-Pak', 'AMCS') THEN 'Known Legacy Software'
    ELSE 'Evaluation or Custom'
  END as data_requirement_type,
  
  -- Calculate expected go-live date based on cohorts from Executive Requirements
  -- This is a simplified version - should be expanded with actual business logic
  CASE
    -- 1-2 trucks, Manual: 1 week (worst case 4 weeks)
    WHEN d.daily_routes <= 2 AND c.current_software IS NULL 
      THEN DATE_ADD(d.close_date, INTERVAL 7 DAY)
    -- 1-2 trucks, Known Legacy: 4 weeks (worst case 6 weeks)
    WHEN d.daily_routes <= 2 AND c.current_software IS NOT NULL 
      THEN DATE_ADD(d.close_date, INTERVAL 28 DAY)
    -- 3-14 trucks, Manual: 2 weeks (worst case 6 weeks)
    WHEN d.daily_routes <= 14 AND c.current_software IS NULL
      THEN DATE_ADD(d.close_date, INTERVAL 14 DAY)
    -- 3-14 trucks, Known Legacy: 5 weeks (worst case 8 weeks)
    WHEN d.daily_routes <= 14 AND c.current_software IN ('RouteWare', 'Soft-Pak', 'AMCS')
      THEN DATE_ADD(d.close_date, INTERVAL 35 DAY)
    -- 3-14 trucks, Evaluation/Custom: 8 weeks (worst case 12 weeks)
    WHEN d.daily_routes <= 14
      THEN DATE_ADD(d.close_date, INTERVAL 56 DAY)
    -- 15-49 trucks, Known Legacy: 8 weeks (worst case 12 weeks)
    WHEN d.daily_routes <= 49 AND c.current_software IN ('RouteWare', 'Soft-Pak', 'AMCS')
      THEN DATE_ADD(d.close_date, INTERVAL 56 DAY)
    -- 15-49 trucks, Evaluation/Custom: 12 weeks (worst case 20 weeks)
    WHEN d.daily_routes <= 49
      THEN DATE_ADD(d.close_date, INTERVAL 84 DAY)
    -- 50+ trucks, Known Legacy: 16 weeks (worst case 24 weeks)
    WHEN c.current_software IN ('RouteWare', 'Soft-Pak', 'AMCS')
      THEN DATE_ADD(d.close_date, INTERVAL 112 DAY)
    -- 50+ trucks, Evaluation/Custom: 20 weeks (worst case 32 weeks)
    ELSE DATE_ADD(d.close_date, INTERVAL 140 DAY)
  END as expected_golive_date,
  
  -- Links
  d.hubspot_deal_url,
  d.hubspot_company_url,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `silver.hubspot_deals` d
LEFT JOIN `gold.dim_customers` c ON d.hubspot_company_id = c.hubspot_company_id;


-- ============================================================================
-- FACT: Implementation Milestones
-- ============================================================================
-- All tasks from Scope with full context for milestone tracking
-- This is where Welcome Call, Data Pull, Training, Go-Live, etc. are tracked

CREATE OR REPLACE TABLE `gold.fact_implementation_milestones` AS
SELECT
  -- IDs
  t.task_id,
  t.scope_company_id,
  c.hubspot_company_id,
  c.customer_name,
  c.customer_stage,
  
  -- Task information
  t.task_title,
  t.task_description,
  t.task_number,
  
  -- Task metadata (for filtering/grouping)
  ts.status_name as task_status,
  ts.status_state,
  tt.type_name as task_type,
  
  -- Assignment
  u.full_name as assigned_to,
  u.email as assigned_to_email,
  
  -- Dates
  t.timeframe_start,
  t.timeframe_end,
  t.due_date,
  t.completed_at,
  t.created_at,
  t.updated_at,
  
  -- Calculated metrics
  CASE 
    WHEN t.completed_at IS NOT NULL 
    THEN DATE_DIFF(DATE(t.completed_at), DATE(t.created_at), DAY)
    ELSE NULL
  END as days_to_complete,
  
  CASE
    WHEN t.completed_at IS NULL AND t.due_date < CURRENT_TIMESTAMP()
    THEN TRUE
    ELSE FALSE
  END as is_overdue,
  
  CASE
    WHEN t.completed_at IS NOT NULL THEN 'Completed'
    WHEN t.due_date < CURRENT_TIMESTAMP() THEN 'Overdue'
    WHEN ts.status_state = 'done' THEN 'Completed'
    WHEN ts.status_state = 'in_progress' THEN 'In Progress'
    ELSE 'Not Started'
  END as milestone_status,
  
  -- Milestone type identification (based on task title patterns)
  -- This should be expanded based on actual task naming conventions
  CASE
    WHEN LOWER(t.task_title) LIKE '%welcome call%' THEN 'Welcome Call'
    WHEN LOWER(t.task_title) LIKE '%im assignment%' OR LOWER(t.task_title) LIKE '%implementation manager%' THEN 'IM Assignment'
    WHEN LOWER(t.task_title) LIKE '%project plan%' THEN 'Project Plan'
    WHEN LOWER(t.task_title) LIKE '%data pull%' OR LOWER(t.task_title) LIKE '%data extract%' THEN 'Data Pull'
    WHEN LOWER(t.task_title) LIKE '%training%' THEN 'Training'
    WHEN LOWER(t.task_title) LIKE '%data sign%' OR LOWER(t.task_title) LIKE '%data approval%' THEN 'Data Sign Off'
    WHEN LOWER(t.task_title) LIKE '%go live%' OR LOWER(t.task_title) LIKE '%go-live%' THEN 'Go Live'
    WHEN LOWER(t.task_title) LIKE '%cs handoff%' OR LOWER(t.task_title) LIKE '%customer success%' THEN 'CS Handoff'
    WHEN LOWER(t.task_title) LIKE '%sales handoff%' THEN 'Sales Handoff'
    ELSE 'Other'
  END as milestone_type,
  
  -- Metadata
  CURRENT_TIMESTAMP() as loaded_at
FROM `silver.scope_tasks` t
LEFT JOIN `gold.dim_customers` c ON t.scope_company_id = c.scope_company_id
LEFT JOIN `gold.dim_task_statuses` ts ON t.task_status_id = ts.status_id
LEFT JOIN `gold.dim_task_types` tt ON t.task_type_id = tt.type_id
LEFT JOIN `gold.dim_users` u ON t.assigned_user_id = u.user_id;


-- ============================================================================
-- VIEW: Customer Journey Summary
-- ============================================================================
-- Aggregated view showing key milestones per customer
-- Used for executive reporting

CREATE OR REPLACE VIEW `gold.view_customer_journey_summary` AS
WITH milestone_dates AS (
  SELECT
    hubspot_company_id,
    MAX(CASE WHEN milestone_type = 'Welcome Call' AND completed_at IS NOT NULL THEN completed_at END) as welcome_call_date,
    MAX(CASE WHEN milestone_type = 'IM Assignment' AND completed_at IS NOT NULL THEN completed_at END) as im_assignment_date,
    MAX(CASE WHEN milestone_type = 'Project Plan' AND completed_at IS NOT NULL THEN completed_at END) as project_plan_date,
    MAX(CASE WHEN milestone_type = 'Data Pull' AND completed_at IS NOT NULL THEN completed_at END) as data_pull_date,
    MAX(CASE WHEN milestone_type = 'Training' AND completed_at IS NOT NULL THEN completed_at END) as training_start_date,
    MAX(CASE WHEN milestone_type = 'Data Sign Off' AND completed_at IS NOT NULL THEN completed_at END) as data_signoff_date,
    MAX(CASE WHEN milestone_type = 'Go Live' AND completed_at IS NOT NULL THEN completed_at END) as golive_date,
    MAX(CASE WHEN milestone_type = 'CS Handoff' AND completed_at IS NOT NULL THEN completed_at END) as cs_handoff_date
  FROM `gold.fact_implementation_milestones`
  WHERE milestone_type != 'Other'
  GROUP BY hubspot_company_id
)
SELECT
  d.deal_id,
  d.hubspot_company_id,
  d.customer_name,
  d.customer_size_cohort,
  d.data_requirement_type,
  d.deal_amount,
  d.implementation_cost,
  d.daily_routes,
  
  -- Key dates
  d.close_date,
  m.im_assignment_date,
  m.welcome_call_date,
  m.project_plan_date,
  m.data_pull_date,
  m.training_start_date,
  m.data_signoff_date,
  m.golive_date,
  m.cs_handoff_date,
  
  -- Expected vs actual
  d.expected_golive_date,
  CASE 
    WHEN m.golive_date IS NOT NULL 
    THEN DATE_DIFF(DATE(m.golive_date), DATE(d.expected_golive_date), DAY)
    ELSE NULL
  END as golive_variance_days,
  
  -- Status
  CASE
    WHEN m.cs_handoff_date IS NOT NULL THEN 'Complete'
    WHEN m.golive_date IS NOT NULL THEN 'Go Live Complete'
    WHEN m.welcome_call_date IS NOT NULL THEN 'In Progress'
    WHEN d.close_date IS NOT NULL THEN 'Pending Start'
    ELSE 'Unknown'
  END as implementation_status,
  
  -- Links
  d.hubspot_deal_url,
  d.hubspot_company_url,
  
  CURRENT_TIMESTAMP() as loaded_at
FROM `gold.fact_deals` d
LEFT JOIN milestone_dates m ON d.hubspot_company_id = m.hubspot_company_id
WHERE d.customer_stage = 'Implementation';  -- Only show customers in implementation
