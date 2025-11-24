-- ============================================================================
-- Raw Layer: Scope External Tables
-- Purpose: Create external tables pointing to GCS JSON data
-- Note: These tables query GCS directly (no data stored in BigQuery)
-- ============================================================================

-- External table for Scope Companies
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_companies`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/companies_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for Scope Users
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_users`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/users_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for Scope Task Statuses
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_task_statuses`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/task-statuses_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for Scope Task Types
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_task_types`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/task-types_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for Scope Company Users
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_company_users`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/company-users_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for Scope Lists
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_lists`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/lists_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for Scope Tasks
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_tasks`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/tasks_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for Scope Field Groups
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_field_groups`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/field-groups_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for Scope Fields
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_fields`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/fields_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for Scope Tags
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_tags`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/tags_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);
