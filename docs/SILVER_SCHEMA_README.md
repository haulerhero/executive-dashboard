# Silver Schema Documentation

## Overview
Created two separate silver schema files for independent execution and cleaner organization:
- `silver_hubspot.sql` - 2 tables (deals, companies)
- `silver_scope.sql` - 9 tables (full Scope data model)

## File: silver_hubspot.sql

### Tables Created:
1. **silver.hubspot_deals**
   - Source: raw.hubspot_deals
   - Filters: Latest run only, dedupe by most recent modification
   - Key fields: deal_id, hubspot_company_id, deal_amount, implementation_cost, daily_routes, close_date
   - Links: hubspot_deal_url, hubspot_company_url

2. **silver.hubspot_companies**
   - Source: raw.hubspot_companies
   - Filters: Latest run only, dedupe by most recent modification
   - Key fields: hubspot_company_id, company_name, domain, current_software
   - Note: Extraction pulls ALL properties, but silver schema only maps known critical fields

## File: silver_scope.sql

### Tables Created (organized by tier):

#### TIER 1: Core Customer and Reference Data
1. **silver.scope_companies**
   - Links to HubSpot via hubspot_company_id
   - Filters: Excludes archived, latest run
   
2. **silver.scope_users**
   - Internal Hauler Hero team members
   - Full name concatenation included
   
3. **silver.scope_task_statuses**
   - Reference data for task states
   
4. **silver.scope_task_types**
   - Reference data for task categories

#### TIER 2: Customer Relationship and Structure
5. **silver.scope_company_users**
   - External customer contacts
   - Links to scope_companies
   
6. **silver.scope_lists**
   - Implementation projects/work organization
   - Handles array fields: owner_user_ids, timeframe
   - Parses timeframe into start/end timestamps

#### TIER 3: Activity and Engagement (Critical for Dashboard)
7. **silver.scope_tasks**
   - Primary milestone tracking (Welcome Call, Data Pull, Training, Go-Live, etc.)
   - Handles array field: list_ids
   - Parses timeframe into start/end timestamps
   - Includes due_date and completed_at for milestone tracking
   - Links to: scope_companies, scope_users, scope_task_statuses, scope_task_types

#### TIER 4: Metadata
8. **silver.scope_field_groups**
   - Custom field organization
   
9. **silver.scope_fields**
   - Custom field definitions
   - Links to scope_field_groups
   
10. **silver.scope_tags**
    - Tag values for enrichment
    - Links to scope_fields

## Key Design Decisions

### Timestamp Handling
- All timestamps parsed to TIMESTAMP type using PARSE_TIMESTAMP
- Format: `%Y-%m-%dT%H:%M:%S` (handles ISO 8601 up to seconds)
- Handles both date-only and full datetime strings

### Array Field Handling
- Lists and Tasks have array fields (owner_user_ids, list_ids, timeframe)
- Stored as JSON for now (raw arrays)
- Timeframe arrays parsed into separate start/end timestamp columns for easier querying
- Consider unnesting in Gold layer for joins/analytics

### Deduplication
- All tables use QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
- Ensures only latest version of each record
- Latest run filtering via _FILE_NAME pattern matching

### Archived Records
- Scope uses soft deletes (archived? flag)
- Python extraction script filters these out at source
- Silver SQL adds COALESCE(archived, false) = false as safety check
- HubSpot deals already filtered by closedate window at extraction

### Run Filtering
- Each table selects from latest run only: `_FILE_NAME LIKE '%' || (SELECT MAX(run) FROM ...) || '%'`
- Prevents duplicate records from multiple extraction runs
- MAX(run) subquery finds most recent extraction timestamp

## Next Steps

### For Gold Layer:
You'll want to create fact and dimension tables that join across these sources:

1. **dim_customers** - Already started in your Gold_-_Star.sql
   - Join scope_companies with hubspot_companies on hubspot_id
   
2. **fact_milestones** - Track all key implementation events
   - Join scope_tasks with task_statuses, task_types, users
   - Pivot or filter for specific milestone types (Welcome Call, Data Pull, etc.)
   
3. **dim_users** - Internal team member dimension
   - Directly from scope_users with role enrichment
   
4. **bridge_task_list** - Many-to-many relationship
   - Unnest task.list_ids to create bridge table

### Array Field Considerations:
- **list_ids in tasks**: Consider unnesting for proper joins to lists
- **owner_user_ids in lists**: Consider unnesting for ownership analysis
- **timeframe arrays**: Already parsed to start/end columns for convenience

### Missing Data Points (from Executive Requirements):
These will likely come from task naming conventions or custom fields:
- Sales Handoff date
- IM Assignment date
- Welcome Call date
- Project Plan sent date
- Data Pull date
- Training dates
- Data Sign Off date
- Go-Live date
- CS Handoff date
- Tenant ID
- MRR (from HubSpot deals.deal_amount if recurring)
- Active Truck Count (may be in HubSpot company custom properties)

Recommend creating a mapping table in Gold to identify which task types/names correspond to which milestone events.
