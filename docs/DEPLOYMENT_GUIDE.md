# Executive Dashboard Automated Pipeline
## Complete Deployment Guide

## Overview
Automated twice-daily pipeline extracting data from HubSpot and Scope APIs, transforming in BigQuery, and serving implementation tracking metrics.

**Data Flow:**
```
HubSpot API  ──┐
               ├──> GCS (Raw NDJSON) ──> BigQuery Raw ──> Silver ──> Gold ──> Sigma Dashboard
Scope API  ───┘
```

**Refresh Schedule:**
- **6am & 6pm PT**: Extract from APIs → GCS
- **7am & 7pm PT**: Transform Raw → Silver → Gold
- **Total pipeline time**: ~30 minutes end-to-end

---

## Architecture Components

### 1. Data Sources
- **HubSpot**: Deals + Companies (closed deals in last 18 months)
- **Scope**: 9 entity types (companies, tasks, users, lists, statuses, types, company_users, field_groups, fields, tags)
- **Join Key**: `hubspot_id` links Scope companies to HubSpot companies

### 2. Storage Layers
```
GCS Bucket: scope-ws-extract/
├── raw/
│   ├── hubspot/
│   │   ├── deals/dt=2024-11-24/hr=06/run=20241124T060000Z/part-00000.json.gz
│   │   └── companies/dt=2024-11-24/hr=06/run=20241124T060000Z/part-00000.json.gz
│   └── scope/
│       ├── companies_search/dt=2024-11-24/hr=06/run=20241124T060000Z/part-00000.json.gz
│       ├── tasks_search/...
│       └── [7 more entity types...]
└── state/
    └── scope/
        ├── companies_search.json  (watermark timestamps)
        └── [8 more state files...]

BigQuery:
├── raw dataset (External tables pointing to GCS)
├── silver dataset (Cleaned, typed tables)
└── gold dataset (Star schema for analytics)
```

### 3. Compute
- **Cloud Run Jobs**: Execute Python extraction scripts
- **BigQuery Scheduled Queries**: Run SQL transformations
- **Cloud Scheduler**: Trigger jobs on schedule

---

## Step-by-Step Deployment

### Phase 1: GitHub Repository Setup

```bash
# Repository structure
executive-dashboard/
├── extraction/
│   ├── hubspot_pull.py
│   ├── scope_pull.py
│   └── requirements.txt
├── sql/
│   ├── raw/
│   │   ├── hubspot_external_tables.sql
│   │   └── scope_external_tables.sql
│   ├── silver/
│   │   ├── silver_hubspot.sql
│   │   └── silver_scope.sql
│   └── gold/
│       └── gold_star_schema.sql
├── docs/
│   ├── EXECUTIVE_REQUIREMENTS.md
│   └── PIPELINE_ARCHITECTURE.md
└── README.md
```

**Initialize repository:**
```bash
cd /path/to/project
git init
git remote add origin https://github.com/your-org/executive-dashboard.git

# Create structure
mkdir -p extraction sql/{raw,silver,gold} docs

# Copy files
cp Hubspot_Pull.py extraction/hubspot_pull.py
cp Scope_Pull.py extraction/scope_pull.py
cp silver_hubspot.sql sql/silver/
cp silver_scope.sql sql/silver/
cp "Executive Reporting Requirements [Onboarding].docx" docs/EXECUTIVE_REQUIREMENTS.md

# Initial commit
git add .
git commit -m "Initial pipeline setup: extraction scripts and silver schemas"
git push -u origin main
```

---

### Phase 2: GCP Infrastructure Setup

#### 2.1 Enable APIs and Create Resources

```bash
# Set project
export PROJECT_ID="your-gcp-project"
export REGION="us-central1"
gcloud config set project $PROJECT_ID

# Enable required APIs
gcloud services enable \
  secretmanager.googleapis.com \
  run.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  cloudscheduler.googleapis.com \
  pubsub.googleapis.com

# Create GCS bucket
gsutil mb -l $REGION gs://scope-ws-extract

# Create BigQuery datasets
bq mk --location=$REGION raw
bq mk --location=$REGION silver
bq mk --location=$REGION gold
```

#### 2.2 Store API Secrets

```bash
# HubSpot API token
echo -n "your-hubspot-private-app-token" | \
  gcloud secrets create global-hubspot-api \
    --data-file=- \
    --replication-policy="automatic"

# Scope API token
echo -n "your-scope-api-token" | \
  gcloud secrets create scope-ws-api-key \
    --data-file=- \
    --replication-policy="automatic"
```

#### 2.3 Create Service Account

```bash
# Create service account for Cloud Run jobs
gcloud iam service-accounts create pipeline-runner \
  --display-name="Executive Dashboard Pipeline Runner"

export SA_EMAIL="pipeline-runner@${PROJECT_ID}.iam.gserviceaccount.com"

# Grant permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser"
```

---

### Phase 3: Deploy Extraction Jobs

#### 3.1 Create Dockerfiles

**extraction/Dockerfile.hubspot:**
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy extraction script
COPY hubspot_pull.py .

# Set entrypoint
CMD ["python", "hubspot_pull.py"]
```

**extraction/Dockerfile.scope:**
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy extraction script
COPY scope_pull.py .

# Set entrypoint
CMD ["python", "scope_pull.py"]
```

**extraction/requirements.txt:**
```
requests==2.31.0
google-cloud-storage==2.10.0
google-cloud-secret-manager==2.16.4
```

#### 3.2 Build and Deploy Cloud Run Jobs

```bash
cd extraction

# Build and push HubSpot extractor
gcloud builds submit \
  --tag gcr.io/${PROJECT_ID}/hubspot-extractor \
  --file Dockerfile.hubspot

gcloud run jobs create hubspot-extraction \
  --image gcr.io/${PROJECT_ID}/hubspot-extractor \
  --service-account $SA_EMAIL \
  --region $REGION \
  --max-retries 2 \
  --task-timeout 10m \
  --set-env-vars "BUCKET=scope-ws-extract,PORTAL_ID=20174054,GCP_PROJECT=${PROJECT_ID}"

# Build and push Scope extractor
gcloud builds submit \
  --tag gcr.io/${PROJECT_ID}/scope-extractor \
  --file Dockerfile.scope

gcloud run jobs create scope-extraction \
  --image gcr.io/${PROJECT_ID}/scope-extractor \
  --service-account $SA_EMAIL \
  --region $REGION \
  --max-retries 2 \
  --task-timeout 15m \
  --set-env-vars "BUCKET=scope-ws-extract,API_BASE=https://api.scope.ws,GCP_PROJECT=${PROJECT_ID}"
```

---

### Phase 4: BigQuery Raw Layer (External Tables)

#### 4.1 Create External Tables SQL

**sql/raw/hubspot_external_tables.sql:**
```sql
-- External table for HubSpot Deals
CREATE OR REPLACE EXTERNAL TABLE `raw.hubspot_deals`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/hubspot/deals/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);

-- External table for HubSpot Companies
CREATE OR REPLACE EXTERNAL TABLE `raw.hubspot_companies`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/hubspot/companies/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true,
  max_bad_records = 100
);
```

**sql/raw/scope_external_tables.sql:**
```sql
-- External tables for all 9 Scope entity types
CREATE OR REPLACE EXTERNAL TABLE `raw.scope_companies`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/companies_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true
);

CREATE OR REPLACE EXTERNAL TABLE `raw.scope_users`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/users_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true
);

CREATE OR REPLACE EXTERNAL TABLE `raw.scope_task_statuses`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/task-statuses_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true
);

CREATE OR REPLACE EXTERNAL TABLE `raw.scope_task_types`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/task-types_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true
);

CREATE OR REPLACE EXTERNAL TABLE `raw.scope_company_users`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/company-users_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true
);

CREATE OR REPLACE EXTERNAL TABLE `raw.scope_lists`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/lists_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true
);

CREATE OR REPLACE EXTERNAL TABLE `raw.scope_tasks`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/tasks_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true
);

CREATE OR REPLACE EXTERNAL TABLE `raw.scope_field_groups`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/field-groups_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true
);

CREATE OR REPLACE EXTERNAL TABLE `raw.scope_fields`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/fields_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true
);

CREATE OR REPLACE EXTERNAL TABLE `raw.scope_tags`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://scope-ws-extract/raw/scope/tags_search/*/*.json.gz'],
  compression = 'GZIP',
  ignore_unknown_values = true
);
```

#### 4.2 Execute External Table Creation

```bash
# Create HubSpot external tables
bq query --use_legacy_sql=false < sql/raw/hubspot_external_tables.sql

# Create Scope external tables
bq query --use_legacy_sql=false < sql/raw/scope_external_tables.sql
```

---

### Phase 5: BigQuery Scheduled Transformations

#### 5.1 Create Scheduled Query for Silver Layer

```bash
# Create scheduled query for HubSpot Silver transformations
bq mk \
  --transfer_config \
  --project_id=$PROJECT_ID \
  --data_source=scheduled_query \
  --display_name="Silver: HubSpot" \
  --schedule="0 7,19 * * *" \
  --target_dataset=silver \
  --params='{
    "query":"'"$(cat sql/silver/silver_hubspot.sql)"'",
    "destination_table_name_template":"",
    "write_disposition":"WRITE_TRUNCATE",
    "partitioning_type":""
  }'

# Create scheduled query for Scope Silver transformations
bq mk \
  --transfer_config \
  --project_id=$PROJECT_ID \
  --data_source=scheduled_query \
  --display_name="Silver: Scope" \
  --schedule="5 7,19 * * *" \
  --target_dataset=silver \
  --params='{
    "query":"'"$(cat sql/silver/silver_scope.sql)"'",
    "destination_table_name_template":"",
    "write_disposition":"WRITE_TRUNCATE",
    "partitioning_type":""
  }'
```

#### 5.2 Create Scheduled Query for Gold Layer

**sql/gold/gold_star_schema.sql** (basic version - expand based on requirements):
```sql
-- Customer Dimension
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
  COALESCE(s.hubspot_company_id, h.hubspot_company_id) as customer_key,
  h.hubspot_company_id,
  s.scope_company_id,
  COALESCE(s.scope_company_name, h.hubspot_company_name) as customer_name,
  h.domain,
  h.current_software,
  h.hubspot_created_at,
  s.scope_created_at,
  CASE
    WHEN s.scope_company_id IS NOT NULL THEN 'Implementation'
    WHEN h.hubspot_company_id IS NOT NULL THEN 'Sales Only'
    ELSE 'Unknown'
  END as customer_stage,
  CURRENT_TIMESTAMP() as loaded_at
FROM hubspot_base h
FULL OUTER JOIN scope_base s ON h.hubspot_company_id = s.hubspot_company_id;

-- Implementation Milestone Facts
CREATE OR REPLACE TABLE `gold.fact_implementation_milestones` AS
SELECT
  t.task_id,
  t.scope_company_id,
  c.hubspot_company_id,
  c.customer_name,
  t.task_title,
  t.task_description,
  ts.status_name as task_status,
  ts.status_state,
  tt.type_name as task_type,
  u.full_name as assigned_to,
  t.timeframe_start,
  t.timeframe_end,
  t.due_date,
  t.completed_at,
  t.created_at,
  t.updated_at,
  -- Calculate days to complete
  CASE 
    WHEN t.completed_at IS NOT NULL 
    THEN DATE_DIFF(DATE(t.completed_at), DATE(t.created_at), DAY)
    ELSE NULL
  END as days_to_complete,
  -- Calculate if overdue
  CASE
    WHEN t.completed_at IS NULL AND t.due_date < CURRENT_TIMESTAMP()
    THEN TRUE
    ELSE FALSE
  END as is_overdue,
  CURRENT_TIMESTAMP() as loaded_at
FROM `silver.scope_tasks` t
LEFT JOIN `gold.dim_customers` c ON t.scope_company_id = c.scope_company_id
LEFT JOIN `silver.scope_task_statuses` ts ON t.task_status_id = ts.status_id
LEFT JOIN `silver.scope_task_types` tt ON t.task_type_id = tt.type_id
LEFT JOIN `silver.scope_users` u ON t.assigned_user_id = u.user_id;

-- Deal Facts
CREATE OR REPLACE TABLE `gold.fact_deals` AS
SELECT
  d.deal_id,
  d.hubspot_company_id,
  c.customer_name,
  d.deal_name,
  d.deal_amount,
  d.implementation_cost,
  d.daily_routes,
  d.deal_stage,
  d.pipeline,
  d.close_date,
  d.create_date,
  d.last_modified_date,
  -- Calculate expected go-live based on close date + cohort timing
  -- This is placeholder - needs business logic based on customer size
  DATE_ADD(d.close_date, INTERVAL 30 DAY) as expected_golive_date,
  CURRENT_TIMESTAMP() as loaded_at
FROM `silver.hubspot_deals` d
LEFT JOIN `gold.dim_customers` c ON d.hubspot_company_id = c.hubspot_company_id;
```

Deploy Gold scheduled query:
```bash
bq mk \
  --transfer_config \
  --project_id=$PROJECT_ID \
  --data_source=scheduled_query \
  --display_name="Gold: Star Schema" \
  --schedule="10 7,19 * * *" \
  --target_dataset=gold \
  --params='{
    "query":"'"$(cat sql/gold/gold_star_schema.sql)"'",
    "destination_table_name_template":"",
    "write_disposition":"WRITE_TRUNCATE",
    "partitioning_type":""
  }'
```

---

### Phase 6: Orchestrate with Cloud Scheduler

#### 6.1 Create Extraction Schedules

```bash
# HubSpot extraction at 6am and 6pm PT (14:00 and 02:00 UTC)
gcloud scheduler jobs create http hubspot-extraction-morning \
  --location=$REGION \
  --schedule="0 14 * * *" \
  --time-zone="America/Los_Angeles" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/hubspot-extraction:run" \
  --http-method=POST \
  --oauth-service-account-email=$SA_EMAIL

gcloud scheduler jobs create http hubspot-extraction-evening \
  --location=$REGION \
  --schedule="0 2 * * *" \
  --time-zone="America/Los_Angeles" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/hubspot-extraction:run" \
  --http-method=POST \
  --oauth-service-account-email=$SA_EMAIL

# Scope extraction at 6am and 6pm PT
gcloud scheduler jobs create http scope-extraction-morning \
  --location=$REGION \
  --schedule="0 14 * * *" \
  --time-zone="America/Los_Angeles" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/scope-extraction:run" \
  --http-method=POST \
  --oauth-service-account-email=$SA_EMAIL

gcloud scheduler jobs create http scope-extraction-evening \
  --location=$REGION \
  --schedule="0 2 * * *" \
  --time-zone="America/Los_Angeles" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/scope-extraction:run" \
  --http-method=POST \
  --oauth-service-account-email=$SA_EMAIL
```

**Note**: BigQuery scheduled queries run at 7am and 7pm PT automatically (configured in Phase 5).

---

### Phase 7: Sigma Dashboard Connection

#### 7.1 Create Service Account for Sigma

```bash
# Create Sigma-specific service account
gcloud iam service-accounts create sigma-dashboard-reader \
  --display-name="Sigma Dashboard Read-Only Access"

export SIGMA_SA_EMAIL="sigma-dashboard-reader@${PROJECT_ID}.iam.gserviceaccount.com"

# Grant BigQuery read permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SIGMA_SA_EMAIL}" \
  --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SIGMA_SA_EMAIL}" \
  --role="roles/bigquery.jobUser"

# Create and download key
gcloud iam service-accounts keys create sigma-key.json \
  --iam-account=$SIGMA_SA_EMAIL
```

#### 7.2 Configure Sigma Connection

1. In Sigma, go to **Connections** → **Add Connection**
2. Select **Google BigQuery**
3. Upload `sigma-key.json`
4. Set default dataset to `gold`
5. Test connection

---

## Testing and Validation

### Test 1: Manual Extraction Run

```bash
# Run HubSpot extraction manually
gcloud run jobs execute hubspot-extraction --region=$REGION --wait

# Run Scope extraction manually
gcloud run jobs execute scope-extraction --region=$REGION --wait

# Check GCS for output
gsutil ls gs://scope-ws-extract/raw/hubspot/deals/
gsutil ls gs://scope-ws-extract/raw/scope/companies_search/
```

### Test 2: Verify External Tables

```bash
# Query external tables
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM raw.hubspot_deals"
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM raw.scope_companies"
```

### Test 3: Run Silver Transformations

```bash
# Run silver transformations manually
bq query --use_legacy_sql=false < sql/silver/silver_hubspot.sql
bq query --use_legacy_sql=false < sql/silver/silver_scope.sql

# Verify results
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM silver.hubspot_deals"
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM silver.scope_tasks"
```

### Test 4: Run Gold Transformations

```bash
bq query --use_legacy_sql=false < sql/gold/gold_star_schema.sql

# Verify star schema
bq query --use_legacy_sql=false "
SELECT 
  customer_stage,
  COUNT(*) as customer_count
FROM gold.dim_customers
GROUP BY customer_stage
"
```

---

## Monitoring and Maintenance

### Cloud Run Job Monitoring

```bash
# View execution history
gcloud run jobs executions list --job=hubspot-extraction --region=$REGION
gcloud run jobs executions list --job=scope-extraction --region=$REGION

# View logs for a specific execution
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=hubspot-extraction" --limit 50
```

### BigQuery Scheduled Query Monitoring

1. Go to **BigQuery** → **Scheduled Queries**
2. Click on each scheduled query to view run history
3. Check for failures and re-run if needed

### Set Up Alerting

```bash
# Create alert for Cloud Run job failures
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="Cloud Run Job Failures" \
  --condition-display-name="Job Execution Failed" \
  --condition-threshold-value=1 \
  --condition-threshold-duration=60s \
  --condition-filter='resource.type="cloud_run_job" AND metric.type="run.googleapis.com/job/completed_execution_count" AND metric.labels.result="failed"'
```

---

## Cost Estimate

**Daily costs** (twice-daily runs):

| Component | Cost per Day |
|-----------|--------------|
| Cloud Run Jobs (2x HubSpot + 2x Scope) | $0.50 |
| BigQuery Storage (50GB) | $1.00 |
| BigQuery Queries (Silver + Gold transforms) | $2.00 |
| GCS Storage (100GB) | $2.00 |
| Cloud Scheduler | $0.30 |
| **Total** | **~$5.80/day** |

**Annual**: ~$2,100

---

## Git Workflow

### Making Changes

```bash
# Make changes to extraction scripts
cd extraction
vim hubspot_pull.py

# Test locally (optional)
python hubspot_pull.py

# Commit and push
git add .
git commit -m "Fix: Update HubSpot date filtering logic"
git push origin main

# Rebuild and redeploy
cd extraction
gcloud builds submit --tag gcr.io/${PROJECT_ID}/hubspot-extractor
gcloud run jobs update hubspot-extraction --image gcr.io/${PROJECT_ID}/hubspot-extractor --region $REGION
```

### SQL Changes

```bash
# Update SQL
vim sql/silver/silver_hubspot.sql

# Test manually
bq query --use_legacy_sql=false < sql/silver/silver_hubspot.sql

# Commit
git add .
git commit -m "Add: Include deal owner field in silver schema"
git push origin main

# Update scheduled query
# Go to BigQuery Console → Scheduled Queries → Edit → Update SQL
```

---

## Next Steps

1. **Enhance Gold Layer** - Add specific milestone tracking logic based on Executive Requirements doc
2. **Create Sigma Dashboards** - Build visualizations for customer journey metrics
3. **Add Data Quality Checks** - Implement validation queries to catch data issues
4. **Set Up CI/CD** - Use Cloud Build triggers on GitHub commits for automatic deployments
5. **Expand Extraction** - Add more HubSpot/Scope fields as needed for reporting

---

## Troubleshooting

### Issue: External tables return no data
**Solution**: Check GCS paths match extraction output
```bash
gsutil ls gs://scope-ws-extract/raw/hubspot/deals/
# Update external table URIs if path doesn't match
```

### Issue: Silver transformations fail with "field not found"
**Solution**: Raw JSON schema doesn't match expected fields
```bash
# Query raw table to see actual field names
bq query --use_legacy_sql=false "SELECT * FROM raw.hubspot_deals LIMIT 1"
```

### Issue: Scheduled queries not running
**Solution**: Check transfer config status
```bash
bq ls --transfer_config --transfer_location=$REGION
# Manually trigger
bq mk --transfer_run --run_time=2024-11-24T15:00:00Z projects/PROJECT_ID/locations/REGION/transferConfigs/CONFIG_ID
```

---

## Support Contacts

- **Pipeline Owner**: [Your Name]
- **GCP Admin**: [Admin Name]
- **Sigma Admin**: [Sigma Admin Name]
- **GitHub Repo**: https://github.com/your-org/executive-dashboard
