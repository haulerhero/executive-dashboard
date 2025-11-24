# executive-dashboard pipeline
Customer Journey Reporting Dashboard - Hubspot -> CRM
Automated data pipeline for tracking customer implementation journey from sales through go-live.

   ## Data Sources
   - **HubSpot**: Sales deals and company data
   - **Scope**: Implementation project management and task tracking

   ## Architecture
   - **Extraction**: Python scripts → Cloud Run Jobs
   - **Storage**: GCS (raw JSON) → BigQuery (external tables)
   - **Transformation**: BigQuery scheduled queries (Silver → Gold)
   - **Visualization**: Sigma dashboards

   ## Documentation
   - [Deployment Guide](docs/DEPLOYMENT_GUIDE.md) - Complete setup instructions
   - [Silver Schema](docs/SILVER_SCHEMA_README.md) - Technical schema documentation
   - [Executive Requirements](docs/EXECUTIVE_REQUIREMENTS.md) - Business requirements

   ## Quick Start
   See [Deployment Guide](docs/DEPLOYMENT_GUIDE.md) for step-by-step setup.

   ## Schedule
   - **Extraction**: 6am & 6pm PT (Cloud Run Jobs)
   - **Transformation**: 7am & 7pm PT (BigQuery scheduled queries)

   ## Cost
   ~$6/day (~$2,100/year)
```
3. Save and close

### Step 8: Commit and Push

1. **Go back to GitHub Desktop**
2. You should see all your changes in the left sidebar
3. **Review changes:** Click on files to see what's being added
4. **Write commit message:**
   - Summary: `Initial pipeline setup: extraction, SQL schemas, and docs`
   - Description: `
     - Added HubSpot and Scope extraction scripts
     - Created Silver layer schemas (11 tables)
     - Created Gold star schema (fact/dim tables)
     - Added complete deployment guide
     `
5. Click **Commit to main**
6. Click **Push origin** (top right)

### Step 9: Verify on GitHub.com

1. In GitHub Desktop, click **Repository → View on GitHub** (or `Ctrl+Shift+G`)
2. Your repository should now be live with this structure:
```
   executive-dashboard/
   ├── extraction/
   │   ├── Dockerfile.hubspot
   │   ├── Dockerfile.scope
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
   │   ├── DEPLOYMENT_GUIDE.md
   │   ├── SILVER_SCHEMA_README.md
   │   └── EXECUTIVE_REQUIREMENTS.md
   └── README.md