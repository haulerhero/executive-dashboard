import os, io, json, gzip, time, datetime as dt
import requests
from google.cloud import storage
from google.cloud import secretmanager
import google.auth

# Configuration
API_BASE = "https://api.hubapi.com"
BUCKET = "scope-ws-extract"
PORTAL_ID = "20174054"
RUN_ID = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
SECRET_NAME = "global-hubspot-api"
PROJECT_ID = None  # Will auto-detect from ADC

# Customer filter: deals closed in last 18 months
CUSTOMER_LOOKBACK_MONTHS = 18

# Deal properties to extract
DEAL_PROPERTIES = [
    "dealname",
    "amount",
    "implementation_cost",
    "daily_routes",
    "closedate",
    "createdate",
    "dealstage",
    "pipeline",
    "hs_lastmodifieddate",
]

# Company properties to extract
COMPANY_PROPERTIES = [
    "name",
    "domain",
    "current_business_management_software__cloned_",
    "createdate",
    "hs_lastmodifieddate",
]


def get_hubspot_token():
    """Return HubSpot API key from Secret Manager"""
    project = PROJECT_ID
    if not project:
        _, project = google.auth.default()

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{SECRET_NAME}/versions/latest"
    resp = client.access_secret_version(request={"name": name})
    return resp.payload.data.decode("utf-8")


def request_page(url, headers, params=None, max_retries=5):
    """GET request with retries for transient errors"""
    backoff = 1.0
    for _ in range(max_retries):
        resp = requests.get(url, headers=headers, params=params, timeout=30)

        if resp.status_code == 429:  # Rate limit
            retry_after = int(resp.headers.get('Retry-After', backoff))
            time.sleep(retry_after)
            backoff = min(backoff * 2, 60)
            continue
        elif resp.status_code in (500, 502, 503):
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

        resp.raise_for_status()
        return resp.json()

    resp.raise_for_status()


def upload_gzip_bytes(bkt_name, key, raw_bytes: bytes):
    """Upload gzipped data to GCS"""
    client = storage.Client()
    blob = client.bucket(bkt_name).blob(key)
    blob.content_type = "application/x-ndjson"
    blob.content_encoding = "gzip"
    blob.upload_from_file(io.BytesIO(raw_bytes), rewind=True)


def get_deal_company_association(deal_id: str, headers: dict) -> str:
    """Get the associated company ID for a deal using v4 associations API"""
    url = f"{API_BASE}/crm/v4/objects/deals/{deal_id}/associations/companies"
    try:
        data = request_page(url, headers)
        results = data.get("results", [])
        if results:
            return results[0].get("toObjectId")
    except Exception as e:
        pass
    return None


def parse_hubspot_date(date_str: str) -> dt.datetime:
    """Parse HubSpot date string to datetime (handles both date and datetime formats)"""
    if not date_str:
        return None
    try:
        # Try full datetime first (e.g., "2023-01-15T12:34:56.789Z")
        if 'T' in date_str:
            # Parse and make naive (remove timezone info for comparison)
            parsed = dt.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return parsed.replace(tzinfo=None)
        # Fall back to date only (e.g., "2023-01-15")
        return dt.datetime.strptime(date_str[:10], '%Y-%m-%d')
    except (ValueError, AttributeError):
        return None


def extract_customer_deals(headers: dict):
    """Extract all deals, then filter by closedate in last N months AND closed won status"""
    print(f"Starting deals extraction (filtering for last {CUSTOMER_LOOKBACK_MONTHS} months, closed won only)...")

    # Calculate cutoff date (naive datetime for comparison)
    cutoff_date = dt.datetime.utcnow() - dt.timedelta(days=CUSTOMER_LOOKBACK_MONTHS * 30)
    print(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d')}")

    url = f"{API_BASE}/crm/v3/objects/deals"
    params = {
        "limit": 100,
        "properties": ",".join(DEAL_PROPERTIES),
        "archived": "false"
    }

    # Output paths
    now = dt.datetime.utcnow()
    key_prefix = f"raw/hubspot/deals/dt={now:%Y-%m-%d}/hr={now:%H}/run={RUN_ID}"
    part_key = f"{key_prefix}/part-00000.json.gz"
    success_key = f"{key_prefix}/_SUCCESS"

    # NDJSON gzip buffer
    ndjson_buffer = io.BytesIO()
    gz = gzip.GzipFile(fileobj=ndjson_buffer, mode="wb")

    total_extracted = 0
    total_filtered = 0
    total_no_closedate = 0
    total_too_old = 0
    total_not_won = 0
    after = None
    customer_company_ids = set()

    while True:
        if after:
            params["after"] = after

        try:
            data = request_page(url, headers, params)
        except requests.HTTPError as e:
            print(f"HTTP error: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for deal in results:
            deal_id = deal.get("id")
            properties = deal.get("properties", {})

            total_extracted += 1

            # Filter 1: Must have closedate
            closedate_str = properties.get("closedate")
            if not closedate_str:
                total_no_closedate += 1
                continue

            closedate = parse_hubspot_date(closedate_str)
            if not closedate:
                total_no_closedate += 1
                continue

            # Filter 2: Must be within date range
            if closedate < cutoff_date:
                total_too_old += 1
                continue

            # Filter 3: Must be "closed won"
            dealstage = properties.get("dealstage", "").lower()
            if dealstage != "closedwon":
                total_not_won += 1
                continue

            # This deal passes all filters!
            total_filtered += 1

            # Get associated company
            company_id = get_deal_company_association(deal_id, headers)
            if company_id:
                customer_company_ids.add(company_id)

            # Construct record with metadata
            record = {
                "id": deal_id,
                "company_id": company_id,
                "hubspot_deal_url": f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-3/{deal_id}",
                "hubspot_company_url": f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-2/{company_id}" if company_id else None,
                **properties
            }

            gz.write((json.dumps(record, separators=(",", ":")) + "\n").encode("utf-8"))

        # Check for next page
        paging = data.get("paging", {})
        after = paging.get("next", {}).get("after")
        if not after:
            break

    gz.close()

    # Upload results
    if total_filtered:
        upload_gzip_bytes(BUCKET, part_key, ndjson_buffer.getvalue())
        upload_gzip_bytes(BUCKET, success_key, gzip.compress(b""))
        print(f"✓ Deals: {total_filtered} closed won deals (from {total_extracted} total)")
        print(f"  Filtered out: {total_no_closedate} no date, {total_too_old} too old, {total_not_won} not won")
        print(f"  → gs://{BUCKET}/{part_key}")
    else:
        print(f"✓ Deals: No closed won deals in last {CUSTOMER_LOOKBACK_MONTHS} months")
        print(
            f"  (scanned {total_extracted}, filtered: {total_no_closedate} no date, {total_too_old} too old, {total_not_won} not won)"
            )

    return total_filtered, customer_company_ids


def extract_customer_companies(headers: dict, customer_company_ids: set):
    """Extract only companies associated with filtered deals (all properties)"""
    print(f"Starting companies extraction ({len(customer_company_ids)} customer companies)...")

    if not customer_company_ids:
        print("✓ Companies: No customer companies to extract")
        return 0

    # Output paths
    now = dt.datetime.utcnow()
    key_prefix = f"raw/hubspot/companies/dt={now:%Y-%m-%d}/hr={now:%H}/run={RUN_ID}"
    part_key = f"{key_prefix}/part-00000.json.gz"
    success_key = f"{key_prefix}/_SUCCESS"

    # NDJSON gzip buffer
    ndjson_buffer = io.BytesIO()
    gz = gzip.GzipFile(fileobj=ndjson_buffer, mode="wb")

    total = 0

    # Fetch each customer company individually with ALL properties
    for company_id in customer_company_ids:
        url = f"{API_BASE}/crm/v3/objects/companies/{company_id}"
        params = {"properties": ",".join(COMPANY_PROPERTIES)}

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            properties = data.get("properties", {})

            # Construct record with metadata
            record = {
                "id": company_id,
                "hubspot_company_url": f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-2/{company_id}",
                **properties
            }

            gz.write((json.dumps(record, separators=(",", ":")) + "\n").encode("utf-8"))
            total += 1

        except requests.HTTPError as e:
            print(f"Warning: Could not fetch company {company_id}: {e}")
            continue

    gz.close()

    # Upload results
    if total:
        upload_gzip_bytes(BUCKET, part_key, ndjson_buffer.getvalue())
        upload_gzip_bytes(BUCKET, success_key, gzip.compress(b""))
        print(f"✓ Companies: {total} customer companies (all properties)")
        print(f"  → gs://{BUCKET}/{part_key}")
    else:
        print("✓ Companies: No companies extracted")

    return total


def main():
    # Auth - get Private App token
    token = get_hubspot_token()
    headers = {"Authorization": f"Bearer {token}"}

    # Step 1: Extract ALL deals, filter by closedate, collect company IDs
    total_deals, customer_company_ids = extract_customer_deals(headers)

    # Step 2: Extract only companies associated with filtered deals
    total_companies = extract_customer_companies(headers, customer_company_ids)

    print(f"\nSummary:")
    print(f"  Deals: {total_deals} (closed in last {CUSTOMER_LOOKBACK_MONTHS} months)")
    print(f"  Companies: {total_companies} (customers)")
    print(f"  Run ID: {RUN_ID}")


if __name__ == "__main__":
    main()