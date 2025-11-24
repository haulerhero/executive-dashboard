import os, io, json, gzip, time, datetime as dt
import requests
from google.cloud import storage
from google.cloud import secretmanager
import google.auth
from typing import Dict, List, Optional
from dataclasses import dataclass

# Base config - same as original
API_BASE = os.getenv("API_BASE", "https://api.scope.ws")
BUCKET = os.getenv("BUCKET", "scope-ws-extract")
RUN_ID = os.getenv("RUN_ID", dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"))
SECRET_NAME = os.getenv("SCOPE_API_TOKEN_SECRET", "scope-ws-api-key")
PROJECT_ID = os.getenv("GCP_PROJECT")


@dataclass
class EndpointConfig:
    """Configuration for each API endpoint"""
    resource: str  # e.g., "companies/search"
    page_size: int = 10  # default page size
    state_key: str = None  # for independent watermarks, defaults to resource
    dependencies: List[str] = None  # endpoints this depends on (for ordering)

    def __post_init__(self):
        if self.state_key is None:
            self.state_key = self.resource.replace('/', '_')
        if self.dependencies is None:
            self.dependencies = []


# Define your endpoint configuration - prioritized for customer journey analytics
ENDPOINTS = {
    # TIER 1: Core customer and reference data (highest priority)
    "companies": EndpointConfig(
        "companies/search",
        page_size=25,
        # Companies with hubspot_id are your key customer dimension
    ),
    "task_statuses": EndpointConfig("task-statuses/search", page_size=100),
    "task_types": EndpointConfig("task-types/search", page_size=100),
    "users": EndpointConfig("users/search", page_size=50),

    # TIER 2: Customer relationship and structure data
    "company_users": EndpointConfig(
        "company-users/search",
        page_size=50,
        dependencies=["companies"]
        # Links HubSpot companies to individual users/contacts
    ),
    "lists": EndpointConfig(
        "lists/search",
        page_size=25,
        dependencies=["users"]
        # Project/work organization structure
    ),

    # TIER 3: Activity and engagement data (customer health indicators)
    "tasks": EndpointConfig(
        "tasks/search",
        page_size=20,
        dependencies=["lists", "task_statuses", "task_types", "companies"]
        # Primary activity/engagement data for customer health metrics
    ),

    # TIER 4: Metadata (lower priority for dashboard, but useful for context)
    "field_groups": EndpointConfig("field-groups/search", page_size=100),
    "fields": EndpointConfig(
        "fields/search",
        page_size=100,
        dependencies=["field_groups"]
        # Custom field definitions (might contain additional HubSpot mappings)
    ),
    "tags": EndpointConfig(
        "tags/search",
        page_size=50,
        dependencies=["fields"]
        # Tag values for additional customer context
    ),
}


def get_token():
    """Return Scope Bearer token. Prefer env override; else read Secret Manager."""
    token = os.getenv("SCOPE_API_TOKEN")
    if token:
        return token
    project = PROJECT_ID
    if not project:
        _, project = google.auth.default()
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{SECRET_NAME}/versions/latest"
    resp = client.access_secret_version(request={"name": name})
    return resp.payload.data.decode("utf-8")


def parse_gs_uri(uri: str):
    assert uri.startswith("gs://"), f"URI must be gs://... got: {uri}"
    _, rest = uri.split("gs://", 1)
    bucket, *key_parts = rest.split("/", 1)
    key = key_parts[0] if key_parts else ""
    return bucket, key


def get_state_uri(endpoint_key: str) -> str:
    """Generate state URI for each endpoint"""
    return f"gs://{BUCKET}/state/scope/{endpoint_key}.json"


def read_state(endpoint_key: str, default_iso: str) -> str:
    """Read last 'updated_after' watermark for specific endpoint"""
    state_uri = get_state_uri(endpoint_key)
    bkt, key = parse_gs_uri(state_uri)
    client = storage.Client()
    blob = client.bucket(bkt).blob(key)
    if not blob.exists():
        return default_iso
    data = json.loads(blob.download_as_text())
    return data.get("updated_after", default_iso)


def write_state(endpoint_key: str, new_iso: str):
    """Write watermark for specific endpoint"""
    state_uri = get_state_uri(endpoint_key)
    bkt, key = parse_gs_uri(state_uri)
    client = storage.Client()
    blob = client.bucket(bkt).blob(key)
    blob.upload_from_string(
        json.dumps({"updated_after": new_iso}) + "\n",
        content_type="application/json"
    )


def request_page(url, headers, json_body=None, max_retries=5):
    """POST-only request with retries for transient errors."""
    backoff = 1.0
    for _ in range(max_retries):
        resp = requests.post(
            url,
            headers={
                **headers,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=json_body,
            timeout=30,
        )
        if resp.status_code in (429, 500, 502, 503):
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()


def upload_gzip_bytes(bkt_name, key, raw_bytes: bytes):
    client = storage.Client()
    blob = client.bucket(bkt_name).blob(key)
    blob.content_type = "application/x-ndjson"
    blob.content_encoding = "gzip"
    blob.upload_from_file(io.BytesIO(raw_bytes), rewind=True)


def extract_endpoint(endpoint_key: str, config: EndpointConfig, headers: Dict) -> int:
    """Extract data for a single endpoint"""
    print(f"Starting extraction for {endpoint_key} ({config.resource})")

    url = f"{API_BASE}/v1/{config.resource.lstrip('/')}"

    # Watermark management
    default_since = (dt.datetime.utcnow() - dt.timedelta(days=7)).replace(microsecond=0).isoformat() + "Z"
    updated_after = read_state(config.state_key, default_since)

    # Output paths
    now = dt.datetime.utcnow()
    key_prefix = f"raw/scope/{config.state_key}/dt={now:%Y-%m-%d}/hr={now:%H}/run={RUN_ID}"
    part_key = f"{key_prefix}/part-00000.json.gz"
    success_key = f"{key_prefix}/_SUCCESS"

    # NDJSON gzip buffer
    ndjson_buffer = io.BytesIO()
    gz = gzip.GzipFile(fileobj=ndjson_buffer, mode="wb")

    total = 0
    max_seen_ts = updated_after
    offset = 0

    # Pagination loop
    while True:
        body = {
            "offset": offset,
            "limit": config.page_size
        }

        try:
            data = request_page(url, headers, json_body=body)
        except requests.HTTPError as e:
            print(f"HTTP error for {endpoint_key}: {e}")
            break

        # Handle different response wrappers
        items = None
        if isinstance(data, dict):
            items = data.get("data") or data.get("items")
        elif isinstance(data, list):
            items = data

        if not items:
            break

        for rec in items:
            # Filter out archived/soft-deleted records (Scope only supports soft delete)
            if rec.get("archived?") is True:
                continue

            # Track timestamps for watermark
            ts = rec.get("updated_at") or rec.get("updatedAt") or rec.get("modifiedAt")
            if ts and ts > max_seen_ts:
                max_seen_ts = ts

            gz.write((json.dumps(rec, separators=(",", ":")) + "\n").encode("utf-8"))
            total += 1

        # Continue pagination if we got a full page
        if len(items) < config.page_size:
            break
        offset += config.page_size

    gz.close()

    # Upload results
    if total:
        upload_gzip_bytes(BUCKET, part_key, ndjson_buffer.getvalue())
        upload_gzip_bytes(BUCKET, success_key, gzip.compress(b""))
        write_state(config.state_key, max_seen_ts)
        print(f"✓ {endpoint_key}: {total} records → gs://{BUCKET}/{part_key}")
    else:
        print(f"✓ {endpoint_key}: No new records")

    return total


def resolve_dependencies(endpoints: Dict[str, EndpointConfig]) -> List[str]:
    """Resolve endpoint dependencies into execution order using topological sort"""
    from collections import defaultdict, deque

    # Build dependency graph
    graph = defaultdict(list)
    in_degree = defaultdict(int)

    # Initialize all endpoints
    for endpoint in endpoints.keys():
        in_degree[endpoint] = 0

    # Build edges (dependency → endpoint)
    for endpoint, config in endpoints.items():
        for dep in config.dependencies:
            if dep in endpoints:  # Only consider configured dependencies
                graph[dep].append(endpoint)
                in_degree[endpoint] += 1

    # Topological sort
    queue = deque([ep for ep in endpoints.keys() if in_degree[ep] == 0])
    result = []

    while queue:
        current = queue.popleft()
        result.append(current)

        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(endpoints):
        raise ValueError("Circular dependency detected in endpoints!")

    return result


def main():
    # Basic validation
    assert BUCKET and "://" not in BUCKET, "BUCKET must be the bucket name only"
    assert API_BASE.startswith("http"), "API_BASE must be a full URL"

    # Auth
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    # Resolve execution order
    try:
        execution_order = resolve_dependencies(ENDPOINTS)
        print(f"Execution order: {' → '.join(execution_order)}")
    except ValueError as e:
        print(f"Dependency error: {e}")
        return

    # Extract each endpoint in dependency order
    total_records = 0
    successful_endpoints = []

    for endpoint_key in execution_order:
        config = ENDPOINTS[endpoint_key]
        try:
            records = extract_endpoint(endpoint_key, config, headers)
            total_records += records
            successful_endpoints.append(endpoint_key)
        except Exception as e:
            print(f"✗ {endpoint_key} failed: {e}")
            # Continue with other endpoints rather than failing completely
            continue

    print(f"\nSummary:")
    print(f"  Total records: {total_records}")
    print(f"  Successful endpoints: {len(successful_endpoints)}/{len(ENDPOINTS)}")
    print(f"  Run ID: {RUN_ID}")


if __name__ == "__main__":
    main()