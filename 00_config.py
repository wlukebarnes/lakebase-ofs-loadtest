# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Shared configuration
# MAGIC
# MAGIC **Set every value once, here.** Each notebook `%run`s this first, then keeps only its own load-shape knobs.
# MAGIC Values shown are from the test workspace the reference results came from; replace them with yours.
# MAGIC
# MAGIC Derived names (online table, Feature Spec, Lakebase endpoint, Postgres location) follow from the values below;
# MAGIC do not hardcode second copies of them in other notebooks.

# COMMAND ----------

# CUSTOMER CONFIGURATION — replace these demo values before you run.

# Unity Catalog: where the source table, Feature Spec, online table, and results live.
CATALOG = "your_catalog"  # REQUIRED: catalog you can write
SCHEMA = "ofs"  # REQUIRED: schema (created if missing)
SOURCE_TABLE_NAME = "entity_features"  # REQUIRED: offline Delta table notebook 01 writes (or your real table)
RESULTS_CATALOG = CATALOG  # OPTIONAL: where results tables go
RESULTS_SCHEMA = SCHEMA  # OPTIONAL

# Feature data. Notebook 01's synthetic table uses keys 1..N_ROWS and columns f01..fNN.
LOOKUP_KEY = "entity_id"  # REQUIRED: primary key / Feature Spec lookup column
N_ROWS = 4_000_000  # REQUIRED: entity cardinality for notebook 01; lower for a smoke test
# Keys must be integers and every key from ENTITY_MIN_ID to ENTITY_MAX_ID must exist: the tests request random keys in
# that range, and a missing row counts as an error on the Postgres and Data API paths.
ENTITY_MIN_ID = 1  # REQUIRED: lowest valid key the load tests request
ENTITY_MAX_ID = N_ROWS  # REQUIRED: highest valid key the load tests request
N_FEATURES = 20  # REQUIRED
FEATURE_NAMES = [f"f{i:02d}" for i in range(1, N_FEATURES + 1)]  # REQUIRED: real feature columns for your table

# Lakebase: the project registered as the Feature Engineering online store.
LAKEBASE_PROJECT = "loadtest"  # REQUIRED
LAKEBASE_BRANCH = "production"  # OPTIONAL
ONLINE_STORE_NAME = None  # OPTIONAL: Feature Engineering online store name; None = LAKEBASE_PROJECT
DATA_API_URL = None  # OPTIONAL: None derives it from the endpoint; override with the URL shown under Lakebase > Data API

# Feature Serving.
ENDPOINT_NAME = "ofs-entity-features"  # REQUIRED: route-optimized endpoint notebook 02 creates
MIN_PROVISIONED_CONCURRENCY = 256  # REQUIRED: floor (multiple of 4); 02 creates and 07 re-applies this size
MAX_PROVISIONED_CONCURRENCY = 512  # REQUIRED: ceiling (multiple of 4); do not mix with workload_size
SCALE_TO_ZERO_ENABLED = False  # Keep False during latency tests

# Identity: one service principal drives every OAuth path.
SECRET_SCOPE = "ofs-loadtest"  # REQUIRED: secret scope holding the SP credentials
SP_CLIENT_ID_KEY = "service_principal_client_id"  # REQUIRED
SP_CLIENT_SECRET_KEY = "service_principal_client_secret"  # REQUIRED
PG_PASSWORD_ROLE = "ofs_loadtest_pw"  # OPTIONAL: native role for the pg_pooled path (notebook 02b)
PG_PASSWORD_KEY = "pg_pooled_password"  # OPTIONAL: secret key holding that role's password

# Latency target used by every notebook's pass/fail.
P95_SLO_MS = 25  # REQUIRED

# Workspace URL for SP clients. None detects the current workspace.
WORKSPACE_HOST = None  # OPTIONAL

# COMMAND ----------

# Derived. Do not edit.
SOURCE = f"{CATALOG}.{SCHEMA}.{SOURCE_TABLE_NAME}"
ONLINE_TABLE = f"{SOURCE}_online"
SPEC_NAME = f"{SOURCE}_spec"
ONLINE_STORE_NAME = ONLINE_STORE_NAME or LAKEBASE_PROJECT
LAKEBASE_ENDPOINT = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/primary"
# Feature Engineering publishes the online table to Postgres as database = catalog, then schema, then table.
PG_DBNAME, PG_SCHEMA, PG_TABLE = ONLINE_TABLE.split(".")


def _detect_workspace_host():
    # Classic clusters: the SDK's default host can be the regional control plane (e.g. https://<region>.cloud...),
    # which SP OAuth rejects. Prefer the workspace URL the cluster reports, then the notebook context.
    try:
        url = spark.conf.get("spark.databricks.workspaceUrl")
        if url:
            return f"https://{url}"
    except Exception:
        pass
    try:  # unsupported notebook-context API; fallback only
        return f"https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}"
    except Exception:
        pass
    from databricks.sdk import WorkspaceClient as _WorkspaceClient

    return _WorkspaceClient().config.host


if not WORKSPACE_HOST:
    WORKSPACE_HOST = _detect_workspace_host()


def sp_client():
    """WorkspaceClient authenticated as the load-test service principal (OAuth M2M)."""
    from databricks.sdk import WorkspaceClient as _WorkspaceClient

    return _WorkspaceClient(
        host=WORKSPACE_HOST,
        client_id=dbutils.secrets.get(SECRET_SCOPE, SP_CLIENT_ID_KEY),
        client_secret=dbutils.secrets.get(SECRET_SCOPE, SP_CLIENT_SECRET_KEY),
        auth_type="oauth-m2m",
    )


def data_plane_token(client, ep):
    """(invocations URL, OAuth token) for a route-optimized serving endpoint `ep`; the token lasts about an hour.

    `_dpts` is an internal databricks-sdk helper (tested on 0.81). If a later SDK renames it, mint the token from the
    endpoint's data_plane_info as described in the route-optimization docs; this is the only place to change.
    """
    info = ep.data_plane_info.query_info
    return info.endpoint_url, client.serving_endpoints_data_plane._dpts.token(
        info.endpoint_url, info.authorization_details
    ).access_token

print("Config:", {"workspace": WORKSPACE_HOST, "source": SOURCE, "online_table": ONLINE_TABLE,
                  "endpoint": ENDPOINT_NAME, "lakebase": LAKEBASE_ENDPOINT, "results": f"{RESULTS_CATALOG}.{RESULTS_SCHEMA}"})
