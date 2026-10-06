# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Publish to Lakebase and configure Feature Serving
# MAGIC
# MAGIC Snapshot-publishes the offline table to the Online Store, creates the Feature Spec, and
# MAGIC creates or updates the Feature Serving endpoint with **explicit provisioned concurrency**
# MAGIC (not `workload_size` Small/Medium/Large).
# MAGIC
# MAGIC **Stop here first.** Fill in `00_config` (catalog, Lakebase project, endpoint name, concurrency, secrets).
# MAGIC
# MAGIC Do not mix `workload_size` with `MIN_PROVISIONED_CONCURRENCY` / `MAX_PROVISIONED_CONCURRENCY`.
# MAGIC
# MAGIC Success: endpoint `READY` and `NOT_UPDATING`. Notebook 03's smoke step is the first live lookup.

# COMMAND ----------

# MAGIC %pip install "databricks-feature-engineering>=0.13.0" "databricks-sdk>=0.81.0" psycopg2 --no-binary psycopg2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer configuration (required)
# MAGIC
# MAGIC Shared values (catalog, tables, endpoint, Lakebase project, secrets, key range, SLO) are set once in
# MAGIC `00_config`. The cell after it holds only this notebook's own settings.
# MAGIC
# MAGIC Values come from `00_config`. Size concurrency with Little's law:
# MAGIC `concurrency ≈ target QPS × latency_seconds`, then add headroom for p95 and bursts.
# MAGIC At 2K QPS, the default 256–512 measured p95 22.2–26.2 ms across our runs and 64–256 measured 24.4–25.9 ms.
# MAGIC Neither is a universal production size: start at 256–512 and step down (README, *Start here*).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Shared values come from 00_config: SOURCE, ONLINE_TABLE, SPEC_NAME, ENDPOINT_NAME, ONLINE_STORE_NAME,
# LOOKUP_KEY, FEATURE_NAMES, MIN/MAX_PROVISIONED_CONCURRENCY, SCALE_TO_ZERO_ENABLED, SECRET_SCOPE,
# SP_CLIENT_ID_KEY, LAKEBASE_ENDPOINT, PG_DBNAME/PG_SCHEMA/PG_TABLE.
ENDPOINT_READY_TIMEOUT_SECONDS = 45 * 60  # OPTIONAL
ENDPOINT_POLL_SECONDS = 20  # OPTIONAL

print("Source / online / spec / endpoint / store:")
print(SOURCE, ONLINE_TABLE, SPEC_NAME, ENDPOINT_NAME, ONLINE_STORE_NAME)
print("Concurrency min/max:", MIN_PROVISIONED_CONCURRENCY, MAX_PROVISIONED_CONCURRENCY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve the Online Store
# MAGIC
# MAGIC This notebook does **not** create a Lakebase project. The Online Store must already exist.

# COMMAND ----------

import json
import time

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

fe = FeatureEngineeringClient()
w = WorkspaceClient()

store = fe.get_online_store(name=ONLINE_STORE_NAME)
if store is None:
    raise RuntimeError(
        f"Online store '{ONLINE_STORE_NAME}' was not found. "
        "Create or register the customer Lakebase project as an Online Store first. "
        "Do not create a second project if one already exists."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snapshot-publish the offline table to Lakebase

# COMMAND ----------

publish_kwargs = dict(
    online_store=store,
    source_table_name=SOURCE,
    publish_mode="SNAPSHOT",
)
try:
    published = fe.publish_table(**publish_kwargs, online_table_name=ONLINE_TABLE)
except TypeError:
    published = fe.publish_table(**publish_kwargs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Feature Spec (reuse if it already exists)

# COMMAND ----------

try:
    fe.create_feature_spec(
        name=SPEC_NAME,
        features=[
            FeatureLookup(
                table_name=SOURCE,
                lookup_key=LOOKUP_KEY,
                feature_names=FEATURE_NAMES,
            )
        ],
    )
except Exception as exc:
    if "already exists" not in str(exc).lower() and "RESOURCE_ALREADY_EXISTS" not in str(exc):
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or update Feature Serving
# MAGIC
# MAGIC Applies min/max provisioned concurrency and waits until the endpoint is `READY` **and**
# MAGIC `NOT_UPDATING`. Watching only `READY` can pass while a config update is still rolling.

# COMMAND ----------

def served_entity():
    return ServedEntityInput(
        name=ENDPOINT_NAME,
        entity_name=SPEC_NAME,
        scale_to_zero_enabled=SCALE_TO_ZERO_ENABLED,
        min_provisioned_concurrency=MIN_PROVISIONED_CONCURRENCY,
        max_provisioned_concurrency=MAX_PROVISIONED_CONCURRENCY,
    )


def wait_until_settled():
    deadline = time.time() + ENDPOINT_READY_TIMEOUT_SECONDS
    ready_state = update_state = None
    while time.time() < deadline:
        ep = w.serving_endpoints.get(name=ENDPOINT_NAME)
        ready_state = str(getattr(getattr(ep, "state", None), "ready", None))
        update_state = str(getattr(getattr(ep, "state", None), "config_update", None))
        is_ready = "READY" in ready_state.upper() and "NOT_READY" not in ready_state.upper()
        is_updated = "NOT_UPDATING" in update_state.upper()
        if is_ready and is_updated:
            return ready_state, update_state
        time.sleep(ENDPOINT_POLL_SECONDS)
    raise TimeoutError(
        f"Endpoint {ENDPOINT_NAME} update did not finish: "
        f"ready={ready_state}, config_update={update_state}"
    )


existing = {e.name for e in w.serving_endpoints.list()}
if ENDPOINT_NAME not in existing:
    w.serving_endpoints.create(
        name=ENDPOINT_NAME,
        # Notebooks 03-07 mint data-plane tokens from data_plane_info, which only route-optimized endpoints have.
        route_optimized=True,
        config=EndpointCoreConfigInput(
            name=ENDPOINT_NAME,
            served_entities=[served_entity()],
        ),
    )
else:
    if not w.serving_endpoints.get(name=ENDPOINT_NAME).route_optimized:
        raise RuntimeError(
            f"Endpoint {ENDPOINT_NAME} exists but is not route-optimized, and that cannot be changed in place. "
            "Delete it (or choose a new ENDPOINT_NAME) and re-run this notebook."
        )
    # A re-run can land while an earlier update is still rolling; update_config would 409.
    wait_until_settled()
    current = (w.serving_endpoints.get(name=ENDPOINT_NAME).config.served_entities or [None])[0]
    wanted = served_entity()
    # Every update_config re-provisions the served entity (~15+ min), so skip it when nothing changed.
    if current is None or any(
        getattr(current, f, None) != getattr(wanted, f)
        for f in ("entity_name", "scale_to_zero_enabled", "min_provisioned_concurrency", "max_provisioned_concurrency")
    ):
        w.serving_endpoints.update_config(
            name=ENDPOINT_NAME,
            served_entities=[wanted],
        )

ready_state, update_state = wait_until_settled()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant the load-test SP all three lookup paths
# MAGIC
# MAGIC - **Feature Serving:** `CAN_QUERY` on the endpoint.
# MAGIC - **Direct Postgres and Data API:** a Lakebase OAuth role for the SP plus `SELECT` on the online
# MAGIC   table. The Data API's `authenticator` role assumes this same Postgres role per request.
# MAGIC
# MAGIC Runs as the notebook user, who must own the Lakebase project. Safe to re-run.
# MAGIC
# MAGIC **Manual step (UI only):** in the Lakebase project, open **Data API**, click **Enable**, and add
# MAGIC the online table's schema to **Exposed schemas**. Notebook 05 derives the Data API URL; set `DATA_API_URL` in
# MAGIC `00_config` only if yours differs from the one shown.

# COMMAND ----------

import psycopg2
from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel

sp_app_id = dbutils.secrets.get(SECRET_SCOPE, SP_CLIENT_ID_KEY)

w.serving_endpoints.update_permissions(
    serving_endpoint_id=w.serving_endpoints.get(name=ENDPOINT_NAME).id,
    access_control_list=[
        ServingEndpointAccessControlRequest(
            service_principal_name=sp_app_id,
            permission_level=ServingEndpointPermissionLevel.CAN_QUERY,
        )
    ],
)

pg_host = w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status.hosts.host
conn = psycopg2.connect(
    host=pg_host,
    user=w.current_user.me().user_name,
    password=w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token,
    dbname=PG_DBNAME,
    sslmode="require",
    connect_timeout=30,
)
conn.autocommit = True
with conn.cursor() as cur:
    # Create the role in SQL, not via the Lakebase roles API/UI: only roles created with
    # databricks_create_role can later be granted to the Data API's authenticator.
    cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (sp_app_id,))
    if not cur.fetchone():
        cur.execute("SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL')", (sp_app_id,))
    cur.execute(f'GRANT CONNECT ON DATABASE "{PG_DBNAME}" TO "{sp_app_id}"')
    cur.execute(f'GRANT USAGE ON SCHEMA "{PG_SCHEMA}" TO "{sp_app_id}"')
    cur.execute(f'GRANT SELECT ON "{PG_SCHEMA}"."{PG_TABLE}" TO "{sp_app_id}"')
    # Data API: PostgREST's authenticator must be a member of the SP role to SET ROLE per request.
    # The authenticator role only exists once the Data API is enabled; re-run this cell after enabling it.
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'authenticator'")
    if not cur.fetchone():
        print("Data API not enabled yet: skipped GRANT to authenticator (dataapi path will 403).")
    else:
        try:
            cur.execute(f'GRANT "{sp_app_id}" TO authenticator')
        except psycopg2.errors.InsufficientPrivilege as exc:
            raise RuntimeError(
                f"Cannot grant {sp_app_id} to authenticator. The role was probably created through the "
                "Lakebase Roles UI/API; drop it there and re-run this cell so databricks_create_role creates it."
            ) from exc
conn.close()
print("SP", sp_app_id, "granted CAN_QUERY and SELECT on", f"{PG_DBNAME}.{PG_SCHEMA}.{PG_TABLE}")

# COMMAND ----------

result = {
    "online_store": ONLINE_STORE_NAME,
    "source_table": SOURCE,
    "online_table": ONLINE_TABLE,
    "feature_spec": SPEC_NAME,
    "endpoint": ENDPOINT_NAME,
    "endpoint_ready": ready_state,
    "endpoint_config_update": update_state,
    "min_provisioned_concurrency": MIN_PROVISIONED_CONCURRENCY,
    "max_provisioned_concurrency": MAX_PROVISIONED_CONCURRENCY,
    "published": str(published),
}
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
