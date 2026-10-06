# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Recovery after a serving configuration cycle
# MAGIC
# MAGIC Queries one known key, reapplies Feature Serving compute config, waits for `READY` **and**
# MAGIC `NOT_UPDATING`, then queries again. This is **not** a Lakebase HA failover test.
# MAGIC
# MAGIC **Stop here first.** 07 re-applies the endpoint's current size from `00_config` (it does not resize, and stops
# MAGIC if the live endpoint differs). Do not downsize a live
# MAGIC endpoint to Medium/Large `workload_size`.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.81.0" requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer configuration (required)
# MAGIC
# MAGIC Shared values (catalog, tables, endpoint, Lakebase project, secrets, key range, SLO) are set once in
# MAGIC `00_config`. The cell after it holds only this notebook's own settings.
# MAGIC
# MAGIC The notebook user needs `CAN_MANAGE` on the endpoint. The service principal needs `CAN_QUERY`
# MAGIC plus workspace access.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Shared values come from 00_config: WORKSPACE_HOST, SECRET_SCOPE, SP keys, ENDPOINT_NAME, SPEC_NAME, LOOKUP_KEY,
# MIN/MAX_PROVISIONED_CONCURRENCY, SCALE_TO_ZERO_ENABLED, P95_SLO_MS.
KNOWN_ENTITY_ID = ENTITY_MIN_ID  # OPTIONAL: a key that must exist in your table
UPDATE_TIMEOUT_SECONDS = 30 * 60  # OPTIONAL: a config update re-provisions the endpoint (15–20 min)
POLL_SECONDS = 10  # OPTIONAL
HTTP_TIMEOUT_SECONDS = 30  # OPTIONAL

print("Endpoint / spec:", ENDPOINT_NAME, SPEC_NAME)
print("Concurrency min/max / known key:", MIN_PROVISIONED_CONCURRENCY, MAX_PROVISIONED_CONCURRENCY, KNOWN_ENTITY_ID)

# COMMAND ----------

# MAGIC %run ./00_results_persist

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query helper (service principal) and manage helper (notebook user)

# COMMAND ----------

import json
import time

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput

w = sp_client()  # from 00_config
wu = WorkspaceClient()


def query(client):
    url, token = data_plane_token(client, client.serving_endpoints.get(ENDPOINT_NAME))  # from 00_config
    t0 = time.perf_counter()
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"dataframe_records": [{LOOKUP_KEY: KNOWN_ENTITY_ID}]},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    return {"status": r.status_code, "ms": (time.perf_counter() - t0) * 1000, "body": r.text[:180]}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query, reapply concurrency config, wait, query again
# MAGIC
# MAGIC Success: HTTP 200 before and after, endpoint `READY` / `NOT_UPDATING`, min/max unchanged.

# COMMAND ----------

# 07 re-applies the *same* size to test that an update causes no downtime. If the live endpoint differs from
# 00_config (e.g. values left over from creating a second endpoint), stop instead of resizing it.
live = (wu.serving_endpoints.get(ENDPOINT_NAME).config.served_entities or [None])[0]
if live is None or (live.min_provisioned_concurrency, live.max_provisioned_concurrency) != (
    MIN_PROVISIONED_CONCURRENCY,
    MAX_PROVISIONED_CONCURRENCY,
):
    raise RuntimeError(
        f"{ENDPOINT_NAME} runs {getattr(live, 'min_provisioned_concurrency', None)}-"
        f"{getattr(live, 'max_provisioned_concurrency', None)} but 00_config says "
        f"{MIN_PROVISIONED_CONCURRENCY}-{MAX_PROVISIONED_CONCURRENCY}; fix 00_config before running 07."
    )

before = query(w)
wu.serving_endpoints.update_config(
    name=ENDPOINT_NAME,
    served_entities=[
        ServedEntityInput(
            name=ENDPOINT_NAME,
            entity_name=SPEC_NAME,
            scale_to_zero_enabled=SCALE_TO_ZERO_ENABLED,
            min_provisioned_concurrency=MIN_PROVISIONED_CONCURRENCY,
            max_provisioned_concurrency=MAX_PROVISIONED_CONCURRENCY,
        )
    ],
)

deadline = time.time() + UPDATE_TIMEOUT_SECONDS
ready_state = None
update_state = None
while time.time() < deadline:
    ep = wu.serving_endpoints.get(ENDPOINT_NAME)
    ready_state = str(getattr(getattr(ep, "state", None), "ready", None))
    update_state = str(getattr(getattr(ep, "state", None), "config_update", None))
    is_ready = "READY" in ready_state.upper() and "NOT_READY" not in ready_state.upper()
    is_updated = "NOT_UPDATING" in update_state.upper()
    if is_ready and is_updated:
        break
    time.sleep(POLL_SECONDS)
else:
    raise TimeoutError(
        "Endpoint update did not finish: "
        f"ready={ready_state}, config_update={update_state}"
    )

ep = wu.serving_endpoints.get(ENDPOINT_NAME)
served_entity = ep.config.served_entities[0]
after = query(w)
result = {
    "before": before,
    "after": after,
    "endpoint_ready": ready_state,
    "endpoint_config_update": update_state,
    "min_provisioned_concurrency": served_entity.min_provisioned_concurrency,
    "max_provisioned_concurrency": served_entity.max_provisioned_concurrency,
    "ha_enabled": False,
}
print(json.dumps(result, indent=2, default=str))

import uuid
from datetime import datetime

persist_run(
    {
        "run_id": str(uuid.uuid4()),
        "started_at": datetime.utcnow(),
        "notebook": "07_recovery",
        "phase": "recovery",
        "endpoint": ENDPOINT_NAME,
        "users": 1,
        "duration_seconds": None,
        "requests": 2,
        "failures": int(before.get("status") != 200) + int(after.get("status") != 200),
        "qps": None,
        "p50_ms": after.get("ms"),
        "p95_ms": after.get("ms"),
        "p99_ms": after.get("ms"),
        "error_rate": None,
        "slo_p95_ms": float(P95_SLO_MS),
        "slo_pass": before.get("status") == 200 and after.get("status") == 200,
        "min_provisioned_concurrency": result.get("min_provisioned_concurrency"),
        "max_provisioned_concurrency": result.get("max_provisioned_concurrency"),
        "notes": f"before={before.get('status')} after={after.get('status')}",
    }
)
dbutils.notebook.exit(json.dumps(result, default=str))
