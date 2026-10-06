# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Bounded soak
# MAGIC
# MAGIC Continuous Feature Serving lookups for a configured duration. Default is **10 minutes**,
# MAGIC not a 24-hour production soak.
# MAGIC
# MAGIC Closed loop: each of `WORKER_THREADS` sends its next request as soon as the last returns, so the load is about
# MAGIC threads ÷ latency (16 threads ≈ 750 QPS at ~21 ms), not a fixed QPS. It checks stability over time; notebook 05
# MAGIC measures latency at a target QPS. The data-plane token is re-minted every 30 minutes for long soaks.
# MAGIC
# MAGIC **Stop here first.** Do not start
# MAGIC `duration_seconds=86400` without a cost cap and an owner watching the run.

# COMMAND ----------

# MAGIC %pip install databricks-sdk requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer configuration (required)
# MAGIC
# MAGIC Shared values (catalog, tables, endpoint, Lakebase project, secrets, key range, SLO) are set once in
# MAGIC `00_config`. The cell after it holds only this notebook's own settings.
# MAGIC
# MAGIC `duration_seconds` can also be passed as a job/widget parameter. The widget default is 600.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Shared values come from 00_config: WORKSPACE_HOST, SECRET_SCOPE, SP keys, ENDPOINT_NAME, LOOKUP_KEY,
# ENTITY_MIN_ID/ENTITY_MAX_ID, P95_SLO_MS.
WORKER_THREADS = 16  # REQUIRED: closed-loop threads; load ≈ threads ÷ latency
DEFAULT_DURATION_SECONDS = 600  # REQUIRED default; 86400 = 24h only after cost approval
HTTP_TIMEOUT_SECONDS = 30  # OPTIONAL

try:
    dbutils.widgets.text("duration_seconds", str(DEFAULT_DURATION_SECONDS), "Soak duration (seconds)")
except Exception:
    pass

duration = DEFAULT_DURATION_SECONDS
try:
    duration = int(dbutils.widgets.get("duration_seconds") or str(DEFAULT_DURATION_SECONDS))
except Exception:
    pass

print("Endpoint / threads / duration:", ENDPOINT_NAME, WORKER_THREADS, duration)

# COMMAND ----------

# MAGIC %run ./00_results_persist

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authenticate

# COMMAND ----------

import json
import random
import threading
import time

import requests
TOKEN_REFRESH_SECONDS = 30 * 60  # data-plane tokens last about an hour

w = sp_client()  # from 00_config
ep = w.serving_endpoints.get(ENDPOINT_NAME)


def mint_token():
    return data_plane_token(w, ep)[1]  # from 00_config


url, token = data_plane_token(w, ep)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the soak
# MAGIC
# MAGIC Keep time-series serving and Lakebase metrics for long soaks. Aggregate p95 can hide drift.

# COMMAND ----------

stop = threading.Event()
lat, errs, lock = [], 0, threading.Lock()


def worker():
    global errs
    s = requests.Session()
    while not stop.is_set():
        t0 = time.perf_counter()
        try:
            r = s.post(
                url,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"dataframe_records": [{LOOKUP_KEY: random.randint(ENTITY_MIN_ID, ENTITY_MAX_ID)}]},
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            ok = r.status_code == 200
        except Exception:
            ok = False
        ms = (time.perf_counter() - t0) * 1000
        with lock:
            lat.append(ms)
            if not ok:
                errs += 1


ts = [threading.Thread(target=worker, daemon=True) for _ in range(WORKER_THREADS)]
t0 = time.time()
for t in ts:
    t.start()
end = t0 + duration
try:
    while time.time() < end:
        time.sleep(min(TOKEN_REFRESH_SECONDS, max(0.0, end - time.time())))
        if time.time() < end:
            token = mint_token()  # workers read the global on every request
finally:
    stop.set()  # stop the load even if a token refresh fails
for t in ts:
    t.join(timeout=10)
elapsed = time.time() - t0
srt = sorted(lat)


def pct(p):
    if not srt:
        return None
    return srt[min(len(srt) - 1, int(round((p / 100) * (len(srt) - 1))))]


result = {
    "duration_seconds": duration,
    "elapsed": elapsed,
    "worker_threads": WORKER_THREADS,
    "n": len(lat),
    "errors": errs,
    "qps": len(lat) / elapsed if elapsed else 0,
    "p50_ms": pct(50),
    "p95_ms": pct(95),
    "p99_ms": pct(99),
    "note": "Long soaks are not auto-started; pass duration_seconds=86400 deliberately",
}
print(json.dumps(result, indent=2, default=str))

import uuid
from datetime import datetime

persist_run(
    {
        "run_id": str(uuid.uuid4()),
        "started_at": datetime.utcnow(),
        "notebook": "06_soak",
        "phase": "soak",
        "endpoint": ENDPOINT_NAME,
        "users": WORKER_THREADS,
        "duration_seconds": result.get("duration_seconds"),
        "requests": result.get("n"),
        "failures": result.get("errors"),
        "qps": result.get("qps"),
        "p50_ms": result.get("p50_ms"),
        "p95_ms": result.get("p95_ms"),
        "p99_ms": result.get("p99_ms"),
        "error_rate": (result.get("errors") or 0) / result["n"] if result.get("n") else 0.0,
        "slo_p95_ms": float(P95_SLO_MS),
        "slo_pass": (result.get("n") or 0) > 0
        and result.get("p95_ms") is not None
        and (result.get("errors") or 0) == 0
        and result["p95_ms"] <= P95_SLO_MS,
        "notes": result.get("note"),
    }
)
dbutils.notebook.exit(json.dumps(result, default=str))
