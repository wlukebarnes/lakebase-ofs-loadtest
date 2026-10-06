# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Feature Serving smoke + Locust ramp
# MAGIC
# MAGIC Mints a route-optimized data-plane OAuth token with a **service principal**, writes a Locust
# MAGIC file from **your** config (key range, hotspot mix, lookup payload), then runs smoke and ramp.
# MAGIC
# MAGIC **Stop here first.** Fill in `00_config`, then set the Locust load shape below.
# MAGIC `users` is concurrent clients, not QPS.
# MAGIC
# MAGIC The Locust user is generated from the configuration cell below (`OFS_MIN_ID`, `OFS_MAX_ID`,
# MAGIC `OFS_HOTSPOT_*`, `OFS_PATH`) and passed as env vars.
# MAGIC
# MAGIC Run this notebook in the **same region** as the endpoint, on the 32-vCPU cluster from `job_rerun.json`. The
# MAGIC smoke is unpaced; the ramp is paced to `RAMP_TARGET_QPS`. Locust uses `FastHttpUser` with one worker process
# MAGIC per core: a single Locust process caps near ~1K requests/s regardless of the endpoint.

# COMMAND ----------

# MAGIC %pip install "locust>=2.20" requests databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer configuration (required)
# MAGIC
# MAGIC Shared values (catalog, tables, endpoint, Lakebase project, secrets, key range, SLO) are set once in
# MAGIC `00_config`. The cell after it holds only this notebook's own settings.
# MAGIC
# MAGIC Store OAuth secrets in a Databricks secret scope. Never paste client secrets into this notebook.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Shared values come from 00_config: WORKSPACE_HOST, SECRET_SCOPE, SP keys, ENDPOINT_NAME, LOOKUP_KEY,
# ENTITY_MIN_ID/ENTITY_MAX_ID, P95_SLO_MS.
HOTSPOT_FRACTION = 0.2  # OPTIONAL: share of requests hitting a hot key range (0.0 disables)
HOTSPOT_MAX_ID = 20_000  # OPTIONAL: hot-key ceiling; ignored when HOTSPOT_FRACTION is 0

# Locust load shape. users = concurrent clients. The smoke is unpaced (each user sends as fast as it can);
# the ramp is paced so the total offered rate is RAMP_TARGET_QPS once all users are running.
SMOKE_USERS = 8
SMOKE_SPAWN_RATE = 8
SMOKE_DURATION = "30s"
RAMP_USERS = 120
RAMP_SPAWN_RATE = 30
RAMP_DURATION = "5m"
RAMP_TARGET_QPS = 2000  # OPTIONAL: None = unpaced closed loop
# One Locust worker process per core. A single Locust process (one Python interpreter) tops out near ~1K
# requests/s regardless of the endpoint; run this notebook on the 32-vCPU cluster (job_rerun.json) for headroom.
LOCUST_PROCESSES = -1  # OPTIONAL: -1 = one per CPU core

RESULTS_DIR = "/tmp/ofs-loadtest-results"  # OPTIONAL: persist to a UC Volume in a customer job

print("Workspace:", WORKSPACE_HOST)
print("Endpoint:", ENDPOINT_NAME)
print("Key range:", ENTITY_MIN_ID, ENTITY_MAX_ID, "hotspot", HOTSPOT_FRACTION, HOTSPOT_MAX_ID)
print("Smoke:", SMOKE_USERS, SMOKE_SPAWN_RATE, SMOKE_DURATION)
print("Ramp:", RAMP_USERS, RAMP_SPAWN_RATE, RAMP_DURATION, "target QPS", RAMP_TARGET_QPS, "| processes", LOCUST_PROCESSES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load results helpers (Unity Catalog persist)

# COMMAND ----------

# MAGIC %run ./00_results_persist

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mint the data-plane token
# MAGIC
# MAGIC Route-optimized Feature Serving rejects PAT. This uses service-principal OAuth M2M.

# COMMAND ----------

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

w = sp_client()  # from 00_config
ep = w.serving_endpoints.get(ENDPOINT_NAME)
url, token = data_plane_token(w, ep)
parsed = urlparse(url)
host = f"{parsed.scheme}://{parsed.netloc}"
path = parsed.path
print("Invocations path:", path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Locust from customer config
# MAGIC
# MAGIC The generated file reads `OFS_MIN_ID`, `OFS_MAX_ID`, `OFS_HOTSPOT_FRACTION`, `OFS_HOTSPOT_MAX`,
# MAGIC `OFS_PATH`, `OFS_TOKEN`, and `OFS_LOOKUP_KEY`. Do not hardcode a workspace id in the path;
# MAGIC the token step supplies `OFS_PATH` from the live endpoint.

# COMMAND ----------

LOCUST_FILE = Path("/tmp/ofs_locustfile.py")
LOCUST_FILE.write_text(
    r'''"""Generated Locust user. Configuration comes from environment variables, not this file."""
from locust import FastHttpUser, constant, constant_throughput, task
import os, random

MIN_ID = int(os.environ["OFS_MIN_ID"])
MAX_ID = int(os.environ["OFS_MAX_ID"])
HOTSPOT_FRACTION = float(os.environ.get("OFS_HOTSPOT_FRACTION", "0"))
HOTSPOT_MAX = int(os.environ.get("OFS_HOTSPOT_MAX", str(MAX_ID)))
PATH = os.environ["OFS_PATH"]
LOOKUP_KEY = os.environ.get("OFS_LOOKUP_KEY", "entity_id")
USER_QPS = float(os.environ.get("OFS_USER_QPS", "0"))  # 0 = unpaced
HEADERS = {"Authorization": f"Bearer {os.environ['OFS_TOKEN']}", "Content-Type": "application/json"}


def entity_id():
    if HOTSPOT_FRACTION > 0 and random.random() < HOTSPOT_FRACTION:
        return random.randint(MIN_ID, min(HOTSPOT_MAX, MAX_ID))
    return random.randint(MIN_ID, MAX_ID)


class FeatureServingUser(FastHttpUser):
    # FastHttpUser (geventhttpclient) costs far less CPU per request than HttpUser (requests).
    wait_time = constant_throughput(USER_QPS) if USER_QPS > 0 else constant(0)

    @task
    def lookup(self):
        payload = {"dataframe_records": [{LOOKUP_KEY: entity_id()}]}
        with self.client.post(PATH, json=payload, headers=HEADERS, catch_response=True, name="/invocations") as resp:
            if resp.status_code != 200:
                resp.failure(f"{resp.status_code}: {resp.text[:160]}")
            else:
                resp.success()
'''
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run smoke, then ramp
# MAGIC
# MAGIC Capture the HTTP body on failures. A 429 that says to increase served-entity provisioned
# MAGIC concurrency is a Feature Serving capacity result, not Lakebase saturation.

# COMMAND ----------

RESULTS = Path(RESULTS_DIR)
RESULTS.mkdir(exist_ok=True)


def run_locust(tag, users, spawn, run_time, target_qps=None):
    prefix = RESULTS / tag
    env = os.environ.copy()
    env.update(
        {
            "OFS_TOKEN": token,
            "OFS_PATH": path,
            "OFS_MIN_ID": str(ENTITY_MIN_ID),
            "OFS_MAX_ID": str(ENTITY_MAX_ID),
            "OFS_HOTSPOT_FRACTION": str(HOTSPOT_FRACTION),
            "OFS_HOTSPOT_MAX": str(HOTSPOT_MAX_ID),
            "OFS_LOOKUP_KEY": LOOKUP_KEY,
            "OFS_USER_QPS": str(target_qps / users) if target_qps else "0",
        }
    )
    # python -m locust: the notebook-scoped install is not always on PATH (classic clusters).
    cmd = [
        sys.executable,
        "-m",
        "locust",
        "--processes",
        str(LOCUST_PROCESSES),
        "-f",
        str(LOCUST_FILE),
        "--headless",
        "-u",
        str(users),
        "-r",
        str(spawn),
        "-t",
        run_time,
        "--host",
        host,
        "--csv",
        str(prefix),
        "--only-summary",
    ]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    stats = {}
    p = Path(f"{prefix}_stats.csv")
    if p.exists():
        rows = list(csv.DictReader(p.open()))
        stats = next((r for r in rows if r.get("Name") in ("Aggregated", "")), rows[-1] if rows else {})
    return {
        "tag": tag,
        "users": users,
        "spawn": spawn,
        "run_time": run_time,
        "returncode": proc.returncode,
        "stats": stats,
        "stderr_tail": proc.stderr[-1200:],
        "rps_host": host,
    }


import uuid
from datetime import datetime

RUN_ID = str(uuid.uuid4())
STARTED = datetime.utcnow()
SLO_P95_MS = float(P95_SLO_MS)
served = getattr(ep.config, "served_entities", [None])[0]
min_c = getattr(served, "min_provisioned_concurrency", None) if served else None
max_c = getattr(served, "max_provisioned_concurrency", None) if served else None

out = {}
for tag, users, spawn, dur, target_qps in (
    ("smoke", SMOKE_USERS, SMOKE_SPAWN_RATE, SMOKE_DURATION, None),
    ("ramp_2k", RAMP_USERS, RAMP_SPAWN_RATE, RAMP_DURATION, RAMP_TARGET_QPS),
):
    result = run_locust(tag, users, spawn, dur, target_qps)
    out[tag] = result
    metrics = locust_stats_to_run(result.get("stats") or {})
    persist_run(
        {
            "run_id": RUN_ID,
            "started_at": STARTED,
            "notebook": "03_locust_run",
            "phase": tag,
            "endpoint": ENDPOINT_NAME,
            "users": users,
            "duration_seconds": None,
            "slo_p95_ms": SLO_P95_MS,
            # A crashed or empty Locust run (no stats, nonzero exit) must not read as a pass.
            "slo_pass": result["returncode"] == 0
            and (metrics.get("requests") or 0) > 0
            and metrics.get("p95_ms") is not None
            and metrics["p95_ms"] <= SLO_P95_MS
            and (metrics.get("failures") or 0) == 0,
            "min_provisioned_concurrency": min_c,
            "max_provisioned_concurrency": max_c,
            "notes": f"spawn={spawn} duration={dur} target_qps={target_qps or 'unpaced'} processes={LOCUST_PROCESSES}",
            **metrics,
        }
    )
    persist_locust_csvs(RUN_ID, tag, RESULTS / tag)

print("run_id", RUN_ID)
print(json.dumps(out, indent=2, default=str)[:8000])
dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, **out}, default=str)[:180000])
