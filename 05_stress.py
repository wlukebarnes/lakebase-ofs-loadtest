# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Target-QPS matrix across access paths and server configurations
# MAGIC
# MAGIC Offers a fixed request rate (open loop, Poisson arrivals) and steps it up, for each way an application can
# MAGIC read the same Lakebase online table, across a matrix of server configurations, in one run:
# MAGIC
# MAGIC | Path | How the lookup reaches Lakebase |
# MAGIC |---|---|
# MAGIC | `fs` | Feature Serving endpoint (route-optimized HTTP, Feature Spec). One path per endpoint in `FS_ENDPOINTS`, so different provisioned-concurrency settings run side by side with no rollout wait |
# MAGIC | `dataapi` | Lakebase Data API (PostgREST-compatible HTTP `GET`) |
# MAGIC | `pg` | Direct Postgres (psycopg2, prepared statement, OAuth database credential, direct endpoint host) |
# MAGIC | `pg_pooled` | Postgres through the Lakebase pooled host (PgBouncer) as a native password role (notebook 02b) |
# MAGIC
# MAGIC **Matrix:** for each Lakebase CU size in `LAKEBASE_CU_CONFIGS`, the notebook resizes the primary endpoint
# MAGIC (~20 s; drops all open connections), waits `RESIZE_SETTLE_SECONDS`, then runs the QPS ladder for every path. It restores the
# MAGIC original CU range at the end. Every row records its configuration (`config`, `lakebase_min_cu`/`max_cu`, Feature
# MAGIC Serving min/max concurrency) so configurations can be compared on the dashboard.
# MAGIC
# MAGIC Latency is measured from each request's **scheduled** send time, so queueing is counted when a path falls behind.
# MAGIC The answer reads "at X QPS on config C, path P gives p95 Y", independent of the caller's pod or connection count.
# MAGIC
# MAGIC **Run on a large single-node cluster** (e.g. 32 vCPU) **as the Lakebase project owner** (the CU resize uses the
# MAGIC notebook identity; lookups use the SP). Each step reports generator CPU and scheduler lag and is flagged
# MAGIC `client_bound` if the generator, not the path, was the limit.
# MAGIC
# MAGIC **Stop here first.** Fill in `00_config`, then set the endpoints, matrix, ladder, and durations below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies
# MAGIC
# MAGIC psycopg2 is built from source so it links the host's OpenSSL: the binary wheels bundle their own OpenSSL,
# MAGIC which aborts on FIPS-enabled (compliance security profile) hosts. Falls back to the binary wheel where libpq headers are missing.

# COMMAND ----------

import subprocess
import sys


def _pip(*args):
    return subprocess.run([sys.executable, "-m", "pip", "install", "-q", *args], capture_output=True, text=True)


base = _pip("databricks-sdk>=0.81.0", "requests", "pandas")
if base.returncode != 0:
    raise RuntimeError(base.stderr[-2000:])
if _pip("psycopg2", "--no-binary", "psycopg2").returncode != 0:
    fallback = _pip("psycopg2-binary")
    if fallback.returncode != 0:
        raise RuntimeError(fallback.stderr[-2000:])
check = subprocess.run([sys.executable, "-c", "import psycopg2"], capture_output=True, text=True)
if check.returncode != 0:
    raise RuntimeError(f"psycopg2 import failed (rc={check.returncode}): {check.stderr[-1000:]}")
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer configuration (required)
# MAGIC
# MAGIC Shared values (catalog, tables, endpoint, Lakebase project, secrets, key range, SLO) are set once in
# MAGIC `00_config`. The cell after it holds only this notebook's own settings.
# MAGIC
# MAGIC `access_paths`, `target_qps`, `lakebase_cu`, and `fs_endpoints` can also be passed as job/widget parameters, e.g.
# MAGIC `fs,pg`, `1000,2000,4000`, `8-8,16-16`, and `ofs-entity-features,ofs-entity-features-512-1024`. An empty `lakebase_cu` runs one round at the current size without resizing.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Shared values come from 00_config: WORKSPACE_HOST, SECRET_SCOPE, SP keys, ENDPOINT_NAME, LOOKUP_KEY,
# ENTITY_MIN_ID/ENTITY_MAX_ID, ONLINE_TABLE, FEATURE_NAMES, LAKEBASE_ENDPOINT, DATA_API_URL, PG_PASSWORD_*, P95_SLO_MS.
FS_ENDPOINTS = (ENDPOINT_NAME,)  # OPTIONAL: one fs path per endpoint (add more to compare concurrency settings)
FEATURE_COLUMNS = FEATURE_NAMES

# Server matrix. (min_cu, max_cu) per round; pin min = max so a round measures one exact size.
LAKEBASE_CU_CONFIGS = ()  # OPTIONAL: () = one round at the current size, no resize. A matrix such as
# ((8, 8), (16, 16), (32, 32)) RESIZES the Lakebase endpoint between rounds (restored at the end); test environments only.
RESIZE_SETTLE_SECONDS = 60  # OPTIONAL: wait after a resize before measuring

# Load shape. QPS is offered load (open loop), not concurrency.
TARGET_QPS_LADDER = (250, 500, 1000, 2000, 4000, 8000, 12000)  # REQUIRED: start conservative in a customer workspace
STEP_SECONDS = 60  # REQUIRED: duration of each step, per path
WARMUP_SECONDS = 5  # OPTIONAL: first seconds of each step excluded from stats (connection setup, cache warm-up)
DRAIN_SECONDS = 5  # OPTIONAL: after a step, requests not done within this count as unfinished
GENERATOR_PROCESSES = 0  # OPTIONAL: 0 = one per CPU core
# Cap on concurrent requests per path across all processes. For pg paths this is the max Postgres connections opened.
# Postgres paths are capped low on purpose: 8K QPS x ~4 ms needs ~30 in flight, and small Lakebase sizes have few
# connection slots (2-4 CU hit "remaining connection slots are reserved" at 256), which also starves FS and the Data API.
MAX_INFLIGHT = {"fs": 1024, "dataapi": 1024, "pg": 64, "pg_pooled": 64}  # OPTIONAL

# A step meets the SLO with zero errors, p95 ≤ P95_SLO_MS (00_config), and offered QPS achieved.
KEEP_UP_RATIO = 0.95  # OPTIONAL: achieved / offered below this fails the SLO
CONTINUE_PAST_SLO = True  # OPTIONAL: keep climbing after an SLO break so every path shows a full curve
HARD_STOP_P95_MS = 4 * P95_SLO_MS  # OPTIONAL: with CONTINUE_PAST_SLO, stop a path once p95 exceeds this...
HARD_STOP_ERROR_RATE = 0.01  # OPTIONAL: ...or more than this share of lookups fail...
HARD_STOP_KEEP_UP = 0.90  # OPTIONAL: ...or it achieves less than this share of offered QPS
CLIENT_BOUND_CPU = 0.85  # OPTIONAL: generator process CPU (fraction of one core) above this flags client_bound
CLIENT_BOUND_SCHED_LAG_MS = 5.0  # OPTIONAL: p99 lateness issuing requests above this flags client_bound
REQUEST_TIMEOUT_SECONDS = 10  # OPTIONAL: HTTP timeout and Postgres statement_timeout
DEFAULT_ACCESS_PATHS = "fs,dataapi,pg"  # OPTIONAL: comma-separated subset (fs = every FS_ENDPOINTS entry); add pg_pooled after 02b


def _widget(name, default, label):
    try:
        dbutils.widgets.text(name, default, label)
        return dbutils.widgets.get(name)
    except Exception:
        return default


ACCESS_PATHS = [p.strip() for p in _widget("access_paths", DEFAULT_ACCESS_PATHS, "Access paths").split(",") if p.strip()]
TARGET_QPS_LADDER = tuple(
    int(x) for x in _widget("target_qps", ",".join(map(str, TARGET_QPS_LADDER)), "Target QPS ladder").split(",") if x.strip()
)
FS_ENDPOINTS = tuple(
    e.strip() for e in _widget("fs_endpoints", ",".join(FS_ENDPOINTS), "Feature Serving endpoints").split(",") if e.strip()
)
_cu = _widget("lakebase_cu", ",".join(f"{a}-{b}" for a, b in LAKEBASE_CU_CONFIGS), "Lakebase CU rounds (min-max,...)")
LAKEBASE_CU_CONFIGS = tuple(tuple(float(v) for v in x.split("-")) for x in _cu.split(",") if x.strip())

import os

N_PROCS = GENERATOR_PROCESSES or len(os.sched_getaffinity(0))

print("Access paths:", ACCESS_PATHS, "| FS endpoints:", FS_ENDPOINTS)
print("Lakebase CU rounds:", LAKEBASE_CU_CONFIGS or "current size only")
print("QPS ladder / step seconds / p95 SLO:", TARGET_QPS_LADDER, STEP_SECONDS, P95_SLO_MS)
print("Generator processes:", N_PROCS)

# COMMAND ----------

# MAGIC %run ./00_results_persist

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authenticate
# MAGIC
# MAGIC One SP drives every OAuth path, so the comparison is transport, not identity. Each path refreshes its credential
# MAGIC at the start of every step (Feature Serving JWT, workspace OAuth token, Lakebase database credential). The
# MAGIC `pg_pooled` path uses the native password role, because the pooler does not accept OAuth. The notebook identity
# MAGIC (`wu`) only resizes the Lakebase endpoint between rounds.

# COMMAND ----------

import json
import math
import multiprocessing as mp
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import psycopg2
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.common.types.fieldmask import FieldMask
from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType

sp_client_id = dbutils.secrets.get(SECRET_SCOPE, SP_CLIENT_ID_KEY)
w = sp_client()  # from 00_config
wu = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access paths
# MAGIC
# MAGIC Each path has three steps:
# MAGIC - `prepare()`: once per step, in the driver. Mints credentials.
# MAGIC - `connect(ctx)`: once per generator thread, on its first request (inside the warm-up window).
# MAGIC - `lookup(client, key)`: the timed call. Returns `(ok, error_text)`.
# MAGIC
# MAGIC `kind` is the access path written to `access_path`; `key` identifies one path instance (one per FS endpoint).
# MAGIC A lookup that returns no row counts as an error on every path.

# COMMAND ----------

class FeatureServingPath:
    kind = "fs"

    def __init__(self, endpoint_name):
        self.target = endpoint_name
        self.key = f"fs:{endpoint_name}"
        self.min_concurrency = self.max_concurrency = None

    def prepare(self):
        ep = w.serving_endpoints.get(self.target)
        served = (ep.config.served_entities or [None])[0]
        self.min_concurrency = getattr(served, "min_provisioned_concurrency", None)
        self.max_concurrency = getattr(served, "max_provisioned_concurrency", None)
        url, token = data_plane_token(w, ep)  # from 00_config
        return {"url": url, "token": token}

    def connect(self, ctx):
        s = requests.Session()
        s.headers.update({"Authorization": f"Bearer {ctx['token']}", "Content-Type": "application/json"})
        return (s, ctx["url"])

    def lookup(self, client, key):
        s, url = client
        r = s.post(url, json={"dataframe_records": [{LOOKUP_KEY: key}]}, timeout=REQUEST_TIMEOUT_SECONDS)
        return (True, None) if r.status_code == 200 else (False, f"{r.status_code}: {r.text[:1000]}")

    def close(self, client):
        client[0].close()


class DataApiPath:
    kind = key = "dataapi"
    target = DATA_API_URL
    min_concurrency = max_concurrency = None

    def prepare(self):
        if not self.target:
            # Same URL the Lakebase UI shows under Data API: endpoint host + workspace id + database.
            host = w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status.hosts.host
            self.target = f"https://{host}/api/2.0/workspace/{w.get_workspace_id()}/rest/{PG_DBNAME}"
        return {
            "url": f"{self.target.rstrip('/')}/{PG_SCHEMA}/{PG_TABLE}",
            "select": ",".join(FEATURE_COLUMNS),
            "headers": w.config.authenticate(),
        }

    def connect(self, ctx):
        s = requests.Session()
        s.headers.update(ctx["headers"])
        return (s, ctx)

    def lookup(self, client, key):
        s, ctx = client
        r = s.get(
            ctx["url"],
            params={LOOKUP_KEY: f"eq.{key}", "select": ctx["select"]},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if r.status_code != 200:
            return False, f"{r.status_code}: {r.text[:1000]}"
        return (True, None) if r.content.strip() != b"[]" else (False, f"no row for {key}")

    def close(self, client):
        client[0].close()


_RETRYABLE_CONNECT = ("too many connection", "failed to acquire permit", "remaining connection slots")


def _connect_with_backoff(**kwargs):
    # Generator threads open connections within the warm-up window. Lakebase throttles concurrent connection attempts
    # and small sizes have few slots, so retry those errors with jitter, the way an app pool fills gradually.
    for attempt in range(8):
        try:
            return psycopg2.connect(**kwargs)
        except psycopg2.OperationalError as exc:
            if attempt == 7 or not any(m in str(exc).lower() for m in _RETRYABLE_CONNECT):
                raise
            time.sleep(random.uniform(0.05, 0.25) * 2**attempt)


_COLS = ", ".join(f'"{c}"' for c in FEATURE_COLUMNS)
_LOOKUP_SQL = f'SELECT {_COLS} FROM "{PG_SCHEMA}"."{PG_TABLE}" WHERE "{LOOKUP_KEY}" = '


class DirectPostgresPath:
    kind = key = "pg"
    target = LAKEBASE_ENDPOINT
    min_concurrency = max_concurrency = None

    def prepare(self):
        return {
            "host": w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status.hosts.host,
            "user": sp_client_id,
            "password": w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token,
        }

    def connect(self, ctx):
        conn = _connect_with_backoff(
            host=ctx["host"],
            user=ctx["user"],
            password=ctx["password"],
            dbname=PG_DBNAME,
            sslmode="require",
            connect_timeout=30,
            options=f"-c statement_timeout={REQUEST_TIMEOUT_SECONDS * 1000}",
        )
        # Autocommit: each SELECT is its own statement, no idle-in-transaction between lookups.
        conn.autocommit = True
        cur = conn.cursor()
        # Server-side PREPARE skips re-parse/re-plan per lookup. It needs a dedicated backend connection.
        cur.execute(f"PREPARE ofs_lookup(bigint) AS {_LOOKUP_SQL}$1")
        return (conn, cur, "EXECUTE ofs_lookup(%s)")

    def lookup(self, client, key):
        _, cur, stmt = client
        cur.execute(stmt, (key,))
        return (True, None) if cur.fetchone() is not None else (False, f"no row for {key}")

    def close(self, client):
        client[0].close()


class PooledPostgresPath(DirectPostgresPath):
    kind = key = "pg_pooled"

    def prepare(self):
        return {
            "host": w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status.hosts.read_write_pooled_host,
            "user": PG_PASSWORD_ROLE,
            "password": dbutils.secrets.get(SECRET_SCOPE, PG_PASSWORD_KEY),
        }

    def connect(self, ctx):
        # PgBouncer rejects the `options` startup parameter, so statement_timeout is not set on this path.
        conn = _connect_with_backoff(
            host=ctx["host"], user=ctx["user"], password=ctx["password"], dbname=PG_DBNAME, sslmode="require",
            connect_timeout=30,
        )
        conn.autocommit = True
        # Transaction pooling hands each statement to any backend, so SQL-level PREPARE would not survive.
        return (conn, conn.cursor(), _LOOKUP_SQL + "%s")


_paths = []
for kind in ACCESS_PATHS:
    if kind == "fs":
        _paths += [FeatureServingPath(e) for e in FS_ENDPOINTS]
    elif kind == "dataapi":
        _paths.append(DataApiPath())
    elif kind == "pg":
        _paths.append(DirectPostgresPath())
    elif kind == "pg_pooled":
        _paths.append(PooledPostgresPath())
    else:
        raise ValueError(f"Unknown access path {kind}; choose from fs, dataapi, pg, pg_pooled")
PATHS = {p.key: p for p in _paths}
print("Path instances:", list(PATHS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open-loop generator
# MAGIC
# MAGIC Each step forks `N_PROCS` processes. Every process issues its share of the target rate with Poisson
# MAGIC inter-arrival times into a thread pool (capped by `MAX_INFLIGHT`), and records latency from the
# MAGIC **scheduled** time. Requests scheduled after warm-up count toward stats; those not finished within
# MAGIC `DRAIN_SECONDS` after the step are `unfinished` (they fail the SLO). Each process also reports its CPU use
# MAGIC and how late it issued requests, so a saturated generator is visible instead of looking like a slow path.

# COMMAND ----------

def _generator_process(path_key, ctx, rate, start_at, max_workers, seed, out_q):
    path = PATHS[path_key]
    rng = random.Random(seed)
    lock = threading.Lock()
    local = threading.local()
    clients, lat, samples, sched_lag = [], [], [], []
    stats = {"errors": 0, "completed": 0, "issued": 0, "inflight": 0, "peak_inflight": 0}

    # Shared perf_counter timeline anchored to the driver's wall-clock start.
    base = time.perf_counter() + (start_at - time.time())
    measure_from = base + WARMUP_SECONDS
    measure_to = base + STEP_SECONDS
    drain_until = measure_to + DRAIN_SECONDS

    def record_error(text):
        stats["errors"] += 1
        # Distinct messages only, head + tail: Feature Serving puts the root cause after a long generic prefix.
        text = text if len(text) <= 600 else f"{text[:200]} … {text[-400:]}"
        if len(samples) < 3 and text not in samples:
            samples.append(text)

    def job(t_sched, key, counted):
        with lock:
            stats["inflight"] += 1
            stats["peak_inflight"] = max(stats["peak_inflight"], stats["inflight"])
        try:
            client = getattr(local, "client", None)
            if client is None:
                client = local.client = path.connect(ctx)
                with lock:
                    clients.append(client)
            ok, err = path.lookup(client, key)
        except Exception as exc:
            ok, err = False, f"{type(exc).__name__}: {exc}"
        done = time.perf_counter()
        with lock:
            stats["inflight"] -= 1
            if counted and done <= drain_until:
                stats["completed"] += 1
                lat.append((done - t_sched) * 1000)
                if not ok:
                    record_error(err)

    cpu0 = os.times()
    executor = ThreadPoolExecutor(max_workers=max_workers)
    t_sched = base
    while True:
        t_sched += rng.expovariate(rate)
        if t_sched >= measure_to:
            break
        delay = t_sched - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        counted = t_sched >= measure_from
        if counted:
            stats["issued"] += 1
            sched_lag.append(max(0.0, time.perf_counter() - t_sched) * 1000)
        executor.submit(job, t_sched, rng.randint(ENTITY_MIN_ID, ENTITY_MAX_ID), counted)
    remaining = drain_until - time.perf_counter()
    if remaining > 0:
        time.sleep(remaining)
    # Queued requests that never started are unfinished; running ones finish within REQUEST_TIMEOUT_SECONDS.
    executor.shutdown(wait=True, cancel_futures=True)
    cpu1 = os.times()
    for client in clients:
        try:
            path.close(client)
        except Exception:
            pass

    wall = max(1e-9, (cpu1.elapsed - cpu0.elapsed))
    lag = sorted(sched_lag)
    out_q.put(
        {
            "lat": lat,
            "samples": samples,
            "cpu": ((cpu1.user - cpu0.user) + (cpu1.system - cpu0.system)) / wall,
            "sched_lag_p99_ms": lag[int(0.99 * (len(lag) - 1))] if lag else 0.0,
            "connections": len(clients),
            **stats,
        }
    )


def _pct(srt, p):
    if not srt:
        return None
    return srt[min(len(srt) - 1, int(round((p / 100) * (len(srt) - 1))))]


def run_step(path, target_qps):
    row = {"access_path": path.kind, "path_key": path.key, "target_qps": target_qps, "n": 0, "errors": 0, "unfinished": 0, "qps": 0.0,
           "p50_ms": None, "p95_ms": None, "p99_ms": None, "peak_inflight": 0, "connections": 0,
           "client_cpu": None, "sched_lag_p99_ms": None, "client_bound": False, "error_samples": []}
    try:
        ctx = path.prepare()
    except Exception as exc:
        # A path that cannot authenticate is broken at this step; the other paths keep going.
        row.update(errors=1, error_samples=[f"prepare: {type(exc).__name__}: {exc}"[:600]])
        return row

    fork = mp.get_context("fork")
    out_q = fork.Queue()
    workers = max(2, math.ceil(MAX_INFLIGHT[path.kind] / N_PROCS))
    start_at = time.time() + 2.0
    procs = [
        fork.Process(
            target=_generator_process,
            args=(path.key, ctx, target_qps / N_PROCS, start_at, workers, random.getrandbits(32), out_q),
            daemon=True,
        )
        for _ in range(N_PROCS)
    ]
    for p in procs:
        p.start()
    parts = []
    deadline = start_at + STEP_SECONDS + DRAIN_SECONDS + REQUEST_TIMEOUT_SECONDS + 60
    for _ in procs:
        try:
            parts.append(out_q.get(timeout=max(1.0, deadline - time.time())))
        except Exception:
            break
    for p in procs:
        p.join(timeout=10)
        if p.is_alive():
            p.terminate()
    if len(parts) < len(procs):
        row["error_samples"].append(f"{len(procs) - len(parts)} generator processes did not report")

    lat = sorted(x for part in parts for x in part["lat"])
    issued = sum(part["issued"] for part in parts)
    completed = sum(part["completed"] for part in parts)
    measured = STEP_SECONDS - WARMUP_SECONDS
    cpus = [part["cpu"] for part in parts]
    lags = [part["sched_lag_p99_ms"] for part in parts]
    for part in parts:
        for s in part["samples"]:
            if len(row["error_samples"]) < 3 and s not in row["error_samples"]:
                row["error_samples"].append(s)
    row.update(
        n=completed,
        errors=sum(part["errors"] for part in parts),
        unfinished=max(0, issued - completed),
        qps=completed / measured,
        p50_ms=_pct(lat, 50),
        p95_ms=_pct(lat, 95),
        p99_ms=_pct(lat, 99),
        peak_inflight=sum(part["peak_inflight"] for part in parts),
        connections=sum(part["connections"] for part in parts),
        client_cpu=max(cpus) if cpus else None,
        sched_lag_p99_ms=max(lags) if lags else None,
    )
    row["client_bound"] = bool(
        (row["client_cpu"] or 0) > CLIENT_BOUND_CPU or (row["sched_lag_p99_ms"] or 0) > CLIENT_BOUND_SCHED_LAG_MS
    )
    return row

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the matrix
# MAGIC
# MAGIC For each Lakebase CU round: resize, settle, then run the QPS ladder for every path (order rotates per step).
# MAGIC A step meets the SLO with zero errors, zero unfinished, p95 ≤ `P95_SLO_MS`, and achieved ≥ `KEEP_UP_RATIO` of
# MAGIC offered. With `CONTINUE_PAST_SLO`, a path keeps climbing past its first SLO break and stops at a hard limit
# MAGIC (`HARD_STOP_*`) or when the generator itself is the bottleneck (`client_bound`). Stop state resets each round.

# COMMAND ----------

def keep_up(row):
    return row["qps"] / row["target_qps"] if row["target_qps"] else 0.0


def breaks_slo(row):
    return (
        (row["errors"] or 0) > 0
        or (row["unfinished"] or 0) > 0
        or row["p95_ms"] is None
        or row["p95_ms"] > P95_SLO_MS
        or keep_up(row) < KEEP_UP_RATIO
    )


def must_stop(row):
    if row["client_bound"]:
        return True
    if not CONTINUE_PAST_SLO:
        return breaks_slo(row)
    error_rate = ((row["errors"] or 0) + (row["unfinished"] or 0)) / max(1, row["n"] + (row["unfinished"] or 0))
    return (
        error_rate > HARD_STOP_ERROR_RATE
        or (row["p95_ms"] or float("inf")) > HARD_STOP_P95_MS
        or keep_up(row) < HARD_STOP_KEEP_UP
    )


def lakebase_cu():
    status = wu.postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status
    return status.autoscaling_limit_min_cu, status.autoscaling_limit_max_cu


def resize_lakebase(min_cu, max_cu):
    wu.postgres.update_endpoint(
        name=LAKEBASE_ENDPOINT,
        endpoint=Endpoint(
            name=LAKEBASE_ENDPOINT,
            spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=min_cu,
                autoscaling_limit_max_cu=max_cu,
            ),
        ),
        update_mask=FieldMask(field_mask=["spec.autoscaling_limit_min_cu", "spec.autoscaling_limit_max_cu"]),
    ).wait()


def config_label(min_cu, max_cu, path):
    label = f"LB {min_cu:g}-{max_cu:g} CU"
    if path.kind == "fs":
        label += f" · FS {path.min_concurrency}-{path.max_concurrency}"
    return label


import uuid
from datetime import datetime

rid = str(uuid.uuid4())
started = datetime.utcnow()
generator = f"{N_PROCS} procs on {os.cpu_count()} vCPU"
unsaved = []  # rows measured but not yet written; saved after each Lakebase round and on failure


def save_unsaved():
    while unsaved:
        row = unsaved.pop(0)
        path = PATHS[row["path_key"]]
        notes = [f"unfinished={row['unfinished']}", f"sched_lag_p99_ms={round(row['sched_lag_p99_ms'] or 0, 2)}"]
        if row["first_unstable"]:
            notes.append("first_unstable")
        if row["error_samples"]:
            notes.append("errors: " + " | ".join(row["error_samples"]))
        persist_run(
            {
                "run_id": rid,
                "started_at": started,
                "notebook": "05_stress",
                "phase": f"qps_{row['target_qps']}",
                "access_path": path.kind,
                "endpoint": path.target,
                "users": row["connections"],
                "duration_seconds": float(STEP_SECONDS - WARMUP_SECONDS),
                "requests": row["n"],
                "failures": row["errors"] + row["unfinished"],
                "qps": row["qps"],
                "p50_ms": row["p50_ms"],
                "p95_ms": row["p95_ms"],
                "p99_ms": row["p99_ms"],
                "error_rate": (row["errors"] + row["unfinished"]) / max(1, row["n"] + row["unfinished"]),
                "slo_p95_ms": float(P95_SLO_MS),
                "slo_pass": not breaks_slo(row) and not row["client_bound"],
                "min_provisioned_concurrency": row["min_concurrency"],
                "max_provisioned_concurrency": row["max_concurrency"],
                "target_qps": float(row["target_qps"]),
                "peak_inflight": row["peak_inflight"],
                "client_cpu": row["client_cpu"],
                "client_bound": row["client_bound"],
                "config": row["config"],
                "lakebase_min_cu": row["lakebase_min_cu"],
                "lakebase_max_cu": row["lakebase_max_cu"],
                "generator": generator,
                "notes": "; ".join(notes)[:2000],
            }
        )


original_cu = lakebase_cu()
rounds = LAKEBASE_CU_CONFIGS or (original_cu,)
ladder = []
try:
    for min_cu, max_cu in rounds:
        if LAKEBASE_CU_CONFIGS:
            resize_lakebase(min_cu, max_cu)
            time.sleep(RESIZE_SETTLE_SECONDS)
        cu_now = lakebase_cu()
        print(f"=== Lakebase round {cu_now[0]:g}-{cu_now[1]:g} CU ===")
        stopped, broke = set(), set()
        active = list(PATHS)
        for step, target in enumerate(TARGET_QPS_LADDER):
            if not active:
                break
            # Rotate which path goes first so no path always pays (or skips) the cache warm-up.
            shift = step % len(active)
            for key in active[shift:] + active[:shift]:
                path = PATHS[key]
                row = run_step(path, target)
                row.update(
                    lakebase_min_cu=cu_now[0],
                    lakebase_max_cu=cu_now[1],
                    min_concurrency=path.min_concurrency,
                    max_concurrency=path.max_concurrency,
                    config=config_label(cu_now[0], cu_now[1], path),
                    first_unstable=breaks_slo(row) and key not in broke,
                )
                ladder.append(row)
                unsaved.append(row)
                print(json.dumps({k: (round(v, 2) if isinstance(v, float) else v) for k, v in row.items()
                                  if k != "error_samples"}, default=str))
                if breaks_slo(row):
                    broke.add(key)
                if must_stop(row):
                    stopped.add(key)
            active = [k for k in active if k not in stopped]
        save_unsaved()
finally:
    try:
        save_unsaved()  # keep every completed step even if the run fails part-way
    finally:
        if LAKEBASE_CU_CONFIGS and lakebase_cu() != original_cu:
            resize_lakebase(*original_cu)
            print(f"Restored Lakebase to {original_cu[0]:g}-{original_cu[1]:g} CU")
print("run_id", rid)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latency menu
# MAGIC
# MAGIC One row per path and configuration: highest offered QPS that met the SLO, and latency at that step. `≥` means
# MAGIC the path never broke within the ladder (or the generator became the limit first), so its real ceiling is higher.

# COMMAND ----------

menu = []
for (config, key) in dict.fromkeys((r["config"], r["path_key"]) for r in ladder):
    rows = [r for r in ladder if r["config"] == config and r["path_key"] == key]
    stable = [r for r in rows if not breaks_slo(r) and not r["client_bound"]]
    best = max(stable, key=lambda r: r["target_qps"]) if stable else None
    unstable = [r["target_qps"] for r in rows if breaks_slo(r) and not r["client_bound"]]
    menu.append(
        {
            "config": config,
            "path": key,
            "max_qps_within_slo": (str(best["target_qps"]) if unstable else f"≥{best['target_qps']}") if best else None,
            "p50_ms": best["p50_ms"] if best else None,
            "p95_ms": best["p95_ms"] if best else None,
            "p99_ms": best["p99_ms"] if best else None,
            "first_unstable_qps": min(unstable) if unstable else None,
        }
    )
display(pd.DataFrame(menu))
display(pd.DataFrame([{k: v for k, v in r.items() if k != "error_samples"} for r in ladder]))

# COMMAND ----------

result = {"run_id": rid, "menu": menu, "rounds": [list(r) for r in rounds], "generator": generator}
dbutils.notebook.exit(json.dumps(result, default=str)[:180000])
