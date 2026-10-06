# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Results tables (Unity Catalog)
# MAGIC
# MAGIC Creates governed tables for Locust and notebook metrics. Later notebooks `%run` this file
# MAGIC so they can `persist_run()`, `persist_locust_csvs()`, and `persist_failures()`.
# MAGIC
# MAGIC `ofs_loadtest_runs.access_path` records how a lookup reached Lakebase: `fs` (Feature Serving),
# MAGIC `dataapi` (Lakebase Data API / PostgREST), `pg` (direct Postgres), or `pg_pooled` (Lakebase pooler). Rows
# MAGIC without it default to `fs`. `config` labels the server configuration a row was measured on (Lakebase CU,
# MAGIC Feature Serving concurrency) so runs at different sizes can be compared.
# MAGIC
# MAGIC Results go to `RESULTS_CATALOG.RESULTS_SCHEMA` from `00_config` (every notebook `%run`s that first).

# COMMAND ----------

if "RESULTS_CATALOG" not in globals():
    raise RuntimeError("Run ./00_config before ./00_results_persist.")
RESULTS_RUNS = f"{RESULTS_CATALOG}.{RESULTS_SCHEMA}.ofs_loadtest_runs"
RESULTS_HISTORY = f"{RESULTS_CATALOG}.{RESULTS_SCHEMA}.ofs_loadtest_history"
RESULTS_FAILURES = f"{RESULTS_CATALOG}.{RESULTS_SCHEMA}.ofs_loadtest_failures"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RESULTS_CATALOG}.{RESULTS_SCHEMA}")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {RESULTS_RUNS} (
      run_id STRING,
      started_at TIMESTAMP,
      notebook STRING,
      phase STRING,
      endpoint STRING,
      users INT,
      duration_seconds DOUBLE,
      requests BIGINT,
      failures BIGINT,
      qps DOUBLE,
      p50_ms DOUBLE,
      p95_ms DOUBLE,
      p99_ms DOUBLE,
      error_rate DOUBLE,
      slo_p95_ms DOUBLE,
      slo_pass BOOLEAN,
      min_provisioned_concurrency INT,
      max_provisioned_concurrency INT,
      notes STRING,
      access_path STRING,
      target_qps DOUBLE,
      peak_inflight INT,
      client_cpu DOUBLE,
      client_bound BOOLEAN,
      config STRING,
      lakebase_min_cu DOUBLE,
      lakebase_max_cu DOUBLE,
      generator STRING
    )
    USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {RESULTS_HISTORY} (
      run_id STRING,
      ts TIMESTAMP,
      phase STRING,
      user_count INT,
      requests_per_s DOUBLE,
      failures_per_s DOUBLE,
      p50_ms DOUBLE,
      p95_ms DOUBLE,
      p99_ms DOUBLE,
      total_requests BIGINT,
      total_failures BIGINT
    )
    USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {RESULTS_FAILURES} (
      run_id STRING,
      phase STRING,
      request_name STRING,
      error STRING,
      occurrences BIGINT,
      classified STRING
    )
    USING DELTA
    """
)

print("Tables ready:", RESULTS_RUNS, RESULTS_HISTORY, RESULTS_FAILURES)

# COMMAND ----------

import csv
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RUNS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("notebook", StringType(), True),
        StructField("phase", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("users", IntegerType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("requests", LongType(), True),
        StructField("failures", LongType(), True),
        StructField("qps", DoubleType(), True),
        StructField("p50_ms", DoubleType(), True),
        StructField("p95_ms", DoubleType(), True),
        StructField("p99_ms", DoubleType(), True),
        StructField("error_rate", DoubleType(), True),
        StructField("slo_p95_ms", DoubleType(), True),
        StructField("slo_pass", BooleanType(), True),
        StructField("min_provisioned_concurrency", IntegerType(), True),
        StructField("max_provisioned_concurrency", IntegerType(), True),
        StructField("notes", StringType(), True),
        StructField("access_path", StringType(), True),
        StructField("target_qps", DoubleType(), True),
        StructField("peak_inflight", IntegerType(), True),
        StructField("client_cpu", DoubleType(), True),
        StructField("client_bound", BooleanType(), True),
        StructField("config", StringType(), True),
        StructField("lakebase_min_cu", DoubleType(), True),
        StructField("lakebase_max_cu", DoubleType(), True),
        StructField("generator", StringType(), True),
    ]
)
HISTORY_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("phase", StringType(), True),
        StructField("user_count", IntegerType(), True),
        StructField("requests_per_s", DoubleType(), True),
        StructField("failures_per_s", DoubleType(), True),
        StructField("p50_ms", DoubleType(), True),
        StructField("p95_ms", DoubleType(), True),
        StructField("p99_ms", DoubleType(), True),
        StructField("total_requests", LongType(), True),
        StructField("total_failures", LongType(), True),
    ]
)
FAILURES_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("phase", StringType(), True),
        StructField("request_name", StringType(), True),
        StructField("error", StringType(), True),
        StructField("occurrences", LongType(), True),
        StructField("classified", StringType(), True),
    ]
)


def _ts(epoch):
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow()


def as_float(row, *keys):
    for k in keys:
        if k in row and row[k] not in (None, ""):
            try:
                return float(str(row[k]).replace(",", "").replace("N/A", ""))
            except Exception:
                continue
    return None


def as_int(row, *keys):
    v = as_float(row, *keys)
    return int(v) if v is not None else None


def as_long(v):
    if v is None:
        return None
    return int(v)


def classify_error(text: str) -> str:
    t = (text or "").lower()
    if "too many concurrent requests" in t and "provisioned concurrency of the served entity" in t:
        return "served_entity_concurrency_429"
    if "workspace exceeded provisioned concurrency" in t:
        return "workspace_concurrency_quota"
    if "exceeded max number of parallel requests" in t:
        return "workspace_parallel_requests"
    if "429" in t:
        return "http_429_other"
    if "timeout" in t:
        return "timeout"
    if "5" in t[:3]:
        return "http_5xx"
    return "other"


def persist_run(row: dict):
    users = row.get("users")
    if not isinstance(users, int):
        users = as_int(row, "users")
    rec = (
        str(row.get("run_id")),
        row.get("started_at") or datetime.utcnow(),
        row.get("notebook"),
        row.get("phase"),
        row.get("endpoint"),
        users,
        None if row.get("duration_seconds") is None else float(row.get("duration_seconds")),
        as_long(row.get("requests")),
        as_long(row.get("failures")),
        None if row.get("qps") is None else float(row.get("qps")),
        None if row.get("p50_ms") is None else float(row.get("p50_ms")),
        None if row.get("p95_ms") is None else float(row.get("p95_ms")),
        None if row.get("p99_ms") is None else float(row.get("p99_ms")),
        None if row.get("error_rate") is None else float(row.get("error_rate")),
        None if row.get("slo_p95_ms") is None else float(row.get("slo_p95_ms")),
        None if row.get("slo_pass") is None else bool(row.get("slo_pass")),
        as_long(row.get("min_provisioned_concurrency")),
        as_long(row.get("max_provisioned_concurrency")),
        row.get("notes"),
        # Notebooks 03/06/07 only exercise Feature Serving; 05 passes fs, dataapi, pg, or pg_pooled.
        row.get("access_path") or "fs",
        None if row.get("target_qps") is None else float(row.get("target_qps")),
        as_long(row.get("peak_inflight")),
        None if row.get("client_cpu") is None else float(row.get("client_cpu")),
        None if row.get("client_bound") is None else bool(row.get("client_bound")),
        row.get("config"),
        None if row.get("lakebase_min_cu") is None else float(row.get("lakebase_min_cu")),
        None if row.get("lakebase_max_cu") is None else float(row.get("lakebase_max_cu")),
        row.get("generator"),
    )
    spark.createDataFrame([rec], schema=RUNS_SCHEMA).write.mode("append").saveAsTable(RESULTS_RUNS)


def persist_locust_csvs(run_id: str, phase: str, prefix: Path):
    hist = Path(f"{prefix}_stats_history.csv")
    fail = Path(f"{prefix}_failures.csv")
    if hist.exists():
        rows = []
        with hist.open() as f:
            for rec in csv.DictReader(f):
                rows.append(
                    (
                        run_id,
                        _ts(rec.get("Timestamp")),
                        phase,
                        as_int(rec, "User Count"),
                        as_float(rec, "Requests/s", "Total Request Count/s"),
                        as_float(rec, "Failures/s", "Total Failure Count/s"),
                        as_float(rec, "50%", "Total Median Response Time"),
                        as_float(rec, "95%"),
                        as_float(rec, "99%"),
                        as_long(as_int(rec, "Total Request Count")),
                        as_long(as_int(rec, "Total Failure Count")),
                    )
                )
        if rows:
            spark.createDataFrame(rows, schema=HISTORY_SCHEMA).write.mode("append").saveAsTable(RESULTS_HISTORY)
    if fail.exists():
        rows = []
        with fail.open() as f:
            for rec in csv.DictReader(f):
                err = rec.get("Error") or rec.get("Message") or ""
                occ = as_int(rec, "Occurrences")
                rows.append(
                    (
                        run_id,
                        phase,
                        rec.get("Name") or rec.get("Method"),
                        err[:2000],
                        as_long(1 if occ is None else occ),
                        classify_error(err),
                    )
                )
        if rows:
            spark.createDataFrame(rows, schema=FAILURES_SCHEMA).write.mode("append").saveAsTable(RESULTS_FAILURES)


def locust_stats_to_run(stats: dict) -> dict:
    req = as_float(stats, "Request Count")
    fail = as_float(stats, "Failure Count") or 0
    return {
        "requests": int(req) if req is not None else None,
        "failures": int(fail),
        "qps": as_float(stats, "Requests/s"),
        "p50_ms": as_float(stats, "50%", "Median Response Time"),
        "p95_ms": as_float(stats, "95%"),
        "p99_ms": as_float(stats, "99%"),
        "error_rate": (fail / req) if req else 0.0,
    }
