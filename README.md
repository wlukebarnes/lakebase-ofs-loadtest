# Lakebase Online Feature Store: access-path latency test

**What this tests, and why.** An application can read online features from a Lakebase online table in four ways: through a Feature Serving endpoint, the Lakebase Data API, a direct Postgres connection, or the Lakebase pooled (PgBouncer) host. They differ in latency, in how far they scale, and in what you have to operate. This package measures all four against the **same table**, at the **same offered request rate**, so you can pick one with numbers: lowest latency at your target QPS (2,000 QPS here) versus the least to run. The Feature Serving endpoint is created **route-optimized** (`route_optimized=True` in notebook 02), the low-latency serving option for online lookups. It is also required here, because only route-optimized endpoints expose the data-plane URL and OAuth token the load generator calls directly.

| Path | How a lookup reaches Lakebase | Trade-off |
|---|---|---|
| `fs` | Feature Serving endpoint (route-optimized HTTP + Feature Spec) | Fully managed and governed; ~18–20 ms median per request measured here, even at 250 QPS; capacity = provisioned concurrency |
| `dataapi` | Lakebase Data API (PostgREST-compatible HTTP `GET`) | No serving endpoint; runs on Lakebase compute, so its ceiling scales with CU |
| `pg` | Direct Postgres (psycopg2, prepared statement, OAuth database credential) | Lowest latency; the client owns connection pooling and the 1-hour credential refresh |
| `pg_pooled` | Postgres via the Lakebase pooled host (PgBouncer) | Same latency as `pg`; absorbs many client connections; requires a native password role (no OAuth) |

The test runs **inside your Databricks workspace, in the same region as Lakebase**, so it isolates the Databricks side of each path. Clients elsewhere (e.g. on EKS) add their own network hop on top.

## Start here

1. **Measure big first.** Before the first run, set the Lakebase project's primary compute to **16–32 CU** autoscaling (Lakebase app → your project → **Compute** → edit). Nothing in this package sets it unless you pass `lakebase_cu`. Keep Feature Serving at the default **256–512** provisioned concurrency. That gives each path's best case. In our runs at this size, at 2,000 QPS:
   - Postgres (direct and pooled) and the Data API met the 25 ms p95 target with margin (p95 5–10 ms).
   - **Feature Serving sat on the line:** p95 22.2–26.2 ms across four runs (six measurements).
2. **Then step down** to the cheapest size that still meets your target. **Use a test project:** notebook 05 resizes the project's primary compute, and every resize drops all open connections.
   - **Lakebase:** pass `lakebase_cu` (e.g. `16-16,8-8,4-4`) with a short ladder such as `target_qps=1000,2000,4000`. Lakebase size barely changed Postgres or Feature Serving latency. It sets how many connections fit (~225 per CU) and how much the Data API can handle (the Data API needed ≥ 16 CU for 2K QPS). Below 8 CU, test one Feature Serving endpoint at a time: each 256–512 endpoint held ~340 Lakebase connections in our test, and 4 CU (901 slots) with two endpoints failed on every path.
   - **Feature Serving:** add a smaller endpoint (see *Adding a second Feature Serving endpoint* under Setup) and list both in `fs_endpoints`. 64–256 measured 24.4–25.9 ms p95 at 2K, at or just over the 25 ms line.
3. **Repeat before deciding.** Run the matrix **2–3 times** (separate job runs) and compare each path's p95 at your target QPS. Feature Serving p95 at 2K moved by 4 ms between our runs at the same size, so a single 55 s step near the target is not a verdict. Count a path as meeting the target only if it does in every repeat.

## Prerequisites

- Unity Catalog catalog/schema you can write (source table, Feature Spec, results tables).
- A Feature Engineering online store backed by Lakebase Autoscaling. Notebook 02 does not create it. Create it once in a notebook (`%pip install "databricks-feature-engineering>=0.13.0"`, serverless or DBR 16.4 LTS ML+): `FeatureEngineeringClient().create_online_store(name="<LAKEBASE_PROJECT>", capacity="CU_8")`, and wait until `get_online_store(...).state` is `AVAILABLE` ([Online Feature Stores](https://docs.databricks.com/aws/en/machine-learning/feature-store/online-feature-store)). That creates the Lakebase project; in our test the store name and project ID were the same (`ONLINE_STORE_NAME` defaults to `LAKEBASE_PROJECT`). Then raise its compute to 16–32 CU in the Lakebase app (*Start here*, step 1). Keep the production branch always on (no scale-to-zero) during tests.
- The Databricks CLI, installed and authenticated to the workspace (used for the secret, import, and job commands below).
- Serverless compute for notebooks and jobs (01, 02, 02b, 06, and 07 run on it), and a SQL warehouse for the dashboard.
- Permissions to create a schema, tables, and functions in the catalog (the Feature Spec is a Unity Catalog function); to create serving endpoints; and to create a secret scope (or `READ` on an existing one).
- A service principal with an OAuth secret, stored in a secret scope as `service_principal_client_id` / `service_principal_client_secret`, with the workspace-access entitlement. **Bring your own:** an existing service principal works; it does not need to be created for this test. It is required because route-optimized Feature Serving only accepts OAuth tokens, which a notebook running as a user cannot mint.
- Permission to create **classic** compute: notebooks 03 and 05 need a large single-node cluster (32 vCPU, e.g. AWS `c6id.8xlarge`, DBR 17.3 LTS). Serverless notebooks have 4 shared cores and cap the load generator near ~1K HTTP QPS.
- You (the person running the notebooks) must own the Lakebase project: 02/02b create roles and grants, and 05 resizes the endpoint between matrix rounds.

## Values you must supply

Everything a run needs is in two files. Defaults marked **keep** work unless you have a reason to change them.

**`00_config.py`** (read by every notebook)

| Variable | Default | Change? | What it is / where to find it |
|---|---|---|---|
| `CATALOG` | `your_catalog` | **Required** | A Unity Catalog catalog you can create schemas and tables in |
| `SCHEMA` | `ofs` | keep | Schema for the source table, Feature Spec, online table, and results (created if missing) |
| `SOURCE_TABLE_NAME` | `entity_features` | keep, or your table | Notebook 01 writes a synthetic table with this name. To test your own data, point this at your table and skip 01 |
| `LOOKUP_KEY`, `ENTITY_MIN_ID`, `ENTITY_MAX_ID`, `N_ROWS`, `N_FEATURES`, `FEATURE_NAMES` | `entity_id`, 1, 4M, 4M, 20, `f01`…`f20` | only with your own table | Primary-key column, the key range the load tests request, and the feature columns |
| `LAKEBASE_PROJECT` | `loadtest` | **Required** | ID of the Lakebase Autoscaling project registered as your online store (Lakebase app → project; `databricks postgres list-projects`) |
| `ENDPOINT_NAME` | `ofs-entity-features` | keep | Feature Serving endpoint notebook 02 creates |
| `MIN_/MAX_PROVISIONED_CONCURRENCY` | 256 / 512 | review | Feature Serving size (multiples of 4). The main cost driver. At 2K QPS, 256–512 measured p95 22.2–26.2 ms and 64–256 measured 24.4–25.9 ms; start at 256–512 and step down (see *Start here*) |
| `SECRET_SCOPE`, `SP_CLIENT_ID_KEY`, `SP_CLIENT_SECRET_KEY` | `ofs-loadtest`, `service_principal_client_id`, `service_principal_client_secret` | keep | Where the service principal's credentials are stored (see below) |
| `PG_PASSWORD_ROLE`, `PG_PASSWORD_KEY` | `ofs_loadtest_pw`, `pg_pooled_password` | keep | Only for the optional pooled-Postgres path (notebook 02b) |
| `RESULTS_CATALOG`, `RESULTS_SCHEMA` | same as `CATALOG`, `SCHEMA` | optional | Where the results tables are written |
| `LAKEBASE_BRANCH`, `ONLINE_STORE_NAME` | `production`, project ID | optional | Lakebase branch the online table lives on; online store name if it differs from the project ID |
| `P95_SLO_MS` | 25 | your target | Latency target used for pass/fail everywhere |
| `WORKSPACE_HOST`, `DATA_API_URL` | auto | leave unset | Derived at run time; set only if detection is wrong |

**Using your own table:** keys must be integers and **every key from `ENTITY_MIN_ID` to `ENTITY_MAX_ID` must exist**. The tests request random keys in that range, and a missing row counts as an error on the Postgres and Data API paths (Feature Serving returns nulls instead), which would skew the comparison.

**`job_rerun.json`** (the benchmark job)

| Field | Default | Change? | What it is |
|---|---|---|---|
| `notebook_path` (3 tasks) | `/Users/you@example.com/ofs-loadtest/…` | **Required** | Workspace folder you imported the notebooks into |
| `single_user_name` | `you@example.com` | **Required** | The user the 32-vCPU cluster runs as; must own the Lakebase project |
| `node_type_id` | `c6id.8xlarge` | keep | 32-vCPU AWS node with local NVMe. Another `…d` type of that size works; `c7i.8xlarge` is rejected without an attached EBS volume |
| `spark_version` | `17.3.x-scala2.13` | keep | DBR 17.3 LTS |

Notebook-specific load settings (Locust users, QPS ladder, soak duration, the CU matrix) live in each notebook's own configuration cell and have safe defaults. Notebook 05 only resizes Lakebase if you pass a `lakebase_cu` matrix.

**Service principal and secret scope** (once; run in a terminal, never paste secrets into notebooks or chat). **If you already have a service principal, use it:** skip the `service-principals create` command. You still need an OAuth secret for it: create one with the second command (or reuse one you hold), then store its application ID and secret in the scope. `<sp-id>` is the service principal's numeric ID, not its application ID. Notebook 02 grants it everything else it needs (endpoint `CAN_QUERY`, a Lakebase role, `SELECT` on the online table, Data API access).

```bash
databricks service-principals create --json '{"displayName": "ofs-loadtest-sp", "entitlements": [{"value": "workspace-access"}]}'
databricks service-principal-secrets-proxy create <sp-id>          # shows the OAuth secret once
databricks secrets create-scope ofs-loadtest
databricks secrets put-secret ofs-loadtest service_principal_client_id --string-value <sp-application-id>
databricks secrets put-secret ofs-loadtest service_principal_client_secret   # prompts for the secret
```

## Setup

1. **Import the folder and fill in the values above** (`00_config.py`, `job_rerun.json`): `databricks workspace import-dir ./lakebase-ofs-loadtest /Workspace/Users/<you>/ofs-loadtest`. Every notebook `%run`s `00_config` first.
2. **Run 01, then 02**, on serverless. 01 writes the synthetic 4M-row source table (skip it to use your own table). 02 publishes the snapshot, creates the Feature Spec and the route-optimized Feature Serving endpoint, and grants the SP access.
3. **Enable the Data API (UI only):** Lakebase project → **Data API** → enable on the online table's database → add the table's schema to **Exposed schemas**. 05 derives the Data API URL; set `DATA_API_URL` in `00_config` only if yours differs.
   **Then re-run notebook 02 from the top** (safe: it skips the endpoint update when nothing changed). It grants the SP role to the Data API `authenticator`; without it the `dataapi` path returns 403. Wait a few minutes before testing: right after exposing a schema some requests return `406 PGRST106`.
4. **Optional, `pg_pooled`** ([Lakebase connection pooling](https://docs.databricks.com/aws/en/oltp/projects/connection-pooling); the pooler only accepts native password roles, not OAuth): enable native Postgres login on the project, store a generated password in the secret scope (commands in 02b's brief), run **02b**, and add `pg_pooled` to n05's `access_paths` in `job_rerun.json` (`fs,dataapi,pg,pg_pooled`).
5. **Run the suite:** `databricks jobs create --json @job_rerun.json`, then run the job: 03 → 05 → 06 (03 and 05 on the 32-vCPU job cluster). To re-run only 05 with other settings (repeats, a CU matrix, a second endpoint):

   ```bash
   databricks jobs run-now --json '{"job_id": <job-id>, "only": ["n05_stress"],
     "notebook_params": {"access_paths": "fs,dataapi,pg", "target_qps": "1000,2000,4000", "lakebase_cu": "16-16,8-8", "fs_endpoints": "<main>,<new>"}}'
   ```

   05's default ladder is 7 steps (250 → 12,000 QPS). Use `target_qps=1000,2000,4000` for `lakebase_cu` matrices so n05 stays under its 2-hour timeout, and keep 2000 in any ladder (the dashboard's headline reads that step).
6. **Optional, 07:** run `07_recovery` by itself to check that a config update causes no lookup errors. It re-applies the endpoint's current size, which re-provisions it (15–20 minutes), and stops if the live size differs from `00_config`.

**Adding a second Feature Serving endpoint** (to compare provisioned concurrency):

1. In `00_config`, set `ENDPOINT_NAME` to a new name and `MIN_/MAX_PROVISIONED_CONCURRENCY` to the new sizes.
2. Run 02 (re-publishes the snapshot, reuses the Feature Spec, creates the endpoint, grants the SP; 15–20 minutes).
3. Set all three values back. If you use `pg_pooled`, re-run 02b (the re-publish can drop its grant).
4. Pass both names to 05: `fs_endpoints=<main>,<new>`.

## How to read the results

**How notebook 05 measures.** Each step offers a fixed target QPS with Poisson arrivals from one generator process per core, for 60 s (the first 5 s excluded). Latency is measured from each request's **scheduled** send time, so if a path falls behind, the queueing is counted rather than hidden. A result reads "at X QPS on config C, path P gives p95 Y", whatever the caller's pod or connection count. `fs_endpoints` measures several Feature Serving endpoints side by side; `lakebase_cu` runs one round per pinned Lakebase size (each resize takes ~20 s and drops all connections; 05 restores the original size at the end, even on failure). Results are saved after each round, so a failed run keeps its completed steps. Each step takes ~70 s per path: 3 sizes × 5 paths × 3 steps is about an hour.

| Term | Meaning |
|---|---|
| Offered QPS (`target_qps`) | The rate the generator sends. The x-axis of every 05 chart |
| Achieved QPS (`qps`) | Requests that completed. Equal to offered while a path keeps up; below offered means it is saturated |
| p50 / p95 / p99 | Latency in ms from the scheduled send time, so queueing is included |
| `slo_pass` | Zero errors, zero unfinished, p95 ≤ `P95_SLO_MS`, and ≥ 95% of offered achieved. `CONTINUE_PAST_SLO` keeps climbing after a failure until a hard stop (p95 > 100 ms, > 1% errors, or < 90% achieved) |
| `failures` | Errors plus `unfinished` requests (not completed within 5 s of the step ending: a backlog) |
| `client_bound` | The generator, not the path, was the limit (CPU or scheduling lag). Never counted as a path's ceiling; ignore it as a measurement |
| `config` | Server sizes for the row, e.g. `LB 16-32 CU · FS 256-512`. Compare like with like |
| `users` (05 rows) | Client connections the generator opened for that path and step |

**Dashboard.** Pick a run in the *Notebook 05 run* filter first; with none selected the tiles and tables mix every run.

1. **Tiles and latency at 2,000 QPS:** the headline. Tiles show the best configuration that met the SLO (blank = none did). The table lists every path × config, fastest first; read p95 against your target and p99 for tail risk.
2. **Latency menu:** per path × config, the highest offered QPS that passed. `≥ N` means it never failed within the ladder; otherwise *First unstable QPS* shows where it broke.
3. **p95 vs offered QPS:** flat means headroom, a bend is queueing starting, a jump is saturation. **Achieved vs offered:** on the diagonal means keeping up; flattening marks the ceiling.
4. **Full curve table:** every step with failures, connections, and `client_bound`. Check here before trusting a surprising number.
5. **Feature Serving phases (03, 06, 07):** Locust charts and phase bars show the latest run of each; the *Phase results* table lists every run. `users` there are concurrent clients, not QPS.

**Interpreting a comparison**

- **Read each path at the QPS you need**, not at its maximum; the ceiling only tells you headroom.
- **Capacity buys headroom and tail, not a faster median.** Feature Serving provisioned concurrency moves its tail and its 429 limit; Lakebase CU moves the Data API's ceiling and the connection slots.
- **Small differences are noise.** Expect ~1 ms between runs on Postgres and a few ms on Feature Serving p95; repeat (*Start here*, step 3) before trusting a borderline pass.
- **Errors name the limit** (`notes` keeps up to three error bodies per row): `429 Too many concurrent requests` = Feature Serving provisioned concurrency; `remaining connection slots are reserved` = Lakebase connections; timeouts on several paths at once = Lakebase compute saturated.
- **This is the in-region floor.** Callers outside Databricks add their own network latency to every path. We expect the ranking to hold, but verify from your clients: direct Postgres from EKS also needs a network path to the Lakebase host and its own OAuth credential refresh.

## Results and dashboard

Every notebook appends to three Delta tables in `RESULTS_CATALOG.RESULTS_SCHEMA`; nothing is overwritten:

| Table | Grain | Contents |
|---|---|---|
| `ofs_loadtest_runs` | run × path × step | `run_id`, `notebook`, `phase`, `access_path`, `target_qps`, `qps`, p50/p95/p99, `failures`, `slo_pass`, `client_bound`, `config`, Lakebase CU, FS concurrency, `generator`, error samples in `notes` |
| `ofs_loadtest_history` | per second | Locust time series (notebook 03) |
| `ofs_loadtest_failures` | error body | Locust failures with a classification |

Compare configurations in one run, then repeats across runs (a path × config meets the target only if every run passed):

```sql
SELECT config, access_path, target_qps, p50_ms, p95_ms, p99_ms, failures, slo_pass
FROM ofs_loadtest_runs WHERE run_id = '<run_id>' ORDER BY target_qps, p95_ms;

SELECT config, access_path, COUNT(DISTINCT run_id) AS runs, MIN(p95_ms) AS best_p95, MAX(p95_ms) AS worst_p95,
       BOOL_AND(slo_pass) AS passed_every_run
FROM ofs_loadtest_runs WHERE notebook = '05_stress' AND target_qps = 2000
GROUP BY config, access_path ORDER BY access_path, worst_p95;
```

**Dashboard** (`ofs_loadtest.lvdash.json`, ships with no data). Its SQL uses unqualified table names; set the catalog and schema when you import it:

```bash
databricks lakeview create --display-name "OFS access-path latency" --warehouse-id <warehouse-id> \
  --dataset-catalog <RESULTS_CATALOG> --dataset-schema <RESULTS_SCHEMA> \
  --serialized-dashboard "$(cat ofs_loadtest.lvdash.json)"
databricks lakeview publish <dashboard-id> --warehouse-id <warehouse-id>
```

## Notebooks

| Notebook | Purpose |
|---|---|
| `00_config` | All shared configuration and the service-principal / token helpers (`%run` by every notebook) |
| `00_results_persist` | Results tables and persist helpers |
| `01_generate_source` | Synthetic Delta table (one current row per key, CDF on, PK) |
| `02_publish_and_serve` | Snapshot publish, Feature Spec, route-optimized endpoint with explicit provisioned concurrency, SP grants (endpoint `CAN_QUERY`, Lakebase role and `SELECT`, Data API `authenticator`) |
| `02b_pooled_role` | Optional: native password role for the pooled host |
| `03_locust_run` | Feature Serving smoke (unpaced) and a ramp paced to 2,000 QPS with Locust (`FastHttpUser`, one process per core) |
| `05_stress` | Open-loop target-QPS ladder and configuration matrix across all paths |
| `06_soak` | Bounded closed-loop Feature Serving soak (default 10 min; `duration_seconds`) |
| `07_recovery` | Optional, run by itself: re-applies the endpoint config and checks lookups before and after |

## Known pitfalls

- **`psycopg2-binary` / `psycopg[binary]` abort on FIPS-enabled (compliance security profile) compute** (`FATAL FIPS SELFTEST FAILURE`): the wheels bundle their own OpenSSL. The notebooks build `psycopg2` from source against the host OpenSSL.
- **Data API roles must be created in SQL** (`databricks_create_role('<sp-app-id>', 'SERVICE_PRINCIPAL')`, then `GRANT "<sp-app-id>" TO authenticator`); roles made in the Lakebase Roles UI/API fail that grant (`ADMIN option`). In our testing the project owner's identity could not query the Data API; test with the SP.
- **Pooled host:** rejects OAuth (`SASL authentication failed`, then `Too many connections attempts`). Use a native password role (02b). Lakebase needs the plaintext password in `CREATE/ALTER ROLE` (it rejects SCRAM verifiers); 02b sends it only over TLS, turns statement logging off where permitted, and never prints it.
- **Feature Serving endpoints must be route-optimized at creation** (`data_plane_info` is empty otherwise; it cannot be enabled in place). Every `update_config` re-provisions for 15+ minutes; 02 skips it when nothing changed.
- **Small Lakebase sizes have few connection slots** (~225 per CU). Opening hundreds of connections at 2–4 CU hits `remaining connection slots are reserved…` and `Failed to acquire permit to connect…`, and Feature Serving and the Data API (same Lakebase compute) fail too. Size client pools to QPS × latency (~30 connections for 8K QPS at ~4 ms); 05 caps each Postgres path at 64 and retries with backoff.
- **Workspace URL on classic clusters:** the SDK's default host can be the regional control plane, which SP OAuth rejects; `00_config` reads the cluster's workspace URL instead.
- **Generator capacity:** an under-provisioned or closed-loop client reports its own ceiling as the path's. One Locust process with `HttpUser` on a 4-core serverless notebook caps near ~1K requests/s, so 03 and 05 run on the 32-vCPU cluster with one process per core. Check `client_bound`.

## Appendix: reference results (a Databricks test workspace, AWS us-east-1)

Synthetic 4M-row table, 20 numeric features, uniform keys, single-key lookups, 32-process generator on `c6id.8xlarge`. Use these for the shape of the trade-off, then re-run with your own table and sizes. Run labels (e.g. `e8828493`) are the first 8 characters of `run_id` in our results tables.

**At 2,000 QPS by Lakebase size (p50 / p95 / p99 ms; 8–32 CU from matrix run `e8828493`, 2 and 4 CU from `57240dd4`):**

| Path | 2 CU | 4 CU | 8 CU | 16 CU | 32 CU |
|---|---|---|---|---|---|
| Direct PG | could not connect | 3.4 / 5.6 / 9.2 | 3.6 / 5.5 / 8.2 | 3.6 / 5.4 / 7.9 | 3.6 / 5.4 / 7.6 |
| Pooled PG | 4.9 / 16.7 / 61.6 | 3.5 / 5.7 / 9.4 | 3.6 / 5.5 / 7.7 | 3.6 / 5.4 / 7.4 | 3.7 / 5.7 / 8.3 |
| Feature Serving 256–512 | times out at 1K | 17.9 / 22.6 / 30.1 | 18.7 / 22.6 / 29.1 | 19.0 / 23.6 / 33.0 | 18.8 / 22.7 / 28.4 |
| Feature Serving 64–256 | — | — | 19.4 / 25.02 ✗ / 32.3 | 19.7 / 25.9 ✗ / 34.8 | 19.7 / 25.8 ✗ / 33.1 |
| Data API | fails at 1K (p95 2.6 s) | fails at 1K (p95 6.6 s) | fails at 1K (p95 0.5 s) | 7.3 / 10.1 / 134.6 | 7.2 / 9.0 / 40.3 |

**Feature Serving p95 at 2,000 QPS, every run (ms):**

| Size | Measurements (run, Lakebase CU) |
|---|---|
| 256–512 | 22.2 (`a66dbd92`, 16–32) · 22.6 (`57240dd4`, 4) · 22.6 / 23.6 / 22.7 (`e8828493`, 8 / 16 / 32) · 26.2 (`21bb1e7f`, 16–32) |
| 64–256 | 24.4 (`4a3f2526`, 4–8) · 25.02 / 25.9 / 25.8 (`e8828493`, 8 / 16 / 32) |

**Read:**
- **Postgres (direct or pooled) is the lowest-latency path at 2K:** ~5× lower p50 and ~4× lower p95 than Feature Serving, at every size from 4 to 32 CU. On 64 connections it stayed within target to 12K QPS (p95 6 → 13 ms from 2K to 12K, run `21bb1e7f`). The pooler added no measurable latency; its value is absorbing a large fleet's connections.
- **Feature Serving's ~19 ms median did not move with size** (250 QPS to 8K, 4 to 32 CU). Provisioned concurrency sets its tail and its limit: 256–512 sits on the 25 ms line at 2K, 64–256 just over it, and 64–256 returned `429 Too many concurrent requests` at 8K (run `4a3f2526`).
- **The Data API is the only path that scales with Lakebase CU:** under 1K QPS at 4–8 CU, 2K at 16–32 CU (spiky p99), saturated at 4K.
- **Locust cross-check** (notebook 03, run `7b3c0520`, 256–512): 596,078 requests at 1,986 QPS, 0 failures, p50 / p95 / p99 17 / 22 / 28 ms, matching 05 from an independent tool (03 sends 20% of requests to a hot key range; 05 uses uniform keys).

**Connection sizing** (observed in this test via `pg_stat_activity`; not documented limits and may change):
- Feature Serving's backend (`application_name = feature_store_lookup`) opened **~340 connections** per 256–512 endpoint as soon as traffic started (up to ~600 at 16–32 CU) and held them. The Data API kept a **~200-connection** pool.
- At 2 CU (450 slots) those two alone exceed the limit: Feature Serving times out and new Postgres connections are refused; the pooled host keeps working because PgBouncer already holds its backend connections. Earlier 2 and 4 CU runs with two Feature Serving endpoints and 256 Postgres connections failed on every path.
- **Rule:** keep *Σ Feature Serving backend pools + Data API pool + application pools (QPS × latency)* under `max_connections` with headroom. Here, 4 CU served Postgres and one Feature Serving endpoint at 2K QPS with no latency penalty; the Data API needed ≥ 16 CU for 2K QPS.
