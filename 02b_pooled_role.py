# Databricks notebook source
# MAGIC %md
# MAGIC # 02b — Native password role for the Lakebase pooled host (optional)
# MAGIC
# MAGIC The Lakebase pooled host (`…-pooler…`, PgBouncer) does **not** accept OAuth database credentials. It needs a
# MAGIC native Postgres role with a password. This notebook creates that role for notebook 05's `pg_pooled` path,
# MAGIC grants it read access to the online table, and checks one lookup through the pooler.
# MAGIC
# MAGIC Only run this if you want the `pg_pooled` path. It requires two changes a customer may not want:
# MAGIC
# MAGIC 1. Native Postgres login enabled on the Lakebase project (off by default):
# MAGIC    `databricks postgres update-project projects/<project> spec.enable_pg_native_login --json '{"spec":{"enable_pg_native_login":true}}'`
# MAGIC 2. A password stored in the secret scope. Generate it outside this notebook so it is never printed:
# MAGIC    `databricks secrets put-secret <scope> pg_pooled_password --string-value "$(openssl rand -hex 24)"`
# MAGIC
# MAGIC Runs as the notebook user, who must own the Lakebase project. Safe to re-run: an existing role gets its
# MAGIC password reset to the secret's current value.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.81.0" psycopg2 --no-binary psycopg2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Shared values come from 00_config: SECRET_SCOPE, PG_PASSWORD_KEY, PG_PASSWORD_ROLE, LAKEBASE_ENDPOINT,
# LAKEBASE_PROJECT, ONLINE_TABLE, LOOKUP_KEY, PG_DBNAME/PG_SCHEMA/PG_TABLE.
PROJECT = f"projects/{LAKEBASE_PROJECT}"

# COMMAND ----------

import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

if not w.postgres.get_project(name=PROJECT).status.enable_pg_native_login:
    raise RuntimeError(f"Native Postgres login is disabled on {PROJECT}. Enable it first (see the brief above).")

password = dbutils.secrets.get(SECRET_SCOPE, PG_PASSWORD_KEY)
hosts = w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status.hosts

# Create the role over the direct host (TLS) as the project owner (OAuth). Lakebase requires the plaintext password
# in CREATE/ALTER ROLE (it rejects pre-hashed SCRAM verifiers: "only supports being given plaintext passwords"), and
# psycopg2 binds parameters client-side, so the literal is part of the statement text. Statement logging is turned
# off for this session where permitted, and the password is never printed.
conn = psycopg2.connect(
    host=hosts.host,
    user=w.current_user.me().user_name,
    password=w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token,
    dbname=PG_DBNAME,
    sslmode="require",
    connect_timeout=30,
)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (PG_PASSWORD_ROLE,))
    verb = "ALTER" if cur.fetchone() else "CREATE"
    try:
        cur.execute("SET log_statement = 'none'")
    except psycopg2.Error:
        pass  # superuser-only on some deployments; the statement is still sent only over TLS
    cur.execute(f'{verb} ROLE "{PG_PASSWORD_ROLE}" LOGIN PASSWORD %s', (password,))
    cur.execute(f'GRANT CONNECT ON DATABASE "{PG_DBNAME}" TO "{PG_PASSWORD_ROLE}"')
    cur.execute(f'GRANT USAGE ON SCHEMA "{PG_SCHEMA}" TO "{PG_PASSWORD_ROLE}"')
    cur.execute(f'GRANT SELECT ON "{PG_SCHEMA}"."{PG_TABLE}" TO "{PG_PASSWORD_ROLE}"')
conn.close()
print(f"{verb.lower()}d role {PG_PASSWORD_ROLE} with SELECT on {PG_DBNAME}.{PG_SCHEMA}.{PG_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check one lookup through the pooled host

# COMMAND ----------

import json

pooled = psycopg2.connect(
    host=hosts.read_write_pooled_host,
    user=PG_PASSWORD_ROLE,
    password=password,
    dbname=PG_DBNAME,
    sslmode="require",
    connect_timeout=30,
)
pooled.autocommit = True
with pooled.cursor() as cur:
    cur.execute(f'SELECT count(*) FROM "{PG_SCHEMA}"."{PG_TABLE}" WHERE "{LOOKUP_KEY}" = %s', (ENTITY_MIN_ID,))
    found = cur.fetchone()[0]
pooled.close()
result = {"pooled_host": hosts.read_write_pooled_host, "role": PG_PASSWORD_ROLE, "lookup_rows": found}
print(json.dumps(result))
if found != 1:
    raise RuntimeError(f"Pooled lookup returned {found} rows for {LOOKUP_KEY}={ENTITY_MIN_ID}")
dbutils.notebook.exit(json.dumps(result))
