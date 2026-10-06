# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Generate the offline feature table
# MAGIC
# MAGIC Creates a synthetic feature table with one current row per entity, a non-null lookup key,
# MAGIC Change Data Feed enabled, and numeric feature columns. The write **overwrites** the target table.
# MAGIC
# MAGIC **Stop here first.** Fill in `00_config`. Values shown are from our test workspace,
# MAGIC not your environment. Do not run until catalog, schema, row count, and feature count
# MAGIC match the customer workspace.
# MAGIC
# MAGIC Run order: **01 → 02 → enable the Data API (UI) → re-run 02 → (02b, optional) → job: 03 → 05 → 06** (07 optional, run by itself).
# MAGIC
# MAGIC Success: `row_count == distinct_ids == N_ROWS`, `min_id == 1`, `max_id == N_ROWS`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer configuration (required)
# MAGIC
# MAGIC Shared values (catalog, tables, endpoint, Lakebase project, secrets, key range, SLO) are set once in
# MAGIC `00_config`. The cell after it holds only this notebook's own settings.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Shared values (CATALOG, SCHEMA, SOURCE, N_ROWS, N_FEATURES, LOOKUP_KEY) come from 00_config.
FULL = SOURCE

print("Using table:", FULL)
print("Rows / features / lookup key:", N_ROWS, N_FEATURES, LOOKUP_KEY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schema and write the feature table
# MAGIC
# MAGIC Synthetic `f01`…`fNN` columns are placeholders. For a customer payload, replace the feature
# MAGIC expressions with the real projection while keeping one unique, non-null lookup key.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

feature_cols = [
    F.expr(f"ln(1 + abs(randn({i}) * (1 + (id % 20) / 20.0)))").alias(f"f{i:02d}")
    for i in range(1, N_FEATURES + 1)
]

df = spark.range(1, N_ROWS + 1).select(F.col("id").alias(LOOKUP_KEY), *feature_cols)
(
    df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FULL)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply table constraints
# MAGIC
# MAGIC Enables Change Data Feed, marks the lookup key NOT NULL, and adds a primary-key constraint.

# COMMAND ----------

spark.sql(
    f"""
    ALTER TABLE {FULL} SET TBLPROPERTIES (
      'delta.enableChangeDataFeed' = 'true'
    )
    """
)
spark.sql(f"ALTER TABLE {FULL} ALTER COLUMN {LOOKUP_KEY} SET NOT NULL")
try:
    spark.sql(f"ALTER TABLE {FULL} ADD CONSTRAINT entity_features_pk PRIMARY KEY ({LOOKUP_KEY})")
except Exception as exc:
    if "already exists" not in str(exc).lower() and "DUPLICATE" not in str(exc).upper():
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate row count and key uniqueness

# COMMAND ----------

import json

stats = spark.sql(
    f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT {LOOKUP_KEY}) AS distinct_ids,
      MIN({LOOKUP_KEY}) AS min_id,
      MAX({LOOKUP_KEY}) AS max_id
    FROM {FULL}
    """
).collect()[0]

result = {
    "table": FULL,
    "row_count": stats.row_count,
    "distinct_ids": stats.distinct_ids,
    "min_id": stats.min_id,
    "max_id": stats.max_id,
}
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
