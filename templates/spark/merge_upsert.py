# ======================================================================
# Merge / Upsert Pattern — Fabric Spark Notebook Template
# Migrated from Talend tDBOutput (all action modes)
#
# Talend tDBOutput supports these action modes:
#   - INSERT             → df.write.mode("append")
#   - UPDATE             → Delta MERGE whenMatchedUpdateAll only
#   - INSERT_OR_UPDATE   → Delta MERGE (match→update, no match→insert)
#   - UPDATE_OR_INSERT   → Delta MERGE (match→update, no match→insert)
#   - DELETE             → Delta DELETE
#   - DELETE_INSERT      → Delta DELETE + INSERT (full replace by key)
#
# This template covers ALL action modes with a configurable pattern.
# ======================================================================

# COMMAND ----------
# Parameters

run_date = "{{run_date}}"
source_table = "{{lakehouse_name}}.{{schema}}.{{source_table}}"
target_table = "{{lakehouse_name}}.{{schema}}.{{target_table}}"
business_keys = {{business_keys}}  # e.g. ["order_id"]
action_mode = "{{action_mode}}"   # "INSERT", "UPDATE", "UPSERT", "DELETE", "DELETE_INSERT"

# COMMAND ----------
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("MergeUpsert")

# COMMAND ----------
# STEP 1: Read source data

source_df = spark.read.table(source_table)
row_count = source_df.count()
logger.info(f"Source rows: {row_count}")

# COMMAND ----------
# STEP 2: Execute the appropriate action mode

merge_condition = " AND ".join(
    [f"target.{k} = source.{k}" for k in business_keys]
)


def action_insert(source_df: DataFrame, target_table: str):
    """Talend tDBOutput action = INSERT"""
    source_df.write.format("delta").mode("append").saveAsTable(target_table)
    logger.info(f"Inserted {row_count} rows into {target_table}")


def action_update(source_df: DataFrame, target_table: str):
    """Talend tDBOutput action = UPDATE"""
    target_delta = DeltaTable.forName(spark, target_table)
    (
        target_delta.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .execute()
    )
    logger.info(f"Updated matching rows in {target_table}")


def action_upsert(source_df: DataFrame, target_table: str):
    """Talend tDBOutput action = INSERT_OR_UPDATE / UPDATE_OR_INSERT"""
    if not DeltaTable.isDeltaTable(spark, target_table):
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        logger.info(f"Created {target_table} with {row_count} rows (first load)")
        return

    target_delta = DeltaTable.forName(spark, target_table)
    (
        target_delta.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"Upserted {row_count} rows into {target_table}")


def action_delete(source_df: DataFrame, target_table: str):
    """Talend tDBOutput action = DELETE"""
    target_delta = DeltaTable.forName(spark, target_table)
    (
        target_delta.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedDelete()
        .execute()
    )
    logger.info(f"Deleted matching rows from {target_table}")


def action_delete_insert(source_df: DataFrame, target_table: str):
    """Talend tDBOutput action = DELETE_INSERT (replace matched rows)"""
    target_delta = DeltaTable.forName(spark, target_table)
    (
        target_delta.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedDelete()
        .execute()
    )
    # Now insert fresh rows
    source_df.write.format("delta").mode("append").saveAsTable(target_table)
    logger.info(f"Delete-inserted {row_count} rows in {target_table}")


# Dispatch to the correct action
ACTION_MAP = {
    "INSERT": action_insert,
    "UPDATE": action_update,
    "UPSERT": action_upsert,
    "INSERT_OR_UPDATE": action_upsert,
    "UPDATE_OR_INSERT": action_upsert,
    "DELETE": action_delete,
    "DELETE_INSERT": action_delete_insert,
}

action_fn = ACTION_MAP.get(action_mode.upper())
if action_fn is None:
    raise ValueError(f"Unknown action mode: {action_mode}. Supported: {list(ACTION_MAP.keys())}")

action_fn(source_df, target_table)

# COMMAND ----------
# STEP 3: Verify result

result_count = spark.read.table(target_table).count()
logger.info(f"Target rows after {action_mode}: {result_count}")

# COMMAND ----------
# TALEND ACTION MODE REFERENCE:
#
# | Talend Action      | Delta Lake Equivalent                      |
# |--------------------|--------------------------------------------|
# | INSERT             | df.write.mode("append")                    |
# | UPDATE             | MERGE ... WHEN MATCHED UPDATE               |
# | INSERT_OR_UPDATE   | MERGE ... WHEN MATCHED UPDATE + NOT INSERT  |
# | UPDATE_OR_INSERT   | Same as INSERT_OR_UPDATE                   |
# | DELETE             | MERGE ... WHEN MATCHED DELETE                |
# | DELETE_INSERT      | MERGE DELETE + df.write.mode("append")      |
#
# Additional Talend options mapped:
# - Commit every N rows → Delta auto-commits (transactional)
# - Use batch mode   → df.write.jdbc(batchsize=N) for JDBC targets
# - Enable debug     → logging.setLevel(logging.DEBUG)
# - Die on error     → raise Exception() (default Python behavior)
