# ======================================================================
# CDC / Watermark Pattern — Fabric Spark Notebook Template
# Migrated from Talend tOracleCDC / tMysqlCDC / tDBInput with WHERE
#
# Change Data Capture using high-watermark column:
#   1. Read the current watermark from control table
#   2. Extract only changed records since last watermark
#   3. Load / merge into target
#   4. Update watermark for next run
#
# Replaces Talend CDC components (tOracleCDC, tMysqlCDC, tMSSqlCDC)
# and incremental patterns using context variables as watermarks.
# ======================================================================

# COMMAND ----------
# Parameters

source_jdbc_url = "jdbc:postgresql://{{server}}.postgres.database.azure.com:5432/{{database}}"
source_table = "{{schema}}.{{source_table}}"
target_table = "{{lakehouse_name}}.{{schema}}.{{target_table}}"
watermark_column = "{{watermark_column}}"  # e.g. "modified_date", "updated_at", "change_id"
business_keys = {{business_keys}}  # e.g. ["id"]
watermark_table = "{{lakehouse_name}}.{{schema}}._watermarks"

# COMMAND ----------
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("CDC_Watermark")

JDBC_PROPERTIES = {
    "user": "{{username}}",
    "password": mssparkutils.credentials.getSecret("{{key_vault}}", "{{secret_name}}"),
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslmode": "require",
}

# COMMAND ----------
# STEP 1: Read current watermark
#
# Talend equivalent: tContextLoad (read last run timestamp from DB/file)
# or tOracleCDC subscriber position

def get_watermark(table_name: str) -> str:
    """Get the last watermark value for a table from the control table."""
    try:
        wm_df = spark.read.table(watermark_table)
        row = wm_df.filter(F.col("table_name") == table_name).first()
        if row:
            return row["last_watermark"]
    except Exception:
        # Watermark table doesn't exist yet — first run
        logger.info("Watermark table not found — running full load")
    return "1900-01-01 00:00:00"  # Default: load everything

last_watermark = get_watermark(target_table)
logger.info(f"Last watermark: {last_watermark}")

# COMMAND ----------
# STEP 2: Extract changed records since last watermark
#
# Talend equivalent:
#   - tOracleCDC: reads from Oracle LogMiner / XStream
#   - tMysqlCDC: reads from MySQL binlog
#   - tDBInput with WHERE modified_date > context.last_run_date
#
# In Fabric, we use a simple watermark query (pull-based CDC)

cdc_query = f"""
    (SELECT * FROM {source_table}
     WHERE {watermark_column} > '{last_watermark}'
     ORDER BY {watermark_column}) AS cdc_extract
"""

cdc_df = spark.read.jdbc(
    url=source_jdbc_url,
    table=cdc_query,
    properties=JDBC_PROPERTIES,
)

row_count = cdc_df.count()
logger.info(f"Changed records since {last_watermark}: {row_count}")

if row_count == 0:
    logger.info("No changes detected — skipping merge")
    # dbutils.notebook.exit("NO_CHANGES")

# COMMAND ----------
# STEP 3: Merge into target (Upsert)
#
# Talend equivalent: tDBOutput with INSERT_OR_UPDATE action
# Delta Lake provides transactional MERGE

if row_count > 0:
    if DeltaTable.isDeltaTable(spark, target_table):
        target_delta = DeltaTable.forName(spark, target_table)

        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in business_keys]
        )

        (
            target_delta.alias("target")
            .merge(cdc_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"Merged {row_count} rows into {target_table}")
    else:
        # First load — create the table
        cdc_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        logger.info(f"Created {target_table} with {row_count} rows")

# COMMAND ----------
# STEP 4: Update watermark
#
# Talend equivalent: tDBRow to update context table, or tContextDump

if row_count > 0:
    new_watermark = cdc_df.agg(F.max(watermark_column)).collect()[0][0]
    logger.info(f"New watermark: {new_watermark}")

    # Upsert watermark record
    wm_record = spark.createDataFrame(
        [(target_table, str(new_watermark), F.current_timestamp())],
        ["table_name", "last_watermark", "updated_at"],
    )

    try:
        wm_delta = DeltaTable.forName(spark, watermark_table)
        (
            wm_delta.alias("target")
            .merge(wm_record.alias("source"), "target.table_name = source.table_name")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception:
        # Create watermark table on first run
        wm_record.write.format("delta").mode("overwrite").saveAsTable(watermark_table)

    logger.info(f"Watermark updated to {new_watermark}")

# COMMAND ----------
# NOTES:
# - For real-time CDC, consider Fabric Mirroring (SQL Server, Azure SQL, Cosmos DB)
# - For event-based CDC, use Debezium → Event Hub → Spark Structured Streaming
# - Watermark column should be indexed on the source for performance
# - For soft deletes, add a filter: WHERE is_deleted = true → target DELETE
# - For hard deletes, compare full key sets: source anti-join target → DELETE
