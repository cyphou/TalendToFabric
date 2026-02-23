# Fabric Notebook — Incremental Load Pattern
# Migrated from Talend job: {{talend_job_name}}
# Pattern: Incremental / CDC load using watermark column
#
# This pattern replaces Talend jobs that use a date/timestamp column
# to identify new or changed records (tOracleInput with WHERE clause).

# COMMAND ----------
# Parameters

run_date = "2026-01-01"
watermark_column = "modified_date"  # Column used for incremental detection
target_table = "{{lakehouse_name}}.{{schema}}.{{table}}"

# COMMAND ----------
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("IncrementalLoad_{{entity}}")

# COMMAND ----------
# Source configuration

JDBC_URL = "jdbc:postgresql://{{server}}.postgres.database.azure.com:5432/{{database}}"
JDBC_PROPERTIES = {
    "user": "{{username}}",
    "password": mssparkutils.credentials.getSecret("{{key_vault}}", "{{secret_name}}"),
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslmode": "require",
}

# COMMAND ----------
# STEP 1: Get current watermark (last loaded timestamp)

def get_watermark(target_table: str, watermark_column: str) -> str:
    """Get the maximum watermark value from the target table."""
    try:
        result = spark.sql(f"SELECT MAX({watermark_column}) as max_watermark FROM {target_table}")
        max_val = result.collect()[0]["max_watermark"]
        if max_val is None:
            return "1900-01-01 00:00:00"
        return str(max_val)
    except Exception:
        logger.warning(f"Target table {target_table} does not exist yet. Using epoch.")
        return "1900-01-01 00:00:00"

current_watermark = get_watermark(target_table, watermark_column)
logger.info(f"Current watermark: {current_watermark}")

# COMMAND ----------
# STEP 2: Extract new/changed records from source

def read_incremental(spark: SparkSession, watermark: str) -> DataFrame:
    """Read records that have changed since the last watermark."""
    query = f"""(
        SELECT *
        FROM {{schema}}.{{table}}
        WHERE {watermark_column} > '{watermark}'
        ORDER BY {watermark_column}
    ) AS incremental"""

    df = spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPERTIES)
    logger.info(f"Found {df.count()} new/changed records since {watermark}")
    return df

incremental_df = read_incremental(spark, current_watermark)
display(incremental_df.limit(10))

# COMMAND ----------
# STEP 3: Apply transformations (migrated from Talend)

def apply_transforms(df: DataFrame) -> DataFrame:
    """Apply business logic — customize based on Talend job."""
    # Add audit columns
    df = df.withColumn("_etl_loaded_at", F.current_timestamp())
    df = df.withColumn("_etl_source", F.lit("{{talend_job_name}}"))
    return df

transformed_df = apply_transforms(incremental_df)

# COMMAND ----------
# STEP 4: Merge into target (upsert)

def upsert_to_target(df: DataFrame, target_table: str, key_columns: list):
    """
    Merge (upsert) new/changed records into the Delta target table.
    This replicates the behavior of Talend tPostgresqlOutput with 'upsert' mode.
    """
    if df.count() == 0:
        logger.info("No new records to merge.")
        return

    # Check if target table exists
    if spark.catalog.tableExists(target_table):
        delta_table = DeltaTable.forName(spark, target_table)

        # Build merge condition
        merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])

        delta_table.alias("target") \
            .merge(df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        logger.info(f"Merged {df.count()} records into {target_table}")
    else:
        # First run — create the table
        df.write.format("delta").saveAsTable(target_table)
        logger.info(f"Created new table {target_table} with {df.count()} records")

# Key columns for the merge — replace with actual primary key(s)
KEY_COLUMNS = ["id"]  # {{primary_key_columns}}

upsert_to_target(transformed_df, target_table, KEY_COLUMNS)

# COMMAND ----------
# STEP 5: Validate

new_watermark = get_watermark(target_table, watermark_column)
total_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]["cnt"]
logger.info(f"Updated watermark: {new_watermark}")
logger.info(f"Total rows in target: {total_count}")
