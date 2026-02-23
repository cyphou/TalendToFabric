# Fabric Notebook — SCD Type 2 Pattern
# Migrated from Talend job: {{talend_job_name}}
# Pattern: Slowly Changing Dimension Type 2
#
# Implements SCD Type 2 by tracking historical changes with
# effective_from, effective_to, and is_current columns.

# COMMAND ----------
# Parameters

run_date = "2026-01-01"
target_table = "{{lakehouse_name}}.{{schema}}.dim_{{entity}}"

# COMMAND ----------
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging
from datetime import datetime

logger = logging.getLogger("SCD2_{{entity}}")

# COMMAND ----------
# Configuration

JDBC_URL = "jdbc:postgresql://{{server}}.postgres.database.azure.com:5432/{{database}}"
JDBC_PROPERTIES = {
    "user": "{{username}}",
    "password": mssparkutils.credentials.getSecret("{{key_vault}}", "{{secret_name}}"),
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslmode": "require",
}

# SCD2 configuration
BUSINESS_KEY = "{{business_key}}"  # Natural/business key (e.g., "product_code")
TRACKED_COLUMNS = [  # Columns that trigger a new version when changed
    # "{{tracked_col_1}}",
    # "{{tracked_col_2}}",
    # "{{tracked_col_3}}",
]
SURROGATE_KEY = "sk_{{entity}}"  # Surrogate key column name

# COMMAND ----------
# STEP 1: Read source data

source_query = "(SELECT * FROM {{schema}}.{{source_table}}) AS src"
source_df = spark.read.jdbc(url=JDBC_URL, table=source_query, properties=JDBC_PROPERTIES)
logger.info(f"Source records: {source_df.count()}")

# COMMAND ----------
# STEP 2: Add hash column for change detection

def add_change_hash(df: DataFrame, tracked_cols: list) -> DataFrame:
    """Add a hash column based on tracked columns for change detection."""
    if not tracked_cols:
        # If no specific columns defined, hash all non-key columns
        tracked_cols = [c for c in df.columns if c != BUSINESS_KEY]

    hash_expr = F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in tracked_cols]))
    return df.withColumn("_row_hash", hash_expr)

source_with_hash = add_change_hash(source_df, TRACKED_COLUMNS)

# COMMAND ----------
# STEP 3: Implement SCD Type 2 merge

def apply_scd2(spark: SparkSession, source_df: DataFrame, target_table: str,
               business_key: str, now: datetime = None):
    """
    Apply SCD Type 2 logic:
    1. New records → Insert with is_current = True
    2. Changed records → Close old version (is_current = False), insert new version
    3. Unchanged records → No action
    4. Deleted records → Optionally close (soft delete)
    """
    if now is None:
        now = datetime.utcnow()

    now_ts = F.lit(now)
    max_date = F.lit(datetime(9999, 12, 31))

    # Prepare source with SCD columns
    source_prepared = source_df \
        .withColumn("effective_from", now_ts) \
        .withColumn("effective_to", max_date) \
        .withColumn("is_current", F.lit(True)) \
        .withColumn("_etl_loaded_at", F.current_timestamp()) \
        .withColumn("_etl_source", F.lit("{{talend_job_name}}"))

    # Check if target exists
    if not spark.catalog.tableExists(target_table):
        # First load — insert all with surrogate keys
        logger.info("First load — creating target table")
        window = Window.orderBy(F.monotonically_increasing_id())
        result = source_prepared.withColumn(SURROGATE_KEY, F.row_number().over(window))
        result.write.format("delta").saveAsTable(target_table)
        logger.info(f"Created {target_table} with {result.count()} rows")
        return

    # Load current dimension records
    target_delta = DeltaTable.forName(spark, target_table)
    current_df = spark.sql(f"SELECT * FROM {target_table} WHERE is_current = true")

    # Identify changes by comparing hashes
    changes = source_df.alias("src").join(
        current_df.alias("tgt"),
        F.col(f"src.{business_key}") == F.col(f"tgt.{business_key}"),
        "full_outer"
    )

    # New records (in source but not in target)
    new_records = changes.filter(F.col(f"tgt.{business_key}").isNull()) \
        .select("src.*")

    # Changed records (hash differs)
    changed_records = changes.filter(
        (F.col(f"tgt.{business_key}").isNotNull()) &
        (F.col(f"src.{business_key}").isNotNull()) &
        (F.col("src._row_hash") != F.col("tgt._row_hash"))
    ).select("src.*")

    # Deleted records (in target but not in source)
    deleted_keys = changes.filter(F.col(f"src.{business_key}").isNull()) \
        .select(F.col(f"tgt.{business_key}").alias(business_key))

    logger.info(f"New: {new_records.count()}, Changed: {changed_records.count()}, Deleted: {deleted_keys.count()}")

    # Generate next surrogate key
    max_sk = spark.sql(f"SELECT COALESCE(MAX({SURROGATE_KEY}), 0) as max_sk FROM {target_table}").collect()[0]["max_sk"]

    # Step A: Close old versions for changed records
    if changed_records.count() > 0:
        changed_keys = changed_records.select(business_key)
        target_delta.alias("tgt").merge(
            changed_keys.alias("close"),
            f"tgt.{business_key} = close.{business_key} AND tgt.is_current = true"
        ).whenMatchedUpdate(set={
            "effective_to": now_ts,
            "is_current": F.lit(False),
        }).execute()

    # Step B: Close deleted records (soft delete)
    if deleted_keys.count() > 0:
        target_delta.alias("tgt").merge(
            deleted_keys.alias("del"),
            f"tgt.{business_key} = del.{business_key} AND tgt.is_current = true"
        ).whenMatchedUpdate(set={
            "effective_to": now_ts,
            "is_current": F.lit(False),
        }).execute()

    # Step C: Insert new versions (new + changed records)
    records_to_insert = new_records.unionByName(changed_records, allowMissingColumns=True)

    if records_to_insert.count() > 0:
        window = Window.orderBy(F.monotonically_increasing_id())
        records_to_insert = records_to_insert \
            .withColumn(SURROGATE_KEY, F.row_number().over(window) + F.lit(max_sk)) \
            .withColumn("effective_from", now_ts) \
            .withColumn("effective_to", max_date) \
            .withColumn("is_current", F.lit(True)) \
            .withColumn("_etl_loaded_at", F.current_timestamp()) \
            .withColumn("_etl_source", F.lit("{{talend_job_name}}"))

        records_to_insert.write.format("delta").mode("append").saveAsTable(target_table)
        logger.info(f"Inserted {records_to_insert.count()} new dimension versions")

apply_scd2(spark, source_with_hash, target_table, BUSINESS_KEY)

# COMMAND ----------
# STEP 4: Validate

total = spark.sql(f"SELECT COUNT(*) as total FROM {target_table}").collect()[0]["total"]
current = spark.sql(f"SELECT COUNT(*) as current FROM {target_table} WHERE is_current = true").collect()[0]["current"]
historical = total - current

logger.info(f"SCD2 complete — Total: {total}, Current: {current}, Historical: {historical}")
display(spark.sql(f"SELECT * FROM {target_table} ORDER BY {BUSINESS_KEY}, effective_from DESC LIMIT 20"))
