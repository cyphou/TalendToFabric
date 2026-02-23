# Fabric Notebook — ETL Template
# Migrated from Talend job: {{talend_job_name}}
# Migration date: {{migration_date}}
# Description: {{description}}
#
# This notebook implements the ETL logic previously handled by Talend.
# It reads from the source, applies transformations, and writes to the Lakehouse.

# COMMAND ----------
# Parameters (set via pipeline or notebook parameters)

# These can be set by the Data Factory pipeline that calls this notebook
run_date = "2026-01-01"  # Will be overridden by pipeline parameter
is_full_load = False

# COMMAND ----------
# Imports

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging
from datetime import datetime

# COMMAND ----------
# Configuration

# Source configuration (PostgreSQL)
JDBC_URL = "jdbc:postgresql://{{server}}.postgres.database.azure.com:5432/{{database}}"
JDBC_PROPERTIES = {
    "user": "{{username}}",
    "password": mssparkutils.credentials.getSecret("{{key_vault}}", "{{secret_name}}"),
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslmode": "require",
}

# Target configuration (Lakehouse)
LAKEHOUSE_PATH = "Tables/{{schema}}/{{table}}"
TARGET_TABLE = "{{lakehouse_name}}.{{schema}}.{{table}}"

# COMMAND ----------
# Logging setup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETL_{{entity}}")
logger.info(f"Starting ETL for {{entity}} — run_date={run_date}, full_load={is_full_load}")

# COMMAND ----------
# STEP 1: Extract — Read from source

def read_source(spark: SparkSession, full_load: bool, run_date: str) -> DataFrame:
    """Read data from source PostgreSQL database."""
    if full_load:
        query = "(SELECT * FROM {{schema}}.{{table}}) AS src"
    else:
        query = f"""(
            SELECT * FROM {{schema}}.{{table}}
            WHERE modified_date >= '{run_date}'
        ) AS src"""

    logger.info(f"Reading from source: {query}")

    df = spark.read.jdbc(
        url=JDBC_URL,
        table=query,
        properties=JDBC_PROPERTIES,
    )

    row_count = df.count()
    logger.info(f"Read {row_count} rows from source")
    return df

source_df = read_source(spark, is_full_load, run_date)
display(source_df.limit(10))

# COMMAND ----------
# STEP 2: Transform — Apply business logic

def apply_transformations(df: DataFrame) -> DataFrame:
    """
    Apply transformations migrated from Talend tMap / tFilter / tAggregate.
    Customize this function based on the specific Talend job logic.
    """

    # --- Migrated from tFilterRow ---
    # df = df.filter(F.col("status") == "ACTIVE")

    # --- Migrated from tMap (column mappings / expressions) ---
    # df = df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
    # df = df.withColumn("amount_eur", F.col("amount_usd") * F.lit(0.92))

    # --- Migrated from tConvertType ---
    # df = df.withColumn("created_date", F.to_date(F.col("created_date_str"), "yyyy-MM-dd"))

    # --- Migrated from tReplace ---
    # df = df.withColumn("phone", F.regexp_replace(F.col("phone"), "[^0-9]", ""))

    # --- Migrated from tUniqRow ---
    # df = df.dropDuplicates(["id"])

    # --- Migrated from tAggregate ---
    # df = df.groupBy("category").agg(
    #     F.count("*").alias("total_count"),
    #     F.sum("amount").alias("total_amount"),
    # )

    # --- Add audit columns ---
    df = df.withColumn("_etl_loaded_at", F.current_timestamp())
    df = df.withColumn("_etl_source", F.lit("{{talend_job_name}}"))

    return df

transformed_df = apply_transformations(source_df)
display(transformed_df.limit(10))

# COMMAND ----------
# STEP 3: Load — Write to Lakehouse (Delta)

def write_to_lakehouse(df: DataFrame, target_table: str, mode: str = "append"):
    """Write DataFrame to Lakehouse Delta table."""
    logger.info(f"Writing {df.count()} rows to {target_table} (mode={mode})")

    df.write \
        .format("delta") \
        .mode(mode) \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)

    logger.info(f"Write complete: {target_table}")

# Use "overwrite" for full load, "append" for incremental
write_mode = "overwrite" if is_full_load else "append"
write_to_lakehouse(transformed_df, TARGET_TABLE, mode=write_mode)

# COMMAND ----------
# STEP 4: Validation

result_df = spark.sql(f"SELECT COUNT(*) as row_count FROM {TARGET_TABLE}")
display(result_df)

logger.info(f"ETL complete for {{entity}} — {result_df.collect()[0]['row_count']} rows in target")
