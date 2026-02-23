# Fabric Notebook — Lookup & Join Pattern
# Migrated from Talend job: {{talend_job_name}}
# Pattern: Lookup join — replaces Talend tMap with lookup input
#
# In Talend, tMap with multiple inputs performs lookups.
# In Spark, this translates to DataFrame joins.

# COMMAND ----------
# Parameters

run_date = "2026-01-01"
target_table = "{{lakehouse_name}}.{{schema}}.{{target_table}}"

# COMMAND ----------
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger("LookupJoin_{{entity}}")

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
# STEP 1: Read main input (Talend Main Row)

main_query = "(SELECT * FROM {{schema}}.{{main_table}}) AS main"
main_df = spark.read.jdbc(url=JDBC_URL, table=main_query, properties=JDBC_PROPERTIES)
logger.info(f"Main input: {main_df.count()} rows")

# COMMAND ----------
# STEP 2: Read lookup inputs (Talend Lookup Rows)

# Lookup 1 — e.g., dimension table
lookup1_query = "(SELECT * FROM {{schema}}.{{lookup_table_1}}) AS lkp1"
lookup1_df = spark.read.jdbc(url=JDBC_URL, table=lookup1_query, properties=JDBC_PROPERTIES)
logger.info(f"Lookup 1: {lookup1_df.count()} rows")

# Lookup 2 — e.g., reference table (add more as needed)
# lookup2_query = "(SELECT * FROM {{schema}}.{{lookup_table_2}}) AS lkp2"
# lookup2_df = spark.read.jdbc(url=JDBC_URL, table=lookup2_query, properties=JDBC_PROPERTIES)

# COMMAND ----------
# STEP 3: Perform Joins (migrated from Talend tMap)

def perform_lookups(main_df: DataFrame, lookup1_df: DataFrame) -> DataFrame:
    """
    Join main input with lookup tables.

    Talend tMap join modes:
    - Left Outer Join → "left" in Spark
    - Inner Join → "inner" in Spark
    - Unique Match (first match) → use dropDuplicates on lookup before join

    Talend tMap reject handling:
    - Inner join rejects → use anti join to capture rejected rows
    """

    # Deduplicate lookup if Talend was set to "first match"
    lookup1_deduped = lookup1_df.dropDuplicates(["{{lookup_key}}"])

    # Rename lookup columns to avoid ambiguity
    for col_name in lookup1_deduped.columns:
        if col_name != "{{lookup_key}}":
            lookup1_deduped = lookup1_deduped.withColumnRenamed(col_name, f"lkp1_{col_name}")

    # Main join — migrated from tMap
    joined_df = main_df.join(
        lookup1_deduped,
        main_df["{{main_join_key}}"] == lookup1_deduped["{{lookup_key}}"],
        how="left",  # Change to "inner" if Talend used inner join mode
    )

    # Handle rejected rows (Talend tMap reject output)
    # rejected_df = main_df.join(
    #     lookup1_deduped,
    #     main_df["{{main_join_key}}"] == lookup1_deduped["{{lookup_key}}"],
    #     how="left_anti",
    # )
    # rejected_df.write.format("delta").mode("append").saveAsTable("{{reject_table}}")

    return joined_df

result_df = perform_lookups(main_df, lookup1_df)

# COMMAND ----------
# STEP 4: Apply column mappings (migrated from tMap output expressions)

def apply_mappings(df: DataFrame) -> DataFrame:
    """Map and transform columns — from Talend tMap output expressions."""

    result = df.select(
        # Direct column mappings
        F.col("{{main_col_1}}").alias("{{output_col_1}}"),
        F.col("lkp1_{{lookup_col_1}}").alias("{{output_col_2}}"),

        # Expression mappings (migrated from Talend tMap expressions)
        # F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("full_name"),
        # F.when(F.col("status") == "A", "Active").otherwise("Inactive").alias("status_desc"),
        # F.coalesce(F.col("lkp1_value"), F.lit(0)).alias("lookup_value"),

        # Audit columns
        F.current_timestamp().alias("_etl_loaded_at"),
        F.lit("{{talend_job_name}}").alias("_etl_source"),
    )

    return result

mapped_df = apply_mappings(result_df)
display(mapped_df.limit(10))

# COMMAND ----------
# STEP 5: Write to target

mapped_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

logger.info(f"Written {mapped_df.count()} rows to {target_table}")
