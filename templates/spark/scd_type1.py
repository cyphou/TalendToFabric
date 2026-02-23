# ======================================================================
# SCD Type 1 Pattern — Fabric Spark Notebook Template
# Migrated from Talend tDBOutput (UPDATE_OR_INSERT) / tMap + tDBOutput
#
# SCD Type 1: Overwrite dimension attributes in-place (no history).
# When a source record matches an existing record by business key,
# update the attribute columns. Otherwise, insert a new row.
# ======================================================================

# COMMAND ----------
# Parameters

run_date = "{{run_date}}"
source_table = "{{lakehouse_name}}.{{schema}}.{{source_table}}"
target_table = "{{lakehouse_name}}.{{schema}}.{{target_table}}"
business_keys = {{business_keys}}  # e.g. ["customer_id"]

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("SCD_Type1")

# COMMAND ----------
# STEP 1: Read source data (Talend Source → Spark DataFrame)

source_df = spark.read.table(source_table)
logger.info(f"Source rows: {source_df.count()}")

# COMMAND ----------
# STEP 2: Perform SCD Type 1 Merge (Talend tDBOutput UPDATE_OR_INSERT → Delta MERGE)
#
# Talend equivalent:
#   tMap → tDBOutput (action = "INSERT_OR_UPDATE" / "UPDATE_OR_INSERT")
#
# Delta Lake MERGE INTO provides atomic upsert:
#   - WHEN MATCHED → UPDATE SET all non-key columns
#   - WHEN NOT MATCHED → INSERT all columns

target_delta = DeltaTable.forName(spark, target_table)

# Build merge condition on business keys
merge_condition = " AND ".join(
    [f"target.{k} = source.{k}" for k in business_keys]
)

# Identify attribute columns (all columns except the business keys)
all_columns = source_df.columns
attribute_columns = [c for c in all_columns if c not in business_keys]

# Build update map: set each attribute column to the source value
update_map = {c: f"source.{c}" for c in attribute_columns}

# Execute the merge
(
    target_delta.alias("target")
    .merge(source_df.alias("source"), merge_condition)
    .whenMatchedUpdate(set=update_map)
    .whenNotMatchedInsertAll()
    .execute()
)

logger.info("SCD Type 1 merge completed successfully")

# COMMAND ----------
# STEP 3: Verify result

result_count = spark.read.table(target_table).count()
logger.info(f"Target rows after merge: {result_count}")

# COMMAND ----------
# NOTES:
# - SCD Type 1 does NOT preserve history — only current values
# - If you need history, use SCD Type 2 (scd_type2.py template)
# - Delta Lake MERGE is transactional and ACID-compliant
# - For Talend tDBOutput with "DELETE_INSERT" action, use:
#     .whenMatchedDelete()  then separate insert
# - For "UPDATE" only action, remove .whenNotMatchedInsertAll()
