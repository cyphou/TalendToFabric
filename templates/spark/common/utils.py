# Shared Utility Functions for Fabric Spark Notebooks
# Used across all migrated Talend jobs

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging
from datetime import datetime
from typing import Optional, List, Dict

logger = logging.getLogger("FabricETLUtils")


# ============================================================
# JDBC Helper Functions
# ============================================================

def read_jdbc_table(
    spark: SparkSession,
    jdbc_url: str,
    table_or_query: str,
    properties: dict,
    num_partitions: int = 1,
    partition_column: str = None,
    lower_bound: int = None,
    upper_bound: int = None,
) -> DataFrame:
    """
    Read from a JDBC source with optional partitioning for parallel reads.
    Replaces Talend tOracleInput / tPostgresqlInput.
    """
    reader = spark.read.jdbc(
        url=jdbc_url,
        table=table_or_query,
        properties=properties,
    )

    if partition_column and num_partitions > 1:
        reader = spark.read.jdbc(
            url=jdbc_url,
            table=table_or_query,
            column=partition_column,
            lowerBound=lower_bound,
            upperBound=upper_bound,
            numPartitions=num_partitions,
            properties=properties,
        )

    return reader


def write_jdbc_table(
    df: DataFrame,
    jdbc_url: str,
    table: str,
    properties: dict,
    mode: str = "append",
) -> None:
    """
    Write to a JDBC target.
    Replaces Talend tOracleOutput / tPostgresqlOutput.
    """
    df.write.jdbc(
        url=jdbc_url,
        table=table,
        mode=mode,
        properties=properties,
    )


# ============================================================
# Delta Lake Helper Functions
# ============================================================

def write_to_delta(
    df: DataFrame,
    target_table: str,
    mode: str = "append",
    partition_by: List[str] = None,
    merge_schema: bool = True,
) -> None:
    """Write DataFrame to a Delta table in the Lakehouse."""
    writer = df.write.format("delta").mode(mode)

    if merge_schema:
        writer = writer.option("mergeSchema", "true")

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.saveAsTable(target_table)
    logger.info(f"Written to {target_table} (mode={mode})")


def upsert_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    key_columns: List[str],
) -> None:
    """
    Upsert (merge) into Delta table.
    Replaces Talend tPostgresqlOutput with 'update or insert' action.
    """
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").saveAsTable(target_table)
        logger.info(f"Created new table {target_table}")
        return

    delta_table = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])

    delta_table.alias("target") \
        .merge(source_df.alias("source"), merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    logger.info(f"Upserted into {target_table}")


# ============================================================
# Data Quality Functions
# ============================================================

def add_audit_columns(df: DataFrame, source_name: str) -> DataFrame:
    """Add standard audit columns to a DataFrame."""
    return df \
        .withColumn("_etl_loaded_at", F.current_timestamp()) \
        .withColumn("_etl_source", F.lit(source_name)) \
        .withColumn("_etl_run_date", F.current_date())


def validate_not_null(df: DataFrame, columns: List[str]) -> DataFrame:
    """Filter out rows with null values in specified columns and log violations."""
    null_condition = None
    for col_name in columns:
        cond = F.col(col_name).isNull()
        null_condition = cond if null_condition is None else (null_condition | cond)

    if null_condition is not None:
        null_count = df.filter(null_condition).count()
        if null_count > 0:
            logger.warning(f"Found {null_count} rows with null values in {columns}")

        return df.filter(~null_condition)
    return df


def validate_unique(df: DataFrame, key_columns: List[str]) -> DataFrame:
    """Check for duplicate keys and deduplicate."""
    original_count = df.count()
    deduped = df.dropDuplicates(key_columns)
    deduped_count = deduped.count()

    if original_count != deduped_count:
        logger.warning(f"Found {original_count - deduped_count} duplicate rows on {key_columns}")

    return deduped


def compute_row_hash(df: DataFrame, columns: List[str] = None) -> DataFrame:
    """Compute a hash for change detection (useful for SCD, CDC)."""
    if columns is None:
        columns = df.columns

    hash_expr = F.md5(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columns])
    )
    return df.withColumn("_row_hash", hash_expr)


# ============================================================
# Transformation Helpers (Common Talend Patterns)
# ============================================================

def apply_column_rename(df: DataFrame, rename_map: Dict[str, str]) -> DataFrame:
    """
    Rename columns based on a mapping.
    Replaces Talend tMap column name mappings.
    """
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def apply_type_cast(df: DataFrame, cast_map: Dict[str, str]) -> DataFrame:
    """
    Cast columns to specified types.
    Replaces Talend tConvertType.
    """
    for col_name, target_type in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    return df


def apply_default_values(df: DataFrame, defaults: Dict[str, any]) -> DataFrame:
    """
    Replace nulls with default values.
    Common pattern in Talend tMap null handling.
    """
    for col_name, default_val in defaults.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(default_val)))
    return df


def split_dataframe(df: DataFrame, conditions: Dict[str, str]) -> Dict[str, DataFrame]:
    """
    Split DataFrame into multiple based on conditions.
    Replaces Talend tMap with multiple output flows or tSplitRow.
    """
    result = {}
    for name, condition in conditions.items():
        result[name] = df.filter(condition)
    return result


# ============================================================
# Logging & Monitoring
# ============================================================

def log_dataframe_stats(df: DataFrame, name: str) -> None:
    """Log basic statistics about a DataFrame — replaces Talend tLogRow."""
    count = df.count()
    cols = len(df.columns)
    logger.info(f"[{name}] Rows: {count}, Columns: {cols}")

    if count > 0:
        # Log sample of null counts
        null_counts = {}
        for col_name in df.columns:
            nulls = df.filter(F.col(col_name).isNull()).count()
            if nulls > 0:
                null_counts[col_name] = nulls

        if null_counts:
            logger.info(f"[{name}] Null counts: {null_counts}")
