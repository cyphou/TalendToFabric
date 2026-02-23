"""
Talend → Spark Notebook Translator
Generates PySpark notebooks from parsed Talend job metadata.

Usage:
    python translate_to_spark.py --inventory ../inventory/talend_job_inventory.csv --output ../output/spark/
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any

import click
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

SCRIPT_DIR = Path(__file__).parent
TEMPLATE_DIR = SCRIPT_DIR.parent / "templates" / "spark"
MAPPING_DIR = SCRIPT_DIR.parent / "mapping"


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class SparkTranslator:
    """Translates Talend job definitions to PySpark notebooks."""

    TEMPLATE_MAP = {
        "Copy": "etl_notebook.py",
        "Transform": "etl_notebook.py",
        "Complex Transform": "etl_notebook.py",
        "Lookup": "lookup_pattern.py",
        "SCD": "scd_type2.py",
        "Incremental": "incremental_load.py",
        "FileTransfer": "etl_notebook.py",
        "FileManagement": "etl_notebook.py",
        "DDL": "etl_notebook.py",
        "API": "etl_notebook.py",
        "Messaging": "etl_notebook.py",
        "Streaming": "etl_notebook.py",
        "ErrorHandling": "etl_notebook.py",
    }

    def __init__(self):
        self.component_map = load_json(MAPPING_DIR / "component_map.json").get("mappings", {})
        self.datatype_map = load_json(MAPPING_DIR / "datatype_map.json")

    def translate_job(self, job: Dict[str, Any]) -> str:
        """Translate a Talend job to a PySpark notebook (as Python string)."""
        job_name = job.get("job_name", "unknown")
        pattern = job.get("pattern", "Copy")
        components = str(job.get("components_used", ""))

        logger.info(f"Translating job: {job_name} (pattern={pattern})")

        # Select template based on pattern
        template_file = self.TEMPLATE_MAP.get(pattern, "etl_notebook.py")

        # Check for specific patterns
        if "SCD" in job_name.upper() or "scd" in str(job.get("notes", "")).lower():
            template_file = "scd_type2.py"
        elif "tMap" in components and ("lookup" in job_name.lower() or components.count("Input") > 1):
            template_file = "lookup_pattern.py"
        elif "incremental" in job_name.lower() or "cdc" in job_name.lower():
            template_file = "incremental_load.py"

        template_path = TEMPLATE_DIR / template_file
        if not template_path.exists():
            logger.warning(f"Template not found: {template_path}. Using default.")
            template_path = TEMPLATE_DIR / "etl_notebook.py"

        with open(template_path, "r", encoding="utf-8") as f:
            notebook_content = f.read()

        # Replace common placeholders
        replacements = {
            "{{talend_job_name}}": job_name,
            "{{description}}": job.get("description", ""),
            "{{migration_date}}": "2026-02-20",
            "{{entity}}": job_name.split("_")[-1] if "_" in job_name else job_name,
            "{{domain}}": job.get("job_folder", "").strip("/").split("/")[-1] if job.get("job_folder") else "default",
        }

        for placeholder, value in replacements.items():
            notebook_content = notebook_content.replace(placeholder, str(value))

        # Add header comment with migration details
        header = self._generate_header(job, components)
        notebook_content = header + "\n" + notebook_content

        return notebook_content

    def _generate_header(self, job: Dict[str, Any], components: str) -> str:
        """Generate a header comment with migration context."""
        lines = [
            f"# {'='*70}",
            f"# AUTO-GENERATED — Migrated from Talend",
            f"# Original Job: {job.get('job_name', 'unknown')}",
            f"# Job Folder: {job.get('job_folder', 'N/A')}",
            f"# Complexity: {job.get('complexity', 'N/A')}",
            f"# Pattern: {job.get('pattern', 'N/A')}",
            f"# Components: {components}",
            f"# Notes: {job.get('notes', '')}",
            f"#",
            f"# TODO: Review and customize the following:",
            f"#   1. Update connection parameters (server, database, credentials)",
            f"#   2. Uncomment and customize transformation logic",
            f"#   3. Set correct primary key columns for upsert/merge",
            f"#   4. Validate output against Talend production data",
            f"# {'='*70}",
        ]
        return "\n".join(lines)

    def _translate_tmap_expressions(self, expressions: Dict[str, str]) -> str:
        """Translate Talend tMap expressions to PySpark equivalent."""
        translations = []
        for expr_name, expr_value in expressions.items():
            spark_expr = self._convert_java_to_pyspark(expr_value)
            translations.append(f'    df = df.withColumn("{expr_name}", {spark_expr})')
        return "\n".join(translations) if translations else "    # No tMap expressions to translate"

    def _convert_java_to_pyspark(self, java_expr: str) -> str:
        """Convert a Talend/Java expression to a PySpark expression."""
        # Common Java → PySpark mappings
        conversions = {
            # String operations
            "StringHandling.TRIM": "F.trim",
            "StringHandling.UPCASE": "F.upper",
            "StringHandling.DOWNCASE": "F.lower",
            "StringHandling.LEFT": "F.substring",
            "StringHandling.RIGHT": "F.substring",  # Needs manual review
            "StringHandling.LEN": "F.length",
            "StringHandling.INDEX": "F.instr",
            "StringHandling.REPLACE": "F.regexp_replace",
            "StringHandling.SUBSTR": "F.substring",
            "StringHandling.CONCAT": "F.concat",
            "StringHandling.LPAD": "F.lpad",
            "StringHandling.RPAD": "F.rpad",
            "StringHandling.LTRIM": "F.ltrim",
            "StringHandling.RTRIM": "F.rtrim",
            "StringHandling.REVERSE": "F.reverse",
            "StringHandling.IS_EMPTY": "F.isnull(col) | (F.length(col) == 0)",
            # Date operations
            "TalendDate.getCurrentDate()": "F.current_date()",
            "TalendDate.getDate": "F.to_date",
            "TalendDate.parseDate": "F.to_timestamp",
            "TalendDate.formatDate": "F.date_format",
            "TalendDate.addDate": "F.date_add",
            "TalendDate.diffDate": "F.datediff",
            "TalendDate.getPartOfDate": "F.extract",
            "TalendDate.compareDate": "F.datediff",
            "TalendDate.getTruncDate": "F.trunc",
            # Numeric operations
            "Numeric.sequence": "F.monotonically_increasing_id()",
            "Numeric.random": "F.rand()",
            "Math.abs": "F.abs",
            "Math.round": "F.round",
            "Math.ceil": "F.ceil",
            "Math.floor": "F.floor",
            "Math.sqrt": "F.sqrt",
            # Type conversions
            "Integer.parseInt": "F.col.cast('int')",
            "Long.parseLong": "F.col.cast('long')",
            "Double.parseDouble": "F.col.cast('double')",
            "Float.parseFloat": "F.col.cast('float')",
            "Boolean.parseBoolean": "F.col.cast('boolean')",
            # Null handling
            "Relational.ISNULL": "F.isnull",
            "Relational.NOT_ISNULL": "~F.isnull",
            "NVL": "F.coalesce",
            # Java patterns
            "row.": "F.col('",
            ".toUpperCase()": ")",
            ".toLowerCase()": ")",
            ".trim()": ")",
            ".length()": ")",
            " == null": ".isNull()",
            " != null": ".isNotNull()",
            ".toString()": ".cast('string')",
            ".equals(": " == ",
            "context.": "params['",  # Talend context → notebook params
        }

        result = java_expr
        for java_pattern, spark_pattern in conversions.items():
            result = result.replace(java_pattern, spark_pattern)

        return f"# AUTO-TRANSLATED (review needed): {result}"

    def _generate_source_code(self, job: Dict[str, Any]) -> str:
        """Generate PySpark source read code based on component type."""
        comp_types = str(job.get("component_types", ""))

        if any(db in comp_types for db in ("tOracleInput", "tPostgresqlInput", "tMSSqlInput", "tMysqlInput", "tDB2Input", "tTeradataInput", "tSybaseInput", "tSnowflakeInput", "tDBInput")):
            return (
                "# Read from database\n"
                "df_source = spark.read \\\n"
                '    .format("jdbc") \\\n'
                '    .option("url", jdbc_url) \\\n'
                '    .option("dbtable", source_table) \\\n'
                '    .option("user", db_user) \\\n'
                '    .option("password", db_password) \\\n'
                "    .load()"
            )
        elif "tFileInputDelimited" in comp_types:
            return 'df_source = spark.read.csv(source_path, header=True, inferSchema=True)'
        elif "tFileInputJSON" in comp_types:
            return 'df_source = spark.read.json(source_path)'
        elif "tFileInputParquet" in comp_types:
            return 'df_source = spark.read.parquet(source_path)'
        elif "tFileInputXML" in comp_types:
            return 'df_source = spark.read.format("xml").option("rowTag", "row").load(source_path)'
        elif "tFileInputAvro" in comp_types:
            return 'df_source = spark.read.format("avro").load(source_path)'
        elif "tFileInputExcel" in comp_types:
            return (
                "df_source = spark.read \\\n"
                '    .format("com.crealytics.spark.excel") \\\n'
                '    .option("header", "true") \\\n'
                '    .option("inferSchema", "true") \\\n'
                "    .load(source_path)"
            )
        elif any(s3 in comp_types for s3 in ("tS3Input", "tS3Get")):
            return 'df_source = spark.read.csv("s3a://bucket/path", header=True)'
        elif "tKafkaInput" in comp_types:
            return (
                "df_source = spark.readStream \\\n"
                '    .format("kafka") \\\n'
                '    .option("kafka.bootstrap.servers", kafka_servers) \\\n'
                '    .option("subscribe", kafka_topic) \\\n'
                "    .load()"
            )
        elif "tCosmosDBInput" in comp_types:
            return (
                "# Azure Cosmos DB source — reuse singleton client, handle 429 with retry-after\n"
                "df_source = spark.read \\\n"
                '    .format("cosmos.oltp") \\\n'
                '    .option("spark.cosmos.accountEndpoint", cosmos_endpoint) \\\n'
                '    .option("spark.cosmos.accountKey", cosmos_key) \\\n'
                '    .option("spark.cosmos.database", cosmos_database) \\\n'
                '    .option("spark.cosmos.container", cosmos_container) \\\n'
                "    .load()"
            )
        elif "tMongoDBInput" in comp_types:
            return 'df_source = spark.read.format("mongo").option("uri", mongo_uri).load()'
        elif "tRESTClient" in comp_types or "tHTTPInput" in comp_types:
            return (
                "import requests\n"
                "response = requests.get(api_url, headers=headers)\n"
                "data = response.json()\n"
                "df_source = spark.createDataFrame(data)"
            )
        elif "tSnowflakeInput" in comp_types:
            return (
                "df_source = spark.read \\\n"
                '    .format("snowflake") \\\n'
                '    .option("sfUrl", snowflake_url) \\\n'
                '    .option("sfDatabase", snowflake_db) \\\n'
                '    .option("sfSchema", snowflake_schema) \\\n'
                '    .option("dbtable", source_table) \\\n'
                "    .load()"
            )
        elif any(az in comp_types for az in ("tAzureBlobInput", "tAzureDataLakeInput")):
            return 'df_source = spark.read.csv("abfss://container@account.dfs.core.windows.net/path", header=True)'
        else:
            return "# TODO: Configure source read\ndf_source = spark.read.format('TODO').load('TODO')"


@click.command()
@click.option("--inventory", "-i", required=True, help="Path to talend_job_inventory.csv")
@click.option("--output", "-o", required=True, help="Output folder for Spark notebooks")
@click.option("--filter-target", default="Spark", help="Only translate jobs targeting this platform")
def main(inventory: str, output: str, filter_target: str):
    """Generate PySpark notebooks from Talend inventory."""
    logger.info(f"Loading inventory from {inventory}")
    df = pd.read_csv(inventory)

    spark_jobs = df[df["target_fabric"] == filter_target]
    logger.info(f"Found {len(spark_jobs)} jobs targeting {filter_target}")

    translator = SparkTranslator()
    os.makedirs(output, exist_ok=True)

    for _, row in spark_jobs.iterrows():
        job = row.to_dict()
        notebook = translator.translate_job(job)

        output_file = os.path.join(output, f"nb_{job.get('job_name', 'unknown')}.py")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(notebook)
        logger.info(f"Generated: {output_file}")

    logger.info(f"Translation complete. {len(spark_jobs)} notebooks generated in {output}")


if __name__ == "__main__":
    main()
