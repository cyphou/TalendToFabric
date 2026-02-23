"""
Unit tests for translator/translate_to_spark.py — SparkTranslator
Tests cover: notebook generation, template selection, header, expression conversion, source code.
"""

import pytest


# ═══════════════════════════════════════════════════════════════════════════
# 1. BASIC NOTEBOOK GENERATION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestSparkBasicNotebook:
    """Test basic PySpark notebook generation."""

    def test_returns_string(self, spark_translator, inv_row_complex_spark):
        result = spark_translator.translate_job(inv_row_complex_spark)
        assert isinstance(result, str)
        assert len(result) > 100

    def test_contains_spark_imports(self, spark_translator, inv_row_complex_spark):
        result = spark_translator.translate_job(inv_row_complex_spark)
        assert "from pyspark.sql" in result

    def test_job_name_in_header(self, spark_translator, inv_row_complex_spark):
        result = spark_translator.translate_job(inv_row_complex_spark)
        assert "complex_spark_etl" in result

    def test_header_contains_migration_marker(self, spark_translator, inv_row_complex_spark):
        result = spark_translator.translate_job(inv_row_complex_spark)
        assert "AUTO-GENERATED" in result
        assert "Migrated from Talend" in result

    def test_header_contains_complexity(self, spark_translator, inv_row_complex_spark):
        result = spark_translator.translate_job(inv_row_complex_spark)
        assert "Complex" in result

    def test_header_contains_pattern(self, spark_translator, inv_row_complex_spark):
        result = spark_translator.translate_job(inv_row_complex_spark)
        assert "Complex Transform" in result


# ═══════════════════════════════════════════════════════════════════════════
# 2. TEMPLATE SELECTION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestSparkTemplateSelection:
    """Test that correct templates are selected for different patterns."""

    def test_copy_uses_etl_template(self, spark_translator):
        assert spark_translator.TEMPLATE_MAP["Copy"] == "etl_notebook.py"

    def test_transform_uses_etl_template(self, spark_translator):
        assert spark_translator.TEMPLATE_MAP["Transform"] == "etl_notebook.py"

    def test_scd_uses_scd_template(self, spark_translator):
        assert spark_translator.TEMPLATE_MAP["SCD"] == "scd_type2.py"

    def test_incremental_uses_incremental_template(self, spark_translator):
        assert spark_translator.TEMPLATE_MAP["Incremental"] == "incremental_load.py"

    def test_lookup_template_exists(self, spark_translator):
        assert spark_translator.TEMPLATE_MAP.get("Lookup") == "lookup_pattern.py"

    def test_scd_pattern_detection(self, spark_translator):
        """Jobs with 'SCD' in the name should select scd_type2.py template."""
        job = {
            "job_name": "load_SCD_customers",
            "pattern": "Copy",
            "components_used": "tOracleInput, tPostgresqlOutput",
            "job_folder": "test/",
            "complexity": "Medium",
            "notes": "",
            "description": "",
        }
        result = spark_translator.translate_job(job)
        # SCD template has distinct content
        assert "SCD" in result or "scd" in result.lower() or "Slowly Changing" in result

    def test_incremental_pattern_detection(self, spark_translator):
        """Jobs with 'incremental' in name should select incremental template."""
        job = {
            "job_name": "incremental_load_orders",
            "pattern": "Copy",
            "components_used": "tOracleInput, tPostgresqlOutput",
            "job_folder": "test/",
            "complexity": "Simple",
            "notes": "",
            "description": "",
        }
        result = spark_translator.translate_job(job)
        assert "incremental" in result.lower()


# ═══════════════════════════════════════════════════════════════════════════
# 3. EXPRESSION CONVERSION (Java → PySpark)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestSparkExpressionConversion:
    """Test Java/Talend → PySpark expression translation."""

    def test_sysdate_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("TalendDate.getCurrentDate()")
        assert "current_date" in result or "F.current_date" in result

    def test_string_trim_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("StringHandling.TRIM(row.name)")
        assert "F.trim" in result

    def test_string_upper_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("StringHandling.UPCASE(row.first_name)")
        assert "F.upper" in result

    def test_string_lower_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("StringHandling.DOWNCASE(row.last_name)")
        assert "F.lower" in result

    def test_nvl_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("NVL(row.status, 'UNKNOWN')")
        assert "F.coalesce" in result

    def test_isnull_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("Relational.ISNULL(row.email)")
        assert "F.isnull" in result

    def test_sequence_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("Numeric.sequence(\"s1\", 1, 1)")
        assert "monotonically_increasing_id" in result

    def test_parseint_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("Integer.parseInt(row.age)")
        assert "cast" in result and "int" in result

    def test_null_check_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("row.value == null")
        assert "isNull" in result

    def test_not_null_check_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("row.value != null")
        assert "isNotNull" in result

    def test_context_variable_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("context.db_host")
        assert "params['" in result

    def test_math_abs_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("Math.abs(row.diff)")
        assert "F.abs" in result

    def test_math_round_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("Math.round(row.amount)")
        assert "F.round" in result

    def test_date_format_conversion(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("TalendDate.formatDate(\"yyyy-MM-dd\", row.dt)")
        assert "F.date_format" in result

    def test_auto_translated_marker(self, spark_translator):
        result = spark_translator._convert_java_to_pyspark("row.value")
        assert "AUTO-TRANSLATED" in result


# ═══════════════════════════════════════════════════════════════════════════
# 4. SOURCE CODE GENERATION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestSparkSourceGeneration:
    """Test PySpark source read code generation."""

    def test_oracle_jdbc_source(self, spark_translator):
        job = {"component_types": "tOracleInput, tPostgresqlOutput"}
        code = spark_translator._generate_source_code(job)
        assert "jdbc" in code.lower()

    def test_csv_source(self, spark_translator):
        job = {"component_types": "tFileInputDelimited"}
        code = spark_translator._generate_source_code(job)
        assert "csv" in code.lower()

    def test_json_source(self, spark_translator):
        job = {"component_types": "tFileInputJSON"}
        code = spark_translator._generate_source_code(job)
        assert "json" in code.lower()

    def test_parquet_source(self, spark_translator):
        job = {"component_types": "tFileInputParquet"}
        code = spark_translator._generate_source_code(job)
        assert "parquet" in code.lower()

    def test_xml_source(self, spark_translator):
        job = {"component_types": "tFileInputXML"}
        code = spark_translator._generate_source_code(job)
        assert "xml" in code.lower()

    def test_avro_source(self, spark_translator):
        job = {"component_types": "tFileInputAvro"}
        code = spark_translator._generate_source_code(job)
        assert "avro" in code.lower()

    def test_excel_source(self, spark_translator):
        job = {"component_types": "tFileInputExcel"}
        code = spark_translator._generate_source_code(job)
        assert "excel" in code.lower()

    def test_s3_source(self, spark_translator):
        job = {"component_types": "tS3Input"}
        code = spark_translator._generate_source_code(job)
        assert "s3" in code.lower()

    def test_kafka_source(self, spark_translator):
        job = {"component_types": "tKafkaInput"}
        code = spark_translator._generate_source_code(job)
        assert "kafka" in code.lower()
        assert "readStream" in code

    def test_cosmosdb_source(self, spark_translator):
        job = {"component_types": "tCosmosDBInput"}
        code = spark_translator._generate_source_code(job)
        assert "cosmos" in code.lower()
        # Per Azure Cosmos DB best practices: reuse singleton, handle 429s
        assert any(keyword in code.lower() for keyword in ("cosmos.oltp", "cosmos"))

    def test_mongodb_source(self, spark_translator):
        job = {"component_types": "tMongoDBInput"}
        code = spark_translator._generate_source_code(job)
        assert "mongo" in code.lower()

    def test_rest_source(self, spark_translator):
        job = {"component_types": "tRESTClient"}
        code = spark_translator._generate_source_code(job)
        assert "requests" in code.lower() or "api" in code.lower()

    def test_snowflake_source(self, spark_translator):
        job = {"component_types": "tSnowflakeInput"}
        code = spark_translator._generate_source_code(job)
        assert "jdbc" in code.lower() or "snowflake" in code.lower()

    def test_azure_blob_source(self, spark_translator):
        job = {"component_types": "tAzureBlobInput"}
        code = spark_translator._generate_source_code(job)
        assert "abfss" in code or "azure" in code.lower()

    def test_fallback_source(self, spark_translator):
        job = {"component_types": "tUnknownInput"}
        code = spark_translator._generate_source_code(job)
        assert "TODO" in code
