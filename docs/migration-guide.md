# Talend to Microsoft Fabric — Migration Guide

> **Toolkit capabilities**: 200+ Talend component mappings, 3 output formats (ADF pipelines, Spark `.py`, Jupyter `.ipynb`), 7 SQL dialect translators, 7 Spark templates, 423 automated tests.

## Phase 1: Discovery & Inventory

### 1.1 Export Talend Project
- Export all jobs, metadata, and connections from Talend Studio
- Preserve folder structure and dependencies
- Supported source databases: **Oracle, SQL Server, MySQL, DB2, Teradata, Snowflake, Sybase** (and more)

### 1.2 Automated Inventory
Run the parser to generate a complete inventory:
```bash
python parser/parse_talend_jobs.py --input <talend_export_path> --output inventory/
```

The parser will:
- Extract 200+ component types across **11 categories** (input, output, transformation, flow_control, error_handling, custom_code, db_operation, file_utility, bigdata, utility, unknown)
- Extract **schemas** from metadata elements
- Parse **tMap expressions** and join conditions
- Classify **Big Data** components (Hadoop, Spark, Sqoop, Hive, Pig, MapReduce)
- Score complexity and route to the optimal Fabric target

### 1.3 Classify Jobs
Each job is automatically classified by:

| Attribute | Values |
|---|---|
| **Complexity** | Simple / Medium / Complex |
| **Pattern** | Copy, Transform, Lookup, Aggregation, Orchestration, Custom Code, BigData |
| **Target** | Data Factory Pipeline, Spark Notebook (.py or .ipynb), Data Factory Data Flow |
| **Priority** | P1 (Critical) / P2 (Important) / P3 (Nice-to-have) |
| **Risk** | Low / Medium / High |

## Phase 2: Architecture & Design

### 2.1 Target Architecture
Define the Fabric workspace layout:
- **Lakehouse**: Landing zone and curated data (Medallion: Bronze → Silver → Gold)
- **Warehouse**: Serving layer for reporting
- **Data Factory**: Orchestration & simple ETL
- **Spark**: Complex transformations, CDC, SCD, streaming

### 2.2 Connection Mapping
Map Talend connections to Fabric linked services/connections:

| Talend Connection | Fabric Equivalent |
|---|---|
| tOracleConnection | Oracle linked service / JDBC in Spark |
| tPostgresqlConnection | Azure Database for PostgreSQL linked service |
| tMSSqlConnection | Azure SQL linked service |
| tMysqlConnection | Azure MySQL linked service |
| tDB2Connection | DB2 linked service / JDBC in Spark |
| tTeradataConnection | Teradata linked service / JDBC in Spark |
| tSnowflakeConnection | Snowflake linked service |
| tSybaseConnection | Sybase linked service / JDBC in Spark |
| tFileInputDelimited | Lakehouse Files section / OneLake |
| tRESTClient | Web activity / REST linked service |
| tSalesforceConnection | Salesforce linked service |
| tMongoDBConnection | MongoDB linked service / Spark connector |
| tKafkaInput | Spark Structured Streaming |

### 2.3 Output Format Decision

| Format | Best For | Command |
|---|---|---|
| **ADF Pipeline JSON** | Simple copies, orchestration, file ops | `translate_to_adf.py` |
| **Spark Notebook (.py)** | Complex transforms, CI/CD, version control | `translate_to_spark.py` |
| **Jupyter Notebook (.ipynb)** | Direct Fabric import, interactive dev | `translate_to_notebook.py` |

### 2.4 Naming Conventions

| Element | Convention | Example |
|---|---|---|
| Pipeline | `pl_<domain>_<action>_<entity>` | `pl_sales_load_customers` |
| Notebook | `nb_<domain>_<action>_<entity>` | `nb_sales_transform_orders` |
| Data Flow | `df_<domain>_<action>_<entity>` | `df_sales_merge_products` |
| Dataset | `ds_<source>_<entity>` | `ds_pg_customers` |
| Linked Service | `ls_<type>_<environment>` | `ls_postgresql_prod` |

## Phase 3: Migration Execution

### 3.1 Simple Copy Jobs → Data Factory Copy Activity
1. Identify source and target
2. Create linked services
3. Create datasets
4. Build Copy Data pipeline
5. Add parameters for filtering (date ranges, etc.)

### 3.2 Transformation Jobs → Spark Notebooks
1. Extract transformation logic from Talend XML
2. Map tMap expressions → PySpark DataFrame operations
3. Map tAggregate → groupBy/agg
4. Map tFilter → filter/where
5. Map tUniqRow → dropDuplicates
6. Map tSort → orderBy
7. Handle tJoin → join operations
8. Convert Java expressions → Python/PySpark

**7 Spark templates available:**

| Template | Use Case |
|---|---|
| `etl_notebook.py` | Standard ETL (read → transform → write) |
| `incremental_load.py` | Incremental/CDC with watermark |
| `lookup_pattern.py` | Lookup/join with broadcast |
| `scd_type1.py` | SCD Type 1 (overwrite current) |
| `scd_type2.py` | SCD Type 2 (history tracking) |
| `cdc_watermark.py` | CDC with high-watermark column |
| `merge_upsert.py` | Merge/Upsert for all tDBOutput actions |

### 3.3 Orchestration Jobs → Data Factory Pipelines
1. Map tRunJob → Execute Pipeline activity
2. Map tPreJob/tPostJob → pipeline events
3. Map tWarn/tDie → pipeline failure handling
4. Map context variables → pipeline parameters
5. Reconstruct dependency chains

### 3.4 SQL Translation (Multi-Dialect → PostgreSQL)
The SQL translator supports **7 source dialects** with automatic detection:

| Source Dialect | Rules | Key Translations |
|---|---|---|
| **Oracle** | 33 | NVL→COALESCE, SYSDATE→CURRENT_TIMESTAMP, DECODE→CASE, (+)→ANSI JOIN |
| **SQL Server** | 38 | ISNULL→COALESCE, GETDATE→CURRENT_TIMESTAMP, TOP→LIMIT, DATEADD→INTERVAL |
| **MySQL** | 34 | IFNULL→COALESCE, NOW→CURRENT_TIMESTAMP, LIMIT offset syntax, backtick removal |
| **DB2** | 27 | VALUE→COALESCE, CURRENT DATE→CURRENT_DATE, FETCH FIRST→LIMIT |
| **Teradata** | 21 | SEL→SELECT, .DATE→CURRENT_DATE, QUALIFY→window function |
| **Snowflake** | 23 | IFF→CASE, DATEADD→INTERVAL, TRY_CAST→CAST, $1 binding |
| **Sybase** | 21 | ISNULL→COALESCE, GETDATE→CURRENT_TIMESTAMP, TOP→LIMIT |

### 3.5 Jupyter Notebook Output
For direct import into Microsoft Fabric:
```bash
python translator/translate_to_notebook.py -i inventory.csv -o output/notebooks/
```

The Notebook translator:
- Wraps the Spark translator output into valid `.ipynb` JSON
- Splits code on `# COMMAND ----------` markers into separate cells
- Converts comment-only blocks to **Markdown cells** for readability
- Sets Fabric-compatible kernel metadata (`synapse_pyspark`)
- Produces notebooks ready for direct upload to Fabric

## Phase 4: Validation

### 4.1 Row Count Validation
Compare record counts between Talend output and Fabric output for each job.

### 4.2 Data Diff Validation
Run hash-based comparison on a sample of records to ensure data integrity.

### 4.3 Performance Benchmarking
Compare execution times and resource consumption.

### 4.4 End-to-End Testing
Run the full pipeline chain and validate final output matches Talend production output.

### 4.5 Automated Test Suite
Run the 423-test suite to validate all translators and mappings:
```bash
python -m pytest tests/ -q --tb=short
```

| Test Module | Tests | Scope |
|---|---:|---|
| `test_parser.py` | 57 | Parser, inventory, metadata, schema extraction |
| `test_translate_to_adf.py` | 34 | ADF pipeline generation (7 pipeline types) |
| `test_translate_to_spark.py` | 43 | Spark notebook generation (7 templates) |
| `test_translate_to_notebook.py` | 34 | Jupyter .ipynb generation |
| `test_sql_translator.py` | 26 | Oracle → PostgreSQL rules |
| `test_sql_dialects.py` | 102 | 6 additional SQL dialects |
| `test_new_features.py` | 93 | Hash, CDC, schema, tMap, bigdata, templates |
| `test_validation.py` | 20 | Schema / row / data validators |
| `test_regression.py` | 17 | End-to-end & determinism |

## Phase 5: Cutover & Go-Live

### 5.1 Parallel Run
Run both Talend and Fabric pipelines simultaneously for a defined period.

### 5.2 Switch Over
Disable Talend jobs and enable Fabric triggers/schedules.

### 5.3 Monitoring Setup
- Configure Fabric monitoring workspace
- Set up alerts for pipeline failures
- Dashboard for pipeline execution metrics

## Phase 6: Decommission

### 6.1 Archive Talend Jobs
Archive the exported Talend project and documentation.

### 6.2 Decommission Talend Infrastructure
Remove Talend server/runtime after validation period.
