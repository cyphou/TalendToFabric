# Talend to Microsoft Fabric Migration Project

![Tests](https://img.shields.io/badge/tests-423_passed-brightgreen)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Dialects](https://img.shields.io/badge/SQL_dialects-7-orange)
![Components](https://img.shields.io/badge/Talend_components-200%2B-purple)
![License](https://img.shields.io/badge/license-MIT-green)

## Overview

This project provides a structured framework for migrating **Talend ETL jobs** to **Microsoft Fabric** (Data Factory Pipelines and/or Spark Notebooks). It includes tooling for inventory, analysis, automated translation, validation, and deployment.

## Project Structure

```
TalendToFabric/
├── README.md                       # This file
├── LICENSE                         # MIT License
├── CONTRIBUTING.md                 # Contribution guidelines
├── CHANGELOG.md                    # Version history
├── requirements.txt                # Top-level dependencies (all modules)
├── pytest.ini                      # Pytest configuration
├── docs/                           # Documentation & guides
│   ├── migration-guide.md          # Step-by-step migration guide
│   ├── component-mapping.md        # Talend component → Fabric mapping
│   ├── architecture.md             # Target architecture in Fabric
│   └── diagrams.md                 # 10 Mermaid diagrams (architecture, flow, mapping...)
│
├── inventory/                      # Talend job inventory & analysis
│   ├── talend_job_inventory.csv    # Master inventory of all Talend jobs
│   ├── dependency_map.json         # Job dependency graph
│   └── complexity_assessment.md    # Complexity scoring & prioritization
│
├── parser/                         # Talend export parser
│   ├── requirements.txt            # Python dependencies
│   ├── parse_talend_jobs.py        # Parse Talend .item/.properties XML
│   ├── extract_metadata.py         # Extract connections, schemas, transforms
│   └── generate_inventory.py       # Auto-generate inventory from Talend export
│
├── mapping/                        # Component translation layer
│   ├── component_map.json          # Talend component → Fabric activity mapping
│   ├── connection_map.json         # Talend connection → Fabric linked service
│   └── datatype_map.json           # Multi-dialect data type translations
│
├── templates/                      # Target Fabric templates
│   ├── data_factory/               # Data Factory pipeline templates
│   │   ├── pipeline_template.json  # Base pipeline JSON template
│   │   ├── copy_activity.json      # Copy Data activity template
│   │   ├── dataflow_template.json  # Mapping Data Flow template
│   │   └── linked_services/        # Linked service definitions
│   │       ├── postgresql.json     # Azure PostgreSQL linked service
│   │       └── lakehouse.json      # Fabric Lakehouse linked service
│   │
│   └── spark/                      # Spark notebook templates
│       ├── etl_notebook.py         # Base PySpark ETL notebook template
│       ├── incremental_load.py     # Incremental/CDC load pattern
│       ├── lookup_pattern.py       # Lookup & join pattern
│       ├── scd_type1.py            # SCD Type 1 pattern (overwrite)
│       ├── scd_type2.py            # SCD Type 2 pattern (history)
│       ├── cdc_watermark.py        # CDC / high-watermark pattern
│       ├── merge_upsert.py         # Merge/Upsert (all tDBOutput actions)
│       └── common/
│           ├── utils.py            # Shared utility functions
│           └── config.py           # Configuration management
│
├── translator/                     # Automated translation engine
│   ├── requirements.txt            # Python dependencies
│   ├── translate_to_adf.py         # Generate Data Factory pipeline JSON
│   ├── translate_to_spark.py       # Generate Spark notebooks (.py)
│   ├── translate_to_notebook.py    # Generate Jupyter Notebooks (.ipynb)
│   └── sql_translator.py          # Multi-dialect SQL → PostgreSQL translator
│                                    # (Oracle, SQL Server, MySQL, DB2, Teradata, Snowflake, Sybase)
│
├── validation/                     # Testing & validation framework
│   ├── requirements.txt            # Python dependencies
│   ├── row_count_validation.py     # Row count comparison
│   ├── data_diff_validation.py     # Data diff / hash comparison
│   ├── schema_validation.py        # Schema conformance checks
│   └── test_cases/                 # Test case definitions
│       └── sample_test.json        # Sample validation test case
│
├── tests/                          # Pytest test suite (423 tests)
│   ├── __init__.py                 # Package marker
│   ├── conftest.py                 # Shared fixtures & helpers
│   ├── run_tests.py                # CLI test runner
│   ├── requirements.txt            # Test-specific dependencies
│   ├── fixtures/                   # 7 Talend .item test fixtures
│   │   ├── simple_copy_job.item
│   │   ├── transform_job.item
│   │   ├── orchestration_job.item
│   │   ├── custom_code_job.item
│   │   ├── complex_etl_job.item
│   │   ├── file_transfer_job.item
│   │   └── cdc_hash_job.item
│   ├── test_parser.py              # 57 tests — parser & inventory
│   ├── test_translate_to_adf.py    # 34 tests — ADF pipeline generation
│   ├── test_translate_to_spark.py  # 43 tests — Spark notebook generation
│   ├── test_translate_to_notebook.py # 34 tests — Jupyter .ipynb generation
│   ├── test_sql_translator.py      # 26 tests — Oracle→PostgreSQL rules
│   ├── test_sql_dialects.py        # 102 tests — SQL Server, MySQL, DB2, Teradata, Snowflake, Sybase
│   ├── test_new_features.py        # 93 tests — Hash, CDC, schema, tMap, bigdata, templates
│   ├── test_validation.py          # 20 tests — schema/row/data validators
│   └── test_regression.py          # 17 tests — end-to-end & determinism
│
├── deployment/                     # CI/CD & deployment
│   ├── azure-pipelines.yml         # Azure DevOps pipeline
│   ├── deploy_fabric.ps1           # PowerShell deployment script
│   └── fabric_api_helpers.ps1      # Fabric REST API helper functions
│
└── config/                         # Project configuration
    ├── environments.json           # Environment-specific settings
    └── migration_config.json       # Migration rules & settings
```

## Getting Started

### Prerequisites

- Python 3.10+
- Microsoft Fabric workspace (with Data Factory and/or Spark)
- Access to Talend project export (XML or `.item` files)
- Azure CLI installed and configured
- (Optional) Azure DevOps for CI/CD

### Step 1: Export Talend Jobs

Export your Talend jobs from Talend Studio:
1. In Talend Studio, right-click the project → **Export Items**
2. Select all jobs, connections, and metadata
3. Export as `.zip` and extract to a local folder

### Step 2: Run the Parser

```bash
cd parser
pip install -r requirements.txt
python parse_talend_jobs.py --input /path/to/talend/export --output ../inventory/
```

### Step 3: Review Inventory & Prioritize

Open `inventory/talend_job_inventory.csv` and review:
- Job complexity scores
- Component usage
- Dependencies
- Recommended migration target (Data Factory vs. Spark)

### Step 4: Translate Jobs

```bash
# For Data Factory pipelines
cd translator
python translate_to_adf.py --inventory ../inventory/talend_job_inventory.csv --output ../output/adf/

# For Spark notebooks (.py)
python translate_to_spark.py --inventory ../inventory/talend_job_inventory.csv --output ../output/spark/

# For Jupyter Notebooks (.ipynb) — ready to import into Fabric
python translate_to_notebook.py --inventory ../inventory/talend_job_inventory.csv --output ../output/notebooks/
```

### Step 5: Validate

```bash
cd validation
python row_count_validation.py --config ../config/migration_config.json
python data_diff_validation.py --config ../config/migration_config.json
```

### Step 6: Deploy to Fabric

```powershell
cd deployment
.\deploy_fabric.ps1 -Environment "dev" -WorkspaceId "<your-workspace-id>"
```

## Migration Decision Matrix

### Target Selection

| Criteria | → Data Factory | → Spark Notebook | Notes |
|---|:---:|:---:|---|
| **Data Movement** | | | |
| Simple source-to-target copy | ✅ | | Copy Activity |
| Multi-source join/merge | | ✅ | Mapping Data Flow possible but Spark preferred |
| Bulk load (tBulkExec variants) | ✅ | ✅ | ADF bulk insert or Spark JDBC batch |
| Cross-database transfer | ✅ | ✅ | ADF if no transform; Spark if transform needed |
| **Transformations** | | | |
| Simple filtering / column mapping | ✅ (Data Flow) | ✅ | ADF Data Flow for low-code |
| Complex tMap (multi-input, expressions) | | ✅ | Spark DataFrame joins + expressions |
| Aggregations (tAggregateRow) | ✅ (Data Flow) | ✅ | |
| Sorting / Dedup (tSortRow, tUniqRow) | ✅ (Data Flow) | ✅ | |
| Lookup/Join patterns | ✅ (Data Flow) | ✅ | |
| Hash in-memory lookup (tHashInput/Output) | | ✅ | Spark broadcast join |
| Normalize / Denormalize | | ✅ | `explode()` / `collect_list()` |
| Data Quality (tPatternCheck, tMatchPairing) | | ✅ | Custom PySpark logic |
| XML / XSLT transforms | | ✅ | `spark-xml` + Python lxml |
| **Change Data Capture** | | | |
| CDC with watermark column | ✅ | ✅ | ADF tumbling window or Spark watermark template |
| Database-native CDC (tOracleCDC, tMysqlCDC…) | | ✅ | Spark + Delta MERGE (see `cdc_watermark.py`) |
| SCD Type 1 (overwrite) | ✅ (Data Flow) | ✅ | Spark Delta MERGE (see `scd_type1.py`) |
| SCD Type 2 (history) | | ✅ | Spark Delta MERGE (see `scd_type2.py`) |
| **Merge / Upsert** | | | |
| INSERT only | ✅ | ✅ | |
| UPDATE / UPSERT | ✅ (Stored Proc) | ✅ | Spark Delta MERGE (see `merge_upsert.py`) |
| DELETE / DELETE_INSERT | | ✅ | Spark Delta MERGE |
| **Code & Logic** | | | |
| Custom Java / Python code | | ✅ | tJava/tJavaRow/tGroovy → PySpark |
| Stored procedure calls (tSP variants) | ✅ (Stored Proc) | ✅ | ADF Stored Procedure Activity |
| DML execution (tDBRow / vendor Row) | ✅ (Stored Proc) | ✅ | `spark.read.jdbc` with `sessionInitStatement` |
| ELT / Pushdown (tELTMap…) | ✅ | ✅ | ADF Script Activity or Spark `spark.sql()` |
| **Orchestration** | | | |
| Parent/child jobs (tRunJob) | ✅ | | ADF Execute Pipeline |
| Pre/Post job steps (tPreJob/tPostJob) | ✅ | | ADF pipeline activities |
| Looping (tLoop, tForEach) | ✅ | ✅ | ADF ForEach / Spark Python loop |
| Error handling (tLogCatcher, tCatch) | ✅ | ✅ | ADF On Failure path / try-except |
| **File & Protocol** | | | |
| File processing (CSV, Excel, XML, JSON) | ✅ | ✅ | |
| SFTP / FTP transfers | ✅ | | ADF Copy with SFTP connector |
| File archive / delete / move | ✅ | ✅ | ADF Delete Activity / Spark `dbutils.fs` |
| REST / SOAP / Web Service calls | ✅ | ✅ | ADF Web Activity / Spark `requests` |
| **Messaging** | | | |
| Kafka / Event Hub ingestion | | ✅ | Spark Structured Streaming |
| JMS / MQ Series | | ✅ | Custom connector in Spark |
| Real-time / streaming | | ✅ | Spark Structured Streaming |
| **Cloud & NoSQL** | | | |
| Azure Blob / ADLS / S3 / GCS | ✅ | ✅ | |
| MongoDB / Cosmos DB / Cassandra | ✅ | ✅ | ADF connector or Spark connector |
| Elasticsearch / Redis / HBase | | ✅ | Spark connector |
| **Big Data / Hadoop** | | | |
| Sqoop (tSqoopImport/Export) | ✅ | ✅ | Deprecated → ADF Copy or Spark JDBC |
| Hive / Impala | | ✅ | Spark `spark.sql()` on Lakehouse |
| Pig / MapReduce | | ✅ | Rewrite as PySpark |
| **Multi-Source SQL Dialects** | | | |
| Oracle → PostgreSQL | ✅ | ✅ | 33 translation rules |
| SQL Server → PostgreSQL | ✅ | ✅ | 38 translation rules |
| MySQL → PostgreSQL | ✅ | ✅ | 34 translation rules |
| DB2 → PostgreSQL | ✅ | ✅ | 27 translation rules |
| Teradata → PostgreSQL | ✅ | ✅ | 21 translation rules |
| Snowflake → PostgreSQL | ✅ | ✅ | 23 translation rules |
| Sybase → PostgreSQL | ✅ | ✅ | 21 translation rules |

### Quick Decision Rule

```
IF   job has custom code (tJava, tJavaRow, tGroovy, tPythonRow)  → Spark
ELIF job has SCD Type 2 or complex CDC                           → Spark
ELIF job has streaming / Kafka / real-time                       → Spark
ELIF job is orchestration-only (tRunJob, tPreJob/PostJob)        → Data Factory
ELIF job is simple copy (≤ 2 transforms)                         → Data Factory
ELIF job has Data Quality / Hash / Normalize                     → Spark
ELSE                                                             → Data Factory (default)
```

## Test Coverage

| Test Module | Tests | Scope |
|---|---:|---|
| `test_parser.py` | 57 | Parser, inventory, metadata |
| `test_translate_to_adf.py` | 34 | ADF pipeline generation |
| `test_translate_to_spark.py` | 43 | Spark notebook generation |
| `test_translate_to_notebook.py` | 34 | Jupyter .ipynb generation |
| `test_sql_translator.py` | 26 | Oracle → PostgreSQL rules |
| `test_sql_dialects.py` | 102 | 6 additional SQL dialects |
| `test_new_features.py` | 93 | Hash, CDC, schema, tMap, bigdata, templates |
| `test_validation.py` | 20 | Schema / row / data validators |
| `test_regression.py` | 17 | End-to-end & determinism |
| **Total** | **423** | |

## License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.
