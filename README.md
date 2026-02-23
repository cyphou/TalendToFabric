# Talend to Microsoft Fabric Migration Project

## Overview

This project provides a structured framework for migrating **Talend ETL jobs** to **Microsoft Fabric** (Data Factory Pipelines and/or Spark Notebooks). It includes tooling for inventory, analysis, automated translation, validation, and deployment.

## Project Structure

```
TalendToFabric/
├── README.md                       # This file
├── pytest.ini                      # Pytest configuration
├── docs/                           # Documentation & guides
│   ├── migration-guide.md          # Step-by-step migration guide
│   ├── component-mapping.md        # Talend component → Fabric mapping
│   ├── architecture.md             # Target architecture in Fabric
│   └── diagrams.md                 # 9 Mermaid diagrams (architecture, flow, mapping...)
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
├── tests/                          # Pytest test suite (299 tests)
│   ├── conftest.py                 # Shared fixtures & helpers
│   ├── run_tests.py                # CLI test runner
│   ├── fixtures/                   # 6 Talend .item test fixtures
│   │   ├── simple_copy_job.item
│   │   ├── transform_job.item
│   │   ├── orchestration_job.item
│   │   ├── custom_code_job.item
│   │   ├── complex_etl_job.item
│   │   └── file_transfer_job.item
│   ├── test_parser.py              # 57 tests — parser & inventory
│   ├── test_translate_to_adf.py    # 34 tests — ADF pipeline generation
│   ├── test_translate_to_spark.py  # 43 tests — Spark notebook generation
│   ├── test_sql_translator.py      # 26 tests — Oracle→PostgreSQL rules
│   ├── test_sql_dialects.py        # 102 tests — SQL Server, MySQL, DB2, Teradata, Snowflake, Sybase
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

# For Spark notebooks
python translate_to_spark.py --inventory ../inventory/talend_job_inventory.csv --output ../output/spark/
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

| Criteria | → Data Factory | → Spark Notebook |
|---|---|---|
| Simple source-to-target copy | ✅ | |
| Complex transformations | | ✅ |
| Lookup/Join patterns | ✅ (Data Flow) | ✅ |
| Aggregations | ✅ (Data Flow) | ✅ |
| Custom Java/Python code | | ✅ |
| File processing (CSV, XML, JSON) | ✅ | ✅ |
| Incremental/CDC loads | ✅ | ✅ |
| SCD Type 2 | | ✅ |
| Orchestration (parent jobs) | ✅ | |
| Real-time/streaming | | ✅ |

## License

Internal use — Microsoft proprietary.
