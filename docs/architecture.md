# Target Architecture in Microsoft Fabric

> **Toolkit scope**: 200+ Talend components, 3 output translators (ADF JSON, Spark `.py`, Jupyter `.ipynb`), 7 SQL dialect translators, 7 Spark templates, 423 automated tests.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Microsoft Fabric Workspace                      │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐    │
│  │  Data Factory │   │    Spark     │   │     Lakehouse        │    │
│  │  (Pipelines)  │   │ (Notebooks)  │   │  ┌───────────────┐  │    │
│  │               │   │  .py or      │   │  │  Bronze (Raw)  │  │    │
│  │  Orchestrate  │──▶│  .ipynb      │──▶│  ├───────────────┤  │    │
│  │  Schedule     │   │  Transform   │   │  │ Silver (Clean) │  │    │
│  │  Monitor      │   │  Cleanse     │   │  ├───────────────┤  │    │
│  └──────────────┘   │  Enrich      │   │  │  Gold (Curated)│  │    │
│                      └──────────────┘   │  └───────────────┘  │    │
│                                         └──────────────────────┘    │
│                                                    │                │
│                                         ┌──────────▼──────────┐    │
│                                         │     Warehouse       │    │
│                                         │   (Serving Layer)   │    │
│                                         └──────────┬──────────┘    │
│                                                    │                │
│                                         ┌──────────▼──────────┐    │
│                                         │   Power BI Reports  │    │
│                                         │   (Semantic Model)  │    │
│                                         └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
                        ▲
                        │
         ┌──────────────┴──────────────┐
         │        Data Sources          │
         │  ┌────────┐  ┌───────────┐  │
         │  │Oracle   │  │  Files   │  │
         │  │SQL Srvr │  │ (CSV,   │  │
         │  │MySQL    │  │  Excel, │  │
         │  │DB2      │  │  XML,   │  │
         │  │Teradata │  │  JSON)  │  │
         │  │Snowflake│  ├─────────┤  │
         │  │Sybase   │  │ Cloud   │  │
         │  │Postgres │  │ (S3,    │  │
         │  └────────┘  │  Blob)  │  │
         │               └─────────┘  │
         └─────────────────────────────┘
```

## Translation Pipeline Architecture

```
┌──────────────────┐     ┌─────────────────────┐     ┌────────────────────────┐
│  Talend .item XML │────▶│  TalendJobParser     │────▶│  Inventory CSV          │
│  + context files  │     │  11 categories       │     │  (scored & classified)  │
└──────────────────┘     │  schema extraction   │     └───────┬────────────────┘
                         │  tMap parsing         │             │
                         │  bigdata detection    │     ┌───────▼────────────────┐
                         └─────────────────────┘     │  Route by target        │
                                                      └───┬───────┬───────┬────┘
                                                          │       │       │
                                              ┌───────────▼┐  ┌──▼─────┐  ┌▼──────────┐
                                              │ADF Translator│  │ Spark  │  │ Notebook  │
                                              │7 pipeline   │  │Translator│ │Translator │
                                              │types        │  │7 templ. │  │.ipynb     │
                                              └──────┬──────┘  └───┬────┘  └────┬──────┘
                                                     │             │            │
                                              ┌──────▼──────┐  ┌──▼─────┐  ┌───▼───────┐
                                              │pl_*.json    │  │nb_*.py │  │nb_*.ipynb │
                                              └─────────────┘  └────────┘  └───────────┘
                                                                    │
                                                          ┌─────────▼──────────┐
                                                          │ SQL Translator      │
                                                          │ 7 dialects → PG    │
                                                          │ (Oracle, SQL Server,│
                                                          │  MySQL, DB2,        │
                                                          │  Teradata, Snowflake│
                                                          │  Sybase)            │
                                                          └────────────────────┘
```

## Medallion Architecture

### Bronze Layer (Raw)
- **Purpose**: Land data as-is from source systems
- **Format**: Parquet files in Lakehouse Files section
- **Retention**: Keep raw data for auditability
- **Example**: Direct copies from PostgreSQL tables

### Silver Layer (Cleaned)
- **Purpose**: Cleansed, validated, and conformed data
- **Format**: Delta tables in Lakehouse Tables section
- **Processing**: Spark notebooks for data quality rules
- **Example**: Deduplicated, null-handled, type-cast data

### Gold Layer (Curated)
- **Purpose**: Business-ready aggregated data
- **Format**: Delta tables in Lakehouse Tables section
- **Processing**: Spark notebooks or Data Flows for business logic
- **Example**: Star schema dimensions and facts

## Component Roles

### Data Factory Pipelines
- **Orchestration**: Schedule and coordinate all activities
- **Simple Ingestion**: Copy data from sources to Bronze layer
- **Monitoring**: Track pipeline runs, alerts on failure
- **Used for**: Talend parent/orchestration jobs, simple copy jobs

### Spark Notebooks
- **Complex Transformations**: Business logic, data quality, SCD, CDC, merge/upsert
- **Bronze → Silver**: Cleansing and standardization
- **Silver → Gold**: Aggregation and business rules
- **Output formats**: `.py` scripts (for CI/CD) or `.ipynb` notebooks (for direct Fabric import)
- **7 templates**: ETL, incremental load, lookup, SCD Type 1, SCD Type 2, CDC watermark, merge/upsert
- **Used for**: Talend jobs with tMap, tAggregate, custom Java code, Hash lookups, Big Data, streaming

### Lakehouse
- **Storage**: OneLake-backed Delta Lake storage
- **Files section**: Landing zone for raw files
- **Tables section**: Managed Delta tables for all layers

### Warehouse
- **Serving Layer**: Optimized for analytical queries
- **Power BI**: Direct Lake mode for fast reporting
- **SQL Endpoint**: T-SQL access for analysts

## Environment Strategy

| Environment | Fabric Workspace | Purpose |
|---|---|---|
| DEV | `ws-talend-migration-dev` | Development and testing |
| UAT | `ws-talend-migration-uat` | User acceptance testing |
| PROD | `ws-talend-migration-prod` | Production workloads |

## SQL Translation Layer

The toolkit includes a multi-dialect SQL translator that converts embedded SQL from **7 source databases** to PostgreSQL:

| Source | Rules | Key Translations |
|---|---|---|
| Oracle | 33 | NVL→COALESCE, SYSDATE→CURRENT_TIMESTAMP, DECODE→CASE |
| SQL Server | 38 | ISNULL→COALESCE, GETDATE→CURRENT_TIMESTAMP, TOP→LIMIT |
| MySQL | 34 | IFNULL→COALESCE, NOW→CURRENT_TIMESTAMP, backtick removal |
| DB2 | 27 | VALUE→COALESCE, CURRENT DATE→CURRENT_DATE, FETCH FIRST→LIMIT |
| Teradata | 21 | SEL→SELECT, .DATE→CURRENT_DATE, QUALIFY→window function |
| Snowflake | 23 | IFF→CASE, TRY_CAST→CAST, DATEADD→INTERVAL |
| Sybase | 21 | ISNULL→COALESCE, GETDATE→CURRENT_TIMESTAMP, TOP→LIMIT |

## Security Model

- **Workspace roles**: Fabric workspace roles for access control
- **Row-level security**: Implemented in semantic models
- **Connection credentials**: Managed via Fabric connections (not hardcoded)
- **Service principals**: Used for CI/CD deployment
