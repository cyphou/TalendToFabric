# Target Architecture in Microsoft Fabric

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Microsoft Fabric Workspace                      │
│                                                                     │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐    │
│  │  Data Factory │   │    Spark     │   │     Lakehouse        │    │
│  │  (Pipelines)  │   │ (Notebooks)  │   │  ┌───────────────┐  │    │
│  │               │   │              │   │  │  Bronze (Raw)  │  │    │
│  │  Orchestrate  │──▶│  Transform   │──▶│  ├───────────────┤  │    │
│  │  Schedule     │   │  Cleanse     │   │  │ Silver (Clean) │  │    │
│  │  Monitor      │   │  Enrich      │   │  ├───────────────┤  │    │
│  └──────────────┘   └──────────────┘   │  │  Gold (Curated)│  │    │
│                                         │  └───────────────┘  │    │
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
         │  │PostgreSQL│  │  Files   │  │
         │  │(from     │  │ (SFTP,  │  │
         │  │ Oracle)  │  │  Blob)  │  │
         │  └────────┘  └───────────┘  │
         └─────────────────────────────┘
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
- **Complex Transformations**: Business logic, data quality, SCD
- **Bronze → Silver**: Cleansing and standardization
- **Silver → Gold**: Aggregation and business rules
- **Used for**: Talend jobs with tMap, tAggregate, custom Java code

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

## Security Model

- **Workspace roles**: Fabric workspace roles for access control
- **Row-level security**: Implemented in semantic models
- **Connection credentials**: Managed via Fabric connections (not hardcoded)
- **Service principals**: Used for CI/CD deployment
