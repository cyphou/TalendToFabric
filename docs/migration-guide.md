# Talend to Microsoft Fabric — Migration Guide

## Phase 1: Discovery & Inventory

### 1.1 Export Talend Project
- Export all jobs, metadata, and connections from Talend Studio
- Preserve folder structure and dependencies

### 1.2 Automated Inventory
Run the parser to generate a complete inventory:
```bash
python parser/parse_talend_jobs.py --input <talend_export_path> --output inventory/
```

### 1.3 Classify Jobs
Each job should be classified by:

| Attribute | Values |
|---|---|
| **Complexity** | Simple / Medium / Complex |
| **Pattern** | Copy, Transform, Lookup, Aggregation, Orchestration, Custom Code |
| **Target** | Data Factory Pipeline, Spark Notebook, Data Factory Data Flow |
| **Priority** | P1 (Critical) / P2 (Important) / P3 (Nice-to-have) |
| **Risk** | Low / Medium / High |

## Phase 2: Architecture & Design

### 2.1 Target Architecture
Define the Fabric workspace layout:
- **Lakehouse**: Landing zone and curated data
- **Warehouse**: Serving layer for reporting
- **Data Factory**: Orchestration & simple ETL
- **Spark**: Complex transformations

### 2.2 Connection Mapping
Map Talend connections to Fabric linked services/connections:

| Talend Connection | Fabric Equivalent |
|---|---|
| tOracleConnection | n/a (source deprecated — data in PostgreSQL) |
| tPostgresqlConnection | Azure Database for PostgreSQL linked service |
| tFileInputDelimited | Lakehouse Files section / OneLake |
| tMysqlConnection | Azure MySQL linked service |
| tRESTClient | Web activity / REST linked service |
| tSalesforceConnection | Salesforce linked service |

### 2.3 Naming Conventions

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

### 3.3 Orchestration Jobs → Data Factory Pipelines
1. Map tRunJob → Execute Pipeline activity
2. Map tPreJob/tPostJob → pipeline events
3. Map tWarn/tDie → pipeline failure handling
4. Map context variables → pipeline parameters
5. Reconstruct dependency chains

### 3.4 SQL Translation (Oracle → PostgreSQL)
Since the underlying database migration is Oracle → PostgreSQL:
- Convert Oracle-specific SQL syntax
- Replace Oracle functions with PostgreSQL equivalents
- Update data types in any embedded SQL

## Phase 4: Validation

### 4.1 Row Count Validation
Compare record counts between Talend output and Fabric output for each job.

### 4.2 Data Diff Validation
Run hash-based comparison on a sample of records to ensure data integrity.

### 4.3 Performance Benchmarking
Compare execution times and resource consumption.

### 4.4 End-to-End Testing
Run the full pipeline chain and validate final output matches Talend production output.

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
