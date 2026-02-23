# Talend Job Complexity Assessment

> Supports **200+ Talend components** across 11 categories, 3 output formats, and 7 SQL dialects.

## Scoring Criteria

Each Talend job is scored on the following dimensions (1–5 scale):

| Dimension | Weight | 1 (Low) | 3 (Medium) | 5 (High) |
|---|---|---|---|---|
| **Component Count** | 20% | 1–3 components | 4–8 components | 9+ components |
| **Transformation Complexity** | 25% | Direct copy | Filters + lookups | Multi-join tMap + aggregation |
| **Custom Code** | 20% | None | Simple expressions | Java routines / tJava / tGroovy |
| **Dependencies** | 15% | Standalone | 1–2 child jobs | 3+ child jobs / complex DAG |
| **Data Volume** | 10% | < 100K rows | 100K – 10M rows | > 10M rows |
| **Error Handling** | 10% | Basic | Conditional flows | Complex retry / compensation |

## Complexity Tiers

| Tier | Score Range | Migration Approach | Typical Effort |
|---|---|---|---|
| **Simple** | 1.0 – 2.0 | Direct translation, mostly automated | 2–4 hours |
| **Medium** | 2.1 – 3.5 | Semi-automated with manual review | 4–12 hours |
| **Complex** | 3.6 – 5.0 | Manual migration with redesign | 12–40 hours |

## Target Recommendation Rules

| Condition | Recommended Target | Output Formats |
|---|---|---|
| Score ≤ 2.0 and pattern = Copy | Data Factory Copy Activity | ADF JSON |
| Score ≤ 3.0 and no custom code | Data Factory Data Flow | ADF JSON |
| Score > 3.0 or has custom code | Spark Notebook | `.py` or `.ipynb` |
| Pattern = Orchestration | Data Factory Pipeline | ADF JSON |
| Has Big Data components | Spark Notebook (forced) | `.py` or `.ipynb` |
| Has Hash lookups (tHashInput/Output) | Spark Notebook (broadcast join) | `.py` or `.ipynb` |
| Has CDC / SCD patterns | Spark Notebook | `.py` or `.ipynb` |
| Has streaming / Kafka / MQ | Spark Notebook | `.py` or `.ipynb` |

### Output Format Selection

| Format | When to Use |
|---|---|
| **ADF Pipeline JSON** (`pl_*.json`) | Orchestration, simple copies, file operations |
| **Spark Script** (`nb_*.py`) | CI/CD pipelines, version control, automated deployment |
| **Jupyter Notebook** (`nb_*.ipynb`) | Direct Fabric import, interactive development, exploration |

## Spark Template Selection

| Template | Trigger Condition |
|---|---|
| `etl_notebook.py` | Standard ETL (read → transform → write) |
| `incremental_load.py` | Has watermark column or incremental pattern |
| `lookup_pattern.py` | Lookup/enrichment with broadcast join |
| `scd_type1.py` | SCD Type 1 (overwrite current values) |
| `scd_type2.py` | SCD Type 2 (history tracking with effective dates) |
| `cdc_watermark.py` | CDC with high-watermark column |
| `merge_upsert.py` | Merge/Upsert for INSERT/UPDATE/DELETE actions |

## SQL Dialect Support

The SQL translator auto-detects the source dialect from the Talend connection type:

| Source Dialect | Translation Rules | Auto-Detect From |
|---|---|---|
| Oracle | 33 | tOracle*, tELTOracle* |
| SQL Server | 38 | tMSSql*, tELTMssql* |
| MySQL | 34 | tMysql*, tELTMysql* |
| DB2 | 27 | tDB2*, tAS400* |
| Teradata | 21 | tTeradata*, tELTTeradata* |
| Snowflake | 23 | tSnowflake* |
| Sybase | 21 | tSybase*, tELTSybase* |

## Sample Assessment

| Job | Components | Transforms | Custom Code | Dependencies | Volume | Error Handling | **Total** | **Tier** | **Target** |
|---|---|---|---|---|---|---|---|---|---|
| sample_copy_customers | 1 | 1 | 1 | 1 | 2 | 1 | **1.15** | Simple | Data Factory |
| sample_transform_orders | 4 | 4 | 1 | 1 | 3 | 2 | **2.75** | Medium | Data Factory |
| sample_parent_daily | 2 | 1 | 1 | 5 | 1 | 3 | **2.05** | Medium | Data Factory |
| sample_file_load | 2 | 2 | 1 | 1 | 2 | 2 | **1.70** | Simple | Data Factory |
| sample_scd2_products | 4 | 5 | 5 | 1 | 3 | 3 | **3.85** | Complex | Spark (.ipynb) |
| sample_cdc_incremental | 3 | 3 | 2 | 1 | 4 | 2 | **2.60** | Medium | Spark (.py) |
| sample_hash_lookup | 5 | 4 | 1 | 2 | 3 | 2 | **2.95** | Medium | Spark (.py) |
| sample_sqoop_import | 3 | 2 | 1 | 1 | 5 | 1 | **2.10** | Medium | Spark (.py) |
