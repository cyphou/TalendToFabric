# Changelog

All notable changes to the TalendToFabric migration toolkit are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/).

---

## [1.5.0] — 2025-02-23

### Added
- **Documentation refresh**: All docs updated with current counts and features
- Updated `migration-guide.md` with 3 output formats, 7 SQL dialect table, 7 Spark templates
- Updated `architecture.md` with Translation Pipeline diagram and SQL Translation Layer
- Updated `diagrams.md` with Diagram 10 (Notebook Translator Flow), all counts corrected
- Updated `complexity_assessment.md` with output format guide, template selection, dialect auto-detect

## [1.4.0] — 2025-02-22

### Added
- **Jupyter Notebook translator** (`translator/translate_to_notebook.py`)
  - Generates `.ipynb` files ready for direct import into Microsoft Fabric
  - Splits on `# COMMAND ----------` markers into separate cells
  - Comment-only blocks → Markdown cells, code → Code cells
  - Fabric-compatible kernel metadata (`synapse_pyspark`)
- 31 tests for notebook translator (`tests/test_translate_to_notebook.py`)
- README updated with notebook output option (Step 4) and test coverage

## [1.3.0] — 2025-02-21

### Added
- **Decision matrix** expanded to 40+ criteria across 12 categories
- Quick Decision Rule pseudocode in README
- Test Coverage table in README

## [1.2.0] — 2025-02-20

### Added
- **Feature gap audit**: ~70 new component mappings
  - Hash operations (tHashInput, tHashOutput)
  - CDC components (tOracleCDC, tMysqlCDC, tMSSqlCDC)
  - DB-specific operations (tOracleSP, tMSSqlSP, DB2, Teradata, Snowflake, Sybase)
  - Big Data (tSqoop, tHive, tPig, tMapReduce, tSpark)
  - XML/XSLT transforms
  - Messaging (Kafka, JMS, MQSeries)
  - MDM, Data Quality, Cloud/NoSQL
- **3 new Spark templates**: `scd_type1.py`, `cdc_watermark.py`, `merge_upsert.py`
- Parser enhancements: schema extraction, tMap expression parsing, bigdata classification
- Category priority fix: exact-match before prefix-match
- 93 new tests (`tests/test_new_features.py`)

## [1.1.0] — 2025-02-19

### Added
- **Multi-source SQL dialect support**: 7 dialects (Oracle, SQL Server, MySQL, DB2, Teradata, Snowflake, Sybase)
- Base class + 7 dialect subclasses in `sql_translator.py`
- Expanded `datatype_map.json` to 16 sections
- 102 dialect-specific tests (`tests/test_sql_dialects.py`)

## [1.0.0] — 2025-02-18

### Added
- Initial release
- **Parser**: `TalendJobParser` extracts components, connections, context params, subjobs from `.item` XML
- **Inventory generator**: Complexity scoring (6 weighted factors), pattern classification, target routing
- **ADF translator**: 7 pipeline types (Copy, Orchestration, DataFlow, File Transfer, File Management, DDL, API)
- **Spark translator**: 4 notebook templates (ETL, Incremental, Lookup, SCD Type 2)
- **SQL translator**: Oracle → PostgreSQL (33 regex rules)
- **Validation**: Schema, row count, and data diff validators
- **Deployment**: Azure DevOps pipeline, PowerShell deployment scripts
- **Test suite**: 197 tests across 7 files
- **Documentation**: Migration guide, architecture, 9 Mermaid diagrams, component mapping
- **Pushed to GitHub**: `https://github.com/cyphou/TalendToFabric`
