# Talend Component → Microsoft Fabric Mapping

> **Coverage**: This mapping covers **200+** Talend components across all categories:
> Sources, Transformations, Outputs, Flow Control, Database Operations, Error Handling,
> File Management, Cloud Storage, NoSQL, Messaging, API/Protocol, CDC, Big Data,
> Data Quality, MDM, Hash/In-Memory, and Utilities.

---

## 1. Database Input Components

| Talend Component | Fabric Data Factory | Fabric Spark (PySpark) |
|---|---|---|
| `tOracleInput` | Copy Activity (Oracle source) | `spark.read.jdbc(...)` |
| `tPostgresqlInput` | Copy Activity (PostgreSQL source) | `spark.read.jdbc(...)` |
| `tMSSqlInput` | Copy Activity (SQL Server source) | `spark.read.jdbc(...)` |
| `tMysqlInput` | Copy Activity (MySQL source) | `spark.read.jdbc(...)` |
| `tDB2Input` | Copy Activity (DB2 source) | `spark.read.jdbc(...)` |
| `tTeradataInput` | Copy Activity (Teradata source) | `spark.read.jdbc(...)` |
| `tSybaseInput` | Copy Activity (Sybase/SAP ASE source) | `spark.read.jdbc(...)` |
| `tRedshiftInput` | Copy Activity (Redshift source) | `spark.read.jdbc(...)` |
| `tSnowflakeInput` | Copy Activity (Snowflake source) | `spark.read.format("snowflake")` |
| `tHiveInput` | Copy Activity (Hive source) | `spark.sql("SELECT ... FROM ...")` |
| `tDBInput` | Copy Activity (generic JDBC/ODBC) | `spark.read.jdbc(...)` with generic driver |
| `tSQLiteInput` | Not supported natively | `spark.read.jdbc(...)` with SQLite driver |
| `tAccessInput` | Not supported natively | Convert to CSV/Parquet first |
| `tInformixInput` | Copy Activity (Informix source) | `spark.read.jdbc(...)` |
| `tVerticaInput` | Copy Activity (Vertica source) | `spark.read.jdbc(...)` |
| `tNetezzaInput` | Copy Activity (Netezza source) | `spark.read.jdbc(...)` |
| `tGreenplumInput` | Copy Activity (Greenplum source) | `spark.read.jdbc(...)` |
| `tAS400Input` | Copy Activity (DB2 for i source) | `spark.read.jdbc(...)` |
| `tSAPHanaInput` | Copy Activity (SAP HANA source) | `spark.read.jdbc(...)` |
| `tImpalaInput` | Not supported natively | `spark.read.jdbc(...)` (Impala driver) |

## 2. Database Output Components

| Talend Component | Fabric Data Factory | Fabric Spark (PySpark) |
|---|---|---|
| `tOracleOutput` | Copy Activity (Oracle sink) | `df.write.jdbc(...)` |
| `tPostgresqlOutput` | Copy Activity (PostgreSQL sink) | `df.write.jdbc(...)` |
| `tMSSqlOutput` | Copy Activity (SQL Server sink) | `df.write.jdbc(...)` |
| `tMysqlOutput` | Copy Activity (MySQL sink) | `df.write.jdbc(...)` |
| `tDB2Output` | Copy Activity (DB2 sink) | `df.write.jdbc(...)` |
| `tTeradataOutput` | Copy Activity (Teradata sink) | `df.write.jdbc(...)` |
| `tRedshiftOutput` | Copy Activity (Redshift sink) | `df.write.jdbc(...)` |
| `tSnowflakeOutput` | Copy Activity (Snowflake sink) | `df.write.format("snowflake")` |
| `tHiveOutput` | Not applicable (write to Lakehouse) | `df.write.saveAsTable(...)` |
| `tDBOutput` | Copy Activity (generic sink) | `df.write.jdbc(...)` with generic driver |
| `tSQLiteOutput` | Not supported natively | `df.write.jdbc(...)` with SQLite driver |
| `tSybaseOutput` | Copy Activity (Sybase/SAP ASE sink) | `df.write.jdbc(...)` |
| `tInformixOutput` | Copy Activity (Informix sink) | `df.write.jdbc(...)` |
| `tVerticaOutput` | Copy Activity (Vertica sink) | `df.write.jdbc(...)` |
| `tNetezzaOutput` | Copy Activity (Netezza sink) | `df.write.jdbc(...)` |
| `tGreenplumOutput` | Copy Activity (Greenplum sink) | `df.write.jdbc(...)` |
| `tAS400Output` | Copy Activity (DB2 for i sink) | `df.write.jdbc(...)` |
| `tSAPHanaOutput` | Copy Activity (SAP HANA sink) | `df.write.jdbc(...)` |
| `tImpalaOutput` | Not supported natively | `df.write.jdbc(...)` (Impala driver) |
| `tAccessOutput` | Not supported natively | Convert to CSV/Parquet first |

## 3. Database Operation Components

| Talend Component | Fabric Data Factory | Fabric Spark (PySpark) |
|---|---|---|
| `tOracleConnection` | Linked Service (Oracle) | JDBC URL + properties |
| `tPostgresqlConnection` | Linked Service (PostgreSQL) | JDBC URL + properties |
| `tMSSqlConnection` | Linked Service (SQL Server) | JDBC URL + properties |
| `tMysqlConnection` | Linked Service (MySQL) | JDBC URL + properties |
| `tDBConnection` | Linked Service (generic) | JDBC URL + properties |
| `tOracleCommit` / `tDBCommit` | Auto-commit in Copy Activity | Explicit commit in JDBC / auto in Spark |
| `tOracleRollback` / `tDBRollback` | Error handling in pipeline | `try-except` in Spark |
| `tOracleSP` / `tDBSP` | Stored Procedure activity | `spark.sql("CALL ...")` |
| `tDBRow` | Script activity / Stored Proc | `spark.sql("INSERT/UPDATE/DELETE ...")` |
| `tDBBulkExec` / `tOracleBulkExec` | Copy Activity (bulk mode) | JDBC batch write / `df.write.jdbc(batchsize=N)` |
| `tMysqlBulkExec` | Copy Activity (bulk mode) | `df.write.jdbc(batchsize=N)` (LOAD DATA INFILE) |
| `tMSSqlBulkExec` | Copy Activity (bulk mode) | `df.write.jdbc(batchsize=N)` (BCP/BULK INSERT) |
| `tPostgresqlBulkExec` | Copy Activity (bulk mode) | `df.write.jdbc(batchsize=N)` (COPY) |
| `tTeradataBulkExec` | Copy Activity (bulk mode) | `df.write.jdbc(batchsize=N)` (FastLoad) |
| `tSnowflakeBulkExec` | Copy Activity (bulk mode) | `df.write.format('snowflake')` (COPY INTO) |
| `tOracleOutputBulk` / `tPostgresqlOutputBulk` / `tMSSqlOutputBulk` | Copy Activity (bulk) | `df.write.jdbc(batchsize=N)` |
| `tCreateTable` | Script activity (DDL) | `spark.sql("CREATE TABLE ...")` |
| `tDropTable` | Script activity (DDL) | `spark.sql("DROP TABLE ...")` |
| `tAlterTable` | Script activity (DDL) | `spark.sql("ALTER TABLE ...")` |
| `tDBTableList` | Get Metadata activity | `spark.catalog.listTables()` |
| `tDBColumnList` | Get Metadata activity | `spark.catalog.listColumns()` |
| `tOracleClose` / `tDBClose` | Automatic (connection pooling) | Connection auto-close |
| `tSetDBAutoCommit` | Automatic | JDBC autoCommit property |
| `tDBLastInsertId` | Script activity | JDBC `getGeneratedKeys()` |
| `tDBValidation` | Get Metadata → Validation | `spark.catalog.tableExists()` |

## 4. File Input Components

| Talend Component | Fabric Data Factory | Fabric Spark (PySpark) |
|---|---|---|
| `tFileInputDelimited` | Copy Activity (DelimitedText source) | `spark.read.csv(...)` |
| `tFileInputExcel` | Copy Activity (Excel source) | `spark.read.format("com.crealytics.spark.excel")` |
| `tFileInputJSON` | Copy Activity (JSON source) | `spark.read.json(...)` |
| `tFileInputParquet` | Copy Activity (Parquet source) | `spark.read.parquet(...)` |
| `tFileInputXML` | Copy Activity (XML source) | `spark.read.format("xml")` |
| `tFileInputAvro` | Copy Activity (Avro source) | `spark.read.format("avro")` |
| `tFileInputPositional` | Copy Activity (FixedWidth — custom) | `spark.read.text(...)` + substring parsing |
| `tFileInputRegex` | Not natively supported | `spark.read.text(...)` + `regexp_extract()` |
| `tFileInputFullRow` | Copy Activity (Binary/Text) | `spark.read.text(...)` |
| `tFileInputLDIF` | Not natively supported | Custom Python parser |
| `tFileInputMSXML` | Copy Activity (XML source) | `spark.read.format("xml")` |
| `tFileInputRaw` | Copy Activity (Binary source) | `spark.sparkContext.binaryFiles(...)` |
| `tFileInputORC` | Copy Activity (ORC source) | `spark.read.orc(...)` |

## 5. File Output Components

| Talend Component | Fabric Data Factory | Fabric Spark (PySpark) |
|---|---|---|
| `tFileOutputDelimited` | Copy Activity (DelimitedText sink) | `df.write.csv(...)` |
| `tFileOutputExcel` | Copy Activity (Excel sink) | `df.write.format("com.crealytics.spark.excel")` |
| `tFileOutputJSON` | Copy Activity (JSON sink) | `df.write.json(...)` |
| `tFileOutputParquet` | Copy Activity (Parquet sink) | `df.write.parquet(...)` |
| `tFileOutputXML` | Copy Activity (XML — via Data Flow) | `df.write.format("xml")` |
| `tFileOutputAvro` | Copy Activity (Avro sink) | `df.write.format("avro")` |
| `tFileOutputPositional` | Not natively supported | Custom `format_string()` + `df.write.text()` |
| `tFileOutputORC` | Copy Activity (ORC sink) | `df.write.orc(...)` |

## 6. Cloud Storage Components

| Talend Component | Fabric Data Factory | Fabric Spark (PySpark) |
|---|---|---|
| `tS3Connection` / `tS3Get` / `tS3Put` | Copy Activity (S3 connector) | `spark.read.csv("s3a://...")` / boto3 |
| `tS3Input` / `tS3Output` | Copy Activity (S3 source/sink) | `spark.read/write` with S3 paths |
| `tAzureBlobInput` / `tAzureBlobOutput` | Copy Activity (Blob source/sink) | `spark.read/write` with `wasbs://` paths |
| `tAzureBlobConnection` / `tAzureBlobGet` / `tAzureBlobPut` | Copy Activity (Blob connector) | `mssparkutils.fs.cp()` / `abfss://` paths |
| `tAzureDataLakeInput` / `tAzureDataLakeOutput` | Copy Activity (ADLS source/sink) | `spark.read/write` with `abfss://` paths |
| `tGCSInput` / `tGCSOutput` | Copy Activity (GCS connector) | `spark.read/write` with `gs://` paths |
| `tHDFSInput` / `tHDFSOutput` | Not applicable | `spark.read/write` with `hdfs://` paths |
| `tFileInputS3` / `tFileOutputS3` | Copy Activity (S3) | `spark.read/write` with S3 paths |

## 7. NoSQL & Document DB Components

| Talend Component | Fabric Data Factory | Fabric Spark (PySpark) |
|---|---|---|
| `tMongoDBInput` / `tMongoDBOutput` | Copy Activity (MongoDB connector) | `spark.read.format("mongo")` |
| `tMongoDBConnection` / `tMongoDBClose` | Linked Service (MongoDB/Cosmos DB) | MongoDB connection string |
| `tCassandraInput` / `tCassandraOutput` | Copy Activity (Cassandra connector) | `spark.read.format("org.apache.spark.sql.cassandra")` |
| `tCouchDBInput` / `tCouchDBOutput` | Not natively supported | `requests` + `spark.createDataFrame()` |
| `tNeo4jInput` / `tNeo4jOutput` | Not natively supported | `neo4j` Python driver |
| `tCosmosDBInput` / `tCosmosDBOutput` | Copy Activity (Cosmos DB connector) | `spark.read.format("cosmos.oltp")` |
| `tDynamoDBInput` / `tDynamoDBOutput` | Not natively supported | `boto3` + `spark.createDataFrame()` |
| `tElasticsearchInput` / `tElasticsearchOutput` | Not natively supported | `elasticsearch-spark` connector |
| `tHBaseInput` / `tHBaseOutput` | Not applicable | `spark.read.format("org.apache.hadoop.hbase.spark")` |
| `tRedisInput` / `tRedisOutput` | Not natively supported | `redis` Python library |

## 8. Messaging & Streaming Components

| Talend Component | Fabric Data Factory | Fabric Spark (PySpark) |
|---|---|---|
| `tKafkaInput` / `tKafkaOutput` | Not natively in ADF (use Event Hub) | `spark.readStream.format("kafka")` |
| `tKafkaConnection` / `tKafkaCommit` | Event Hub linked service | Kafka bootstrap config |
| `tJMSInput` / `tJMSOutput` | Not natively supported | `stomp` / `jms` Python libraries |
| `tActiveMQInput` / `tActiveMQOutput` | Not natively supported | `stomp` Python library |
| `tRabbitMQInput` / `tRabbitMQOutput` | Not natively supported | `pika` Python library |
| `tAzureEventHubInput` / `tAzureEventHubOutput` | Event Hub connector | `spark.readStream.format("eventhubs")` |
| `tMQSeriesInput` / `tMQSeriesOutput` | Not natively supported | `pymqi` Python library |

## 9. API & Protocol Components

| Talend Component | Fabric Data Factory | Fabric Spark (PySpark) |
|---|---|---|
| `tRESTClient` | Web Activity / Copy Activity (REST) | `requests` library + `spark.createDataFrame()` |
| `tHTTPInput` / `tHTTPClient` | Web Activity / Copy Activity (HTTP) | `requests` / `urllib3` |
| `tSOAP` / `tWebServiceInput` | Web Activity (SOAP) | `zeep` / `requests` with XML payload |
| `tSalesforceInput` / `tSalesforceOutput` | Copy Activity (Salesforce connector) | `simple-salesforce` library |
| `tSalesforceConnection` / `tSalesforceBulkExec` | Salesforce linked service | `simple-salesforce` bulk API |
| `tSAPInput` / `tSAPOutput` | Copy Activity (SAP connector) | `pyrfc` / SAP HANA JDBC |
| `tSAPConnection` / `tSAPBWInput` | SAP linked service | SAP HANA / RFC connector |
| `tLDAPInput` / `tLDAPOutput` | Not natively supported | `python-ldap` library |
| `tGraphQLInput` | Web Activity (POST with GraphQL) | `requests` with GraphQL query |
| `tODataInput` | Copy Activity (OData source) | `requests` + OData URL |
| `tServiceNowInput` | Copy Activity (ServiceNow connector) | `requests` + ServiceNow API |

## 10. Transformation Components

| Talend Component | Fabric Data Factory (Data Flow) | Fabric Spark (PySpark) |
|---|---|---|
| `tMap` | Derived Column + Join | `df.withColumn(...)` / `df.join(...)` |
| `tFilter` / `tFilterRow` | Filter transformation | `df.filter(...)` / `df.where(...)` |
| `tAggregate` / `tAggregateRow` | Aggregate transformation | `df.groupBy(...).agg(...)` |
| `tSort` / `tSortRow` | Sort transformation | `df.orderBy(...)` |
| `tUniqRow` | Aggregate (Group by all) | `df.dropDuplicates(...)` |
| `tDenormalize` | Pivot transformation | `df.groupBy(...).pivot(...)` |
| `tNormalize` | Unpivot transformation / Flatten | `df.select(explode(...))` |
| `tReplace` | Derived Column (replace) | `df.withColumn(..., regexp_replace(...))` |
| `tConvertType` | Derived Column (cast) | `df.withColumn(..., col.cast(...))` |
| `tUnite` | Union transformation | `df1.unionByName(df2)` |
| `tSplitRow` | Conditional Split | `df.filter(condition)` (multiple) |
| `tJoin` | Join transformation | `df1.join(df2, ...)` |
| `tXMLMap` | Flatten + Derived Column | `spark.read.format("xml")` + transforms |
| `tReplicate` | Multiple output sinks / Copy | Write df to multiple targets |
| `tSampleRow` | Top N / Percentage sampling | `df.sample(fraction)` / `df.limit(N)` |
| `tHashRow` | Derived Column (hash) | `F.md5(F.concat_ws(...))` / `F.sha2(...)` |
| `tGenKey` | Derived Column (surrogate key) | `F.monotonically_increasing_id()` / `F.row_number()` |
| `tRowGenerator` | Not applicable (test data) | Manual DataFrame creation / `range()` |
| `tFixedFlowInput` | Not applicable (test data) | `spark.createDataFrame([...])` |
| `tExtractXMLField` | Flatten transformation | `F.from_xml()` / xpath extraction |
| `tExtractJSONField` | Parse (JSON) transformation | `F.from_json()` / `F.get_json_object()` |
| `tExtractRegexFields` | Derived Column (regex) | `F.regexp_extract()` |
| `tExtractDelimitedFields` | Derived Column (split) | `F.split()` + `getItem()` |
| `tFuzzyMatch` / `tRecordMatching` | Not natively supported | `fuzzywuzzy` / `recordlinkage` / Spark ML |
| `tMatchGroup` | Not natively supported | `recordlinkage` / custom grouping logic |
| `tSchemaComplianceCheck` | Data quality rules | Custom validation with `df.filter()` |
| `tFlowMeter` | Pipeline monitoring metrics | `df.count()` + logging |
| `tWindowInput` | Window transformation | `F.window()` / `Window.partitionBy().orderBy()` |
| `tCacheIn` / `tCacheOut` | Not applicable | `df.cache()` / `df.persist()` |
| `tBufferInput` / `tBufferOutput` | Not applicable | `df.cache()` / `df.persist()` |
| `tSetDynamicSchema` | Schema drift handling | Dynamic column selection |
| `tDataMasking` | Not natively supported | Custom masking with `F.sha2()` / `F.regexp_replace()` |
| `tDenormalizeRow` | Pivot transformation | `df.groupBy(...).pivot(...)` |
| `tNormalizeRow` | Unpivot transformation | `F.explode()` / `stack()` |
| `tFillEmptyField` | Derived Column (COALESCE) | `F.coalesce()` / `df.na.fill()` |
| `tFilterColumns` | Select transformation | `df.select(...)` / `df.drop(...)` |
| `tRenameColumns` | Derived Column (rename) | `df.withColumnRenamed(...)` |
| `tSortWithinGroups` | Sort within window | `Window.partitionBy().orderBy()` |
| `tLevenshteinDistance` | Not natively supported | `Levenshtein` from `pyspark.sql.functions` |

## 11. ELT / Pushdown Components

| Talend Component | Fabric Data Factory | Fabric Spark (PySpark) |
|---|---|---|
| `tELTInput` | Copy Activity source | `spark.sql("SELECT ...")` |
| `tELTOutput` | Copy Activity sink | `spark.sql("INSERT INTO ...")` |
| `tELTMap` | Data Flow transformations | `spark.sql()` with JOINs/transforms |
| `tELTAggregate` | Data Flow Aggregate | `spark.sql()` with GROUP BY |
| `tELTFilter` | Data Flow Filter | `spark.sql()` with WHERE |
| `tELTSort` | Data Flow Sort | `spark.sql()` with ORDER BY |
| `tELTUnite` | Data Flow Union | `spark.sql()` with UNION ALL |
| `tELTJoin` | Data Flow Join | `spark.sql()` with JOIN |
| `tELTDistinct` | Data Flow Aggregate | `spark.sql()` with DISTINCT |
| `tELTCreateTable` | Script activity | `spark.sql("CREATE TABLE ...")` |

## 12. Flow Control Components

| Talend Component | Fabric Data Factory | Fabric Spark |
|---|---|---|
| `tRunJob` | Execute Pipeline activity | `%run` / `mssparkutils.notebook.run()` |
| `tPreJob` | Pipeline "On Start" event | Code at beginning of notebook |
| `tPostJob` | Pipeline "On Completion" event | `try/finally` block |
| `tWarn` | Set Variable + Web Activity (alert) | `logging.warning()` |
| `tDie` | Fail activity | `raise Exception(...)` |
| `tFlowToIterate` | ForEach activity | `for item in collection:` |
| `tLoop` / `tForEach` | Until / ForEach activity | `while`/`for` loop |
| `tParallelize` | ForEach (concurrent) | Spark parallelism (native) |
| `tContextLoad` | Pipeline Parameters / Variables | Notebook parameters / `%run config` |
| `tIfRow` | If Condition activity | `if` statement in Python |
| `tTimeout` | Pipeline timeout setting | `signal.alarm()` / thread timeout |
| `tFlowControl` | Pipeline dependency chains | Sequential code execution |
| `tIterateToFlow` | Variable → ForEach array | List comprehension → DataFrame |

## 13. Error Handling Components

| Talend Component | Fabric Data Factory | Fabric Spark |
|---|---|---|
| `tCatch` / `tCatchError` | Pipeline On Failure handler | `try/except` block |
| `tLogCatcher` | Pipeline monitoring / Log Analytics | `logging` module + appenders |
| `tStatCatcher` | Pipeline run statistics | Custom metrics with `df.count()` + logging |
| `tAssert` / `tAssertCatcher` | Pipeline validation + Fail activity | `assert` statement / `raise` on condition |
| `tFlowControl` | Pipeline dependency conditions | `if/else` flow control |
| `tConnectionReset` | Automatic retry policy | JDBC reconnection logic |
| `tOnError` | Pipeline On Failure handler | `except` clause |
| `tOnComponentOk` / `tOnComponentError` | Activity dependency (Succeeded/Failed) | `try/except` per function |
| `tOnSubjobOk` / `tOnSubjobError` | Activity dependency conditions | `try/except` per code block |

## 14. File & Directory Utility Components

| Talend Component | Fabric Data Factory | Fabric Spark / Python |
|---|---|---|
| `tFileExist` | Get Metadata activity (exists) | `mssparkutils.fs.exists()` |
| `tFileDelete` | Delete activity | `mssparkutils.fs.rm()` |
| `tFileCopy` | Copy Activity | `mssparkutils.fs.cp()` |
| `tFileMove` / `tFileRename` | Copy + Delete (no native move) | `mssparkutils.fs.mv()` |
| `tFileList` | Get Metadata activity (childItems) | `mssparkutils.fs.ls()` |
| `tFileProperties` | Get Metadata activity | `mssparkutils.fs.ls()` (includes size/modified) |
| `tFileCompare` | Not natively supported | Hash comparison in Python |
| `tFileArchive` | Not natively supported | `zipfile` / `tarfile` Python modules |
| `tFileUnarchive` | Not natively supported | `zipfile` / `tarfile` Python modules |
| `tFileTouch` | Script activity | `mssparkutils.fs.put()` with empty content |
| `tDirectoryCreate` | Script activity | `mssparkutils.fs.mkdirs()` |
| `tDirectoryList` | Get Metadata activity | `mssparkutils.fs.ls()` |
| `tDirectoryDelete` | Delete activity | `mssparkutils.fs.rm(recursive=True)` |
| `tFileInputLDIF` | Not natively supported | Custom Python parser |
| `tFTPGet` / `tFTPPut` | Copy Activity (FTP connector) | `ftplib` Python library |
| `tSFTPGet` / `tSFTPPut` | Copy Activity (SFTP connector) | `paramiko` Python library |
| `tFTPConnection` / `tSFTPConnection` | FTP/SFTP linked service | `paramiko` / `ftplib` connection |
| `tFTPDelete` / `tSFTPDelete` | Script activity | `paramiko.SFTPClient.remove()` |
| `tFTPRename` / `tSFTPRename` | Script activity | `paramiko.SFTPClient.rename()` |
| `tFTPList` / `tSFTPList` | Get Metadata activity | `paramiko.SFTPClient.listdir()` |

## 15. General Utility Components

| Talend Component | Fabric Equivalent |
|---|---|
| `tLogRow` | Pipeline monitoring / `print()` / `display()` |
| `tSendMail` | Web Activity → Logic App / `smtplib` in notebook |
| `tSystem` | Script Activity / `subprocess` in notebook |
| `tSleep` | Wait activity / `time.sleep()` |
| `tSetGlobalVar` | Set Variable activity / Python variable |
| `tContextDump` | Pipeline parameter logging | `print(locals())` |
| `tContextEnvironment` | Pipeline parameters per env | Environment-specific config |
| `tSSH` / `tSSHRemote` | Not natively supported | `paramiko` SSH client |
| `tLibraryLoad` | Not applicable | `pip install` / `%pip` in notebook |
| `tMsgBox` | Not applicable (UI only) | `logging.info()` |
| `tSetEncoding` | Automatic (UTF-8 default) | `.option("encoding", "...")` |
| `tPrintVars` | Pipeline variable logging | `print()` / `display()` |
| `tTimestamp` | Pipeline timestamp variable | `datetime.utcnow()` |

## 16. Custom Code Components

| Talend Component | Fabric Data Factory | Fabric Spark |
|---|---|---|
| `tJava` | Script Activity (limited) | Python code block |
| `tJavaRow` | Not supported — use Spark | `df.withColumn()` with UDF |
| `tJavaFlex` | Not supported — use Spark | Custom PySpark with init/main/end blocks |
| `tGroovy` | Script Activity (limited) | Python code block |
| `tPythonRow` | Not applicable | Direct PySpark / UDF |
| `tJavaScript` | Not supported | Python equivalent |
| `tSetKeystore` | Key Vault reference | `mssparkutils.credentials.getSecret()` |
| `tLibraryLoad` | Not applicable | `%pip install` / `sc.addPyFile()` |

## 17. Data Quality Components

| Talend Component | Fabric Data Factory | Fabric Spark |
|---|---|---|
| `tSchemaComplianceCheck` | Data quality rules (Data Flow) | Custom validation `df.filter()` |
| `tDataMasking` | Not natively supported | `F.sha2()` / `F.regexp_replace()` |
| `tPatternCheck` | Derived Column (regex match) | `F.rlike()` / `F.regexp_extract()` |
| `tValueCount` | Aggregate transformation | `df.groupBy().count()` |
| `tSurvivorshipRules` | Not natively supported | Custom PySpark logic |
| `tRuleSurvivorship` | Not natively supported | Custom PySpark logic |
| `tMatchPairing` | Not natively supported | `recordlinkage` library |
| `tStandardize` | Derived Column (expressions) | Custom `F.when()` / lookup tables |

## 18. MDM & Big Data Components

| Talend Component | Fabric Data Factory | Fabric Spark |
|---|---|---|
| `tMDMInput` / `tMDMOutput` | REST API / Web Activity | `requests` + Fabric API |
| `tSparkInput` / `tSparkOutput` | Not applicable (native Spark) | `spark.read` / `df.write` |
| `tPigLoad` / `tPigStore` | Not applicable | Rewrite as PySpark |
| `tMapReduceInput` / `tMapReduceOutput` | Not applicable | Rewrite as PySpark |
| `tImpalaInput` / `tImpalaOutput` | Not natively supported | `spark.read.jdbc(...)` with Impala JDBC |

## 19. Change Data Capture (CDC) Components

| Talend Component | Fabric Data Factory | Fabric Spark |
|---|---|---|
| `tOracleCDC` | Copy Activity (incremental) | Watermark query / Fabric Mirroring |
| `tMysqlCDC` | Copy Activity (incremental) | Debezium → Event Hub → Spark Streaming |
| `tMSSqlCDC` | Copy Activity (incremental) | Fabric Mirroring / Watermark pattern |
| `tPostgresqlCDC` | Copy Activity (incremental) | Logical replication / Watermark pattern |
| `tDBCDC` | Copy Activity (incremental) | Watermark column + Delta MERGE |

> **Note:** Talend CDC components use database-specific log readers (LogMiner, binlog, etc.).
> In Fabric, prefer **Fabric Mirroring** (for SQL Server, Azure SQL, Cosmos DB) or
> **watermark-based incremental loads** (see `templates/spark/cdc_watermark.py`).

## 20. Hash / In-Memory Lookup Components

| Talend Component | Fabric Data Factory | Fabric Spark |
|---|---|---|
| `tHashInput` | Not applicable | `broadcast(df)` / `df.cache()` |
| `tHashOutput` | Not applicable | `df.cache()` / `df.persist(StorageLevel.MEMORY_ONLY)` |

> **Note:** In Talend, `tHashOutput` writes rows to an in-memory hash table, and `tHashInput`
> reads them back — typically used for multi-pass processing or lookup caching within a job.
> In Spark, use `df.cache()` or `broadcast(df)` for equivalent behavior. For tMap lookups
> that use tHashInput, translate to broadcast joins: `main_df.join(broadcast(lookup_df), ...)`.

## 21. Database-Specific Operations (per vendor)

| Operation | Oracle | SQL Server | MySQL | PostgreSQL | DB2 |
|---|---|---|---|---|---|
| **Connection** | `tOracleConnection` | `tMSSqlConnection` | `tMysqlConnection` | `tPostgresqlConnection` | `tDB2Connection` |
| **Row (DML)** | `tOracleRow` | `tMSSqlRow` | `tMysqlRow` | `tPostgresqlRow` | `tDB2Row` |
| **Stored Proc** | `tOracleSP` | `tMSSqlSP` | `tMysqlSP` | `tPostgresqlSP` | `tDBSP` |
| **Bulk Exec** | `tOracleBulkExec` | `tMSSqlBulkExec` | `tMysqlBulkExec` | `tPostgresqlBulkExec` | — |
| **Commit** | `tOracleCommit` | `tMSSqlCommit` | `tMysqlCommit` | `tPostgresqlCommit` | `tDBCommit` |
| **Close** | `tOracleClose` | `tMSSqlClose` | `tMysqlClose` | `tPostgresqlClose` | `tDBClose` |
| **CDC** | `tOracleCDC` | `tMSSqlCDC` | `tMysqlCDC` | `tPostgresqlCDC` | — |

> All map to Fabric Data Factory activities (Linked Service + Script/Copy) or Spark JDBC operations.

## 22. Additional Database Connectors

| Talend Component | Fabric Data Factory | Fabric Spark |
|---|---|---|
| `tVerticaInput/Output/Connection` | Copy Activity (Vertica) | `spark.read.jdbc(...)` / `df.write.jdbc(...)` |
| `tNetezzaInput/Output/Connection` | Copy Activity (Netezza) | `spark.read.jdbc(...)` / `df.write.jdbc(...)` |
| `tGreenplumInput/Output/Connection` | Copy Activity (Greenplum) | `spark.read.jdbc(...)` / `df.write.jdbc(...)` |
| `tAS400Input/Output/Connection` | Copy Activity (DB2 for i) | `spark.read.jdbc(...)` (jt400 driver) |
| `tSAPHanaInput/Output/Connection` | Copy Activity (SAP HANA) | `spark.read.jdbc(...)` (ngdbc driver) |
| `tImpalaInput/Output/Connection` | Not supported natively | `spark.read.jdbc(...)` (Impala driver) |
| `tAccessInput/Output` | Not supported natively | Convert to CSV/Parquet first |

## 23. Big Data / Hadoop Components (Deprecated → Spark)

| Talend Component | Fabric Equivalent | Notes |
|---|---|---|
| `tSqoopImport` | `spark.read.jdbc(...)` | Sqoop deprecated — use Spark JDBC |
| `tSqoopExport` | `df.write.jdbc(...)` | Sqoop deprecated — use Spark JDBC |
| `tSparkConfiguration` | `SparkSession.builder.config()` | Set via Fabric Spark environment |
| `tPigLoad` / `tPigMap` / `tPigStore` | PySpark equivalents | Pig deprecated — rewrite as PySpark |
| `tMapReduceInput` / `tMapReduceOutput` | PySpark equivalents | MapReduce deprecated — rewrite as PySpark |
| `tHiveCreateTable` | `spark.sql("CREATE TABLE ...")` | Native to Spark/Lakehouse |
| `tHiveLoad` | `spark.sql("INSERT INTO ...")` | Native to Spark/Lakehouse |
| `tHiveRow` | `spark.sql("DML ...")` | Native to Spark/Lakehouse |

## 24. Advanced XML Components

| Talend Component | Fabric Data Factory | Fabric Spark |
|---|---|---|
| `tXSLT` | Not supported | `lxml.etree.XSLT()` in Python |
| `tAdvancedFileOutputXML` | Data Flow (XML sink) | `lxml.etree.Element()` — build XML tree |
| `tWebServiceOutput` | Web Activity (POST) | Flask/FastAPI endpoint or Logic App |

## 25. Merge / Upsert Patterns (tDBOutput Action Modes)

| Talend Action Mode | Delta Lake Equivalent | Template |
|---|---|---|
| `INSERT` | `df.write.mode("append")` | `merge_upsert.py` |
| `UPDATE` | `MERGE ... WHEN MATCHED UPDATE` | `merge_upsert.py` |
| `INSERT_OR_UPDATE` | `MERGE ... WHEN MATCHED UPDATE + NOT MATCHED INSERT` | `merge_upsert.py` |
| `UPDATE_OR_INSERT` | Same as INSERT_OR_UPDATE | `merge_upsert.py` |
| `DELETE` | `MERGE ... WHEN MATCHED DELETE` | `merge_upsert.py` |
| `DELETE_INSERT` | `MERGE DELETE + df.write.mode("append")` | `merge_upsert.py` |

> See `templates/spark/merge_upsert.py` for the full implementation of all action modes.

## 26. Available Spark Templates

| Template | Talend Pattern | File |
|---|---|---|
| ETL Notebook | tInput → tMap → tOutput | `templates/spark/etl_notebook.py` |
| Incremental Load | tDBInput with WHERE date > last_run | `templates/spark/incremental_load.py` |
| Lookup & Join | tMap with lookup inputs | `templates/spark/lookup_pattern.py` |
| SCD Type 1 | tDBOutput (UPDATE_OR_INSERT) | `templates/spark/scd_type1.py` |
| SCD Type 2 | tDBOutput with history tracking | `templates/spark/scd_type2.py` |
| CDC / Watermark | tOracleCDC / tMysqlCDC / tDBInput incremental | `templates/spark/cdc_watermark.py` |
| Merge / Upsert | tDBOutput (all action modes) | `templates/spark/merge_upsert.py` |
