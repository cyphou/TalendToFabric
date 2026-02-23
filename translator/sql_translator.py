"""
Multi-Dialect SQL → PostgreSQL Translator
Converts source-specific SQL syntax (Oracle, SQL Server, MySQL, DB2, Teradata,
Snowflake, Sybase) to PostgreSQL equivalents.

Usage:
    python sql_translator.py --dialect oracle --input queries.sql --output queries_pg.sql
    python sql_translator.py --dialect sqlserver --inline "SELECT TOP 10 * FROM employees"
    python sql_translator.py --dialect mysql --inline "SELECT IFNULL(name, 'N/A') FROM t"
"""

import re
import logging
from typing import Dict, List, Tuple, Optional

import click

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ═══════════════════════════════════════════════════════════════════════
#  Base Translator
# ═══════════════════════════════════════════════════════════════════════

class SQLTranslatorBase:
    """Base class for dialect-specific SQL → PostgreSQL translators."""

    dialect: str = "generic"

    def __init__(self):
        self.rules: List[Tuple[str, str, str]] = []

    def translate(self, sql: str) -> str:
        """Translate a single SQL statement to PostgreSQL."""
        translated = sql
        applied_rules = []

        for pattern, replacement, description in self.rules:
            new_sql = re.sub(pattern, replacement, translated, flags=re.IGNORECASE)
            if new_sql != translated:
                applied_rules.append(description)
                translated = new_sql

        if applied_rules:
            logger.info(f"[{self.dialect}] Applied {len(applied_rules)} rules: {', '.join(applied_rules)}")

        # Clean up extra whitespace
        translated = re.sub(r'\s+', ' ', translated).strip()
        translated = re.sub(r'\s+;', ';', translated)

        return translated

    def translate_file(self, input_path: str, output_path: str) -> None:
        """Translate all SQL in a file."""
        with open(input_path, "r", encoding="utf-8") as f:
            source_sql = f.read()

        # Split into statements
        statements = source_sql.split(";")
        translated_statements = []

        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                translated = self.translate(stmt)
                translated_statements.append(translated)

        pg_sql = ";\n\n".join(translated_statements)
        if translated_statements:
            pg_sql += ";"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(f"-- Auto-translated from {self.dialect.upper()} to PostgreSQL\n")
            f.write(f"-- Review carefully before executing\n\n")
            f.write(pg_sql)

        logger.info(f"Translated {len(translated_statements)} statements → {output_path}")


# ═══════════════════════════════════════════════════════════════════════
#  Oracle → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

class OracleToPostgreSQLTranslator(SQLTranslatorBase):
    """Translates Oracle SQL to PostgreSQL SQL."""

    dialect = "oracle"

    def __init__(self):
        super().__init__()
        self.rules = [
            # Pseudo-tables
            (r'\bFROM\s+DUAL\b', '', 'Remove FROM DUAL'),
            (r'\bSELECT\s+(.*?)\s+FROM\s+DUAL', r'SELECT \1', 'Remove DUAL'),

            # Date/time functions
            (r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', 'SYSDATE → CURRENT_TIMESTAMP'),
            (r'\bSYSTIMESTAMP\b', 'CURRENT_TIMESTAMP', 'SYSTIMESTAMP → CURRENT_TIMESTAMP'),
            (r'\bGETDATE\(\)', 'CURRENT_TIMESTAMP', 'GETDATE() → CURRENT_TIMESTAMP'),

            # String functions
            (r'\bNVL\s*\(', 'COALESCE(', 'NVL → COALESCE'),
            (r'\bNVL2\s*\(\s*([^,]+),\s*([^,]+),\s*([^)]+)\)',
             r'CASE WHEN \1 IS NOT NULL THEN \2 ELSE \3 END', 'NVL2 → CASE'),
            (r'\bDECODE\s*\(\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^)]+)\)',
             r'CASE WHEN \1 = \2 THEN \3 ELSE \4 END', 'DECODE (simple) → CASE'),
            (r'\|\|', '||', 'String concatenation (same in PostgreSQL)'),
            (r'\bSUBSTR\s*\(', 'SUBSTRING(', 'SUBSTR → SUBSTRING'),
            (r'\bINSTR\s*\(', 'POSITION(', 'INSTR → POSITION (simplified)'),
            (r'\bLENGTH\s*\(', 'LENGTH(', 'LENGTH (compatible)'),

            # Numeric functions
            (r'\bTRUNC\s*\(\s*([^,)]+)\s*\)', r'TRUNC(\1)', 'TRUNC (compatible)'),
            (r'\bTRUNC\s*\(\s*([^,]+),\s*([^)]+)\)', r'TRUNC(\1, \2)', 'TRUNC with precision'),

            # Date arithmetic
            (r"ADD_MONTHS\s*\(\s*([^,]+),\s*([^)]+)\)",
             r"\1 + INTERVAL '\2 months'", 'ADD_MONTHS → interval'),
            (r"MONTHS_BETWEEN\s*\(\s*([^,]+),\s*([^)]+)\)",
             r"EXTRACT(EPOCH FROM (\1 - \2)) / 2592000", 'MONTHS_BETWEEN → EXTRACT'),

            # Date formatting
            (r"TO_CHAR\s*\(\s*([^,]+),\s*'([^']+)'\s*\)",
             r"TO_CHAR(\1, '\2')", 'TO_CHAR (compatible, review format)'),
            (r"TO_DATE\s*\(\s*'([^']+)',\s*'([^']+)'\s*\)",
             r"TO_DATE('\1', '\2')", 'TO_DATE (compatible, review format)'),
            (r"TO_NUMBER\s*\(\s*([^)]+)\s*\)",
             r"CAST(\1 AS NUMERIC)", 'TO_NUMBER → CAST AS NUMERIC'),

            # Sequence
            (r'(\w+)\.NEXTVAL', r"nextval('\1')", 'sequence.NEXTVAL → nextval()'),
            (r'(\w+)\.CURRVAL', r"currval('\1')", 'sequence.CURRVAL → currval()'),

            # Outer join syntax (old Oracle +)
            (r'\(\+\)', '', 'Remove Oracle (+) outer join syntax — rewrite as ANSI JOIN'),

            # ROWNUM
            (r'WHERE\s+ROWNUM\s*<=\s*(\d+)', r'LIMIT \1', 'ROWNUM <= N → LIMIT N'),
            (r'AND\s+ROWNUM\s*<=\s*(\d+)', r'LIMIT \1', 'ROWNUM in AND → LIMIT'),
            (r'\bROWNUM\b', 'ROW_NUMBER() OVER ()', 'ROWNUM → ROW_NUMBER()'),

            # Data types in CAST/CREATE
            (r'\bVARCHAR2\s*\(', 'VARCHAR(', 'VARCHAR2 → VARCHAR'),
            (r'\bNUMBER\s*\((\d+)\s*\)', r'NUMERIC(\1)', 'NUMBER(p) → NUMERIC(p)'),
            (r'\bNUMBER\s*\((\d+),\s*(\d+)\)', r'NUMERIC(\1,\2)', 'NUMBER(p,s) → NUMERIC(p,s)'),
            (r'\bNUMBER\b', 'NUMERIC', 'NUMBER → NUMERIC'),
            (r'\bCLOB\b', 'TEXT', 'CLOB → TEXT'),
            (r'\bBLOB\b', 'BYTEA', 'BLOB → BYTEA'),
            (r'\bRAW\s*\(\d+\)', 'BYTEA', 'RAW → BYTEA'),

            # Miscellaneous
            (r'\bLISTAGG\s*\(\s*([^,]+),\s*([^)]+)\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+([^)]+)\)',
             r"STRING_AGG(\1, \2 ORDER BY \3)", 'LISTAGG → STRING_AGG'),
        ]


# ═══════════════════════════════════════════════════════════════════════
#  SQL Server → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

class SqlServerToPostgreSQLTranslator(SQLTranslatorBase):
    """Translates T-SQL (SQL Server / Azure SQL) to PostgreSQL SQL."""

    dialect = "sqlserver"

    def __init__(self):
        super().__init__()
        self.rules = [
            # TOP → LIMIT (simple cases)
            (r'\bSELECT\s+TOP\s+(\d+)\b', r'SELECT', 'SELECT TOP N → SELECT'),
            # We append LIMIT N in a post-processing step

            # Date/time functions
            (r'\bGETDATE\s*\(\)', 'CURRENT_TIMESTAMP', 'GETDATE() → CURRENT_TIMESTAMP'),
            (r'\bGETUTCDATE\s*\(\)', "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'", 'GETUTCDATE() → UTC timestamp'),
            (r'\bSYSDATETIME\s*\(\)', 'CURRENT_TIMESTAMP', 'SYSDATETIME() → CURRENT_TIMESTAMP'),
            (r'\bDATEADD\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r"\3 + INTERVAL '\2 \1'", 'DATEADD → interval'),
            (r'\bDATEDIFF\s*\(\s*DAY\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'(\2::date - \1::date)', 'DATEDIFF(day) → date subtraction'),
            (r'\bDATEDIFF\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r"EXTRACT(EPOCH FROM (\3 - \2))", 'DATEDIFF(unit) → EXTRACT'),
            (r'\bDATENAME\s*\(\s*(\w+)\s*,\s*([^)]+)\s*\)',
             r"TO_CHAR(\2, '\1')", 'DATENAME → TO_CHAR'),
            (r'\bDATEPART\s*\(\s*(\w+)\s*,\s*([^)]+)\s*\)',
             r'EXTRACT(\1 FROM \2)', 'DATEPART → EXTRACT'),

            # Null handling
            (r'\bISNULL\s*\(', 'COALESCE(', 'ISNULL → COALESCE'),

            # Type conversion
            (r'\bCONVERT\s*\(\s*VARCHAR\s*(?:\(\d+\))?\s*,\s*([^)]+)\s*\)',
             r'\1::VARCHAR', 'CONVERT(VARCHAR, x) → x::VARCHAR'),
            (r'\bCONVERT\s*\(\s*INT\s*,\s*([^)]+)\s*\)',
             r'\1::INTEGER', 'CONVERT(INT, x) → x::INTEGER'),
            (r'\bCONVERT\s*\(\s*(\w+)\s*,\s*([^)]+)\s*\)',
             r'\2::\1', 'CONVERT(type, x) → x::type'),

            # String functions
            (r'\bLEN\s*\(', 'LENGTH(', 'LEN → LENGTH'),
            (r'\bCHARINDEX\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'POSITION(\1 IN \2)', 'CHARINDEX → POSITION'),
            (r'\bSTUFF\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'OVERLAY(\1 PLACING \4 FROM \2 FOR \3)', 'STUFF → OVERLAY'),
            (r'\+\s*(?=\')', '|| ', 'String concat + → ||'),

            # Identity / sequences
            (r'@@IDENTITY', "lastval()", '@@IDENTITY → lastval()'),
            (r'\bSCOPE_IDENTITY\s*\(\)', "lastval()", 'SCOPE_IDENTITY() → lastval()'),
            (r'\bIDENTITY\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)',
             'GENERATED ALWAYS AS IDENTITY', 'IDENTITY(seed,inc) → GENERATED AS IDENTITY'),

            # Bracket quoting → double quotes
            (r'\[([^\]]+)\]', r'"\1"', '[bracket] → "double quote"'),

            # Temp tables
            (r'#(\w+)', r'temp_\1', '#temp_table → temp_table'),

            # Data types
            (r'\bNVARCHAR\s*\(\s*MAX\s*\)', 'TEXT', 'NVARCHAR(MAX) → TEXT'),
            (r'\bVARCHAR\s*\(\s*MAX\s*\)', 'TEXT', 'VARCHAR(MAX) → TEXT'),
            (r'\bNVARCHAR\s*\(', 'VARCHAR(', 'NVARCHAR → VARCHAR'),
            (r'\bNTEXT\b', 'TEXT', 'NTEXT → TEXT'),
            (r'\bBIT\b', 'BOOLEAN', 'BIT → BOOLEAN'),
            (r'\bDATETIME2\b', 'TIMESTAMP', 'DATETIME2 → TIMESTAMP'),
            (r'\bDATETIME\b', 'TIMESTAMP', 'DATETIME → TIMESTAMP'),
            (r'\bSMALLDATETIME\b', 'TIMESTAMP', 'SMALLDATETIME → TIMESTAMP'),
            (r'\bMONEY\b', 'NUMERIC(19,4)', 'MONEY → NUMERIC(19,4)'),
            (r'\bSMALLMONEY\b', 'NUMERIC(10,4)', 'SMALLMONEY → NUMERIC(10,4)'),
            (r'\bUNIQUEIDENTIFIER\b', 'UUID', 'UNIQUEIDENTIFIER → UUID'),
            (r'\bIMAGE\b', 'BYTEA', 'IMAGE → BYTEA'),
            (r'\bVARBINARY\s*\(\s*MAX\s*\)', 'BYTEA', 'VARBINARY(MAX) → BYTEA'),
            (r'\bTINYINT\b', 'SMALLINT', 'TINYINT → SMALLINT'),

            # Miscellaneous
            (r'\bNOLOCK\b', '', 'Remove NOLOCK hint'),
            (r'\bWITH\s*\(\s*NOLOCK\s*\)', '', 'Remove WITH(NOLOCK)'),
            (r'\bSET\s+NOCOUNT\s+ON\b', '', 'Remove SET NOCOUNT ON'),
        ]

    def translate(self, sql: str) -> str:
        """Override to handle TOP → LIMIT rewrite."""
        # Extract TOP N before applying rules
        top_match = re.search(r'\bSELECT\s+TOP\s+(\d+)\b', sql, re.IGNORECASE)
        top_n = top_match.group(1) if top_match else None

        translated = super().translate(sql)

        # Append LIMIT if TOP was found
        if top_n and 'LIMIT' not in translated.upper():
            translated = translated.rstrip(';').rstrip() + f' LIMIT {top_n}'

        return translated


# ═══════════════════════════════════════════════════════════════════════
#  MySQL → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

class MySqlToPostgreSQLTranslator(SQLTranslatorBase):
    """Translates MySQL SQL to PostgreSQL SQL."""

    dialect = "mysql"

    def __init__(self):
        super().__init__()
        self.rules = [
            # Null handling
            (r'\bIFNULL\s*\(', 'COALESCE(', 'IFNULL → COALESCE'),

            # Date/time
            (r'\bNOW\s*\(\)', 'CURRENT_TIMESTAMP', 'NOW() → CURRENT_TIMESTAMP'),
            (r'\bCURDATE\s*\(\)', 'CURRENT_DATE', 'CURDATE() → CURRENT_DATE'),
            (r'\bCURTIME\s*\(\)', 'CURRENT_TIME', 'CURTIME() → CURRENT_TIME'),
            (r'\bDATE_ADD\s*\(\s*([^,]+)\s*,\s*INTERVAL\s+(\d+)\s+(\w+)\s*\)',
             r"\1 + INTERVAL '\2 \3'", 'DATE_ADD → interval addition'),
            (r'\bDATE_SUB\s*\(\s*([^,]+)\s*,\s*INTERVAL\s+(\d+)\s+(\w+)\s*\)',
             r"\1 - INTERVAL '\2 \3'", 'DATE_SUB → interval subtraction'),
            (r'\bDATE_FORMAT\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'TO_CHAR(\1, \2)', 'DATE_FORMAT → TO_CHAR'),
            (r'\bSTR_TO_DATE\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'TO_DATE(\1, \2)', 'STR_TO_DATE → TO_DATE'),
            (r'\bUNIX_TIMESTAMP\s*\(\s*([^)]*)\s*\)',
             r'EXTRACT(EPOCH FROM \1)', 'UNIX_TIMESTAMP → EXTRACT(EPOCH)'),

            # String functions
            (r'\bGROUP_CONCAT\s*\(\s*([^)]+)\s*\)',
             r'STRING_AGG(\1, ",")', 'GROUP_CONCAT → STRING_AGG'),
            (r'\bLOCATE\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'POSITION(\1 IN \2)', 'LOCATE → POSITION'),
            (r'\bCONCAT_WS\s*\(', 'CONCAT_WS(', 'CONCAT_WS (compatible)'),
            (r'\bSUBSTRING_INDEX\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'SPLIT_PART(\1, \2, \3)', 'SUBSTRING_INDEX → SPLIT_PART'),

            # Flow control
            (r'\bIF\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'CASE WHEN \1 THEN \2 ELSE \3 END', 'IF() → CASE WHEN'),

            # LIMIT with offset: MySQL uses LIMIT offset, count → PG uses LIMIT count OFFSET offset
            (r'\bLIMIT\s+(\d+)\s*,\s*(\d+)', r'LIMIT \2 OFFSET \1', 'LIMIT x,y → LIMIT y OFFSET x'),

            # Auto increment
            (r'\bAUTO_INCREMENT\b', 'GENERATED ALWAYS AS IDENTITY', 'AUTO_INCREMENT → IDENTITY'),
            (r'\bINT\s+UNSIGNED\b', 'BIGINT', 'INT UNSIGNED → BIGINT'),
            (r'\bBIGINT\s+UNSIGNED\b', 'NUMERIC(20)', 'BIGINT UNSIGNED → NUMERIC(20)'),
            (r'\bUNSIGNED\b', '', 'Remove UNSIGNED'),

            # Backtick quoting
            (r'`([^`]+)`', r'"\1"', 'Backtick → double quote'),

            # Data types
            (r'\bTINYINT\s*\(\s*1\s*\)', 'BOOLEAN', 'TINYINT(1) → BOOLEAN'),
            (r'\bTINYINT\b', 'SMALLINT', 'TINYINT → SMALLINT'),
            (r'\bMEDIUMINT\b', 'INTEGER', 'MEDIUMINT → INTEGER'),
            (r'\bMEDIUMTEXT\b', 'TEXT', 'MEDIUMTEXT → TEXT'),
            (r'\bLONGTEXT\b', 'TEXT', 'LONGTEXT → TEXT'),
            (r'\bMEDIUMBLOB\b', 'BYTEA', 'MEDIUMBLOB → BYTEA'),
            (r'\bLONGBLOB\b', 'BYTEA', 'LONGBLOB → BYTEA'),
            (r'\bTINYBLOB\b', 'BYTEA', 'TINYBLOB → BYTEA'),
            (r'\bDOUBLE\b(?!\s+PRECISION)', 'DOUBLE PRECISION', 'DOUBLE → DOUBLE PRECISION'),
            (r'\bYEAR\b', 'INTEGER', 'YEAR → INTEGER'),

            # Engine / charset (remove MySQL-specific clauses)
            (r'\bENGINE\s*=\s*\w+', '', 'Remove ENGINE='),
            (r'\bDEFAULT\s+CHARSET\s*=\s*\w+', '', 'Remove DEFAULT CHARSET='),
            (r'\bCOLLATE\s*=?\s*\w+', '', 'Remove COLLATE'),
            (r'\bCHARACTER\s+SET\s+\w+', '', 'Remove CHARACTER SET'),
        ]


# ═══════════════════════════════════════════════════════════════════════
#  DB2 → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

class DB2ToPostgreSQLTranslator(SQLTranslatorBase):
    """Translates IBM DB2 SQL to PostgreSQL SQL."""

    dialect = "db2"

    def __init__(self):
        super().__init__()
        self.rules = [
            # Row limiting
            (r'\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY\b', r'LIMIT \1', 'FETCH FIRST N ROWS → LIMIT'),
            (r'\bOPTIMIZE\s+FOR\s+\d+\s+ROWS?\b', '', 'Remove OPTIMIZE FOR N ROWS'),

            # Null handling
            (r'\bVALUE\s*\(', 'COALESCE(', 'VALUE() → COALESCE()'),

            # Date/time
            (r'\bCURRENT\s+DATE\b', 'CURRENT_DATE', 'CURRENT DATE → CURRENT_DATE'),
            (r'\bCURRENT\s+TIME\b', 'CURRENT_TIME', 'CURRENT TIME → CURRENT_TIME'),
            (r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP', 'CURRENT TIMESTAMP → CURRENT_TIMESTAMP'),
            (r'\bDATE\s*\(\s*([^)]+)\s*\)', r'\1::DATE', 'DATE() → ::DATE cast'),
            (r'\bTIMESTAMP\s*\(\s*([^)]+)\s*\)', r'\1::TIMESTAMP', 'TIMESTAMP() → ::TIMESTAMP cast'),
            (r'\b(\w+)\s*\+\s*(\d+)\s+DAYS?\b', r"\1 + INTERVAL '\2 days'", 'date + N DAYS → interval'),
            (r'\b(\w+)\s*-\s*(\d+)\s+DAYS?\b', r"\1 - INTERVAL '\2 days'", 'date - N DAYS → interval'),
            (r'\b(\w+)\s*\+\s*(\d+)\s+MONTHS?\b', r"\1 + INTERVAL '\2 months'", 'date + N MONTHS → interval'),

            # String functions
            (r'\bCONCAT\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'\1 || \2', 'CONCAT(a,b) → a || b'),
            (r'\bPOSSTR\s*\(', 'POSITION(', 'POSSTR → POSITION'),
            (r'\bSTRIP\s*\(', 'TRIM(', 'STRIP → TRIM'),

            # Sequences
            (r'\bNEXT\s+VALUE\s+FOR\s+(\w+)',
             r"nextval('\1')", 'NEXT VALUE FOR seq → nextval()'),
            (r'\bPREVIOUS\s+VALUE\s+FOR\s+(\w+)',
             r"currval('\1')", 'PREVIOUS VALUE FOR seq → currval()'),

            # Isolation / locking (remove)
            (r'\bWITH\s+UR\b', '', 'Remove WITH UR'),
            (r'\bWITH\s+CS\b', '', 'Remove WITH CS'),
            (r'\bWITH\s+RS\b', '', 'Remove WITH RS'),
            (r'\bWITH\s+RR\b', '', 'Remove WITH RR'),

            # Data types
            (r'\bGRAPHIC\s*\(\d+\)', 'VARCHAR', 'GRAPHIC → VARCHAR'),
            (r'\bVARGRAPHIC\s*\(\d+\)', 'VARCHAR', 'VARGRAPHIC → VARCHAR'),
            (r'\bDBCLOB\b', 'TEXT', 'DBCLOB → TEXT'),
            (r'\bDECFLOAT\b', 'NUMERIC', 'DECFLOAT → NUMERIC'),
            (r'\bCLOB\b', 'TEXT', 'CLOB → TEXT'),
            (r'\bBLOB\b', 'BYTEA', 'BLOB → BYTEA'),

            # Misc
            (r'\bFOR\s+READ\s+ONLY\b', '', 'Remove FOR READ ONLY'),
        ]


# ═══════════════════════════════════════════════════════════════════════
#  Teradata → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

class TeradataToPostgreSQLTranslator(SQLTranslatorBase):
    """Translates Teradata SQL to PostgreSQL SQL."""

    dialect = "teradata"

    def __init__(self):
        super().__init__()
        self.rules = [
            # SEL shorthand
            (r'^\s*\bSEL\b', 'SELECT', 'SEL → SELECT'),

            # Row limiting
            (r'\bSAMPLE\s+(\d+)\b', r'ORDER BY RANDOM() LIMIT \1', 'SAMPLE N → ORDER BY RANDOM() LIMIT N'),
            (r'\bTOP\s+(\d+)\b', '', 'Remove TOP (append LIMIT)'),

            # QUALIFY → subquery with ROW_NUMBER
            (r'\bQUALIFY\s+ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(([^)]+)\)\s*=\s*(\d+)',
             r'/* QUALIFY rewrite needed: ROW_NUMBER() OVER(\1) = \2 — use subquery */',
             'QUALIFY → subquery (manual review)'),

            # Date/time
            (r'\bDATE\b(?!\s*\()', 'CURRENT_DATE', 'DATE → CURRENT_DATE'),
            (r'\bTIME\b(?!\s*\()', 'CURRENT_TIME', 'TIME → CURRENT_TIME'),
            (r"FORMAT\s+'([^']+)'", r"/* FORMAT '\1' — use TO_CHAR */", 'FORMAT → TO_CHAR (manual review)'),

            # String functions
            (r'\bINDEX\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'POSITION(\2 IN \1)', 'INDEX(s,sub) → POSITION(sub IN s)'),
            (r'\bTRIM\s*\(\s*(BOTH|LEADING|TRAILING)\s+([^)]+)\s+FROM\s+([^)]+)\s*\)',
             r'TRIM(\1 \2 FROM \3)', 'TRIM (compatible)'),

            # Named alias
            (r'\bNAMED\b', 'AS', 'NAMED → AS'),

            # Data types
            (r'\bBYTEINT\b', 'SMALLINT', 'BYTEINT → SMALLINT'),
            (r'\bNUMBER\s*\((\d+)\s*\)', r'NUMERIC(\1)', 'NUMBER(p) → NUMERIC(p)'),
            (r'\bNUMBER\s*\((\d+),\s*(\d+)\)', r'NUMERIC(\1,\2)', 'NUMBER(p,s) → NUMERIC(p,s)'),
            (r'\bNUMBER\b', 'NUMERIC', 'NUMBER → NUMERIC'),

            # Volatile tables
            (r'\bCREATE\s+VOLATILE\s+TABLE\b', 'CREATE TEMPORARY TABLE', 'VOLATILE TABLE → TEMPORARY TABLE'),
            (r'\bON\s+COMMIT\s+PRESERVE\s+ROWS\b', 'ON COMMIT PRESERVE ROWS', 'ON COMMIT PRESERVE ROWS (compatible)'),

            # Collect stats
            (r'\bCOLLECT\s+STAT(ISTIC)?S?\b[^;]*', 'ANALYZE', 'COLLECT STATS → ANALYZE'),

            # Multi-value INSERT (Teradata-specific)
            (r'\bINS\b', 'INSERT', 'INS → INSERT'),

            # Set operators
            (r'\bMINUS\b', 'EXCEPT', 'MINUS → EXCEPT'),

            # Locking
            (r'\bLOCKING\s+\w+\s+FOR\s+ACCESS\b', '', 'Remove LOCKING ... FOR ACCESS'),
        ]


# ═══════════════════════════════════════════════════════════════════════
#  Snowflake → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

class SnowflakeToPostgreSQLTranslator(SQLTranslatorBase):
    """Translates Snowflake SQL to PostgreSQL SQL."""

    dialect = "snowflake"

    def __init__(self):
        super().__init__()
        self.rules = [
            # Data types
            (r'\bVARIANT\b', 'JSONB', 'VARIANT → JSONB'),
            (r'\bARRAY\b(?!\s*\[)', 'JSONB', 'ARRAY → JSONB'),
            (r'\bOBJECT\b(?!\s+\w)', 'JSONB', 'OBJECT → JSONB'),

            # Semi-structured access
            (r':(\w+)', r"->>'\1'", 'col:key → col->>''key'''),
            (r'\bFLATTEN\s*\(\s*INPUT\s*=>\s*([^)]+)\s*\)',
             r"jsonb_array_elements(\1)", 'FLATTEN → jsonb_array_elements'),
            (r'\bPARSE_JSON\s*\(', 'CAST(', 'PARSE_JSON → CAST (review)'),

            # Timestamp variants
            (r'\bTIMESTAMP_NTZ\b', 'TIMESTAMP', 'TIMESTAMP_NTZ → TIMESTAMP'),
            (r'\bTIMESTAMP_LTZ\b', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP_LTZ → TIMESTAMPTZ'),
            (r'\bTIMESTAMP_TZ\b', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP_TZ → TIMESTAMPTZ'),

            # Number
            (r'\bNUMBER\s*\((\d+),\s*(\d+)\)', r'NUMERIC(\1,\2)', 'NUMBER(p,s) → NUMERIC(p,s)'),
            (r'\bNUMBER\s*\((\d+)\s*\)', r'NUMERIC(\1)', 'NUMBER(p) → NUMERIC(p)'),
            (r'\bNUMBER\b', 'NUMERIC', 'NUMBER → NUMERIC'),
            (r'\bFLOAT\b(?!\s+PRECISION)', 'DOUBLE PRECISION', 'FLOAT → DOUBLE PRECISION'),

            # Functions
            (r'\bIFF\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'CASE WHEN \1 THEN \2 ELSE \3 END', 'IFF → CASE WHEN'),
            (r'\bTRY_CAST\s*\(\s*([^)]+)\s+AS\s+([^)]+)\s*\)',
             r'CAST(\1 AS \2)', 'TRY_CAST → CAST (add error handling)'),
            (r'\bZEROIFNULL\s*\(', 'COALESCE(', 'ZEROIFNULL → COALESCE(x, 0)'),
            (r'\bNVL\s*\(', 'COALESCE(', 'NVL → COALESCE'),
            (r'\bDATEADD\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r"\3 + INTERVAL '\2 \1'", 'DATEADD → interval'),
            (r'\bDATEDIFF\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r"EXTRACT(EPOCH FROM (\3 - \2))", 'DATEDIFF → EXTRACT'),
            (r'\bTO_VARCHAR\s*\(', 'TO_CHAR(', 'TO_VARCHAR → TO_CHAR'),
            (r'\bTO_DECIMAL\s*\(', 'CAST(', 'TO_DECIMAL → CAST'),

            # Staging / Snowflake-specific (remove or comment)
            (r'\bCOPY\s+INTO\b', '/* COPY INTO — rewrite for PostgreSQL */', 'COPY INTO → comment'),
            (r'\bCREATE\s+OR\s+REPLACE\s+STAGE\b', '/* CREATE STAGE — N/A */', 'CREATE STAGE → comment'),
            (r'@(\w+)', r'/* stage @\1 */', 'Stage reference → comment'),
        ]


# ═══════════════════════════════════════════════════════════════════════
#  Sybase → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

class SybaseToPostgreSQLTranslator(SQLTranslatorBase):
    """Translates Sybase ASE SQL to PostgreSQL SQL. (Shares many T-SQL features.)"""

    dialect = "sybase"

    def __init__(self):
        super().__init__()
        self.rules = [
            # Date/time functions (same as SQL Server)
            (r'\bGETDATE\s*\(\)', 'CURRENT_TIMESTAMP', 'GETDATE() → CURRENT_TIMESTAMP'),
            (r'\bDATEADD\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r"\3 + INTERVAL '\2 \1'", 'DATEADD → interval'),
            (r'\bDATEDIFF\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r"EXTRACT(EPOCH FROM (\3 - \2))", 'DATEDIFF → EXTRACT'),
            (r'\bDATEPART\s*\(\s*(\w+)\s*,\s*([^)]+)\s*\)',
             r'EXTRACT(\1 FROM \2)', 'DATEPART → EXTRACT'),

            # Null handling
            (r'\bISNULL\s*\(', 'COALESCE(', 'ISNULL → COALESCE'),

            # Type conversion
            (r'\bCONVERT\s*\(\s*(\w+)\s*,\s*([^)]+)\s*\)',
             r'\2::\1', 'CONVERT(type, x) → x::type'),

            # String functions
            (r'\bLEN\s*\(', 'LENGTH(', 'LEN → LENGTH'),
            (r'\bCHARINDEX\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
             r'POSITION(\1 IN \2)', 'CHARINDEX → POSITION'),
            (r'\+\s*(?=\')', '|| ', 'String concat + → ||'),

            # Identity / sequences
            (r'@@IDENTITY', "lastval()", '@@IDENTITY → lastval()'),
            (r'@@ROWCOUNT', '/* @@ROWCOUNT — use GET DIAGNOSTICS ROW_COUNT */', '@@ROWCOUNT → comment'),

            # Data types
            (r'\bNVARCHAR\s*\(', 'VARCHAR(', 'NVARCHAR → VARCHAR'),
            (r'\bUNITEXT\b', 'TEXT', 'UNITEXT → TEXT'),
            (r'\bIMAGE\b', 'BYTEA', 'IMAGE → BYTEA'),
            (r'\bBIT\b', 'BOOLEAN', 'BIT → BOOLEAN'),
            (r'\bDATETIME\b', 'TIMESTAMP', 'DATETIME → TIMESTAMP'),
            (r'\bSMALLDATETIME\b', 'TIMESTAMP', 'SMALLDATETIME → TIMESTAMP'),
            (r'\bMONEY\b', 'NUMERIC(19,4)', 'MONEY → NUMERIC(19,4)'),
            (r'\bSMALLMONEY\b', 'NUMERIC(10,4)', 'SMALLMONEY → NUMERIC(10,4)'),
            (r'\bTINYINT\b', 'SMALLINT', 'TINYINT → SMALLINT'),

            # Misc
            (r'\bSET\s+NOCOUNT\s+ON\b', '', 'Remove SET NOCOUNT ON'),
        ]


# ═══════════════════════════════════════════════════════════════════════
#  Factory function & registry
# ═══════════════════════════════════════════════════════════════════════

DIALECT_REGISTRY: Dict[str, type] = {
    "oracle": OracleToPostgreSQLTranslator,
    "sqlserver": SqlServerToPostgreSQLTranslator,
    "mssql": SqlServerToPostgreSQLTranslator,           # alias
    "mysql": MySqlToPostgreSQLTranslator,
    "db2": DB2ToPostgreSQLTranslator,
    "teradata": TeradataToPostgreSQLTranslator,
    "snowflake": SnowflakeToPostgreSQLTranslator,
    "sybase": SybaseToPostgreSQLTranslator,
}


def get_translator(dialect: str) -> SQLTranslatorBase:
    """Factory: return the appropriate translator for the given source dialect."""
    key = dialect.lower().strip()
    cls = DIALECT_REGISTRY.get(key)
    if cls is None:
        supported = ", ".join(sorted(set(DIALECT_REGISTRY.keys())))
        raise ValueError(f"Unsupported dialect '{dialect}'. Supported: {supported}")
    return cls()


@click.command()
@click.option("--dialect", "-d", default="oracle",
              type=click.Choice(sorted(set(DIALECT_REGISTRY.keys())), case_sensitive=False),
              help="Source SQL dialect (default: oracle)")
@click.option("--input", "-i", "input_path", help="Path to source SQL file")
@click.option("--output", "-o", "output_path", help="Path to output PostgreSQL SQL file")
@click.option("--inline", help="Translate a single inline SQL statement")
def main(dialect: str, input_path: str, output_path: str, inline: str):
    """Translate source SQL to PostgreSQL SQL."""
    translator = get_translator(dialect)

    if inline:
        result = translator.translate(inline)
        print(f"{dialect.upper()}:     {inline}")
        print(f"PostgreSQL: {result}")
    elif input_path and output_path:
        translator.translate_file(input_path, output_path)
    else:
        click.echo("Provide either --input/--output or --inline. Use --help for details.")


if __name__ == "__main__":
    main()
