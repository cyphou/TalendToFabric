"""
Unit tests for multi-dialect SQL → PostgreSQL translators.
Covers: SQL Server, MySQL, DB2, Teradata, Snowflake, Sybase.
"""

import pytest


# ═══════════════════════════════════════════════════════════════════════
#  SQL Server → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.sql
class TestSqlServerBasic:
    """SQL Server (T-SQL) → PostgreSQL basic translations."""

    def test_getdate(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT GETDATE()")
        assert "CURRENT_TIMESTAMP" in result

    def test_isnull(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT ISNULL(name, 'N/A') FROM employees")
        assert "COALESCE" in result

    def test_top_to_limit(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT TOP 10 * FROM employees")
        assert "LIMIT 10" in result
        assert "TOP" not in result.upper().replace("LIMIT", "")

    def test_convert_varchar(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT CONVERT(VARCHAR, salary) FROM t")
        assert "::VARCHAR" in result

    def test_convert_int(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT CONVERT(INT, val) FROM t")
        assert "::INTEGER" in result

    def test_len(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT LEN(name) FROM t")
        assert "LENGTH" in result

    def test_charindex(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT CHARINDEX('@', email) FROM t")
        assert "POSITION" in result

    def test_dateadd(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT DATEADD(day, 7, hire_date) FROM t")
        assert "INTERVAL" in result

    def test_datediff_day(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT DATEDIFF(DAY, start_date, end_date) FROM t")
        assert "::date" in result or "EXTRACT" in result

    def test_datepart(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT DATEPART(year, hire_date) FROM t")
        assert "EXTRACT" in result

    def test_scope_identity(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT SCOPE_IDENTITY()")
        assert "lastval()" in result

    def test_at_identity(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT @@IDENTITY")
        assert "lastval()" in result

    def test_bracket_quoting(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT [Column Name] FROM [My Table]")
        assert '"Column Name"' in result or '"Column' in result

    def test_temp_table(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT * FROM #temp_orders")
        assert "temp_temp_orders" in result


@pytest.mark.unit
@pytest.mark.sql
class TestSqlServerDataTypes:
    """SQL Server data type conversions."""

    def test_nvarchar_max(self, sqlserver_translator):
        result = sqlserver_translator.translate("CREATE TABLE t (notes NVARCHAR(MAX))")
        assert "TEXT" in result

    def test_varchar_max(self, sqlserver_translator):
        result = sqlserver_translator.translate("CREATE TABLE t (notes VARCHAR(MAX))")
        assert "TEXT" in result

    def test_bit(self, sqlserver_translator):
        result = sqlserver_translator.translate("CREATE TABLE t (active BIT)")
        assert "BOOLEAN" in result

    def test_datetime(self, sqlserver_translator):
        result = sqlserver_translator.translate("CREATE TABLE t (created DATETIME)")
        assert "TIMESTAMP" in result

    def test_datetime2(self, sqlserver_translator):
        result = sqlserver_translator.translate("CREATE TABLE t (created DATETIME2)")
        assert "TIMESTAMP" in result

    def test_money(self, sqlserver_translator):
        result = sqlserver_translator.translate("CREATE TABLE t (price MONEY)")
        assert "NUMERIC(19,4)" in result

    def test_uniqueidentifier(self, sqlserver_translator):
        result = sqlserver_translator.translate("CREATE TABLE t (guid UNIQUEIDENTIFIER)")
        assert "UUID" in result

    def test_image(self, sqlserver_translator):
        result = sqlserver_translator.translate("CREATE TABLE t (pic IMAGE)")
        assert "BYTEA" in result

    def test_varbinary_max(self, sqlserver_translator):
        result = sqlserver_translator.translate("CREATE TABLE t (data VARBINARY(MAX))")
        assert "BYTEA" in result

    def test_tinyint(self, sqlserver_translator):
        result = sqlserver_translator.translate("CREATE TABLE t (val TINYINT)")
        assert "SMALLINT" in result

    def test_nolock_removal(self, sqlserver_translator):
        result = sqlserver_translator.translate("SELECT * FROM orders WITH(NOLOCK)")
        assert "NOLOCK" not in result


# ═══════════════════════════════════════════════════════════════════════
#  MySQL → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.sql
class TestMySqlBasic:
    """MySQL → PostgreSQL basic translations."""

    def test_ifnull(self, mysql_translator):
        result = mysql_translator.translate("SELECT IFNULL(name, 'N/A') FROM employees")
        assert "COALESCE" in result

    def test_now(self, mysql_translator):
        result = mysql_translator.translate("SELECT NOW()")
        assert "CURRENT_TIMESTAMP" in result

    def test_curdate(self, mysql_translator):
        result = mysql_translator.translate("SELECT CURDATE()")
        assert "CURRENT_DATE" in result

    def test_date_add(self, mysql_translator):
        result = mysql_translator.translate("SELECT DATE_ADD(hire_date, INTERVAL 6 MONTH) FROM t")
        assert "INTERVAL" in result

    def test_date_sub(self, mysql_translator):
        result = mysql_translator.translate("SELECT DATE_SUB(end_date, INTERVAL 30 DAY) FROM t")
        assert "INTERVAL" in result

    def test_group_concat(self, mysql_translator):
        result = mysql_translator.translate("SELECT GROUP_CONCAT(name) FROM employees")
        assert "STRING_AGG" in result

    def test_locate(self, mysql_translator):
        result = mysql_translator.translate("SELECT LOCATE('@', email) FROM t")
        assert "POSITION" in result

    def test_if_function(self, mysql_translator):
        result = mysql_translator.translate("SELECT IF(status = 'A', 'Active', 'Inactive') FROM t")
        assert "CASE WHEN" in result

    def test_limit_offset(self, mysql_translator):
        result = mysql_translator.translate("SELECT * FROM orders LIMIT 10, 20")
        assert "LIMIT 20" in result
        assert "OFFSET 10" in result

    def test_backtick_quoting(self, mysql_translator):
        result = mysql_translator.translate("SELECT `column name` FROM `my_table`")
        assert '"column name"' in result or '"column' in result


@pytest.mark.unit
@pytest.mark.sql
class TestMySqlDataTypes:
    """MySQL data type conversions."""

    def test_tinyint_1_boolean(self, mysql_translator):
        result = mysql_translator.translate("CREATE TABLE t (active TINYINT(1))")
        assert "BOOLEAN" in result

    def test_mediumint(self, mysql_translator):
        result = mysql_translator.translate("CREATE TABLE t (val MEDIUMINT)")
        assert "INTEGER" in result

    def test_mediumtext(self, mysql_translator):
        result = mysql_translator.translate("CREATE TABLE t (notes MEDIUMTEXT)")
        assert "TEXT" in result

    def test_longtext(self, mysql_translator):
        result = mysql_translator.translate("CREATE TABLE t (notes LONGTEXT)")
        assert "TEXT" in result

    def test_longblob(self, mysql_translator):
        result = mysql_translator.translate("CREATE TABLE t (data LONGBLOB)")
        assert "BYTEA" in result

    def test_double(self, mysql_translator):
        result = mysql_translator.translate("CREATE TABLE t (val DOUBLE)")
        assert "DOUBLE PRECISION" in result

    def test_year(self, mysql_translator):
        result = mysql_translator.translate("CREATE TABLE t (yr YEAR)")
        assert "INTEGER" in result

    def test_auto_increment(self, mysql_translator):
        result = mysql_translator.translate("CREATE TABLE t (id INT AUTO_INCREMENT)")
        assert "IDENTITY" in result or "SERIAL" in result

    def test_unsigned_removal(self, mysql_translator):
        result = mysql_translator.translate("CREATE TABLE t (val INT UNSIGNED)")
        assert "UNSIGNED" not in result

    def test_engine_removal(self, mysql_translator):
        result = mysql_translator.translate("CREATE TABLE t (id INT) ENGINE=InnoDB")
        assert "ENGINE" not in result


# ═══════════════════════════════════════════════════════════════════════
#  DB2 → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.sql
class TestDB2Basic:
    """IBM DB2 → PostgreSQL basic translations."""

    def test_fetch_first(self, db2_translator):
        result = db2_translator.translate("SELECT * FROM orders FETCH FIRST 10 ROWS ONLY")
        assert "LIMIT 10" in result

    def test_value_coalesce(self, db2_translator):
        result = db2_translator.translate("SELECT VALUE(name, 'N/A') FROM t")
        assert "COALESCE" in result

    def test_current_date(self, db2_translator):
        result = db2_translator.translate("SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1")
        assert "CURRENT_DATE" in result

    def test_current_timestamp(self, db2_translator):
        result = db2_translator.translate("SELECT CURRENT TIMESTAMP FROM t")
        assert "CURRENT_TIMESTAMP" in result

    def test_date_arithmetic_days(self, db2_translator):
        result = db2_translator.translate("SELECT hire_date + 30 DAYS FROM employees")
        assert "INTERVAL" in result
        assert "days" in result.lower()

    def test_next_value_for(self, db2_translator):
        result = db2_translator.translate("SELECT NEXT VALUE FOR my_seq FROM t")
        assert "nextval" in result

    def test_previous_value_for(self, db2_translator):
        result = db2_translator.translate("SELECT PREVIOUS VALUE FOR my_seq FROM t")
        assert "currval" in result

    def test_with_ur(self, db2_translator):
        result = db2_translator.translate("SELECT * FROM orders WITH UR")
        assert "WITH UR" not in result

    def test_for_read_only(self, db2_translator):
        result = db2_translator.translate("SELECT * FROM orders FOR READ ONLY")
        assert "FOR READ ONLY" not in result

    def test_posstr(self, db2_translator):
        result = db2_translator.translate("SELECT POSSTR(name, 'a') FROM t")
        assert "POSITION" in result


@pytest.mark.unit
@pytest.mark.sql
class TestDB2DataTypes:
    """DB2 data type conversions."""

    def test_graphic(self, db2_translator):
        result = db2_translator.translate("CREATE TABLE t (name GRAPHIC(100))")
        assert "VARCHAR" in result

    def test_vargraphic(self, db2_translator):
        result = db2_translator.translate("CREATE TABLE t (name VARGRAPHIC(200))")
        assert "VARCHAR" in result

    def test_dbclob(self, db2_translator):
        result = db2_translator.translate("CREATE TABLE t (notes DBCLOB)")
        assert "TEXT" in result

    def test_decfloat(self, db2_translator):
        result = db2_translator.translate("CREATE TABLE t (val DECFLOAT)")
        assert "NUMERIC" in result


# ═══════════════════════════════════════════════════════════════════════
#  Teradata → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.sql
class TestTeradataBasic:
    """Teradata → PostgreSQL basic translations."""

    def test_sel_shorthand(self, teradata_translator):
        result = teradata_translator.translate("SEL * FROM employees")
        assert result.upper().startswith("SELECT")

    def test_sample(self, teradata_translator):
        result = teradata_translator.translate("SELECT * FROM orders SAMPLE 100")
        assert "LIMIT 100" in result

    def test_named_alias(self, teradata_translator):
        result = teradata_translator.translate("SELECT col1 NAMED alias1 FROM t")
        assert "AS" in result
        assert "NAMED" not in result

    def test_volatile_table(self, teradata_translator):
        result = teradata_translator.translate("CREATE VOLATILE TABLE tmp (id INTEGER)")
        assert "TEMPORARY TABLE" in result
        assert "VOLATILE" not in result

    def test_collect_stats(self, teradata_translator):
        result = teradata_translator.translate("COLLECT STATISTICS ON my_table COLUMN col1")
        assert "ANALYZE" in result

    def test_minus_to_except(self, teradata_translator):
        result = teradata_translator.translate("SELECT * FROM a MINUS SELECT * FROM b")
        assert "EXCEPT" in result

    def test_ins_shorthand(self, teradata_translator):
        result = teradata_translator.translate("INS INTO employees VALUES (1, 'John')")
        assert "INSERT" in result


@pytest.mark.unit
@pytest.mark.sql
class TestTeradataDataTypes:
    """Teradata data type conversions."""

    def test_byteint(self, teradata_translator):
        result = teradata_translator.translate("CREATE TABLE t (flag BYTEINT)")
        assert "SMALLINT" in result

    def test_number(self, teradata_translator):
        result = teradata_translator.translate("CREATE TABLE t (val NUMBER)")
        assert "NUMERIC" in result


# ═══════════════════════════════════════════════════════════════════════
#  Snowflake → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.sql
class TestSnowflakeBasic:
    """Snowflake → PostgreSQL basic translations."""

    def test_variant(self, snowflake_translator):
        result = snowflake_translator.translate("CREATE TABLE t (data VARIANT)")
        assert "JSONB" in result

    def test_flatten(self, snowflake_translator):
        result = snowflake_translator.translate("SELECT * FROM TABLE(FLATTEN(INPUT => col1))")
        assert "jsonb_array_elements" in result

    def test_iff(self, snowflake_translator):
        result = snowflake_translator.translate("SELECT IFF(status = 'A', 'Active', 'Inactive') FROM t")
        assert "CASE WHEN" in result

    def test_try_cast(self, snowflake_translator):
        result = snowflake_translator.translate("SELECT TRY_CAST(val AS INTEGER) FROM t")
        assert "CAST" in result

    def test_nvl(self, snowflake_translator):
        result = snowflake_translator.translate("SELECT NVL(name, 'N/A') FROM t")
        assert "COALESCE" in result

    def test_dateadd(self, snowflake_translator):
        result = snowflake_translator.translate("SELECT DATEADD(day, 7, hire_date) FROM t")
        assert "INTERVAL" in result

    def test_to_varchar(self, snowflake_translator):
        result = snowflake_translator.translate("SELECT TO_VARCHAR(amount) FROM t")
        assert "TO_CHAR" in result


@pytest.mark.unit
@pytest.mark.sql
class TestSnowflakeDataTypes:
    """Snowflake data type conversions."""

    def test_timestamp_ntz(self, snowflake_translator):
        result = snowflake_translator.translate("CREATE TABLE t (created TIMESTAMP_NTZ)")
        assert "TIMESTAMP" in result
        assert "TIMESTAMP_NTZ" not in result

    def test_timestamp_ltz(self, snowflake_translator):
        result = snowflake_translator.translate("CREATE TABLE t (created TIMESTAMP_LTZ)")
        assert "TIME ZONE" in result

    def test_number_precision(self, snowflake_translator):
        result = snowflake_translator.translate("CREATE TABLE t (val NUMBER(10,2))")
        assert "NUMERIC(10,2)" in result

    def test_float(self, snowflake_translator):
        result = snowflake_translator.translate("CREATE TABLE t (val FLOAT)")
        assert "DOUBLE PRECISION" in result


# ═══════════════════════════════════════════════════════════════════════
#  Sybase → PostgreSQL
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.sql
class TestSybaseBasic:
    """Sybase ASE → PostgreSQL basic translations."""

    def test_getdate(self, sybase_translator):
        result = sybase_translator.translate("SELECT GETDATE()")
        assert "CURRENT_TIMESTAMP" in result

    def test_isnull(self, sybase_translator):
        result = sybase_translator.translate("SELECT ISNULL(name, 'N/A') FROM employees")
        assert "COALESCE" in result

    def test_convert(self, sybase_translator):
        result = sybase_translator.translate("SELECT CONVERT(VARCHAR, amount) FROM t")
        assert "::" in result

    def test_len(self, sybase_translator):
        result = sybase_translator.translate("SELECT LEN(description) FROM t")
        assert "LENGTH" in result

    def test_charindex(self, sybase_translator):
        result = sybase_translator.translate("SELECT CHARINDEX('@', email) FROM t")
        assert "POSITION" in result

    def test_dateadd(self, sybase_translator):
        result = sybase_translator.translate("SELECT DATEADD(month, 3, hire_date) FROM employees")
        assert "INTERVAL" in result

    def test_at_identity(self, sybase_translator):
        result = sybase_translator.translate("SELECT @@IDENTITY")
        assert "lastval()" in result


@pytest.mark.unit
@pytest.mark.sql
class TestSybaseDataTypes:
    """Sybase data type conversions."""

    def test_unitext(self, sybase_translator):
        result = sybase_translator.translate("CREATE TABLE t (notes UNITEXT)")
        assert "TEXT" in result

    def test_image(self, sybase_translator):
        result = sybase_translator.translate("CREATE TABLE t (pic IMAGE)")
        assert "BYTEA" in result

    def test_bit(self, sybase_translator):
        result = sybase_translator.translate("CREATE TABLE t (active BIT)")
        assert "BOOLEAN" in result

    def test_money(self, sybase_translator):
        result = sybase_translator.translate("CREATE TABLE t (price MONEY)")
        assert "NUMERIC(19,4)" in result

    def test_datetime(self, sybase_translator):
        result = sybase_translator.translate("CREATE TABLE t (created DATETIME)")
        assert "TIMESTAMP" in result

    def test_tinyint(self, sybase_translator):
        result = sybase_translator.translate("CREATE TABLE t (val TINYINT)")
        assert "SMALLINT" in result


# ═══════════════════════════════════════════════════════════════════════
#  Factory / Registry
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.sql
class TestDialectFactory:
    """Test the get_translator factory and dialect registry."""

    def test_get_oracle(self):
        from sql_translator import get_translator
        t = get_translator("oracle")
        assert t.dialect == "oracle"

    def test_get_sqlserver(self):
        from sql_translator import get_translator
        t = get_translator("sqlserver")
        assert t.dialect == "sqlserver"

    def test_get_mssql_alias(self):
        from sql_translator import get_translator
        t = get_translator("mssql")
        assert t.dialect == "sqlserver"

    def test_get_mysql(self):
        from sql_translator import get_translator
        t = get_translator("mysql")
        assert t.dialect == "mysql"

    def test_get_db2(self):
        from sql_translator import get_translator
        t = get_translator("db2")
        assert t.dialect == "db2"

    def test_get_teradata(self):
        from sql_translator import get_translator
        t = get_translator("teradata")
        assert t.dialect == "teradata"

    def test_get_snowflake(self):
        from sql_translator import get_translator
        t = get_translator("snowflake")
        assert t.dialect == "snowflake"

    def test_get_sybase(self):
        from sql_translator import get_translator
        t = get_translator("sybase")
        assert t.dialect == "sybase"

    def test_unsupported_dialect_raises(self):
        from sql_translator import get_translator
        with pytest.raises(ValueError, match="Unsupported dialect"):
            get_translator("nosqldb")

    def test_case_insensitive(self):
        from sql_translator import get_translator
        t = get_translator("ORACLE")
        assert t.dialect == "oracle"
