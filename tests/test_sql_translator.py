"""
Unit tests for translator/sql_translator.py — OracleToPostgreSQLTranslator
Covers: date functions, string functions, data types, sequences, ROWNUM, DECODE, NVL, LISTAGG.
"""

import os
import pytest


@pytest.mark.unit
@pytest.mark.sql
class TestSQLTranslatorBasic:
    """Basic Oracle → PostgreSQL translations."""

    def test_sysdate(self, sql_translator):
        result = sql_translator.translate("SELECT SYSDATE FROM DUAL")
        assert "CURRENT_TIMESTAMP" in result
        assert "SYSDATE" not in result
        assert "DUAL" not in result

    def test_systimestamp(self, sql_translator):
        result = sql_translator.translate("SELECT SYSTIMESTAMP FROM my_table")
        assert "CURRENT_TIMESTAMP" in result
        assert "SYSTIMESTAMP" not in result

    def test_getdate(self, sql_translator):
        result = sql_translator.translate("SELECT GETDATE() FROM DUAL")
        assert "CURRENT_TIMESTAMP" in result

    def test_nvl(self, sql_translator):
        result = sql_translator.translate("SELECT NVL(name, 'N/A') FROM employees")
        assert "COALESCE" in result
        assert "NVL" not in result.split("COALESCE")[0]  # no NVL before COALESCE

    def test_nvl2(self, sql_translator):
        result = sql_translator.translate("SELECT NVL2(bonus, bonus * 1.1, 0) FROM employees")
        assert "CASE WHEN" in result
        assert "IS NOT NULL" in result

    def test_decode_simple(self, sql_translator):
        result = sql_translator.translate("SELECT DECODE(status, 'A', 'Active', 'Unknown') FROM t")
        assert "CASE WHEN" in result

    def test_substr(self, sql_translator):
        result = sql_translator.translate("SELECT SUBSTR(name, 1, 5) FROM employees")
        assert "SUBSTRING" in result

    def test_to_number(self, sql_translator):
        result = sql_translator.translate("SELECT TO_NUMBER(salary) FROM employees")
        assert "CAST" in result
        assert "NUMERIC" in result


@pytest.mark.unit
@pytest.mark.sql
class TestSQLTranslatorDataTypes:
    """Oracle data type → PostgreSQL data type conversions."""

    def test_varchar2(self, sql_translator):
        result = sql_translator.translate("CREATE TABLE t (name VARCHAR2(100))")
        assert "VARCHAR(" in result
        assert "VARCHAR2" not in result

    def test_number_precision(self, sql_translator):
        result = sql_translator.translate("CREATE TABLE t (amount NUMBER(10,2))")
        assert "NUMERIC(10,2)" in result

    def test_number_no_precision(self, sql_translator):
        result = sql_translator.translate("CREATE TABLE t (val NUMBER)")
        assert "NUMERIC" in result

    def test_clob(self, sql_translator):
        result = sql_translator.translate("CREATE TABLE t (notes CLOB)")
        assert "TEXT" in result

    def test_blob(self, sql_translator):
        result = sql_translator.translate("CREATE TABLE t (data BLOB)")
        assert "BYTEA" in result


@pytest.mark.unit
@pytest.mark.sql
class TestSQLTranslatorSequences:
    """Sequence translation rules."""

    def test_nextval(self, sql_translator):
        result = sql_translator.translate("SELECT my_seq.NEXTVAL FROM DUAL")
        assert "nextval('my_seq')" in result

    def test_currval(self, sql_translator):
        result = sql_translator.translate("SELECT my_seq.CURRVAL FROM DUAL")
        assert "currval('my_seq')" in result


@pytest.mark.unit
@pytest.mark.sql
class TestSQLTranslatorRownum:
    """ROWNUM → LIMIT / ROW_NUMBER translations."""

    def test_where_rownum(self, sql_translator):
        result = sql_translator.translate("SELECT * FROM t WHERE ROWNUM <= 10")
        assert "LIMIT 10" in result

    def test_and_rownum(self, sql_translator):
        result = sql_translator.translate("SELECT * FROM t WHERE status = 'A' AND ROWNUM <= 5")
        assert "LIMIT 5" in result


@pytest.mark.unit
@pytest.mark.sql
class TestSQLTranslatorDateArithmetic:
    """Date arithmetic conversions."""

    def test_add_months(self, sql_translator):
        result = sql_translator.translate("SELECT ADD_MONTHS(hire_date, 6) FROM employees")
        assert "INTERVAL" in result or "interval" in result
        assert "months" in result.lower()

    def test_months_between(self, sql_translator):
        result = sql_translator.translate("SELECT MONTHS_BETWEEN(end_date, start_date) FROM t")
        assert "EXTRACT" in result or "EPOCH" in result


@pytest.mark.unit
@pytest.mark.sql
class TestSQLTranslatorAggregates:
    """Aggregate function conversions."""

    def test_listagg(self, sql_translator):
        result = sql_translator.translate(
            "SELECT LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) FROM employees"
        )
        assert "STRING_AGG" in result


@pytest.mark.unit
@pytest.mark.sql
class TestSQLTranslatorComplex:
    """Complex multi-rule SQL translation scenarios."""

    def test_multiple_rules_in_one_query(self, sql_translator):
        oracle_sql = """
        SELECT NVL(e.name, 'Unknown'),
               SUBSTR(e.email, 1, 10),
               SYSDATE
        FROM employees e
        WHERE ROWNUM <= 100
        """
        result = sql_translator.translate(oracle_sql)
        assert "COALESCE" in result
        assert "SUBSTRING" in result
        assert "CURRENT_TIMESTAMP" in result
        assert "LIMIT 100" in result

    def test_create_table_with_oracle_types(self, sql_translator):
        oracle_ddl = """
        CREATE TABLE orders (
            id NUMBER(10),
            amount NUMBER(12,2),
            description VARCHAR2(500),
            notes CLOB,
            attachment BLOB
        )
        """
        result = sql_translator.translate(oracle_ddl)
        assert "NUMERIC" in result
        assert "VARCHAR(" in result
        assert "TEXT" in result
        assert "BYTEA" in result

    def test_idempotent_translation(self, sql_translator):
        """Translating already-PostgreSQL syntax should not corrupt it."""
        pg_sql = "SELECT CURRENT_TIMESTAMP, COALESCE(name, 'N/A') FROM employees LIMIT 10"
        result = sql_translator.translate(pg_sql)
        # Should remain largely the same
        assert "CURRENT_TIMESTAMP" in result
        assert "COALESCE" in result

    def test_empty_input(self, sql_translator):
        result = sql_translator.translate("")
        assert result == ""

    def test_no_changes_needed(self, sql_translator):
        pure_pg = "SELECT id, name FROM employees WHERE status = 'ACTIVE'"
        result = sql_translator.translate(pure_pg)
        assert "id" in result
        assert "name" in result


@pytest.mark.unit
@pytest.mark.sql
class TestSQLTranslatorFile:
    """Test file-based translation."""

    def test_translate_file(self, sql_translator, tmp_output):
        input_file = tmp_output / "oracle.sql"
        output_file = tmp_output / "postgres.sql"

        input_file.write_text(
            "SELECT SYSDATE FROM DUAL;\n"
            "SELECT NVL(status, 'UNKNOWN') FROM orders;\n"
            "SELECT my_seq.NEXTVAL FROM DUAL;\n"
        )

        sql_translator.translate_file(str(input_file), str(output_file))

        assert output_file.exists()
        content = output_file.read_text()
        assert "CURRENT_TIMESTAMP" in content
        assert "COALESCE" in content
        assert "nextval" in content
        assert "Auto-translated" in content
