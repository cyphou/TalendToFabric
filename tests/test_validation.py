"""
Unit tests for validation modules:
  - validation/row_count_validation.py
  - validation/data_diff_validation.py
  - validation/schema_validation.py
"""

import pytest
import pandas as pd


# ═══════════════════════════════════════════════════════════════════════════
# 1. ROW COUNT VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.validation
class TestRowCountValidation:
    """Test RowCountValidator."""

    @pytest.fixture
    def validator(self, migration_config):
        from row_count_validation import RowCountValidator
        return RowCountValidator(migration_config)

    def test_identical_counts_pass(self, validator):
        tc = {
            "entity": "customers",
            "source_row_count": 1000,
            "target_row_count": 1000,
            "threshold_pct": 0,
        }
        result = validator.validate_single(tc)
        assert result["passed"] is True
        assert result["difference"] == 0
        assert result["difference_pct"] == 0.0

    def test_within_threshold_pass(self, validator):
        tc = {
            "entity": "orders",
            "source_row_count": 1000,
            "target_row_count": 1005,
            "threshold_pct": 1.0,
        }
        result = validator.validate_single(tc)
        assert result["passed"] is True

    def test_exceeds_threshold_fail(self, validator):
        tc = {
            "entity": "products",
            "source_row_count": 1000,
            "target_row_count": 900,
            "threshold_pct": 1.0,
        }
        result = validator.validate_single(tc)
        assert result["passed"] is False
        assert result["difference"] == -100
        assert abs(result["difference_pct"]) > 1.0

    def test_zero_source_count(self, validator):
        tc = {
            "entity": "empty_table",
            "source_row_count": 0,
            "target_row_count": 0,
            "threshold_pct": 0,
        }
        result = validator.validate_single(tc)
        assert result["passed"] is True

    def test_validate_all(self, validator):
        cases = [
            {"entity": "a", "source_row_count": 100, "target_row_count": 100, "threshold_pct": 0},
            {"entity": "b", "source_row_count": 200, "target_row_count": 210, "threshold_pct": 10},
            {"entity": "c", "source_row_count": 300, "target_row_count": 100, "threshold_pct": 0},
        ]
        results = validator.validate_all(cases)
        assert len(results) == 3
        assert results[0]["passed"] is True
        assert results[1]["passed"] is True
        assert results[2]["passed"] is False

    def test_result_has_timestamp(self, validator):
        tc = {"entity": "t", "source_row_count": 10, "target_row_count": 10, "threshold_pct": 0}
        result = validator.validate_single(tc)
        assert "timestamp" in result

    def test_save_results(self, validator, tmp_output):
        tc = {"entity": "t", "source_row_count": 10, "target_row_count": 10, "threshold_pct": 0}
        validator.validate_all([tc])
        out_file = str(tmp_output / "results.json")
        validator.save_results(out_file)
        import json
        with open(out_file) as f:
            data = json.load(f)
        assert len(data) == 1


# ═══════════════════════════════════════════════════════════════════════════
# 2. DATA DIFF VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.validation
class TestDataDiffValidation:
    """Test DataDiffValidator."""

    @pytest.fixture
    def validator(self, migration_config):
        from data_diff_validation import DataDiffValidator
        return DataDiffValidator(migration_config)

    def test_identical_data_passes(self, validator, sample_source_df, sample_target_df):
        result = validator.validate_dataframe_pair(
            entity="customers",
            source_df=sample_source_df,
            target_df=sample_target_df,
            key_columns=["id"],
        )
        assert result["passed"] is True
        assert result["matched"] == 5
        assert result["mismatched"] == 0
        assert result["missing_in_target"] == 0
        assert result["extra_in_target"] == 0
        assert result["match_rate_pct"] == 100.0

    def test_diff_detected(self, validator, sample_source_df, sample_target_df_with_diff):
        result = validator.validate_dataframe_pair(
            entity="customers",
            source_df=sample_source_df,
            target_df=sample_target_df_with_diff,
            key_columns=["id"],
        )
        assert result["passed"] is False
        assert result["mismatched"] >= 1     # row 3 changed
        assert result["missing_in_target"] >= 1  # row 5 missing
        assert result["extra_in_target"] >= 1    # row 6 extra

    def test_compare_specific_columns(self, validator, sample_source_df, sample_target_df):
        result = validator.validate_dataframe_pair(
            entity="customers",
            source_df=sample_source_df,
            target_df=sample_target_df,
            key_columns=["id"],
            compare_columns=["name", "status"],
        )
        assert result["passed"] is True

    def test_sample_size(self, validator):
        """Test with sampling — create larger dataset."""
        import numpy as np
        np.random.seed(42)
        n = 500
        source = pd.DataFrame({
            "id": range(n),
            "value": np.random.rand(n),
        })
        target = source.copy()

        result = validator.validate_dataframe_pair(
            entity="large_table",
            source_df=source,
            target_df=target,
            key_columns=["id"],
            sample_size=100,
        )
        assert result["passed"] is True
        assert result["source_rows"] <= 100

    def test_empty_dataframes(self, validator):
        empty = pd.DataFrame({"id": [], "val": []})
        result = validator.validate_dataframe_pair(
            entity="empty", source_df=empty, target_df=empty, key_columns=["id"]
        )
        assert result["passed"] is True
        assert result["matched"] == 0

    def test_result_structure(self, validator, sample_source_df, sample_target_df):
        result = validator.validate_dataframe_pair(
            entity="test", source_df=sample_source_df, target_df=sample_target_df, key_columns=["id"]
        )
        required_keys = {
            "entity", "source_rows", "target_rows", "matched", "mismatched",
            "missing_in_target", "extra_in_target", "match_rate_pct", "passed", "timestamp",
        }
        assert required_keys.issubset(set(result.keys()))


# ═══════════════════════════════════════════════════════════════════════════
# 3. SCHEMA VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.validation
class TestSchemaValidation:
    """Test SchemaValidator."""

    @pytest.fixture
    def validator(self, migration_config):
        from schema_validation import SchemaValidator
        return SchemaValidator(migration_config)

    def test_matching_schemas_pass(self, validator):
        expected = [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "VARCHAR", "nullable": True},
        ]
        actual = [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "VARCHAR", "nullable": True},
        ]
        result = validator.validate_schema("customers", expected, actual)
        assert result["passed"] is True
        assert len(result["missing_columns"]) == 0
        assert len(result["type_mismatches"]) == 0

    def test_missing_column_fails(self, validator):
        expected = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "email", "type": "VARCHAR"},
        ]
        actual = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
        ]
        result = validator.validate_schema("customers", expected, actual)
        assert result["passed"] is False
        assert "email" in result["missing_columns"]

    def test_extra_column_does_not_fail(self, validator):
        expected = [
            {"name": "id", "type": "INTEGER"},
        ]
        actual = [
            {"name": "id", "type": "INTEGER"},
            {"name": "extra_col", "type": "VARCHAR"},
        ]
        result = validator.validate_schema("t", expected, actual)
        assert result["passed"] is True
        assert "extra_col" in result["extra_columns"]

    def test_type_mismatch_fails(self, validator):
        expected = [{"name": "amount", "type": "NUMERIC"}]
        actual = [{"name": "amount", "type": "VARCHAR"}]
        result = validator.validate_schema("t", expected, actual)
        assert result["passed"] is False
        assert len(result["type_mismatches"]) == 1
        assert result["type_mismatches"][0]["column"] == "amount"

    def test_case_insensitive_column_names(self, validator):
        expected = [{"name": "ID", "type": "INTEGER"}]
        actual = [{"name": "id", "type": "INTEGER"}]
        result = validator.validate_schema("t", expected, actual)
        assert result["passed"] is True

    def test_multiple_issues(self, validator):
        expected = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "age", "type": "INTEGER"},
        ]
        actual = [
            {"name": "id", "type": "BIGINT"},
            {"name": "name", "type": "VARCHAR"},
            # age missing
        ]
        result = validator.validate_schema("t", expected, actual)
        assert result["passed"] is False
        assert "age" in result["missing_columns"]
        assert len(result["type_mismatches"]) == 1

    def test_save_results(self, validator, tmp_output):
        expected = [{"name": "id", "type": "INTEGER"}]
        actual = [{"name": "id", "type": "INTEGER"}]
        validator.validate_schema("t", expected, actual)
        out_file = str(tmp_output / "schema_results.json")
        validator.save_results(out_file)
        import json
        with open(out_file) as f:
            data = json.load(f)
        assert len(data) == 1
