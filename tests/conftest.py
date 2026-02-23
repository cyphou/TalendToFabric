"""
Shared pytest fixtures for Talend-to-Fabric migration tests.
"""

import os
import sys
import json
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, List

import pytest
import pandas as pd

# ── Ensure project root importable ──────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "parser"))
sys.path.insert(0, str(PROJECT_ROOT / "translator"))
sys.path.insert(0, str(PROJECT_ROOT / "validation"))

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


# ── Temporary directory ─────────────────────────────────────────────────────

@pytest.fixture
def tmp_output(tmp_path):
    """Provide a temporary output directory."""
    return tmp_path


# ── Fixture files ───────────────────────────────────────────────────────────

@pytest.fixture
def fixtures_dir():
    """Return path to the test fixtures directory."""
    return FIXTURES_DIR


@pytest.fixture
def simple_copy_item():
    return FIXTURES_DIR / "simple_copy_job.item"


@pytest.fixture
def transform_item():
    return FIXTURES_DIR / "transform_job.item"


@pytest.fixture
def orchestration_item():
    return FIXTURES_DIR / "orchestration_job.item"


@pytest.fixture
def complex_etl_item():
    return FIXTURES_DIR / "complex_etl_job.item"


@pytest.fixture
def file_transfer_item():
    return FIXTURES_DIR / "file_transfer_job.item"


@pytest.fixture
def custom_code_item():
    return FIXTURES_DIR / "custom_code_job.item"


@pytest.fixture
def cdc_hash_item():
    return FIXTURES_DIR / "cdc_hash_job.item"


# ── Parser instances ────────────────────────────────────────────────────────

@pytest.fixture
def parser():
    """Return a TalendJobParser pointed at the fixtures directory."""
    from parse_talend_jobs import TalendJobParser
    return TalendJobParser(str(FIXTURES_DIR))


@pytest.fixture
def parsed_jobs(parser):
    """Parse all fixture .item files and return the list of job dicts."""
    return parser.parse_all()


# ── Parsed single-job helpers ───────────────────────────────────────────────

@pytest.fixture
def parsed_simple_copy(parser, simple_copy_item):
    return parser.parse_item_file(simple_copy_item)


@pytest.fixture
def parsed_transform(parser, transform_item):
    return parser.parse_item_file(transform_item)


@pytest.fixture
def parsed_orchestration(parser, orchestration_item):
    return parser.parse_item_file(orchestration_item)


@pytest.fixture
def parsed_complex_etl(parser, complex_etl_item):
    return parser.parse_item_file(complex_etl_item)


@pytest.fixture
def parsed_file_transfer(parser, file_transfer_item):
    return parser.parse_item_file(file_transfer_item)


@pytest.fixture
def parsed_custom_code(parser, custom_code_item):
    return parser.parse_item_file(custom_code_item)


@pytest.fixture
def parsed_cdc_hash(parser, cdc_hash_item):
    return parser.parse_item_file(cdc_hash_item)


# ── Inventory row helpers ───────────────────────────────────────────────────

def _make_inventory_row(
    job_name: str,
    pattern: str = "Copy",
    complexity: str = "Simple",
    complexity_score: float = 1.5,
    components_used: str = "tOracleInput, tPostgresqlOutput",
    component_types: str = "tOracleInput, tPostgresqlOutput",
    has_custom_code: bool = False,
    target_fabric: str = "DataFactory",
    **kwargs,
) -> Dict[str, Any]:
    return {
        "job_name": job_name,
        "job_folder": "test/",
        "description": f"Test job {job_name}",
        "complexity": complexity,
        "complexity_score": complexity_score,
        "pattern": pattern,
        "source_type": "tOracleInput",
        "target_type": "tPostgresqlOutput",
        "components_used": components_used,
        "component_types": component_types,
        "component_count": len(components_used.split(",")),
        "has_custom_code": has_custom_code,
        "estimated_effort_hours": 2,
        "target_fabric": target_fabric,
        "priority": "P2",
        "status": "Not Started",
        "notes": "",
        **kwargs,
    }


@pytest.fixture
def inv_row_simple_copy() -> Dict[str, Any]:
    return _make_inventory_row("simple_copy", pattern="Copy", target_fabric="DataFactory")


@pytest.fixture
def inv_row_transform() -> Dict[str, Any]:
    return _make_inventory_row(
        "transform_etl",
        pattern="Transform",
        complexity="Medium",
        complexity_score=3.0,
        components_used="tOracleInput, tMap, tFilterRow, tPostgresqlOutput",
        target_fabric="DataFactory",
    )


@pytest.fixture
def inv_row_orchestration() -> Dict[str, Any]:
    return _make_inventory_row(
        "orchestrate_master",
        pattern="Orchestration",
        complexity="Medium",
        components_used="tPreJob, tRunJob, tRunJob, tPostJob, tLogCatcher, tSendMail",
        target_fabric="DataFactory",
    )


@pytest.fixture
def inv_row_complex_spark() -> Dict[str, Any]:
    return _make_inventory_row(
        "complex_spark_etl",
        pattern="Complex Transform",
        complexity="Complex",
        complexity_score=4.2,
        components_used="tOracleInput, tFileInputDelimited, tMap, tAggregateRow, tSortRow, tJavaRow, tPostgresqlOutput",
        has_custom_code=True,
        target_fabric="Spark",
    )


@pytest.fixture
def inv_row_file_transfer() -> Dict[str, Any]:
    return _make_inventory_row(
        "sftp_ingest",
        pattern="Copy",
        components_used="tSFTPGet, tFileInputDelimited, tMap, tPostgresqlOutput, tFileArchive, tFileDelete",
        target_fabric="DataFactory",
    )


# ── Translator instances ────────────────────────────────────────────────────

@pytest.fixture
def adf_translator():
    from translate_to_adf import ADFTranslator
    return ADFTranslator()


@pytest.fixture
def spark_translator():
    from translate_to_spark import SparkTranslator
    return SparkTranslator()


# ── SQL Translator instance ─────────────────────────────────────────────────

@pytest.fixture
def sql_translator():
    from sql_translator import OracleToPostgreSQLTranslator
    return OracleToPostgreSQLTranslator()


@pytest.fixture
def sqlserver_translator():
    from sql_translator import SqlServerToPostgreSQLTranslator
    return SqlServerToPostgreSQLTranslator()


@pytest.fixture
def mysql_translator():
    from sql_translator import MySqlToPostgreSQLTranslator
    return MySqlToPostgreSQLTranslator()


@pytest.fixture
def db2_translator():
    from sql_translator import DB2ToPostgreSQLTranslator
    return DB2ToPostgreSQLTranslator()


@pytest.fixture
def teradata_translator():
    from sql_translator import TeradataToPostgreSQLTranslator
    return TeradataToPostgreSQLTranslator()


@pytest.fixture
def snowflake_translator():
    from sql_translator import SnowflakeToPostgreSQLTranslator
    return SnowflakeToPostgreSQLTranslator()


@pytest.fixture
def sybase_translator():
    from sql_translator import SybaseToPostgreSQLTranslator
    return SybaseToPostgreSQLTranslator()


# ── Validation helpers ──────────────────────────────────────────────────────

@pytest.fixture
def sample_source_df() -> pd.DataFrame:
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "amount": [100.0, 200.5, 300.0, 150.75, 500.0],
        "status": ["ACTIVE", "ACTIVE", "INACTIVE", "ACTIVE", "INACTIVE"],
    })


@pytest.fixture
def sample_target_df() -> pd.DataFrame:
    """Target DF matching source exactly."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "amount": [100.0, 200.5, 300.0, 150.75, 500.0],
        "status": ["ACTIVE", "ACTIVE", "INACTIVE", "ACTIVE", "INACTIVE"],
    })


@pytest.fixture
def sample_target_df_with_diff() -> pd.DataFrame:
    """Target DF with diffs: row 3 changed, row 5 missing, row 6 extra."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 6],
        "name": ["Alice", "Bob", "CHANGED", "Diana", "Frank"],
        "amount": [100.0, 200.5, 999.99, 150.75, 600.0],
        "status": ["ACTIVE", "ACTIVE", "INACTIVE", "ACTIVE", "ACTIVE"],
    })


@pytest.fixture
def migration_config() -> Dict[str, Any]:
    """Minimal migration config for validation tests."""
    return {
        "project": {"name": "Test Migration"},
        "validation": {
            "row_count_threshold_pct": 0,
            "data_diff_sample_size": 10000,
        },
    }
