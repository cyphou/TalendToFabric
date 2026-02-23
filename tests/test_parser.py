"""
Unit tests for parser/parse_talend_jobs.py — TalendJobParser
Tests cover: simple copy, transforms, orchestration, complex ETL, file transfer, custom code.
"""

import pytest
from pathlib import Path


# ═══════════════════════════════════════════════════════════════════════════
# 1. SIMPLE COPY JOB
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestSimpleCopyJob:
    """Parse simple_copy_job.item — tOracleInput → tPostgresqlOutput."""

    def test_parse_returns_dict(self, parsed_simple_copy):
        assert parsed_simple_copy is not None
        assert isinstance(parsed_simple_copy, dict)

    def test_job_name(self, parsed_simple_copy):
        assert parsed_simple_copy["name"] == "simple_copy_job"

    def test_component_count(self, parsed_simple_copy):
        assert parsed_simple_copy["component_count"] == 2

    def test_component_types(self, parsed_simple_copy):
        types = parsed_simple_copy["component_types"]
        assert "tOracleInput" in types
        assert "tPostgresqlOutput" in types

    def test_input_components_detected(self, parsed_simple_copy):
        inputs = parsed_simple_copy["input_components"]
        assert len(inputs) >= 1
        assert any(c["component_type"] == "tOracleInput" for c in inputs)

    def test_output_components_detected(self, parsed_simple_copy):
        outputs = parsed_simple_copy["output_components"]
        assert len(outputs) >= 1
        assert any(c["component_type"] == "tPostgresqlOutput" for c in outputs)

    def test_no_custom_code(self, parsed_simple_copy):
        assert parsed_simple_copy["has_custom_code"] is False

    def test_connections_extracted(self, parsed_simple_copy):
        conns = parsed_simple_copy["connections"]
        assert len(conns) == 1
        assert conns[0]["source"] == "tOracleInput_1"
        assert conns[0]["target"] == "tPostgresqlOutput_1"

    def test_context_params_extracted(self, parsed_simple_copy):
        ctx = parsed_simple_copy["context_params"]
        assert len(ctx) == 2
        names = [p["name"] for p in ctx]
        assert "db_host" in names
        assert "db_port" in names

    def test_categories(self, parsed_simple_copy):
        cats = parsed_simple_copy["categorized"]
        assert "input" in cats
        assert "output" in cats

    def test_transformation_components_empty(self, parsed_simple_copy):
        assert len(parsed_simple_copy["transformation_components"]) == 0

    def test_parameters_extracted(self, parsed_simple_copy):
        oracle = next(c for c in parsed_simple_copy["components"] if c["component_type"] == "tOracleInput")
        assert oracle["parameters"]["HOST"] == "oracle-server"
        assert oracle["parameters"]["QUERY"] == "SELECT * FROM HR.EMPLOYEES"


# ═══════════════════════════════════════════════════════════════════════════
# 2. TRANSFORM JOB
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestTransformJob:
    """Parse transform_job.item — tOracleInput → tMap → tFilterRow → tPostgresqlOutput."""

    def test_component_count(self, parsed_transform):
        assert parsed_transform["component_count"] == 4

    def test_transformation_components(self, parsed_transform):
        transforms = parsed_transform["transformation_components"]
        types = [c["component_type"] for c in transforms]
        assert "tMap" in types
        assert "tFilterRow" in types

    def test_tmap_parameters(self, parsed_transform):
        tmap = next(c for c in parsed_transform["components"] if c["component_type"] == "tMap")
        assert "EXPRESSION_total" in tmap["parameters"]
        assert "MAP_full_name" in tmap["parameters"]

    def test_connections_chain(self, parsed_transform):
        conns = parsed_transform["connections"]
        assert len(conns) == 3
        sources = [c["source"] for c in conns]
        targets = [c["target"] for c in conns]
        assert "tOracleInput_1" in sources
        assert "tMap_1" in sources
        assert "tFilterRow_1" in sources
        assert "tPostgresqlOutput_1" in targets

    def test_no_custom_code(self, parsed_transform):
        assert parsed_transform["has_custom_code"] is False

    def test_no_subjobs(self, parsed_transform):
        assert len(parsed_transform["subjobs"]) == 0


# ═══════════════════════════════════════════════════════════════════════════
# 3. ORCHESTRATION JOB
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestOrchestrationJob:
    """Parse orchestration_job.item — tPreJob, tRunJob×2, tPostJob, error handling."""

    def test_component_count(self, parsed_orchestration):
        assert parsed_orchestration["component_count"] == 6

    def test_flow_control_components(self, parsed_orchestration):
        fc = parsed_orchestration["flow_control_components"]
        types = [c["component_type"] for c in fc]
        assert "tPreJob" in types
        assert "tRunJob" in types
        assert "tPostJob" in types

    def test_error_handling_components(self, parsed_orchestration):
        eh = parsed_orchestration["error_handling_components"]
        types = [c["component_type"] for c in eh]
        assert "tLogCatcher" in types

    def test_utility_components(self, parsed_orchestration):
        utils = parsed_orchestration["utility_components"]
        types = [c["component_type"] for c in utils]
        assert "tSendMail" in types

    def test_subjobs(self, parsed_orchestration):
        subjobs = parsed_orchestration["subjobs"]
        assert len(subjobs) == 2
        names = [s["name"] for s in subjobs]
        assert "main_flow" in names
        assert "error_handling" in names

    def test_connections(self, parsed_orchestration):
        conns = parsed_orchestration["connections"]
        assert len(conns) >= 4

    def test_no_custom_code(self, parsed_orchestration):
        assert parsed_orchestration["has_custom_code"] is False


# ═══════════════════════════════════════════════════════════════════════════
# 4. COMPLEX ETL JOB
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestComplexETLJob:
    """Parse complex_etl_job.item — multi-source, tMap, aggregate, custom code, errors."""

    def test_component_count(self, parsed_complex_etl):
        assert parsed_complex_etl["component_count"] >= 12

    def test_multiple_input_sources(self, parsed_complex_etl):
        inputs = parsed_complex_etl["input_components"]
        types = [c["component_type"] for c in inputs]
        assert "tOracleInput" in types
        assert "tFileInputDelimited" in types
        assert "tFileInputJSON" in types

    def test_transformation_chain(self, parsed_complex_etl):
        transforms = parsed_complex_etl["transformation_components"]
        types = [c["component_type"] for c in transforms]
        assert "tMap" in types
        assert "tAggregateRow" in types
        assert "tSortRow" in types
        assert "tUniqRow" in types

    def test_custom_code_detected(self, parsed_complex_etl):
        assert parsed_complex_etl["has_custom_code"] is True
        cc = parsed_complex_etl["custom_code_components"]
        assert any(c["component_type"] == "tJavaRow" for c in cc)

    def test_error_handling(self, parsed_complex_etl):
        eh = parsed_complex_etl["error_handling_components"]
        types = [c["component_type"] for c in eh]
        assert "tLogCatcher" in types
        assert "tCatch" in types

    def test_db_operations(self, parsed_complex_etl):
        db_ops = parsed_complex_etl["db_operation_components"]
        types = [c["component_type"] for c in db_ops]
        assert "tOracleConnection" in types

    def test_multiple_outputs(self, parsed_complex_etl):
        outputs = parsed_complex_etl["output_components"]
        types = [c["component_type"] for c in outputs]
        assert "tPostgresqlOutput" in types
        assert "tFileOutputDelimited" in types

    def test_context_params(self, parsed_complex_etl):
        ctx = parsed_complex_etl["context_params"]
        names = [p["name"] for p in ctx]
        assert "env" in names
        assert "batch_date" in names
        assert "oracle_host" in names
        assert "pg_host" in names

    def test_subjobs(self, parsed_complex_etl):
        subjobs = parsed_complex_etl["subjobs"]
        assert len(subjobs) == 4

    def test_categorized_coverage(self, parsed_complex_etl):
        cats = parsed_complex_etl["categorized"]
        assert "input" in cats
        assert "output" in cats
        assert "transformation" in cats
        assert "custom_code" in cats
        assert "error_handling" in cats
        assert "db_operation" in cats

    def test_connections_count(self, parsed_complex_etl):
        conns = parsed_complex_etl["connections"]
        assert len(conns) >= 9


# ═══════════════════════════════════════════════════════════════════════════
# 5. FILE TRANSFER JOB
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestFileTransferJob:
    """Parse file_transfer_job.item — SFTP get, process, archive, delete."""

    def test_component_types(self, parsed_file_transfer):
        types = parsed_file_transfer["component_types"]
        assert "tSFTPGet" in types
        assert "tFileInputDelimited" in types
        assert "tMap" in types
        assert "tPostgresqlOutput" in types
        assert "tFileArchive" in types
        assert "tFileDelete" in types

    def test_file_utility_components(self, parsed_file_transfer):
        fu = parsed_file_transfer["file_utility_components"]
        types = [c["component_type"] for c in fu]
        assert "tFileArchive" in types
        assert "tFileDelete" in types
        # tSFTPGet is classified as file_utility
        assert "tSFTPGet" in types

    def test_connections(self, parsed_file_transfer):
        conns = parsed_file_transfer["connections"]
        assert len(conns) == 5


# ═══════════════════════════════════════════════════════════════════════════
# 6. CUSTOM CODE JOB
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestCustomCodeJob:
    """Parse custom_code_job.item — tJava, tJavaRow, tJavaFlex."""

    def test_custom_code_flag(self, parsed_custom_code):
        assert parsed_custom_code["has_custom_code"] is True

    def test_custom_code_components(self, parsed_custom_code):
        cc = parsed_custom_code["custom_code_components"]
        types = [c["component_type"] for c in cc]
        assert "tJava" in types
        assert "tJavaRow" in types
        assert "tJavaFlex" in types

    def test_code_parameters(self, parsed_custom_code):
        java_row = next(
            c for c in parsed_custom_code["components"] if c["component_type"] == "tJavaRow"
        )
        assert "CODE" in java_row["parameters"]
        assert "toUpperCase" in java_row["parameters"]["CODE"]

    def test_transformation_fixed_flow(self, parsed_custom_code):
        transforms = parsed_custom_code["transformation_components"]
        types = [c["component_type"] for c in transforms]
        assert "tFixedFlowInput" in types


# ═══════════════════════════════════════════════════════════════════════════
# 7. BATCH PARSING (parse_all)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestParseAll:
    """Test parse_all() over the fixtures directory."""

    def test_all_fixtures_parsed(self, parsed_jobs):
        assert len(parsed_jobs) == 6  # 6 fixture files

    def test_all_jobs_have_required_keys(self, parsed_jobs):
        required_keys = {
            "name", "folder", "file_path", "components", "component_types",
            "component_count", "connections", "context_params", "subjobs",
            "has_custom_code", "input_components", "output_components",
            "transformation_components", "categorized",
        }
        for job in parsed_jobs:
            assert required_keys.issubset(job.keys()), f"Missing keys in {job['name']}"

    def test_no_empty_component_types(self, parsed_jobs):
        for job in parsed_jobs:
            assert job["component_count"] > 0, f"Job {job['name']} has 0 components"


# ═══════════════════════════════════════════════════════════════════════════
# 8. METADATA EXTRACTOR
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestMetadataExtractor:
    """Test parser/extract_metadata.py — MetadataExtractor."""

    @pytest.fixture
    def extractor(self, parsed_jobs):
        from extract_metadata import MetadataExtractor
        return MetadataExtractor(parsed_jobs)

    def test_extract_connections(self, extractor):
        conns = extractor.extract_connections()
        assert isinstance(conns, list)
        assert len(conns) > 0
        # Should find Oracle and PostgreSQL connections
        comp_types = [c["component_type"] for c in conns]
        assert any("Oracle" in ct for ct in comp_types)
        assert any("Postgresql" in ct for ct in comp_types)

    def test_extract_transformations(self, extractor):
        transforms = extractor.extract_transformations()
        assert isinstance(transforms, list)
        assert len(transforms) > 0
        types = [t["component_type"] for t in transforms]
        assert "tMap" in types

    def test_extract_custom_code(self, extractor):
        code = extractor.extract_custom_code()
        assert isinstance(code, list)
        assert len(code) > 0
        assert any(c["component_type"] == "tJavaRow" for c in code)

    def test_extract_sql_statements(self, extractor):
        sqls = extractor.extract_sql_statements()
        assert isinstance(sqls, list)
        # simple_copy has a QUERY param
        assert len(sqls) > 0


# ═══════════════════════════════════════════════════════════════════════════
# 9. INVENTORY GENERATOR
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestInventoryGenerator:
    """Test parser/generate_inventory.py — scoring and classification."""

    def test_generate_inventory(self, parsed_jobs):
        import pandas as pd
        from generate_inventory import generate_inventory
        df = generate_inventory(parsed_jobs)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(parsed_jobs)

    def test_required_columns(self, parsed_jobs):
        from generate_inventory import generate_inventory
        df = generate_inventory(parsed_jobs)
        required = {
            "job_name", "complexity", "complexity_score", "pattern",
            "target_fabric", "component_count", "has_custom_code",
        }
        assert required.issubset(set(df.columns))

    def test_complexity_levels(self, parsed_jobs):
        from generate_inventory import generate_inventory
        df = generate_inventory(parsed_jobs)
        assert set(df["complexity"].unique()).issubset({"Simple", "Medium", "Complex"})

    def test_pattern_detection(self, parsed_jobs):
        from generate_inventory import generate_inventory
        df = generate_inventory(parsed_jobs)
        patterns = set(df["pattern"].unique())
        # Orchestration job should be detected
        orch = df[df["job_name"] == "orchestration_job"]
        if len(orch) > 0:
            assert orch.iloc[0]["pattern"] == "Orchestration"

    def test_custom_code_jobs_target_spark(self, parsed_jobs):
        from generate_inventory import generate_inventory
        df = generate_inventory(parsed_jobs)
        custom_jobs = df[df["has_custom_code"] == True]
        for _, row in custom_jobs.iterrows():
            assert row["target_fabric"] == "Spark", f"Custom code job {row['job_name']} should target Spark"

    def test_score_ranges(self, parsed_jobs):
        from generate_inventory import generate_inventory
        df = generate_inventory(parsed_jobs)
        assert (df["complexity_score"] >= 1.0).all()
        assert (df["complexity_score"] <= 5.0).all()

    def test_scoring_functions(self):
        from generate_inventory import (
            score_component_count,
            score_transformation_complexity,
            score_custom_code,
        )
        # Component count scoring
        assert score_component_count(2) == 1.0
        assert score_component_count(5) == 2.0
        assert score_component_count(10) == 4.0
        assert score_component_count(20) == 5.0

        # Custom code scoring
        assert score_custom_code(True) == 5.0
        assert score_custom_code(False) == 1.0

        # Transformation scoring
        assert score_transformation_complexity([]) == 1.0
        assert score_transformation_complexity([{"component_type": "tMap"}]) >= 1.0
