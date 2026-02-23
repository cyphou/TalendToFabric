"""
End-to-end regression and integration tests.
These tests run the full pipeline: parse XML → generate inventory → translate to ADF + Spark
and verify the outputs are structurally valid and consistent across runs.
"""

import json
import os
import sys
from pathlib import Path

import pytest
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "parser"))
sys.path.insert(0, str(PROJECT_ROOT / "translator"))

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _full_pipeline(tmp_path):
    """Run the full migration pipeline and return (jobs, inventory_df, adf_pipelines, spark_notebooks)."""
    from parse_talend_jobs import TalendJobParser
    from generate_inventory import generate_inventory
    from translate_to_adf import ADFTranslator
    from translate_to_spark import SparkTranslator

    # Step 1 — Parse
    parser = TalendJobParser(str(FIXTURES_DIR))
    jobs = parser.parse_all()

    # Step 2 — Generate inventory
    inventory_df = generate_inventory(jobs)

    # Step 3 — Translate to ADF
    adf_translator = ADFTranslator()
    adf_pipelines = {}
    for _, row in inventory_df[inventory_df["target_fabric"] == "DataFactory"].iterrows():
        job = row.to_dict()
        pipeline = adf_translator.translate_job(job)
        adf_pipelines[job["job_name"]] = pipeline

        # Persist to disk
        out_path = tmp_path / "adf" / f"{pipeline['name']}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(pipeline, f, indent=2, default=str)

    # Step 4 — Translate to Spark
    spark_translator = SparkTranslator()
    spark_notebooks = {}
    for _, row in inventory_df[inventory_df["target_fabric"] == "Spark"].iterrows():
        job = row.to_dict()
        notebook = spark_translator.translate_job(job)
        spark_notebooks[job["job_name"]] = notebook

        out_path = tmp_path / "spark" / f"nb_{job['job_name']}.py"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(notebook)

    # Step 5 — Save inventory
    inv_path = tmp_path / "inventory.csv"
    inventory_df.to_csv(inv_path, index=False)

    return jobs, inventory_df, adf_pipelines, spark_notebooks


# ═══════════════════════════════════════════════════════════════════════════
# 1. END-TO-END PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
@pytest.mark.regression
class TestEndToEndPipeline:
    """Run the full pipeline over all fixtures and validate outputs."""

    @pytest.fixture(scope="class")
    def pipeline_result(self, tmp_path_factory):
        tmp = tmp_path_factory.mktemp("e2e")
        return _full_pipeline(tmp)

    def test_all_fixtures_parsed(self, pipeline_result):
        jobs, *_ = pipeline_result
        assert len(jobs) == 6

    def test_inventory_generated(self, pipeline_result):
        _, inv_df, *_ = pipeline_result
        assert len(inv_df) == 6

    def test_every_job_has_target(self, pipeline_result):
        _, inv_df, *_ = pipeline_result
        assert inv_df["target_fabric"].notna().all()
        assert set(inv_df["target_fabric"].unique()).issubset({"DataFactory", "Spark"})

    def test_adf_pipelines_generated(self, pipeline_result):
        _, inv_df, adf_pipelines, _ = pipeline_result
        adf_count = len(inv_df[inv_df["target_fabric"] == "DataFactory"])
        assert len(adf_pipelines) == adf_count

    def test_spark_notebooks_generated(self, pipeline_result):
        _, inv_df, _, spark_notebooks = pipeline_result
        spark_count = len(inv_df[inv_df["target_fabric"] == "Spark"])
        assert len(spark_notebooks) == spark_count

    def test_all_adf_pipelines_have_name(self, pipeline_result):
        _, _, adf_pipelines, _ = pipeline_result
        for name, pipeline in adf_pipelines.items():
            assert "name" in pipeline
            assert pipeline["name"].startswith("pl_")

    def test_all_adf_pipelines_serializable(self, pipeline_result):
        _, _, adf_pipelines, _ = pipeline_result
        for name, pipeline in adf_pipelines.items():
            s = json.dumps(pipeline, indent=2, default=str)
            reparsed = json.loads(s)
            assert reparsed["name"] == pipeline["name"]

    def test_all_spark_notebooks_have_imports(self, pipeline_result):
        _, _, _, spark_notebooks = pipeline_result
        for name, nb in spark_notebooks.items():
            assert "from pyspark.sql" in nb, f"Notebook {name} missing spark imports"

    def test_all_spark_notebooks_have_header(self, pipeline_result):
        _, _, _, spark_notebooks = pipeline_result
        for name, nb in spark_notebooks.items():
            assert "Migrated from Talend" in nb, f"Notebook {name} missing migration header"


# ═══════════════════════════════════════════════════════════════════════════
# 2. NON-REGRESSION — DETERMINISTIC OUTPUT
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.regression
class TestNonRegression:
    """Verify that running the pipeline twice produces identical outputs."""

    def test_deterministic_parsing(self):
        from parse_talend_jobs import TalendJobParser

        parser1 = TalendJobParser(str(FIXTURES_DIR))
        run1 = parser1.parse_all()

        parser2 = TalendJobParser(str(FIXTURES_DIR))
        run2 = parser2.parse_all()

        assert len(run1) == len(run2)
        for j1, j2 in zip(
            sorted(run1, key=lambda j: j["name"]),
            sorted(run2, key=lambda j: j["name"]),
        ):
            assert j1["name"] == j2["name"]
            assert j1["component_count"] == j2["component_count"]
            assert j1["component_types"] == j2["component_types"]
            assert j1["has_custom_code"] == j2["has_custom_code"]

    def test_deterministic_inventory(self):
        from parse_talend_jobs import TalendJobParser
        from generate_inventory import generate_inventory

        parser = TalendJobParser(str(FIXTURES_DIR))
        jobs = parser.parse_all()

        inv1 = generate_inventory(jobs)
        inv2 = generate_inventory(jobs)

        pd.testing.assert_frame_equal(inv1, inv2)

    def test_deterministic_adf_translation(self):
        from translate_to_adf import ADFTranslator

        translator = ADFTranslator()
        job = {
            "job_name": "regression_test",
            "pattern": "Copy",
            "components_used": "tOracleInput, tPostgresqlOutput",
            "component_types": "tOracleInput, tPostgresqlOutput",
            "has_custom_code": False,
            "target_fabric": "DataFactory",
        }

        result1 = json.dumps(translator.translate_job(job), sort_keys=True)
        result2 = json.dumps(translator.translate_job(job), sort_keys=True)
        assert result1 == result2

    def test_deterministic_sql_translation(self):
        from sql_translator import OracleToPostgreSQLTranslator

        translator = OracleToPostgreSQLTranslator()
        oracle_sql = "SELECT NVL(name, 'N/A'), SYSDATE, SUBSTR(email, 1, 5) FROM employees WHERE ROWNUM <= 10"

        result1 = translator.translate(oracle_sql)
        result2 = translator.translate(oracle_sql)
        assert result1 == result2


# ═══════════════════════════════════════════════════════════════════════════
# 3. CROSS-MODULE CONSISTENCY
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
@pytest.mark.regression
class TestCrossModuleConsistency:
    """Verify consistency between parser output and translator inputs."""

    def test_component_map_covers_fixture_components(self):
        """Every component in fixtures should be in the component_map or classifiable."""
        from parse_talend_jobs import TalendJobParser

        parser = TalendJobParser(str(FIXTURES_DIR))
        jobs = parser.parse_all()

        all_types = set()
        for job in jobs:
            all_types.update(job["component_types"])

        # Load component map
        map_path = PROJECT_ROOT / "mapping" / "component_map.json"
        with open(map_path) as f:
            cmap = json.load(f)
        mapped_components = set(cmap.get("mappings", {}).keys())

        unmapped = all_types - mapped_components
        # Remove comment keys and utility components that might not be in map
        unmapped = {c for c in unmapped if not c.startswith("_comment")}

        # Allow some unmapped (utilities, log), but main components should be mapped
        main_unmapped = {c for c in unmapped if c not in (
            "tFixedFlowInput", "tLogRow", "tFileArchive",
            "tFileDelete", "tSendMail", "tCatch",
        )}
        assert len(main_unmapped) == 0, f"Unmapped main components: {main_unmapped}"

    def test_inventory_scores_are_consistent(self):
        """Verify that complex jobs always score higher than simple ones."""
        from parse_talend_jobs import TalendJobParser
        from generate_inventory import generate_inventory

        parser = TalendJobParser(str(FIXTURES_DIR))
        jobs = parser.parse_all()
        inv = generate_inventory(jobs)

        simple = inv[inv["job_name"] == "simple_copy_job"]
        complex_j = inv[inv["job_name"] == "complex_etl_job"]

        if len(simple) > 0 and len(complex_j) > 0:
            assert complex_j.iloc[0]["complexity_score"] > simple.iloc[0]["complexity_score"]

    def test_custom_code_jobs_always_target_spark(self):
        """Jobs with custom Java code should always target Spark."""
        from parse_talend_jobs import TalendJobParser
        from generate_inventory import generate_inventory

        parser = TalendJobParser(str(FIXTURES_DIR))
        jobs = parser.parse_all()
        inv = generate_inventory(jobs)

        custom_code_jobs = inv[inv["has_custom_code"] == True]
        for _, row in custom_code_jobs.iterrows():
            assert row["target_fabric"] == "Spark", (
                f"Job '{row['job_name']}' has custom code but targets {row['target_fabric']}"
            )

    def test_sql_translator_handles_embedded_queries(self):
        """SQL queries extracted from fixtures should translate without errors."""
        from parse_talend_jobs import TalendJobParser
        from extract_metadata import MetadataExtractor
        from sql_translator import OracleToPostgreSQLTranslator

        parser = TalendJobParser(str(FIXTURES_DIR))
        jobs = parser.parse_all()

        extractor = MetadataExtractor(jobs)
        sql_statements = extractor.extract_sql_statements()

        translator = OracleToPostgreSQLTranslator()
        for stmt in sql_statements:
            sql = stmt.get("sql", "")
            if sql and len(sql.strip()) > 5:
                # Should not raise
                result = translator.translate(sql)
                assert isinstance(result, str)
                assert len(result) > 0
