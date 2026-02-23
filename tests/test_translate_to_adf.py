"""
Unit tests for translator/translate_to_adf.py — ADFTranslator
Tests cover: copy pipeline, orchestration, dataflow, file transfer, DDL, API pipelines.
"""

import json
import pytest


# ═══════════════════════════════════════════════════════════════════════════
# 1. COPY PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestADFCopyPipeline:
    """Test ADF translation of simple copy jobs."""

    def test_returns_dict(self, adf_translator, inv_row_simple_copy):
        result = adf_translator.translate_job(inv_row_simple_copy)
        assert isinstance(result, dict)

    def test_pipeline_name(self, adf_translator, inv_row_simple_copy):
        result = adf_translator.translate_job(inv_row_simple_copy)
        assert result["name"] == "pl_simple_copy"

    def test_has_activities(self, adf_translator, inv_row_simple_copy):
        result = adf_translator.translate_job(inv_row_simple_copy)
        activities = result["properties"]["activities"]
        assert len(activities) == 1

    def test_activity_type_is_copy(self, adf_translator, inv_row_simple_copy):
        result = adf_translator.translate_job(inv_row_simple_copy)
        activity = result["properties"]["activities"][0]
        assert activity["type"] == "Copy"

    def test_annotations_include_migration_tag(self, adf_translator, inv_row_simple_copy):
        result = adf_translator.translate_job(inv_row_simple_copy)
        annotations = result["properties"]["annotations"]
        assert "MigratedFromTalend" in annotations

    def test_description(self, adf_translator, inv_row_simple_copy):
        result = adf_translator.translate_job(inv_row_simple_copy)
        desc = result["properties"]["description"]
        assert "simple_copy" in desc

    def test_output_is_valid_json(self, adf_translator, inv_row_simple_copy):
        result = adf_translator.translate_job(inv_row_simple_copy)
        # Ensure serializable
        json_str = json.dumps(result, indent=2)
        assert len(json_str) > 0
        reparsed = json.loads(json_str)
        assert reparsed["name"] == result["name"]


# ═══════════════════════════════════════════════════════════════════════════
# 2. ORCHESTRATION PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestADFOrchestrationPipeline:
    """Test ADF translation of orchestration jobs."""

    def test_pipeline_name(self, adf_translator, inv_row_orchestration):
        result = adf_translator.translate_job(inv_row_orchestration)
        assert "orchestrate" in result["name"]

    def test_description_mentions_orchestration(self, adf_translator, inv_row_orchestration):
        result = adf_translator.translate_job(inv_row_orchestration)
        desc = result["properties"]["description"]
        assert "Orchestration" in desc or "orchestrate" in desc.lower()

    def test_valid_json(self, adf_translator, inv_row_orchestration):
        result = adf_translator.translate_job(inv_row_orchestration)
        assert json.dumps(result) is not None


# ═══════════════════════════════════════════════════════════════════════════
# 3. TRANSFORM / DATAFLOW PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestADFDataflowPipeline:
    """Test ADF translation for transform jobs (uses dataflow)."""

    def test_pipeline_generated(self, adf_translator, inv_row_transform):
        result = adf_translator.translate_job(inv_row_transform)
        assert result is not None
        assert "name" in result

    def test_dataflow_activity(self, adf_translator, inv_row_transform):
        result = adf_translator.translate_job(inv_row_transform)
        activities = result["properties"]["activities"]
        assert len(activities) >= 1
        # Transform pattern uses ExecuteDataFlow
        activity_types = [a["type"] for a in activities]
        assert "ExecuteDataFlow" in activity_types

    def test_dataflow_reference(self, adf_translator, inv_row_transform):
        result = adf_translator.translate_job(inv_row_transform)
        activities = result["properties"]["activities"]
        df_activity = next(a for a in activities if a["type"] == "ExecuteDataFlow")
        ref = df_activity["typeProperties"]["dataflow"]
        assert ref["type"] == "DataFlowReference"
        assert "transform_etl" in ref["referenceName"]


# ═══════════════════════════════════════════════════════════════════════════
# 4. FILE TRANSFER PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestADFFileTransferPipeline:
    """Test ADF file transfer pattern."""

    def test_copy_pipeline_for_file_job(self, adf_translator, inv_row_file_transfer):
        """File transfer jobs that don't match specific patterns default to copy."""
        result = adf_translator.translate_job(inv_row_file_transfer)
        assert result is not None
        assert "pl_" in result["name"]


# ═══════════════════════════════════════════════════════════════════════════
# 5. PIPELINE STRUCTURE INVARIANTS
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestADFPipelineStructure:
    """Structural invariants that all ADF pipelines must satisfy."""

    @pytest.fixture(params=[
        "inv_row_simple_copy",
        "inv_row_transform",
        "inv_row_orchestration",
        "inv_row_complex_spark",
        "inv_row_file_transfer",
    ])
    def any_pipeline(self, request, adf_translator):
        row = request.getfixturevalue(request.param)
        return adf_translator.translate_job(row)

    def test_has_name(self, any_pipeline):
        assert "name" in any_pipeline
        assert any_pipeline["name"].startswith("pl_")

    def test_has_properties(self, any_pipeline):
        assert "properties" in any_pipeline

    def test_has_activities(self, any_pipeline):
        assert "activities" in any_pipeline["properties"]
        assert isinstance(any_pipeline["properties"]["activities"], list)

    def test_serializable(self, any_pipeline):
        serialized = json.dumps(any_pipeline, indent=2, default=str)
        assert len(serialized) > 10
        json.loads(serialized)  # must not raise
