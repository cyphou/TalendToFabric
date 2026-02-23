"""
Unit tests for translator/translate_to_notebook.py — NotebookTranslator
Tests cover: .ipynb structure, cell splitting, markdown detection, metadata,
             template selection, CLI output, and round-trip validity.
"""

import json
import pytest
from pathlib import Path


# ═══════════════════════════════════════════════════════════════════════════
# 1. BASIC NOTEBOOK STRUCTURE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestNotebookStructure:
    """Validate that translate_job returns a valid .ipynb dict."""

    def test_returns_dict(self, notebook_translator, inv_row_simple_copy):
        result = notebook_translator.translate_job(inv_row_simple_copy)
        assert isinstance(result, dict)

    def test_has_nbformat(self, notebook_translator, inv_row_simple_copy):
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        assert nb["nbformat"] == 4
        assert nb["nbformat_minor"] == 5

    def test_has_metadata(self, notebook_translator, inv_row_simple_copy):
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        assert "metadata" in nb
        assert "kernelspec" in nb["metadata"]

    def test_kernel_name(self, notebook_translator, inv_row_simple_copy):
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        ks = nb["metadata"]["kernelspec"]
        assert ks["language"] == "python"
        assert ks["name"] == "synapse_pyspark"

    def test_has_cells(self, notebook_translator, inv_row_simple_copy):
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        assert "cells" in nb
        assert isinstance(nb["cells"], list)
        assert len(nb["cells"]) >= 1

    def test_fabric_metadata(self, notebook_translator, inv_row_simple_copy):
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        ms = nb["metadata"].get("microsoft", {})
        assert "host" in ms
        assert "fabricNotebook" in ms["host"]
        assert nb["metadata"]["microsoft"]["host"]["fabricNotebook"]["displayName"] == "nb_simple_copy"


# ═══════════════════════════════════════════════════════════════════════════
# 2. CELL TYPES
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestCellTypes:
    """Validate code and markdown cell generation."""

    def test_code_cells_exist(self, notebook_translator, inv_row_complex_spark):
        nb = notebook_translator.translate_job(inv_row_complex_spark)
        code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
        assert len(code_cells) >= 1

    def test_markdown_cells_exist(self, notebook_translator, inv_row_complex_spark):
        nb = notebook_translator.translate_job(inv_row_complex_spark)
        md_cells = [c for c in nb["cells"] if c["cell_type"] == "markdown"]
        assert len(md_cells) >= 1

    def test_code_cell_has_outputs(self, notebook_translator, inv_row_simple_copy):
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
        for cell in code_cells:
            assert "outputs" in cell
            assert "execution_count" in cell

    def test_markdown_cell_has_no_outputs(self, notebook_translator, inv_row_simple_copy):
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        md_cells = [c for c in nb["cells"] if c["cell_type"] == "markdown"]
        for cell in md_cells:
            assert "outputs" not in cell

    def test_cell_source_is_list(self, notebook_translator, inv_row_simple_copy):
        """Jupyter spec requires source to be a list of strings."""
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        for cell in nb["cells"]:
            assert isinstance(cell["source"], list)
            for line in cell["source"]:
                assert isinstance(line, str)


# ═══════════════════════════════════════════════════════════════════════════
# 3. CELL SPLITTING LOGIC
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestCellSplitting:
    """Verify the # COMMAND ---------- separator creates proper cells."""

    def test_etl_template_splits_into_multiple_cells(self, notebook_translator, inv_row_simple_copy):
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        # etl_notebook.py has multiple COMMAND sections
        assert len(nb["cells"]) >= 4

    def test_complex_job_has_more_cells(self, notebook_translator, inv_row_complex_spark):
        nb = notebook_translator.translate_job(inv_row_complex_spark)
        assert len(nb["cells"]) >= 4

    def test_no_command_separator_in_output(self, notebook_translator, inv_row_simple_copy):
        """The COMMAND separator should be consumed, not appear in cells."""
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        for cell in nb["cells"]:
            text = "".join(cell["source"])
            assert "COMMAND ----------" not in text

    def test_header_becomes_markdown(self, notebook_translator, inv_row_simple_copy):
        """The auto-generated header (all comments) should become a markdown cell."""
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        first_cell = nb["cells"][0]
        assert first_cell["cell_type"] == "markdown"
        text = "".join(first_cell["source"])
        assert "AUTO-GENERATED" in text or "Migrated from Talend" in text


# ═══════════════════════════════════════════════════════════════════════════
# 4. MARKDOWN CONVERSION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestMarkdownConversion:
    """Verify comment-only blocks are rendered as clean markdown."""

    def test_comment_prefix_stripped(self, notebook_translator):
        """Leading '# ' should be stripped from markdown cells."""
        block = "# Step 1: Extract\n# Read from source database"
        md = notebook_translator._comments_to_markdown(block)
        assert md == "Step 1: Extract\nRead from source database"

    def test_bare_hash_becomes_empty_line(self, notebook_translator):
        block = "# Header\n#\n# Body"
        md = notebook_translator._comments_to_markdown(block)
        assert md == "Header\n\nBody"

    def test_is_markdown_block_all_comments(self, notebook_translator):
        assert notebook_translator._is_markdown_block("# line 1\n# line 2")

    def test_is_markdown_block_with_code(self, notebook_translator):
        assert not notebook_translator._is_markdown_block("# comment\ndf = spark.read.csv('x')")


# ═══════════════════════════════════════════════════════════════════════════
# 5. JSON SERIALIZATION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestNotebookJSON:
    """Validate that the JSON output is valid and parseable."""

    def test_translate_job_to_json_returns_string(self, notebook_translator, inv_row_simple_copy):
        result = notebook_translator.translate_job_to_json(inv_row_simple_copy)
        assert isinstance(result, str)

    def test_json_is_valid(self, notebook_translator, inv_row_simple_copy):
        result = notebook_translator.translate_job_to_json(inv_row_simple_copy)
        parsed = json.loads(result)
        assert parsed["nbformat"] == 4

    def test_json_roundtrip(self, notebook_translator, inv_row_complex_spark):
        """Serialize → deserialize → compare."""
        nb = notebook_translator.translate_job(inv_row_complex_spark)
        json_str = json.dumps(nb, indent=1, ensure_ascii=False)
        parsed = json.loads(json_str)
        assert parsed["nbformat"] == nb["nbformat"]
        assert len(parsed["cells"]) == len(nb["cells"])


# ═══════════════════════════════════════════════════════════════════════════
# 6. TEMPLATE ROUTING (inherits from SparkTranslator)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestNotebookTemplateRouting:
    """Confirm template selection is consistent with SparkTranslator."""

    def test_scd_job(self, notebook_translator):
        job = {
            "job_name": "scd_customers",
            "pattern": "Copy",
            "components_used": "tOracleInput, tPostgresqlOutput",
            "job_folder": "test/",
            "complexity": "Medium",
            "notes": "",
            "description": "",
        }
        nb = notebook_translator.translate_job(job)
        text = "".join("".join(c["source"]) for c in nb["cells"])
        assert "scd" in text.lower() or "SCD" in text or "Slowly Changing" in text

    def test_incremental_job(self, notebook_translator):
        job = {
            "job_name": "incremental_load_orders",
            "pattern": "Copy",
            "components_used": "tOracleInput, tPostgresqlOutput",
            "job_folder": "test/",
            "complexity": "Simple",
            "notes": "",
            "description": "",
        }
        nb = notebook_translator.translate_job(job)
        text = "".join("".join(c["source"]) for c in nb["cells"])
        assert "incremental" in text.lower()

    def test_lookup_job(self, notebook_translator):
        job = {
            "job_name": "lookup_customers",
            "pattern": "Lookup",
            "components_used": "tOracleInput, tMap, tDBInput, tPostgresqlOutput",
            "job_folder": "test/",
            "complexity": "Medium",
            "notes": "",
            "description": "",
        }
        nb = notebook_translator.translate_job(job)
        text = "".join("".join(c["source"]) for c in nb["cells"])
        assert "lookup" in text.lower() or "join" in text.lower()


# ═══════════════════════════════════════════════════════════════════════════
# 7. DIFFERENT JOB PATTERNS
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestNotebookVariousPatterns:
    """Generate notebooks for multiple inventory row types."""

    def test_simple_copy_notebook(self, notebook_translator, inv_row_simple_copy):
        nb = notebook_translator.translate_job(inv_row_simple_copy)
        assert nb["nbformat"] == 4
        assert len(nb["cells"]) >= 2

    def test_transform_notebook(self, notebook_translator, inv_row_transform):
        nb = notebook_translator.translate_job(inv_row_transform)
        assert nb["nbformat"] == 4
        assert len(nb["cells"]) >= 2

    def test_complex_spark_notebook(self, notebook_translator, inv_row_complex_spark):
        nb = notebook_translator.translate_job(inv_row_complex_spark)
        assert nb["nbformat"] == 4
        # Complex jobs should have custom-code indicator
        text = "".join("".join(c["source"]) for c in nb["cells"])
        assert "complex_spark_etl" in text

    def test_file_transfer_notebook(self, notebook_translator, inv_row_file_transfer):
        nb = notebook_translator.translate_job(inv_row_file_transfer)
        assert nb["nbformat"] == 4
        assert len(nb["cells"]) >= 2


# ═══════════════════════════════════════════════════════════════════════════
# 8. FILE OUTPUT (write to disk)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.translator
class TestNotebookFileOutput:
    """Write .ipynb to disk and verify it's valid."""

    def test_write_ipynb(self, notebook_translator, inv_row_simple_copy, tmp_path):
        ipynb_json = notebook_translator.translate_job_to_json(inv_row_simple_copy)
        out_file = tmp_path / "nb_simple_copy.ipynb"
        out_file.write_text(ipynb_json, encoding="utf-8")

        assert out_file.exists()
        assert out_file.stat().st_size > 100

        # Re-parse to validate
        loaded = json.loads(out_file.read_text(encoding="utf-8"))
        assert loaded["nbformat"] == 4
        assert len(loaded["cells"]) >= 1

    def test_write_multiple_notebooks(self, notebook_translator, tmp_path):
        jobs = [
            {"job_name": "job_a", "pattern": "Copy", "components_used": "tOracleInput, tPostgresqlOutput",
             "job_folder": "a/", "complexity": "Simple", "notes": "", "description": ""},
            {"job_name": "job_b", "pattern": "Transform", "components_used": "tOracleInput, tMap, tPostgresqlOutput",
             "job_folder": "b/", "complexity": "Medium", "notes": "", "description": ""},
        ]
        for job in jobs:
            ipynb = notebook_translator.translate_job_to_json(job)
            (tmp_path / f"nb_{job['job_name']}.ipynb").write_text(ipynb, encoding="utf-8")

        assert (tmp_path / "nb_job_a.ipynb").exists()
        assert (tmp_path / "nb_job_b.ipynb").exists()
