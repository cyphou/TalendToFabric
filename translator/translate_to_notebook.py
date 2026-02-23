"""
Talend → Jupyter Notebook Translator
Generates .ipynb notebooks from parsed Talend job metadata.

Wraps the SparkTranslator logic and outputs Jupyter Notebook JSON format
instead of plain .py files.  Each ``# COMMAND ----------`` marker in the
PySpark template is converted to a new notebook code cell.

Usage:
    python translate_to_notebook.py --inventory ../inventory/talend_job_inventory.csv --output ../output/notebooks/
"""

import os
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Any

import click
import pandas as pd

# Re-use the Spark translator engine
from translate_to_spark import SparkTranslator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Jupyter notebook constants
NOTEBOOK_FORMAT = 4            # nbformat version
NOTEBOOK_FORMAT_MINOR = 5      # nbformat minor
PYTHON_VERSION = "3.10"
KERNEL_DISPLAY = "PySpark (Python 3)"
KERNEL_NAME = "synapse_pyspark"  # Microsoft Fabric kernel name


class NotebookTranslator:
    """Translates Talend job definitions to Jupyter Notebook (.ipynb) files.

    Internally delegates PySpark code generation to ``SparkTranslator`` and
    then converts the flat ``.py`` text into a structured ``.ipynb`` JSON
    document with one cell per ``# COMMAND ----------`` section.
    """

    def __init__(self):
        self.spark_translator = SparkTranslator()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def translate_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Return a complete .ipynb dict for *job*."""
        py_content = self.spark_translator.translate_job(job)
        cells = self._split_into_cells(py_content)
        notebook = self._build_notebook(cells, job)
        return notebook

    def translate_job_to_json(self, job: Dict[str, Any], indent: int = 1) -> str:
        """Return the .ipynb JSON string for *job*."""
        return json.dumps(self.translate_job(job), indent=indent, ensure_ascii=False)

    # ------------------------------------------------------------------
    # Notebook assembly
    # ------------------------------------------------------------------

    def _build_notebook(
        self,
        cells: List[Dict[str, Any]],
        job: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build the top-level .ipynb structure."""
        job_name = job.get("job_name", "unknown")
        return {
            "nbformat": NOTEBOOK_FORMAT,
            "nbformat_minor": NOTEBOOK_FORMAT_MINOR,
            "metadata": self._build_metadata(job_name),
            "cells": cells,
        }

    @staticmethod
    def _build_metadata(job_name: str) -> Dict[str, Any]:
        """Return notebook-level metadata (kernel, language, Fabric tags)."""
        return {
            "kernel_info": {
                "name": KERNEL_NAME,
            },
            "kernelspec": {
                "display_name": KERNEL_DISPLAY,
                "language": "python",
                "name": KERNEL_NAME,
            },
            "language_info": {
                "name": "python",
                "version": PYTHON_VERSION,
                "mimetype": "text/x-python",
                "file_extension": ".py",
                "codemirror_mode": {"name": "ipython", "version": 3},
            },
            "microsoft": {
                "host": {
                    "fabricNotebook": {
                        "notebookId": "",
                        "displayName": f"nb_{job_name}",
                    },
                },
            },
        }

    # ------------------------------------------------------------------
    # Cell splitting
    # ------------------------------------------------------------------

    # The Spark templates use  ``# COMMAND ----------`` as a separator.
    _CELL_SEPARATOR = re.compile(r"^#\s*COMMAND\s*[-─]+", re.MULTILINE)

    def _split_into_cells(self, py_content: str) -> List[Dict[str, Any]]:
        """Split a flat PySpark script into notebook cells.

        Strategy
        --------
        1. Split on ``# COMMAND ----------`` markers.
        2. Detect header comments at the start → markdown cell.
        3. Detect comment-only blocks → markdown cell.
        4. Everything else → code cell.
        """
        raw_blocks = self._CELL_SEPARATOR.split(py_content)

        cells: List[Dict[str, Any]] = []
        for block in raw_blocks:
            block = block.strip("\n")
            if not block.strip():
                continue

            # Decide: markdown or code?
            if self._is_markdown_block(block):
                md_text = self._comments_to_markdown(block)
                cells.append(self._make_markdown_cell(md_text))
            else:
                cells.append(self._make_code_cell(block))

        # Guarantee at least one cell
        if not cells:
            cells.append(self._make_code_cell("# Empty notebook — no Talend logic translated"))

        return cells

    @staticmethod
    def _is_markdown_block(block: str) -> bool:
        """Return True if *block* consists entirely of ``#``-prefixed comments or blanks."""
        for line in block.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                return False
        return True

    @staticmethod
    def _comments_to_markdown(block: str) -> str:
        """Convert a comment-only block to markdown text."""
        lines = []
        for line in block.splitlines():
            stripped = line.strip()
            if stripped.startswith("# "):
                lines.append(stripped[2:])
            elif stripped == "#":
                lines.append("")
            elif stripped.startswith("#"):
                lines.append(stripped[1:])
            else:
                lines.append(stripped)
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Cell factories
    # ------------------------------------------------------------------

    @staticmethod
    def _make_code_cell(source: str) -> Dict[str, Any]:
        """Return a Jupyter code cell dict."""
        return {
            "cell_type": "code",
            "metadata": {},
            "outputs": [],
            "execution_count": None,
            "source": source.splitlines(keepends=True),
        }

    @staticmethod
    def _make_markdown_cell(source: str) -> Dict[str, Any]:
        """Return a Jupyter markdown cell dict."""
        return {
            "cell_type": "markdown",
            "metadata": {},
            "source": source.splitlines(keepends=True),
        }


# ───────────────────────────────────────────────────────────────────────
# CLI
# ───────────────────────────────────────────────────────────────────────

@click.command()
@click.option("--inventory", "-i", required=True, help="Path to talend_job_inventory.csv")
@click.option("--output", "-o", required=True, help="Output folder for .ipynb notebooks")
@click.option("--filter-target", default="Spark", help="Only translate jobs targeting this platform")
def main(inventory: str, output: str, filter_target: str):
    """Generate Jupyter Notebooks (.ipynb) from Talend inventory."""
    logger.info(f"Loading inventory from {inventory}")
    df = pd.read_csv(inventory)

    jobs = df[df["target_fabric"] == filter_target]
    logger.info(f"Found {len(jobs)} jobs targeting {filter_target}")

    translator = NotebookTranslator()
    os.makedirs(output, exist_ok=True)

    for _, row in jobs.iterrows():
        job = row.to_dict()
        ipynb_json = translator.translate_job_to_json(job)

        output_file = os.path.join(output, f"nb_{job.get('job_name', 'unknown')}.ipynb")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(ipynb_json)
        logger.info(f"Generated: {output_file}")

    logger.info(f"Translation complete. {len(jobs)} notebooks generated in {output}")


if __name__ == "__main__":
    main()
