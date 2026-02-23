"""
Row Count Validation — Compares row counts between Talend output and Fabric output.

Usage:
    python row_count_validation.py --config ../config/migration_config.json
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any

import click
import pandas as pd
from rich.console import Console
from rich.table import Table

console = Console()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class RowCountValidator:
    """Validates row counts between source (Talend) and target (Fabric) tables."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.results: List[Dict[str, Any]] = []

    def validate_all(self, test_cases: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run row count validation for all test cases."""
        for case in test_cases:
            result = self.validate_single(case)
            self.results.append(result)
        return self.results

    def validate_single(self, test_case: Dict[str, Any]) -> Dict[str, Any]:
        """Validate row count for a single table/entity."""
        entity = test_case.get("entity", "unknown")
        source_count = test_case.get("source_row_count")
        target_count = test_case.get("target_row_count")

        # If counts not provided, query them
        if source_count is None:
            source_count = self._get_row_count(
                test_case.get("source_connection"),
                test_case.get("source_query", f"SELECT COUNT(*) FROM {test_case.get('source_table', '')}"),
            )

        if target_count is None:
            target_count = self._get_row_count(
                test_case.get("target_connection"),
                test_case.get("target_query", f"SELECT COUNT(*) FROM {test_case.get('target_table', '')}"),
            )

        # Calculate diff
        if source_count is not None and target_count is not None:
            diff = target_count - source_count
            diff_pct = (diff / source_count * 100) if source_count > 0 else 0
            threshold = test_case.get("threshold_pct", 0)
            passed = abs(diff_pct) <= threshold
        else:
            diff = None
            diff_pct = None
            passed = False

        return {
            "entity": entity,
            "source_table": test_case.get("source_table", ""),
            "target_table": test_case.get("target_table", ""),
            "source_count": source_count,
            "target_count": target_count,
            "difference": diff,
            "difference_pct": round(diff_pct, 2) if diff_pct is not None else None,
            "threshold_pct": test_case.get("threshold_pct", 0),
            "passed": passed,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _get_row_count(self, connection_config: Dict, query: str) -> int:
        """Execute a count query against a database."""
        if not connection_config:
            logger.warning("No connection config provided — returning None")
            return None

        try:
            from sqlalchemy import create_engine, text

            conn_str = connection_config.get("connection_string", "")
            engine = create_engine(conn_str)

            with engine.connect() as conn:
                result = conn.execute(text(query))
                count = result.scalar()
                return int(count) if count is not None else 0

        except Exception as e:
            logger.error(f"Failed to get row count: {e}")
            return None

    def display_results(self) -> None:
        """Display results in a rich table."""
        table = Table(title="Row Count Validation Results")
        table.add_column("Entity", style="cyan")
        table.add_column("Source Count", justify="right")
        table.add_column("Target Count", justify="right")
        table.add_column("Difference", justify="right")
        table.add_column("Diff %", justify="right")
        table.add_column("Status", justify="center")

        for r in self.results:
            status = "[green]PASS[/green]" if r["passed"] else "[red]FAIL[/red]"
            table.add_row(
                r["entity"],
                str(r["source_count"] or "N/A"),
                str(r["target_count"] or "N/A"),
                str(r["difference"] or "N/A"),
                f"{r['difference_pct']}%" if r["difference_pct"] is not None else "N/A",
                status,
            )

        console.print(table)

        passed = sum(1 for r in self.results if r["passed"])
        total = len(self.results)
        console.print(f"\n[bold]Results: {passed}/{total} passed[/bold]")

    def save_results(self, output_path: str) -> None:
        """Save results to JSON."""
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2)
        logger.info(f"Results saved to {output_path}")


@click.command()
@click.option("--config", "-c", required=True, help="Path to migration_config.json")
@click.option("--test-cases", "-t", help="Path to test cases JSON (overrides config)")
@click.option("--output", "-o", default="validation_results.json", help="Output file path")
def main(config: str, test_cases: str, output: str):
    """Run row count validation between Talend and Fabric outputs."""
    with open(config, "r", encoding="utf-8") as f:
        config_data = json.load(f)

    # Load test cases
    if test_cases:
        with open(test_cases, "r", encoding="utf-8") as f:
            cases = json.load(f)
    else:
        cases = config_data.get("validation", {}).get("row_count_tests", [])

    if not cases:
        console.print("[yellow]No test cases found. Create test cases in config or provide --test-cases.[/yellow]")
        return

    validator = RowCountValidator(config_data)
    validator.validate_all(cases)
    validator.display_results()
    validator.save_results(output)


if __name__ == "__main__":
    main()
