"""
Schema Validation — Validates that target table schemas match expected definitions.

Usage:
    python schema_validation.py --config ../config/migration_config.json
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any

import click
from rich.console import Console
from rich.table import Table

console = Console()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class SchemaValidator:
    """Validates that target schemas conform to expected definitions."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.results: List[Dict[str, Any]] = []

    def validate_schema(
        self,
        entity: str,
        expected_columns: List[Dict[str, str]],
        actual_columns: List[Dict[str, str]],
    ) -> Dict[str, Any]:
        """
        Compare expected vs actual schema.

        Each column dict has: {"name": str, "type": str, "nullable": bool}
        """
        expected_map = {c["name"].lower(): c for c in expected_columns}
        actual_map = {c["name"].lower(): c for c in actual_columns}

        missing_cols = set(expected_map.keys()) - set(actual_map.keys())
        extra_cols = set(actual_map.keys()) - set(expected_map.keys())
        common_cols = set(expected_map.keys()) & set(actual_map.keys())

        type_mismatches = []
        for col in common_cols:
            exp_type = expected_map[col].get("type", "").upper()
            act_type = actual_map[col].get("type", "").upper()
            if exp_type != act_type:
                type_mismatches.append({
                    "column": col,
                    "expected_type": exp_type,
                    "actual_type": act_type,
                })

        passed = len(missing_cols) == 0 and len(type_mismatches) == 0

        result = {
            "entity": entity,
            "expected_columns": len(expected_columns),
            "actual_columns": len(actual_columns),
            "missing_columns": list(missing_cols),
            "extra_columns": list(extra_cols),
            "type_mismatches": type_mismatches,
            "passed": passed,
            "timestamp": datetime.utcnow().isoformat(),
        }

        self.results.append(result)
        return result

    def display_results(self) -> None:
        """Display validation results."""
        table = Table(title="Schema Validation Results")
        table.add_column("Entity", style="cyan")
        table.add_column("Expected", justify="right")
        table.add_column("Actual", justify="right")
        table.add_column("Missing", justify="right", style="red")
        table.add_column("Extra", justify="right", style="yellow")
        table.add_column("Type Mismatches", justify="right", style="red")
        table.add_column("Status", justify="center")

        for r in self.results:
            status = "[green]PASS[/green]" if r["passed"] else "[red]FAIL[/red]"
            table.add_row(
                r["entity"],
                str(r["expected_columns"]),
                str(r["actual_columns"]),
                str(len(r["missing_columns"])),
                str(len(r["extra_columns"])),
                str(len(r["type_mismatches"])),
                status,
            )

        console.print(table)

        # Show details for failures
        for r in self.results:
            if not r["passed"]:
                if r["missing_columns"]:
                    console.print(f"\n[red]{r['entity']} — Missing columns:[/red] {', '.join(r['missing_columns'])}")
                if r["type_mismatches"]:
                    console.print(f"\n[red]{r['entity']} — Type mismatches:[/red]")
                    for m in r["type_mismatches"]:
                        console.print(f"  {m['column']}: expected {m['expected_type']}, got {m['actual_type']}")

    def save_results(self, output_path: str) -> None:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2)


@click.command()
@click.option("--config", "-c", required=True, help="Path to migration_config.json")
@click.option("--output", "-o", default="schema_results.json", help="Output file path")
def main(config: str, output: str):
    """Run schema validation."""
    console.print("[bold]Schema Validator[/bold]")
    console.print("Use SchemaValidator.validate_schema() with expected and actual column definitions.")


if __name__ == "__main__":
    main()
