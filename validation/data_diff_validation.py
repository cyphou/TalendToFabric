"""
Data Diff Validation — Hash-based comparison between source and target data.
Detects missing, extra, or mismatched records.

Usage:
    python data_diff_validation.py --config ../config/migration_config.json
"""

import json
import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

import click
import pandas as pd
from rich.console import Console
from rich.table import Table

console = Console()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class DataDiffValidator:
    """Validates data integrity by comparing hashes between source and target."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.results: List[Dict[str, Any]] = []

    def validate_dataframe_pair(
        self,
        entity: str,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        key_columns: List[str],
        compare_columns: Optional[List[str]] = None,
        sample_size: int = None,
    ) -> Dict[str, Any]:
        """
        Compare two DataFrames (source vs target) using hash-based comparison.

        Args:
            entity: Name of the entity being compared
            source_df: Source (Talend output) DataFrame
            target_df: Target (Fabric output) DataFrame
            key_columns: Columns that uniquely identify a row
            compare_columns: Columns to compare (None = all non-key columns)
            sample_size: If set, only compare this many rows
        """
        if compare_columns is None:
            compare_columns = [c for c in source_df.columns if c not in key_columns]

        # Sample if needed
        if sample_size and len(source_df) > sample_size:
            source_df = source_df.sample(n=sample_size, random_state=42)
            # Get matching keys from target
            keys = source_df[key_columns]
            target_df = target_df.merge(keys, on=key_columns, how="inner")

        # Compute row hashes
        all_cols = key_columns + compare_columns
        source_hashes = self._compute_hashes(source_df, key_columns, compare_columns)
        target_hashes = self._compute_hashes(target_df, key_columns, compare_columns)

        # Compare
        source_keys = set(source_hashes.keys())
        target_keys = set(target_hashes.keys())

        missing_in_target = source_keys - target_keys
        extra_in_target = target_keys - source_keys
        common_keys = source_keys & target_keys

        mismatched = set()
        for key in common_keys:
            if source_hashes[key] != target_hashes[key]:
                mismatched.add(key)

        matched = common_keys - mismatched

        result = {
            "entity": entity,
            "source_rows": len(source_df),
            "target_rows": len(target_df),
            "matched": len(matched),
            "mismatched": len(mismatched),
            "missing_in_target": len(missing_in_target),
            "extra_in_target": len(extra_in_target),
            "match_rate_pct": round(len(matched) / max(len(source_df), 1) * 100, 2),
            "passed": len(mismatched) == 0 and len(missing_in_target) == 0,
            "sample_mismatched_keys": list(mismatched)[:10],
            "sample_missing_keys": list(missing_in_target)[:10],
            "timestamp": datetime.utcnow().isoformat(),
        }

        self.results.append(result)
        return result

    def _compute_hashes(
        self, df: pd.DataFrame, key_columns: List[str], compare_columns: List[str]
    ) -> Dict[str, str]:
        """Compute a hash for each row based on compare columns, keyed by key columns."""
        hashes = {}

        for _, row in df.iterrows():
            key = tuple(str(row[k]) for k in key_columns)
            values = "||".join(str(row.get(c, "NULL")) for c in compare_columns)
            row_hash = hashlib.md5(values.encode("utf-8")).hexdigest()
            hashes[key] = row_hash

        return hashes

    def display_results(self) -> None:
        """Display results in a rich table."""
        table = Table(title="Data Diff Validation Results")
        table.add_column("Entity", style="cyan")
        table.add_column("Source", justify="right")
        table.add_column("Target", justify="right")
        table.add_column("Matched", justify="right", style="green")
        table.add_column("Mismatched", justify="right", style="red")
        table.add_column("Missing", justify="right", style="yellow")
        table.add_column("Extra", justify="right", style="yellow")
        table.add_column("Match %", justify="right")
        table.add_column("Status", justify="center")

        for r in self.results:
            status = "[green]PASS[/green]" if r["passed"] else "[red]FAIL[/red]"
            table.add_row(
                r["entity"],
                str(r["source_rows"]),
                str(r["target_rows"]),
                str(r["matched"]),
                str(r["mismatched"]),
                str(r["missing_in_target"]),
                str(r["extra_in_target"]),
                f"{r['match_rate_pct']}%",
                status,
            )

        console.print(table)

    def save_results(self, output_path: str) -> None:
        """Save results to JSON."""
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2, default=str)
        logger.info(f"Results saved to {output_path}")


@click.command()
@click.option("--config", "-c", required=True, help="Path to migration_config.json")
@click.option("--output", "-o", default="datadiff_results.json", help="Output file path")
def main(config: str, output: str):
    """Run data diff validation between Talend and Fabric outputs."""
    with open(config, "r", encoding="utf-8") as f:
        config_data = json.load(f)

    console.print("[bold]Data Diff Validation[/bold]")
    console.print("This validator compares source and target data using hash-based comparison.")
    console.print("To use, load your source and target data as Pandas DataFrames and call validate_dataframe_pair().")
    console.print("\nExample usage in a script or notebook:")
    console.print("""
    from data_diff_validation import DataDiffValidator

    validator = DataDiffValidator(config)
    source_df = pd.read_csv("talend_output.csv")
    target_df = pd.read_csv("fabric_output.csv")

    result = validator.validate_dataframe_pair(
        entity="customers",
        source_df=source_df,
        target_df=target_df,
        key_columns=["customer_id"],
    )

    validator.display_results()
    validator.save_results("results.json")
    """)


if __name__ == "__main__":
    main()
