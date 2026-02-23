"""
Inventory Generator — Generates a CSV inventory from parsed Talend jobs
with complexity scoring and target recommendation.

Usage:
    python generate_inventory.py --input parsed_jobs.json --output talend_job_inventory.csv
"""

import json
import logging
from typing import Dict, List, Any

import click
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# Complexity scoring weights
WEIGHTS = {
    "component_count": 0.20,
    "transformation_complexity": 0.25,
    "custom_code": 0.20,
    "dependencies": 0.15,
    "data_volume": 0.10,
    "error_handling": 0.10,
}

# Component types that indicate orchestration
ORCHESTRATION_TYPES = {"tRunJob", "tPreJob", "tPostJob"}

# Component types that indicate custom code
CUSTOM_CODE_TYPES = {"tJava", "tJavaRow", "tJavaFlex", "tGroovy"}

# Transformation component types
TRANSFORM_TYPES = {
    "tMap", "tFilter", "tFilterRow", "tAggregate", "tAggregateRow",
    "tSort", "tSortRow", "tUniqRow", "tDenormalize", "tNormalize",
    "tReplace", "tConvertType", "tUnite", "tSplitRow", "tJoin",
}


def score_component_count(count: int) -> float:
    """Score based on number of components."""
    if count <= 3:
        return 1.0
    elif count <= 5:
        return 2.0
    elif count <= 8:
        return 3.0
    elif count <= 12:
        return 4.0
    else:
        return 5.0


def score_transformation_complexity(transforms: List[Dict]) -> float:
    """Score based on transformation complexity."""
    if not transforms:
        return 1.0
    count = len(transforms)
    has_tmap = any(t.get("component_type") == "tMap" for t in transforms)
    has_aggregate = any(t.get("component_type") in ("tAggregate", "tAggregateRow") for t in transforms)

    score = min(count, 5)
    if has_tmap and has_aggregate:
        score = min(score + 1, 5)
    return float(score)


def score_custom_code(has_code: bool) -> float:
    """Score based on presence of custom code."""
    return 5.0 if has_code else 1.0


def score_dependencies(connections: List[Dict], component_types: List[str]) -> float:
    """Score based on job dependencies."""
    run_job_count = sum(1 for ct in component_types if ct == "tRunJob")
    if run_job_count == 0:
        return 1.0
    elif run_job_count <= 2:
        return 2.5
    elif run_job_count <= 4:
        return 3.5
    else:
        return 5.0


def determine_pattern(job: Dict[str, Any]) -> str:
    """Determine the dominant pattern of the job."""
    types = set(job.get("component_types", []))

    if types & ORCHESTRATION_TYPES:
        return "Orchestration"
    elif types & CUSTOM_CODE_TYPES:
        return "Custom Code"
    elif types & {"tMap", "tJoin"} and types & {"tAggregate", "tAggregateRow"}:
        return "Complex Transform"
    elif types & TRANSFORM_TYPES:
        return "Transform"
    else:
        return "Copy"


def determine_target(score: float, pattern: str, has_custom_code: bool) -> str:
    """Recommend the Fabric target based on score and pattern."""
    if pattern == "Orchestration":
        return "DataFactory"
    if has_custom_code:
        return "Spark"
    if score <= 2.0 and pattern == "Copy":
        return "DataFactory"
    if score <= 3.0:
        return "DataFactory"
    return "Spark"


def generate_inventory(jobs: List[Dict[str, Any]]) -> pd.DataFrame:
    """Generate an inventory DataFrame from parsed jobs."""
    rows = []

    for job in jobs:
        transform_components = job.get("transformation_components", [])
        component_types = job.get("component_types", [])
        has_custom = job.get("has_custom_code", False)
        connections = job.get("connections", [])

        # Calculate complexity scores
        s_components = score_component_count(job.get("component_count", 0))
        s_transforms = score_transformation_complexity(transform_components)
        s_custom = score_custom_code(has_custom)
        s_deps = score_dependencies(connections, component_types)
        s_volume = 2.0  # Default — can't determine from metadata alone
        s_errors = 2.0  # Default

        total_score = (
            s_components * WEIGHTS["component_count"]
            + s_transforms * WEIGHTS["transformation_complexity"]
            + s_custom * WEIGHTS["custom_code"]
            + s_deps * WEIGHTS["dependencies"]
            + s_volume * WEIGHTS["data_volume"]
            + s_errors * WEIGHTS["error_handling"]
        )

        # Determine classification
        pattern = determine_pattern(job)
        target = determine_target(total_score, pattern, has_custom)
        complexity = "Simple" if total_score <= 2.0 else ("Medium" if total_score <= 3.5 else "Complex")

        # Determine source/target types
        source_types = [c["component_type"] for c in job.get("input_components", [])]
        target_types = [c["component_type"] for c in job.get("output_components", [])]

        rows.append({
            "job_name": job["name"],
            "job_folder": job["folder"],
            "description": "",
            "complexity": complexity,
            "complexity_score": round(total_score, 2),
            "pattern": pattern,
            "source_type": ", ".join(source_types) if source_types else "N/A",
            "target_type": ", ".join(target_types) if target_types else "N/A",
            "components_used": ", ".join(component_types),
            "component_count": job.get("component_count", 0),
            "has_custom_code": has_custom,
            "estimated_effort_hours": 2 if complexity == "Simple" else (8 if complexity == "Medium" else 20),
            "target_fabric": target,
            "priority": "P2",
            "status": "Not Started",
            "notes": "",
        })

    return pd.DataFrame(rows)


@click.command()
@click.option("--input", "-i", "input_path", required=True, help="Path to parsed_jobs.json")
@click.option("--output", "-o", "output_path", required=True, help="Path to output CSV")
def main(input_path: str, output_path: str):
    """Generate an inventory CSV from parsed Talend jobs."""
    logger.info(f"Loading parsed jobs from {input_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    df = generate_inventory(jobs)

    df.to_csv(output_path, index=False)
    logger.info(f"Inventory saved to {output_path} ({len(df)} jobs)")

    # Print summary
    print(f"\n{'='*60}")
    print(f"Total jobs: {len(df)}")
    print(f"\nBy complexity:")
    print(df["complexity"].value_counts().to_string())
    print(f"\nBy target:")
    print(df["target_fabric"].value_counts().to_string())
    print(f"\nBy pattern:")
    print(df["pattern"].value_counts().to_string())
    print(f"\nEstimated total effort: {df['estimated_effort_hours'].sum()} hours")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
