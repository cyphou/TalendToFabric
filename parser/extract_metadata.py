"""
Talend Metadata Extractor — Extracts database connections, schemas,
and transformation logic from parsed Talend job metadata.

Usage:
    python extract_metadata.py --input parsed_jobs.json --output metadata/
"""

import json
import os
import logging
from pathlib import Path
from typing import Dict, List, Any

import click

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class MetadataExtractor:
    """Extracts structured metadata from parsed Talend jobs."""

    # Talend connection parameter names
    CONNECTION_PARAMS = {
        "HOST": "host",
        "PORT": "port",
        "DBNAME": "database",
        "SCHEMA_DB": "schema",
        "USER": "username",
        "PASS": "password",
        "DB_VERSION": "version",
        "TABLE": "table_name",
        "QUERY": "query",
        "FILENAME": "filename",
        "URL": "url",
    }

    def __init__(self, jobs: List[Dict[str, Any]]):
        self.jobs = jobs

    def extract_connections(self) -> List[Dict[str, Any]]:
        """Extract all database/file connections used across all jobs."""
        connections = []
        seen = set()

        for job in self.jobs:
            for component in job.get("components", []):
                comp_type = component.get("component_type", "")
                params = component.get("parameters", {})

                # Detect connection-related components
                if any(kw in comp_type.lower() for kw in ("input", "output", "connection", "sftp", "ftp", "rest")):
                    conn_key = f"{comp_type}:{params.get('HOST', '')}:{params.get('DBNAME', '')}:{params.get('TABLE', '')}"

                    if conn_key not in seen:
                        seen.add(conn_key)
                        conn_info = {
                            "job_name": job["name"],
                            "component_type": comp_type,
                            "unique_name": component.get("unique_name", ""),
                        }
                        # Map known parameters
                        for talend_key, friendly_key in self.CONNECTION_PARAMS.items():
                            if talend_key in params:
                                conn_info[friendly_key] = params[talend_key]

                        connections.append(conn_info)

        return connections

    def extract_schemas(self) -> List[Dict[str, Any]]:
        """Extract schema definitions (column metadata) from components."""
        schemas = []

        for job in self.jobs:
            for component in job.get("components", []):
                params = component.get("parameters", {})
                # Look for schema-related parameters
                schema_cols = []
                for key, value in params.items():
                    if "SCHEMA" in key.upper() or "COLUMN" in key.upper():
                        schema_cols.append({"param": key, "value": value})

                if schema_cols:
                    schemas.append({
                        "job_name": job["name"],
                        "component": component.get("component_type"),
                        "unique_name": component.get("unique_name"),
                        "schema_params": schema_cols,
                    })

        return schemas

    def extract_transformations(self) -> List[Dict[str, Any]]:
        """Extract transformation logic (tMap expressions, filters, etc.)."""
        transforms = []

        for job in self.jobs:
            for component in job.get("components", []):
                comp_type = component.get("component_type", "")
                params = component.get("parameters", {})

                if component.get("is_transformation"):
                    transform = {
                        "job_name": job["name"],
                        "component_type": comp_type,
                        "unique_name": component.get("unique_name", ""),
                        "parameters": params,
                    }

                    # Extract specific transformation details
                    if comp_type == "tMap":
                        transform["type"] = "mapping"
                        transform["expressions"] = {
                            k: v for k, v in params.items()
                            if "EXPRESSION" in k.upper() or "MAP" in k.upper()
                        }
                    elif comp_type in ("tFilter", "tFilterRow"):
                        transform["type"] = "filter"
                        transform["conditions"] = {
                            k: v for k, v in params.items()
                            if "CONDITION" in k.upper() or "FILTER" in k.upper()
                        }
                    elif comp_type in ("tAggregate", "tAggregateRow"):
                        transform["type"] = "aggregation"
                        transform["aggregations"] = {
                            k: v for k, v in params.items()
                            if "GROUP" in k.upper() or "OPERATION" in k.upper()
                        }
                    elif comp_type in ("tSort", "tSortRow"):
                        transform["type"] = "sort"
                    elif comp_type == "tUniqRow":
                        transform["type"] = "deduplicate"
                    elif comp_type == "tJoin":
                        transform["type"] = "join"

                    transforms.append(transform)

        return transforms

    def extract_custom_code(self) -> List[Dict[str, Any]]:
        """Extract custom Java/Groovy code blocks."""
        code_blocks = []

        for job in self.jobs:
            for component in job.get("components", []):
                comp_type = component.get("component_type", "")
                params = component.get("parameters", {})

                if comp_type in ("tJava", "tJavaRow", "tJavaFlex", "tGroovy"):
                    code = {
                        "job_name": job["name"],
                        "component_type": comp_type,
                        "unique_name": component.get("unique_name", ""),
                    }
                    # Extract code content
                    for key, value in params.items():
                        if "CODE" in key.upper() or "SCRIPT" in key.upper():
                            code["code_content"] = value

                    code_blocks.append(code)

        return code_blocks

    def extract_sql_statements(self) -> List[Dict[str, Any]]:
        """Extract embedded SQL queries from components."""
        sql_statements = []

        for job in self.jobs:
            for component in job.get("components", []):
                params = component.get("parameters", {})

                for key, value in params.items():
                    if key.upper() in ("QUERY", "SQL_QUERY", "TABLE_ACTION_QUERY"):
                        if value and len(value.strip()) > 5:
                            sql_statements.append({
                                "job_name": job["name"],
                                "component": component.get("component_type"),
                                "unique_name": component.get("unique_name", ""),
                                "param_name": key,
                                "sql": value,
                            })

        return sql_statements


@click.command()
@click.option("--input", "-i", "input_path", required=True, help="Path to parsed_jobs.json")
@click.option("--output", "-o", "output_path", required=True, help="Path to metadata output folder")
def main(input_path: str, output_path: str):
    """Extract metadata from parsed Talend jobs."""
    logger.info(f"Loading parsed jobs from {input_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    extractor = MetadataExtractor(jobs)

    os.makedirs(output_path, exist_ok=True)

    # Extract and save each metadata type
    extractions = {
        "connections.json": extractor.extract_connections(),
        "schemas.json": extractor.extract_schemas(),
        "transformations.json": extractor.extract_transformations(),
        "custom_code.json": extractor.extract_custom_code(),
        "sql_statements.json": extractor.extract_sql_statements(),
    }

    for filename, data in extractions.items():
        output_file = os.path.join(output_path, filename)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        logger.info(f"Saved {len(data)} items to {output_file}")

    logger.info("Metadata extraction complete.")


if __name__ == "__main__":
    main()
