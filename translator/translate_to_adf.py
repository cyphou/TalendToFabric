"""
Talend → Data Factory Translator
Generates ADF pipeline JSON from parsed Talend job metadata.

Usage:
    python translate_to_adf.py --inventory ../inventory/talend_job_inventory.csv --output ../output/adf/
"""

import os
import json
import copy
import logging
from pathlib import Path
from typing import Dict, List, Any

import click
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Base paths
SCRIPT_DIR = Path(__file__).parent
TEMPLATE_DIR = SCRIPT_DIR.parent / "templates" / "data_factory"
MAPPING_DIR = SCRIPT_DIR.parent / "mapping"


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_component_map() -> dict:
    return load_json(MAPPING_DIR / "component_map.json").get("mappings", {})


def load_connection_map() -> dict:
    return load_json(MAPPING_DIR / "connection_map.json").get("mappings", {})


def load_pipeline_template() -> dict:
    return load_json(TEMPLATE_DIR / "pipeline_template.json")


def load_copy_activity_template() -> dict:
    return load_json(TEMPLATE_DIR / "copy_activity.json")


class ADFTranslator:
    """Translates Talend job definitions to ADF pipeline JSON."""

    def __init__(self):
        self.component_map = load_component_map()
        self.connection_map = load_connection_map()
        self.pipeline_template = load_pipeline_template()
        self.copy_template = load_copy_activity_template()

    def translate_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Translate a single Talend job to an ADF pipeline definition."""
        job_name = job.get("job_name", "unknown")
        pattern = job.get("pattern", "Copy")

        logger.info(f"Translating job: {job_name} (pattern={pattern})")

        if pattern == "Orchestration":
            return self._create_orchestration_pipeline(job)
        elif pattern == "Copy":
            return self._create_copy_pipeline(job)
        elif pattern == "FileTransfer":
            return self._create_file_transfer_pipeline(job)
        elif pattern == "FileManagement":
            return self._create_file_management_pipeline(job)
        elif pattern == "DDL":
            return self._create_ddl_pipeline(job)
        elif pattern == "API":
            return self._create_api_pipeline(job)
        else:
            return self._create_dataflow_pipeline(job)

    def _resolve_source_type(self, job: Dict[str, Any]) -> str:
        """Resolve the ADF source type from job metadata using the component map."""
        comp_types = str(job.get("component_types", ""))
        for comp_name, mapping in self.component_map.items():
            if comp_name in comp_types and mapping.get("category") in ("db_input", "file_input", "cloud_storage", "nosql", "api"):
                return mapping.get("data_factory_source_type", "")
        return ""

    def _resolve_sink_type(self, job: Dict[str, Any]) -> str:
        """Resolve the ADF sink type from job metadata using the component map."""
        comp_types = str(job.get("component_types", ""))
        for comp_name, mapping in self.component_map.items():
            if comp_name in comp_types and mapping.get("category") in ("db_output", "file_output", "cloud_storage", "nosql", "api"):
                return mapping.get("data_factory_sink_type", "")
        return ""

    def _create_copy_pipeline(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Create a simple copy pipeline."""
        pipeline = copy.deepcopy(self.pipeline_template)
        job_name = job.get("job_name", "unknown")

        # Replace placeholders
        pipeline["name"] = f"pl_{job_name}"
        pipeline["properties"]["description"] = f"Migrated from Talend job: {job_name}"
        pipeline["properties"]["annotations"] = ["MigratedFromTalend", job_name]

        # Create copy activity
        activity = copy.deepcopy(self.copy_template)
        activity["name"] = f"Copy_{job_name}"
        activity["description"] = f"Copy data — migrated from Talend {job_name}"

        # Resolve source and sink types from component map
        source_type = self._resolve_source_type(job)
        sink_type = self._resolve_sink_type(job)
        if source_type:
            activity["typeProperties"]["source"]["type"] = source_type
        if sink_type:
            activity["typeProperties"]["sink"]["type"] = sink_type

        pipeline["properties"]["activities"] = [activity]

        return pipeline

    def _create_orchestration_pipeline(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Create an orchestration pipeline with Execute Pipeline activities."""
        pipeline = copy.deepcopy(self.pipeline_template)
        job_name = job.get("job_name", "unknown")

        pipeline["name"] = f"pl_orchestrate_{job_name}"
        pipeline["properties"]["description"] = f"Orchestration pipeline — migrated from Talend {job_name}"

        # Load dependency map to find child jobs
        dep_map_path = SCRIPT_DIR.parent / "inventory" / "dependency_map.json"
        activities = []

        if dep_map_path.exists():
            dep_map = load_json(dep_map_path)
            job_deps = dep_map.get("jobs", {}).get(job_name, {})
            children = job_deps.get("children", [])

            previous_activity = None
            for child in children:
                child_name = child.get("job_name", "")
                activity = {
                    "name": f"Execute_{child_name}",
                    "type": "ExecutePipeline",
                    "dependsOn": [],
                    "policy": {
                        "secureInput": False,
                    },
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": f"pl_{child_name}",
                            "type": "PipelineReference",
                        },
                        "waitOnCompletion": True,
                        "parameters": {
                            "RunDate": {"value": "@pipeline().parameters.RunDate", "type": "Expression"},
                        },
                    },
                }

                # Handle dependencies based on Talend job order
                condition = child.get("condition", "always")
                if previous_activity and condition == "on_success_of_previous":
                    activity["dependsOn"] = [{
                        "activity": previous_activity,
                        "dependencyConditions": ["Succeeded"],
                    }]
                elif previous_activity and condition == "always":
                    activity["dependsOn"] = [{
                        "activity": previous_activity,
                        "dependencyConditions": ["Completed"],
                    }]

                activities.append(activity)
                previous_activity = activity["name"]

        pipeline["properties"]["activities"] = activities
        return pipeline

    def _create_dataflow_pipeline(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Create a pipeline that invokes a Mapping Data Flow."""
        pipeline = copy.deepcopy(self.pipeline_template)
        job_name = job.get("job_name", "unknown")

        pipeline["name"] = f"pl_{job_name}"
        pipeline["properties"]["description"] = f"Data flow pipeline — migrated from Talend {job_name}"

        activity = {
            "name": f"DataFlow_{job_name}",
            "type": "ExecuteDataFlow",
            "dependsOn": [],
            "policy": {
                "timeout": "0.02:00:00",
                "retry": 2,
                "retryIntervalInSeconds": 30,
            },
            "typeProperties": {
                "dataflow": {
                    "referenceName": f"df_{job_name}",
                    "type": "DataFlowReference",
                },
                "compute": {
                    "coreCount": 8,
                    "computeType": "General",
                },
            },
        }

        pipeline["properties"]["activities"] = [activity]
        return pipeline

    def _create_file_transfer_pipeline(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Create a pipeline for FTP/SFTP file transfer jobs."""
        pipeline = copy.deepcopy(self.pipeline_template)
        job_name = job.get("job_name", "unknown")

        pipeline["name"] = f"pl_{job_name}"
        pipeline["properties"]["description"] = f"File transfer pipeline — migrated from Talend {job_name}"

        activity = copy.deepcopy(self.copy_template)
        activity["name"] = f"Copy_{job_name}"
        activity["description"] = f"File transfer — migrated from Talend {job_name}"

        # Determine source/sink from component map
        source_type = self._resolve_source_type(job)
        if source_type:
            activity["typeProperties"]["source"]["type"] = source_type

        pipeline["properties"]["activities"] = [activity]
        return pipeline

    def _create_file_management_pipeline(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Create a pipeline for file management jobs (delete, move, list, archive)."""
        pipeline = copy.deepcopy(self.pipeline_template)
        job_name = job.get("job_name", "unknown")

        pipeline["name"] = f"pl_{job_name}"
        pipeline["properties"]["description"] = f"File management pipeline — migrated from Talend {job_name}"

        activities = []

        # Get Metadata activity for file listing
        get_metadata_activity = {
            "name": f"GetMetadata_{job_name}",
            "type": "GetMetadata",
            "dependsOn": [],
            "policy": {"timeout": "0.00:10:00", "retry": 1},
            "typeProperties": {
                "dataset": {
                    "referenceName": f"ds_{job_name}_source",
                    "type": "DatasetReference",
                },
                "fieldList": ["childItems", "exists", "itemName", "size", "lastModified"],
            },
        }
        activities.append(get_metadata_activity)

        # Delete activity if job involves file deletion
        comp_types = str(job.get("component_types", ""))
        if "tFileDelete" in comp_types or "tDirectoryDelete" in comp_types:
            delete_activity = {
                "name": f"Delete_{job_name}",
                "type": "Delete",
                "dependsOn": [{"activity": f"GetMetadata_{job_name}", "dependencyConditions": ["Succeeded"]}],
                "policy": {"timeout": "0.00:10:00", "retry": 1},
                "typeProperties": {
                    "dataset": {
                        "referenceName": f"ds_{job_name}_source",
                        "type": "DatasetReference",
                    },
                    "enableLogging": True,
                },
            }
            activities.append(delete_activity)

        pipeline["properties"]["activities"] = activities
        return pipeline

    def _create_ddl_pipeline(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Create a pipeline for DDL operations (create/drop/alter table)."""
        pipeline = copy.deepcopy(self.pipeline_template)
        job_name = job.get("job_name", "unknown")

        pipeline["name"] = f"pl_{job_name}"
        pipeline["properties"]["description"] = f"DDL operations pipeline — migrated from Talend {job_name}"

        activity = {
            "name": f"Script_{job_name}",
            "type": "Script",
            "dependsOn": [],
            "policy": {"timeout": "0.00:30:00", "retry": 2},
            "typeProperties": {
                "scripts": [{
                    "type": "NonQuery",
                    "text": "-- TODO: Add DDL statements from Talend job",
                }],
            },
            "linkedServiceName": {
                "referenceName": "ls_target_database",
                "type": "LinkedServiceReference",
            },
        }

        pipeline["properties"]["activities"] = [activity]
        return pipeline

    def _create_api_pipeline(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Create a pipeline for API / Web Service calls."""
        pipeline = copy.deepcopy(self.pipeline_template)
        job_name = job.get("job_name", "unknown")

        pipeline["name"] = f"pl_{job_name}"
        pipeline["properties"]["description"] = f"API pipeline — migrated from Talend {job_name}"

        activity = {
            "name": f"WebCall_{job_name}",
            "type": "WebActivity",
            "dependsOn": [],
            "policy": {"timeout": "0.00:10:00", "retry": 2, "retryIntervalInSeconds": 30},
            "typeProperties": {
                "url": "https://TODO-update-url",
                "method": "GET",
                "headers": {},
            },
        }

        pipeline["properties"]["activities"] = [activity]
        return pipeline


@click.command()
@click.option("--inventory", "-i", required=True, help="Path to talend_job_inventory.csv")
@click.option("--output", "-o", required=True, help="Output folder for ADF pipeline JSON files")
@click.option("--filter-target", default="DataFactory", help="Only translate jobs targeting this platform")
def main(inventory: str, output: str, filter_target: str):
    """Generate ADF pipeline JSON from Talend inventory."""
    logger.info(f"Loading inventory from {inventory}")
    df = pd.read_csv(inventory)

    # Filter for Data Factory target jobs
    adf_jobs = df[df["target_fabric"] == filter_target]
    logger.info(f"Found {len(adf_jobs)} jobs targeting {filter_target}")

    translator = ADFTranslator()
    os.makedirs(output, exist_ok=True)

    for _, row in adf_jobs.iterrows():
        job = row.to_dict()
        pipeline = translator.translate_job(job)

        output_file = os.path.join(output, f"{pipeline.get('name', 'unknown')}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(pipeline, f, indent=2, default=str)
        logger.info(f"Generated: {output_file}")

    logger.info(f"Translation complete. {len(adf_jobs)} pipelines generated in {output}")


if __name__ == "__main__":
    main()
