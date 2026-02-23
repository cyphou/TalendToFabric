"""
Talend Job Parser — Parses Talend Studio exported .item and .properties XML files
to extract job definitions, components, connections, and metadata.

Usage:
    python parse_talend_jobs.py --input <talend_export_folder> --output <output_folder>
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

import click
from lxml import etree
from rich.console import Console
from rich.table import Table

console = Console()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Talend XML namespaces
TALEND_NS = {
    "talendfile": "platform:/resource/org.talend.model/model/TalendFile.xsd",
    "xmi": "http://www.omg.org/XMI",
}


class TalendJobParser:
    """Parses Talend .item XML files to extract job structure and metadata."""

    def __init__(self, input_path: str):
        self.input_path = Path(input_path)
        self.jobs: List[Dict[str, Any]] = []

    def parse_all(self) -> List[Dict[str, Any]]:
        """Parse all .item files found in the input folder recursively."""
        item_files = list(self.input_path.rglob("*.item"))
        logger.info(f"Found {len(item_files)} .item files to parse")

        for item_file in item_files:
            try:
                job = self.parse_item_file(item_file)
                if job:
                    self.jobs.append(job)
                    logger.info(f"Parsed: {job['name']}")
            except Exception as e:
                logger.error(f"Failed to parse {item_file}: {e}")

        return self.jobs

    def parse_item_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Parse a single Talend .item XML file."""
        try:
            tree = etree.parse(str(file_path))
            root = tree.getroot()
        except etree.XMLSyntaxError as e:
            logger.warning(f"XML parse error in {file_path}: {e}")
            return None

        job_name = file_path.stem
        job_folder = str(file_path.parent.relative_to(self.input_path))

        # Extract components (nodes)
        components = self._extract_components(root)

        # Extract connections between components
        connections = self._extract_connections(root)

        # Extract context parameters
        context_params = self._extract_context_params(root)

        # Extract subjob info
        subjobs = self._extract_subjobs(root)

        # Determine job characteristics
        has_custom_code = any(
            c["component_type"] in ("tJava", "tJavaRow", "tJavaFlex", "tGroovy", "tPythonRow", "tJavaScript")
            for c in components
        )

        component_types = [c["component_type"] for c in components]

        # Categorize components
        categorized = {}
        for c in components:
            cat = c.get("category", "unknown")
            categorized.setdefault(cat, []).append(c)

        return {
            "name": job_name,
            "folder": job_folder,
            "file_path": str(file_path),
            "components": components,
            "component_types": component_types,
            "component_count": len(components),
            "connections": connections,
            "context_params": context_params,
            "subjobs": subjobs,
            "has_custom_code": has_custom_code,
            "input_components": [c for c in components if c.get("is_input")],
            "output_components": [c for c in components if c.get("is_output")],
            "transformation_components": [
                c for c in components if c.get("is_transformation")
            ],
            "flow_control_components": [
                c for c in components if c.get("is_flow_control")
            ],
            "error_handling_components": [
                c for c in components if c.get("is_error_handling")
            ],
            "custom_code_components": [
                c for c in components if c.get("is_custom_code")
            ],
            "db_operation_components": [
                c for c in components if c.get("is_db_operation")
            ],
            "file_utility_components": [
                c for c in components if c.get("is_file_utility")
            ],
            "utility_components": [
                c for c in components if c.get("is_utility")
            ],
            "unknown_components": [
                c for c in components if c.get("category") == "unknown"
            ],
            "categorized": categorized,
        }

    def _extract_components(self, root: etree._Element) -> List[Dict[str, Any]]:
        """Extract all components (nodes) from the job XML."""
        components = []

        # Look for node elements — Talend uses different schemas depending on version
        for node in root.iter():
            if callable(node.tag):
                continue
            tag = etree.QName(node.tag).localname if "}" in node.tag else node.tag
            if tag == "node":
                comp = self._parse_node(node)
                if comp:
                    components.append(comp)

        return components

    def _parse_node(self, node: etree._Element) -> Optional[Dict[str, Any]]:
        """Parse a single Talend node (component) element."""
        component_type = node.get("componentName", "")
        unique_name = node.get("componentVersion", "")

        # Try to get the unique name from elementParameter
        for param in node.iter():
            if callable(param.tag):
                continue
            tag = etree.QName(param.tag).localname if "}" in param.tag else param.tag
            if tag == "elementParameter":
                if param.get("name") == "UNIQUE_NAME":
                    unique_name = param.get("value", "")

        if not component_type:
            return None

        # ── Classify component ──────────────────────────────────────────
        # Input prefixes: any component prefixed by these AND containing "Input" / "Get"
        input_prefixes = (
            # Database sources
            "tOracle", "tPostgresql", "tMSSql", "tMysql", "tDB2", "tTeradata",
            "tSybase", "tRedshift", "tSnowflake", "tHive", "tDB", "tSQLite",
            "tInformix", "tAccess",
            # File sources
            "tFileInput", "tFileInputDelimited", "tFileInputExcel", "tFileInputJSON",
            "tFileInputXML", "tFileInputParquet", "tFileInputAvro", "tFileInputORC",
            "tFileInputPositional", "tFileInputRegex", "tFileInputFullRow",
            "tFileInputRaw", "tFileInputLDIF",
            # Cloud storage
            "tS3", "tAzureBlob", "tAzureDataLake", "tGCS", "tHDFS",
            # NoSQL
            "tMongo", "tCosmosDB", "tCassandra", "tCouchDB", "tNeo4j",
            "tDynamoDB", "tElasticsearch", "tHBase", "tRedis",
            # Messaging
            "tKafka", "tJMS", "tActiveMQ", "tRabbitMQ", "tAzureEventHub", "tMQSeries",
            # API / Protocol
            "tREST", "tHTTP", "tSOAP", "tWebService",
            "tSalesforce", "tSAP", "tLDAP", "tGraphQL", "tOData", "tServiceNow",
            # SFTP / FTP
            "tSFTP", "tFTP",
            # ELT
            "tELT",
        )

        # Output prefixes: any component prefixed by these AND containing "Output" / "Put"
        output_prefixes = (
            # Database sinks
            "tOracle", "tPostgresql", "tMSSql", "tMysql", "tDB2", "tTeradata",
            "tRedshift", "tSnowflake", "tHive", "tDB", "tSQLite",
            # File sinks
            "tFileOutput", "tFileOutputDelimited", "tFileOutputExcel",
            "tFileOutputJSON", "tFileOutputXML", "tFileOutputParquet",
            "tFileOutputAvro", "tFileOutputORC", "tFileOutputPositional",
            # Cloud storage
            "tS3", "tAzureBlob", "tAzureDataLake", "tGCS", "tHDFS",
            # NoSQL
            "tMongo", "tCosmosDB", "tCassandra", "tCouchDB", "tNeo4j",
            "tDynamoDB", "tElasticsearch", "tHBase", "tRedis",
            # Messaging
            "tKafka", "tJMS", "tActiveMQ", "tRabbitMQ", "tAzureEventHub",
            # API / Protocol
            "tSalesforce", "tSAP", "tLDAP",
            # SFTP / FTP
            "tSFTP", "tFTP",
            # ELT
            "tELT",
            # Logging / Debug
            "tLog",
        )

        # Transformation components (exact match)
        transform_types = (
            # Core transforms
            "tMap", "tFilter", "tFilterRow", "tAggregate", "tAggregateRow",
            "tSort", "tSortRow", "tUniqRow", "tDenormalize", "tNormalize",
            "tReplace", "tConvertType", "tUnite", "tSplitRow", "tJoin",
            "tXMLMap",
            # Advanced transforms
            "tReplicate", "tSampleRow", "tHashRow", "tGenKey",
            "tRowGenerator", "tFixedFlowInput",
            "tExtractXMLField", "tExtractJSONField",
            "tExtractRegexFields", "tExtractDelimitedFields",
            "tFuzzyMatch", "tRecordMatching", "tMatchGroup",
            "tSchemaComplianceCheck", "tFlowMeter", "tWindowInput",
            "tCacheIn", "tCacheOut", "tBufferInput", "tBufferOutput",
            "tSetDynamicSchema", "tDataMasking",
            "tDenormalizeRow", "tNormalizeRow",
            "tFillEmptyField", "tFilterColumns", "tRenameColumns",
            "tSortWithinGroups", "tLevenshteinDistance",
            # ELT transforms
            "tELTMap", "tELTAggregate", "tELTFilter", "tELTSort",
            "tELTUnite", "tELTJoin", "tELTDistinct",
        )

        # Flow control components
        flow_control_types = (
            "tRunJob", "tPreJob", "tPostJob", "tWarn", "tDie",
            "tFlowToIterate", "tLoop", "tForEach", "tParallelize",
            "tContextLoad", "tIfRow", "tTimeout", "tFlowControl",
            "tIterateToFlow",
        )

        # Error handling components
        error_handling_types = (
            "tCatch", "tCatchError", "tLogCatcher", "tStatCatcher",
            "tAssert", "tAssertCatcher", "tConnectionReset",
            "tOnError", "tOnComponentOk", "tOnComponentError",
            "tOnSubjobOk", "tOnSubjobError",
        )

        # Custom code components
        custom_code_types = (
            "tJava", "tJavaRow", "tJavaFlex", "tGroovy",
            "tPythonRow", "tJavaScript", "tSetKeystore", "tLibraryLoad",
        )

        # DB operation components
        db_operation_types = (
            "tOracleConnection", "tPostgresqlConnection", "tMSSqlConnection",
            "tMysqlConnection", "tDBConnection",
            "tOracleCommit", "tDBCommit", "tOracleRollback", "tDBRollback",
            "tOracleSP", "tDBSP", "tDBRow",
            "tDBBulkExec", "tOracleBulkExec",
            "tCreateTable", "tDropTable", "tAlterTable",
            "tDBTableList", "tDBColumnList",
            "tOracleClose", "tDBClose",
            "tSetDBAutoCommit", "tDBLastInsertId", "tDBValidation",
        )

        # File / Directory utility components
        file_utility_types = (
            "tFileExist", "tFileDelete", "tFileCopy", "tFileMove", "tFileRename",
            "tFileList", "tFileProperties", "tFileCompare",
            "tFileArchive", "tFileUnarchive", "tFileTouch",
            "tDirectoryCreate", "tDirectoryList", "tDirectoryDelete",
            "tFTPGet", "tFTPPut", "tFTPConnection", "tFTPDelete", "tFTPList",
            "tFTPRename",
            "tSFTPGet", "tSFTPPut", "tSFTPConnection", "tSFTPDelete",
            "tSFTPRename", "tSFTPList",
        )

        # General utility components
        utility_types = (
            "tLogRow", "tSendMail", "tSystem", "tSleep",
            "tSetGlobalVar", "tContextDump", "tContextEnvironment",
            "tSSH", "tSSHRemote", "tMsgBox", "tSetEncoding",
            "tPrintVars", "tTimestamp",
        )

        # Classification logic
        is_input = (
            any(component_type.startswith(p) and ("Input" in component_type or "Get" in component_type)
                for p in input_prefixes)
            or component_type.endswith("Input")
        )
        is_output = (
            any(component_type.startswith(p) and ("Output" in component_type or "Put" in component_type)
                for p in output_prefixes)
            or component_type.endswith("Output")
        )
        is_transformation = component_type in transform_types
        is_flow_control = component_type in flow_control_types
        is_error_handling = component_type in error_handling_types
        is_custom_code = component_type in custom_code_types
        is_db_operation = component_type in db_operation_types
        is_file_utility = component_type in file_utility_types
        is_utility = component_type in utility_types

        # Determine category
        if is_input:
            category = "input"
        elif is_output:
            category = "output"
        elif is_transformation:
            category = "transformation"
        elif is_flow_control:
            category = "flow_control"
        elif is_error_handling:
            category = "error_handling"
        elif is_custom_code:
            category = "custom_code"
        elif is_db_operation:
            category = "db_operation"
        elif is_file_utility:
            category = "file_utility"
        elif is_utility:
            category = "utility"
        else:
            category = "unknown"

        # Extract parameters
        parameters = {}
        for param in node.iter():
            if callable(param.tag):
                continue
            tag = etree.QName(param.tag).localname if "}" in param.tag else param.tag
            if tag == "elementParameter":
                param_name = param.get("name", "")
                param_value = param.get("value", "")
                if param_name and param_value:
                    parameters[param_name] = param_value

        return {
            "component_type": component_type,
            "unique_name": unique_name,
            "category": category,
            "is_input": is_input,
            "is_output": is_output,
            "is_transformation": is_transformation,
            "is_flow_control": is_flow_control,
            "is_error_handling": is_error_handling,
            "is_custom_code": is_custom_code,
            "is_db_operation": is_db_operation,
            "is_file_utility": is_file_utility,
            "is_utility": is_utility,
            "parameters": parameters,
        }

    def _extract_connections(self, root: etree._Element) -> List[Dict[str, Any]]:
        """Extract connections between components."""
        connections = []

        for elem in root.iter():
            if callable(elem.tag):
                continue
            tag = etree.QName(elem.tag).localname if "}" in elem.tag else elem.tag
            if tag == "connection":
                conn = {
                    "source": elem.get("source", ""),
                    "target": elem.get("target", ""),
                    "connector_name": elem.get("connectorName", ""),
                    "line_style": elem.get("lineStyle", ""),
                    "label": elem.get("label", ""),
                }
                connections.append(conn)

        return connections

    def _extract_context_params(self, root: etree._Element) -> List[Dict[str, str]]:
        """Extract context parameters (variables) from the job."""
        params = []

        for elem in root.iter():
            if callable(elem.tag):
                continue
            tag = etree.QName(elem.tag).localname if "}" in elem.tag else elem.tag
            if tag == "contextParameter":
                params.append({
                    "name": elem.get("name", ""),
                    "type": elem.get("type", ""),
                    "value": elem.get("value", ""),
                    "prompt": elem.get("prompt", ""),
                    "comment": elem.get("comment", ""),
                })

        return params

    def _extract_subjobs(self, root: etree._Element) -> List[Dict[str, Any]]:
        """Extract subjob definitions."""
        subjobs = []

        for elem in root.iter():
            if callable(elem.tag):
                continue
            tag = etree.QName(elem.tag).localname if "}" in elem.tag else elem.tag
            if tag == "subjob":
                subjobs.append({
                    "name": elem.get("name", ""),
                    "start_node": elem.get("startNode", ""),
                })

        return subjobs


def display_summary(jobs: List[Dict[str, Any]]) -> None:
    """Display a rich table summary of parsed jobs."""
    table = Table(title="Talend Job Inventory")
    table.add_column("Job Name", style="cyan")
    table.add_column("Folder", style="dim")
    table.add_column("Components", justify="right")
    table.add_column("Custom Code", justify="center")
    table.add_column("Input", justify="right")
    table.add_column("Output", justify="right")
    table.add_column("Transforms", justify="right")

    for job in jobs:
        table.add_row(
            job["name"],
            job["folder"],
            str(job["component_count"]),
            "Yes" if job["has_custom_code"] else "No",
            str(len(job["input_components"])),
            str(len(job["output_components"])),
            str(len(job["transformation_components"])),
        )

    console.print(table)


@click.command()
@click.option("--input", "-i", "input_path", required=True, help="Path to Talend export folder")
@click.option("--output", "-o", "output_path", required=True, help="Path to output folder")
@click.option("--format", "-f", "output_format", default="json", type=click.Choice(["json", "csv"]))
def main(input_path: str, output_path: str, output_format: str):
    """Parse Talend exported jobs and generate inventory."""
    console.print(f"[bold green]Talend Job Parser[/bold green]")
    console.print(f"Input: {input_path}")
    console.print(f"Output: {output_path}")

    parser = TalendJobParser(input_path)
    jobs = parser.parse_all()

    if not jobs:
        console.print("[yellow]No jobs found. Check the input path.[/yellow]")
        return

    display_summary(jobs)

    # Save output
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, f"parsed_jobs.{output_format}")

    if output_format == "json":
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, default=str)
    elif output_format == "csv":
        import pandas as pd
        flat_jobs = []
        for job in jobs:
            flat_jobs.append({
                "name": job["name"],
                "folder": job["folder"],
                "component_count": job["component_count"],
                "component_types": ", ".join(job["component_types"]),
                "has_custom_code": job["has_custom_code"],
                "input_count": len(job["input_components"]),
                "output_count": len(job["output_components"]),
                "transform_count": len(job["transformation_components"]),
                "context_params": len(job["context_params"]),
            })
        df = pd.DataFrame(flat_jobs)
        df.to_csv(output_file, index=False)

    console.print(f"[bold green]Output saved to {output_file}[/bold green]")


if __name__ == "__main__":
    main()
