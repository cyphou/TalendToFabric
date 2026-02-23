"""
Unit tests for newly added features:
  - tHashInput / tHashOutput classification
  - CDC component classification
  - Big Data component classification
  - DB-specific operations (Row, SP, BulkExec, Commit, Close)
  - Schema column extraction (_extract_schemas)
  - tMap expression extraction (_extract_tmap_details)
  - New Spark templates existence
  - Component map & connection map coverage
"""

import json
import pytest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
MAPPING_DIR = PROJECT_ROOT / "mapping"
TEMPLATES_DIR = PROJECT_ROOT / "templates" / "spark"


# ═══════════════════════════════════════════════════════════════════════════
# 1. HASH COMPONENT CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestHashComponents:
    """Validate tHashInput / tHashOutput are classified as transformation."""

    def test_hash_output_in_transformations(self, parsed_cdc_hash):
        transforms = parsed_cdc_hash["transformation_components"]
        types = [c["component_type"] for c in transforms]
        assert "tHashOutput" in types

    def test_hash_input_in_transformations(self, parsed_cdc_hash):
        transforms = parsed_cdc_hash["transformation_components"]
        types = [c["component_type"] for c in transforms]
        assert "tHashInput" in types

    def test_hash_output_category(self, parsed_cdc_hash):
        ho = next(c for c in parsed_cdc_hash["components"] if c["component_type"] == "tHashOutput")
        assert ho["category"] == "transformation"
        assert ho["is_transformation"] is True

    def test_hash_input_category(self, parsed_cdc_hash):
        hi = next(c for c in parsed_cdc_hash["components"] if c["component_type"] == "tHashInput")
        assert hi["category"] == "transformation"
        assert hi["is_transformation"] is True

    def test_hash_output_has_hash_name_param(self, parsed_cdc_hash):
        ho = next(c for c in parsed_cdc_hash["components"] if c["component_type"] == "tHashOutput")
        assert ho["parameters"].get("HASH_NAME") == "customerCache"


# ═══════════════════════════════════════════════════════════════════════════
# 2. DB-SPECIFIC OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestDBSpecificOps:
    """Validate vendor-specific DB operations are classified as db_operation."""

    def test_mysql_connection(self, parsed_cdc_hash):
        db_ops = parsed_cdc_hash["db_operation_components"]
        types = [c["component_type"] for c in db_ops]
        assert "tMysqlConnection" in types

    def test_mysql_commit(self, parsed_cdc_hash):
        db_ops = parsed_cdc_hash["db_operation_components"]
        types = [c["component_type"] for c in db_ops]
        assert "tMysqlCommit" in types

    def test_mysql_close(self, parsed_cdc_hash):
        db_ops = parsed_cdc_hash["db_operation_components"]
        types = [c["component_type"] for c in db_ops]
        assert "tMysqlClose" in types

    def test_all_db_ops_have_correct_category(self, parsed_cdc_hash):
        db_components = ["tMysqlConnection", "tMysqlCommit", "tMysqlClose"]
        for comp in parsed_cdc_hash["components"]:
            if comp["component_type"] in db_components:
                assert comp["category"] == "db_operation", (
                    f"{comp['component_type']} should be db_operation, got {comp['category']}"
                )


# ═══════════════════════════════════════════════════════════════════════════
# 3. SCHEMA COLUMN EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestSchemaExtraction:
    """Validate _extract_schemas extracts column metadata from <column> elements."""

    def test_schemas_present(self, parsed_cdc_hash):
        schemas = parsed_cdc_hash["schemas"]
        assert isinstance(schemas, list)
        assert len(schemas) > 0

    def test_mysql_input_has_columns(self, parsed_cdc_hash):
        schemas = parsed_cdc_hash["schemas"]
        mysql_schemas = [s for s in schemas if s["component"] == "tMysqlInput"]
        assert len(mysql_schemas) >= 1

    def test_column_details(self, parsed_cdc_hash):
        schemas = parsed_cdc_hash["schemas"]
        mysql_input_schema = next(
            (s for s in schemas if s["unique_name"] == "tMysqlInput_1"), None
        )
        assert mysql_input_schema is not None
        columns = mysql_input_schema["columns"]
        assert len(columns) == 5

        # Check first column (id)
        id_col = next(c for c in columns if c["name"] == "id")
        assert id_col["talend_type"] == "id_Integer"
        assert id_col["db_type"] == "INT"
        assert id_col["key"] == "true"
        assert id_col["nullable"] == "false"

    def test_column_name_extraction(self, parsed_cdc_hash):
        schemas = parsed_cdc_hash["schemas"]
        mysql_input_schema = next(
            (s for s in schemas if s["unique_name"] == "tMysqlInput_1"), None
        )
        col_names = [c["name"] for c in mysql_input_schema["columns"]]
        assert col_names == ["id", "name", "email", "status", "modified_date"]

    def test_email_column_nullable(self, parsed_cdc_hash):
        schemas = parsed_cdc_hash["schemas"]
        mysql_input_schema = next(
            (s for s in schemas if s["unique_name"] == "tMysqlInput_1"), None
        )
        email_col = next(c for c in mysql_input_schema["columns"] if c["name"] == "email")
        assert email_col["nullable"] == "true"

    def test_column_default_value(self, parsed_cdc_hash):
        schemas = parsed_cdc_hash["schemas"]
        mysql_input_schema = next(
            (s for s in schemas if s["unique_name"] == "tMysqlInput_1"), None
        )
        status_col = next(c for c in mysql_input_schema["columns"] if c["name"] == "status")
        assert status_col["default"] == "active"

    def test_column_pattern(self, parsed_cdc_hash):
        schemas = parsed_cdc_hash["schemas"]
        mysql_input_schema = next(
            (s for s in schemas if s["unique_name"] == "tMysqlInput_1"), None
        )
        date_col = next(c for c in mysql_input_schema["columns"] if c["name"] == "modified_date")
        assert date_col["pattern"] == "yyyy-MM-dd HH:mm:ss"


# ═══════════════════════════════════════════════════════════════════════════
# 4. TMAP EXPRESSION EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestTMapExtraction:
    """Validate _extract_tmap_details extracts tMap structure from nodeData."""

    def test_tmap_details_present(self, parsed_cdc_hash):
        tmap_details = parsed_cdc_hash["tmap_details"]
        assert isinstance(tmap_details, list)
        assert len(tmap_details) == 1

    def test_tmap_unique_name(self, parsed_cdc_hash):
        detail = parsed_cdc_hash["tmap_details"][0]
        assert detail["unique_name"] == "tMap_1"

    def test_tmap_input_tables(self, parsed_cdc_hash):
        detail = parsed_cdc_hash["tmap_details"][0]
        input_tables = detail["input_tables"]
        assert len(input_tables) == 2
        names = [t["name"] for t in input_tables]
        assert "orders" in names
        assert "customerCache" in names

    def test_tmap_input_inner_join(self, parsed_cdc_hash):
        detail = parsed_cdc_hash["tmap_details"][0]
        cache_table = next(t for t in detail["input_tables"] if t["name"] == "customerCache")
        assert cache_table["inner_join"] == "true"
        assert cache_table["join_model"] == "INNER_JOIN"

    def test_tmap_output_tables(self, parsed_cdc_hash):
        detail = parsed_cdc_hash["tmap_details"][0]
        output_tables = detail["output_tables"]
        assert len(output_tables) == 2
        names = [t["name"] for t in output_tables]
        assert "enriched_orders" in names
        assert "rejected" in names

    def test_tmap_reject_table(self, parsed_cdc_hash):
        detail = parsed_cdc_hash["tmap_details"][0]
        reject_table = next(t for t in detail["output_tables"] if t["name"] == "rejected")
        assert reject_table["reject"] == "true"
        assert reject_table["reject_inner_join"] == "true"

    def test_tmap_var_table(self, parsed_cdc_hash):
        detail = parsed_cdc_hash["tmap_details"][0]
        var_tables = detail["var_tables"]
        assert len(var_tables) == 1
        assert var_tables[0]["name"] == "Var"
        assert len(var_tables[0]["columns"]) == 1
        assert var_tables[0]["columns"][0]["name"] == "tax_rate"
        assert var_tables[0]["columns"][0]["expression"] == "0.2"

    def test_tmap_output_expressions(self, parsed_cdc_hash):
        detail = parsed_cdc_hash["tmap_details"][0]
        enriched = next(t for t in detail["output_tables"] if t["name"] == "enriched_orders")
        col_names = [c["name"] for c in enriched["columns"]]
        assert "order_id" in col_names
        assert "customer_name" in col_names
        assert "amount_with_tax" in col_names

        # Check computed expression
        amount_tax = next(c for c in enriched["columns"] if c["name"] == "amount_with_tax")
        assert amount_tax["expression"] == "orders.amount * 1.2"

    def test_tmap_all_expressions_collected(self, parsed_cdc_hash):
        detail = parsed_cdc_hash["tmap_details"][0]
        expressions = detail["expressions"]
        assert len(expressions) > 0
        # Should include expressions from all tables
        tables = {e["table"] for e in expressions}
        assert "orders" in tables or "customerCache" in tables
        assert "enriched_orders" in tables

    def test_tmap_lookup_modes(self, parsed_cdc_hash):
        detail = parsed_cdc_hash["tmap_details"][0]
        orders_table = next(t for t in detail["input_tables"] if t["name"] == "orders")
        assert orders_table["matching_mode"] == "ALL_MATCHES"
        assert orders_table["lookup_mode"] == "LOAD_ONCE"

        cache_table = next(t for t in detail["input_tables"] if t["name"] == "customerCache")
        assert cache_table["matching_mode"] == "UNIQUE_MATCH"


# ═══════════════════════════════════════════════════════════════════════════
# 5. OVERALL JOB STRUCTURE (cdc_hash_job)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestCDCHashJobStructure:
    """Validate overall structure of cdc_hash_job fixture."""

    def test_component_count(self, parsed_cdc_hash):
        assert parsed_cdc_hash["component_count"] == 10

    def test_component_types(self, parsed_cdc_hash):
        types = parsed_cdc_hash["component_types"]
        assert "tMysqlConnection" in types
        assert "tMysqlInput" in types
        assert "tHashOutput" in types
        assert "tHashInput" in types
        assert "tMap" in types
        assert "tMysqlOutput" in types
        assert "tLogRow" in types
        assert "tMysqlCommit" in types
        assert "tMysqlClose" in types

    def test_input_components(self, parsed_cdc_hash):
        inputs = parsed_cdc_hash["input_components"]
        types = [c["component_type"] for c in inputs]
        assert "tMysqlInput" in types

    def test_output_components(self, parsed_cdc_hash):
        outputs = parsed_cdc_hash["output_components"]
        types = [c["component_type"] for c in outputs]
        assert "tMysqlOutput" in types

    def test_context_params(self, parsed_cdc_hash):
        ctx = parsed_cdc_hash["context_params"]
        names = [p["name"] for p in ctx]
        assert "db_host" in names
        assert "db_name" in names
        assert "last_cdc_timestamp" in names

    def test_connections_count(self, parsed_cdc_hash):
        conns = parsed_cdc_hash["connections"]
        assert len(conns) == 9

    def test_categorized_keys(self, parsed_cdc_hash):
        cats = parsed_cdc_hash["categorized"]
        assert "input" in cats
        assert "output" in cats
        assert "transformation" in cats
        assert "db_operation" in cats
        assert "utility" in cats  # tLogRow


# ═══════════════════════════════════════════════════════════════════════════
# 6. SPARK TEMPLATES EXISTENCE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestNewSparkTemplates:
    """Verify new Spark template files exist and have content."""

    @pytest.mark.parametrize("template_name", [
        "scd_type1.py",
        "cdc_watermark.py",
        "merge_upsert.py",
    ])
    def test_template_exists(self, template_name):
        path = TEMPLATES_DIR / template_name
        assert path.exists(), f"Template {template_name} not found at {path}"

    @pytest.mark.parametrize("template_name", [
        "scd_type1.py",
        "cdc_watermark.py",
        "merge_upsert.py",
    ])
    def test_template_not_empty(self, template_name):
        path = TEMPLATES_DIR / template_name
        content = path.read_text(encoding="utf-8")
        assert len(content) > 50, f"Template {template_name} seems too small"

    def test_scd_type1_has_merge(self):
        content = (TEMPLATES_DIR / "scd_type1.py").read_text(encoding="utf-8")
        assert "MERGE" in content or "merge" in content or "whenMatchedUpdate" in content

    def test_cdc_watermark_has_watermark(self):
        content = (TEMPLATES_DIR / "cdc_watermark.py").read_text(encoding="utf-8")
        assert "watermark" in content.lower()

    def test_merge_upsert_has_actions(self):
        content = (TEMPLATES_DIR / "merge_upsert.py").read_text(encoding="utf-8")
        assert "INSERT" in content
        assert "UPDATE" in content
        assert "DELETE" in content


# ═══════════════════════════════════════════════════════════════════════════
# 7. COMPONENT MAP COVERAGE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestComponentMapCoverage:
    """Verify new component entries in component_map.json."""

    @pytest.fixture(scope="class")
    def component_map(self):
        with open(MAPPING_DIR / "component_map.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("mappings", data)

    @pytest.mark.parametrize("component", [
        "tHashInput",
        "tHashOutput",
        "tOracleCDC",
        "tMysqlCDC",
        "tMSSqlCDC",
        "tPostgresqlCDC",
        "tDBCDC",
        "tOracleRow",
        "tMSSqlRow",
        "tMysqlRow",
        "tPostgresqlRow",
        "tDB2Row",
        "tMSSqlSP",
        "tMysqlSP",
        "tPostgresqlSP",
        "tMysqlBulkExec",
        "tMSSqlBulkExec",
        "tPostgresqlBulkExec",
        "tTeradataBulkExec",
        "tSnowflakeBulkExec",
        "tVerticaInput",
        "tVerticaOutput",
        "tNetezzaInput",
        "tNetezzaOutput",
        "tGreenplumInput",
        "tGreenplumOutput",
        "tSqoopImport",
        "tSqoopExport",
        "tSparkConfiguration",
        "tXSLT",
    ])
    def test_component_in_map(self, component_map, component):
        assert component in component_map, f"{component} missing from component_map.json"


# ═══════════════════════════════════════════════════════════════════════════
# 8. CONNECTION MAP COVERAGE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestConnectionMapCoverage:
    """Verify new connection entries in connection_map.json."""

    @pytest.fixture(scope="class")
    def connection_map(self):
        with open(MAPPING_DIR / "connection_map.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("mappings", data)

    @pytest.mark.parametrize("connection", [
        "tVerticaConnection",
        "tNetezzaConnection",
        "tGreenplumConnection",
        "tAS400Connection",
        "tSAPHanaConnection",
        "tImpalaConnection",
    ])
    def test_connection_in_map(self, connection_map, connection):
        assert connection in connection_map, f"{connection} missing from connection_map.json"


# ═══════════════════════════════════════════════════════════════════════════
# 9. PARSER CLASSIFICATION PARAMETRIZED
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parser
class TestParserClassificationRules:
    """Verify the parser classification logic for various component types
    using a minimal single-node XML file generated on the fly."""

    @pytest.fixture
    def parser(self, tmp_path):
        from parse_talend_jobs import TalendJobParser
        return TalendJobParser(str(tmp_path))

    def _make_item_xml(self, component_name, tmp_path):
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<talendfile:ProcessType xmlns:talendfile="platform:/resource/org.talend.model/model/TalendFile.xsd"
    xmlns:xmi="http://www.omg.org/XMI" xmi:version="2.0">
  <node componentName="{component_name}" componentVersion="0.1">
    <elementParameter name="UNIQUE_NAME" value="{component_name}_1"/>
  </node>
</talendfile:ProcessType>"""
        item_file = tmp_path / f"{component_name}_test.item"
        item_file.write_text(xml, encoding="utf-8")
        return item_file

    @pytest.mark.parametrize("component,expected_category", [
        ("tHashInput", "transformation"),
        ("tHashOutput", "transformation"),
        ("tXSLT", "transformation"),
        ("tPatternCheck", "transformation"),
        ("tMysqlConnection", "db_operation"),
        ("tMSSqlSP", "db_operation"),
        ("tMysqlCommit", "db_operation"),
        ("tPostgresqlClose", "db_operation"),
        ("tOracleCDC", "db_operation"),
        ("tMysqlBulkExec", "db_operation"),
        ("tSqoopImport", "bigdata"),
        ("tSqoopExport", "bigdata"),
        ("tSparkConfiguration", "bigdata"),
        ("tPigLoad", "bigdata"),
        ("tMapReduceInput", "bigdata"),
    ])
    def test_component_classification(self, parser, tmp_path, component, expected_category):
        item_file = self._make_item_xml(component, tmp_path)
        result = parser.parse_item_file(item_file)
        assert result is not None
        comp = result["components"][0]
        assert comp["category"] == expected_category, (
            f"{component}: expected {expected_category}, got {comp['category']}"
        )
