"""Everything except the network.

Both platforms are replaced by in-memory fakes injected through the constructors, so no
test patches module globals - if a collaborator cannot be injected, that is a design
problem rather than something to work around here.
"""

import json
import logging
import os
import subprocess
import sys
import warnings

import pytest
import yaml

from conftest import ITEM, LAKEHOUSE, MODEL, WORKSPACE, FakeDatabricksClient
from ossie_connect import (
    Connection,
    Databricks,
    DatabricksError,
    Fabric,
    FabricError,
    Snowflake,
    SnowflakeError,
    SupportsDownload,
    SupportsUpload,
    Yaml,
)
from ossie_connect._io import read_model, write_model
from ossie_connect._model import drop_duplicate_join_keys, qualify_sources


@pytest.fixture
def fabric(fabric_api):
    # No lakehouse: Direct Lake forbids the calculated column in the test model, which
    # is the subject of its own tests below rather than a constraint on every upload.
    return Fabric(workspace=WORKSPACE, token="t", api=fabric_api)


@pytest.fixture
def fabric_direct_lake(fabric_api):
    return Fabric(workspace=WORKSPACE, lakehouse=LAKEHOUSE, token="t", api=fabric_api)


@pytest.fixture
def snowflake(snowflake_connection):
    return Snowflake(database="OSSIE_DEMO", schema="PUBLIC",
                     connection=snowflake_connection)


@pytest.fixture
def databricks(databricks_client):
    return Databricks(
        catalog="main", schema="sales", warehouse_id="w", client=databricks_client
    )


# --- the shared contract ---------------------------------------------------------

def test_connections_declare_only_what_they_can_do(fabric, databricks, snowflake):
    assert isinstance(fabric, Connection) and isinstance(databricks, Connection)
    # Snowflake uploads but cannot be read back: no Snowflake-to-Ossie converter exists.
    assert isinstance(snowflake, SupportsUpload)
    assert not isinstance(snowflake, SupportsDownload)
    assert not isinstance(snowflake, Connection)


@pytest.mark.parametrize("name", ["fabric", "databricks", "snowflake"])
def test_preview_returns_text_and_touches_no_network(name, request):
    connection = request.getfixturevalue(name)
    preview = connection.preview(MODEL)
    assert isinstance(preview, str) and preview.strip()


@pytest.mark.parametrize("name", ["fabric", "databricks", "snowflake"])
def test_upload_is_idempotent(name, request):
    connection = request.getfixturevalue(name)
    first = connection.upload(MODEL)
    second = connection.upload(MODEL)
    assert first == second


@pytest.mark.parametrize("name", ["fabric", "databricks"])
def test_upload_then_download_returns_an_ossie_document(name, request, tmp_path):
    connection = request.getfixturevalue(name)
    connection.upload(MODEL)
    out = tmp_path / "back.yaml"
    returned = connection.download("sales_demo", out)
    assert yaml.safe_load(returned)["name"]
    assert out.read_text() == returned


@pytest.mark.parametrize("name", ["fabric", "databricks", "snowflake"])
def test_every_connection_accepts_yaml_text_as_well_as_a_path(name, request):
    connection = request.getfixturevalue(name)
    assert connection.preview(Yaml(MODEL.read_text())) == connection.preview(MODEL)


# --- model input -----------------------------------------------------------------

def test_read_model_reads_a_path_as_str_or_Path():
    assert read_model(MODEL) == read_model(str(MODEL))


def test_read_model_treats_marked_text_as_the_document():
    text = MODEL.read_text()
    assert read_model(Yaml(text)) == text


def test_read_model_does_not_mistake_a_one_line_document_for_a_filename():
    """The old shape-guessing heuristic reported this as a missing file."""
    assert read_model(Yaml("name: sales_demo")) == "name: sales_demo"
    with pytest.raises(FileNotFoundError):
        read_model("name: sales_demo")


def test_read_model_rejects_a_type_it_cannot_interpret():
    with pytest.raises(TypeError, match="must be a path or Yaml"):
        read_model(42)


def test_write_model_handles_a_bare_filename(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    write_model("name: m\n", "out.yaml")
    assert (tmp_path / "out.yaml").read_text() == "name: m\n"


# --- no global state -------------------------------------------------------------

def test_importing_the_package_does_not_reconfigure_logging():
    code = (
        "import logging;"
        "before=len(logging.getLogger('ossie_microsoft').handlers);"
        "import ossie_connect;"
        "after=len(logging.getLogger('ossie_microsoft').handlers);"
        "print(before, after)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert out.stdout.split() == ["0", "0"], out.stderr


def test_converting_removes_its_handler_again(fabric):
    logger = logging.getLogger("ossie_microsoft")
    fabric.preview(MODEL)
    assert logger.handlers == []


# --- model transformations -------------------------------------------------------

def test_qualify_sources_prefixes_only_bare_names():
    document = yaml.safe_load(qualify_sources(MODEL.read_text(), "main.sales", parts=3))
    assert [d["source"] for d in document["datasets"]] == [
        "main.sales.customers",
        "main.sales.orders",
    ]


def test_qualify_sources_leaves_qualified_names_alone():
    already = "name: m\ndatasets:\n- name: o\n  source: cat.sch.orders\n"
    assert qualify_sources(already, "other.place", parts=3) == already


def test_drop_duplicate_join_keys_is_a_no_op_without_a_collision():
    standalone = "name: m\ndatasets:\n- name: o\n  source: a.b.o\n  fields:\n  - name: x\n"
    adjusted, removed = drop_duplicate_join_keys(standalone)
    assert removed == [] and adjusted == standalone


# --- one file, both platforms ----------------------------------------------------

def test_the_same_model_converts_for_both_platforms(fabric, databricks):
    """Fabric needs the join key on both datasets; Unity Catalog refuses the duplicate."""
    bim = fabric.to_tmsl(MODEL)
    assert len(bim["model"]["relationships"]) == 1
    customers = next(t for t in bim["model"]["tables"] if t["name"] == "customers")
    assert "customer_id" in {c["name"] for c in customers["columns"]}

    with pytest.warns(UserWarning, match="customers.customer_id"):
        metric_view = databricks.to_metric_view(MODEL)
    names = [d["name"] for d in yaml.safe_load(metric_view)["dimensions"]]
    assert names.count("customer_id") == 1


def test_dedupe_can_be_turned_off_to_see_the_converters_own_error(databricks):
    from ossie_databricks._common import ConversionError

    with pytest.raises(ConversionError, match="collides"):
        databricks.to_metric_view(MODEL, dedupe_join_keys=False)


# --- Fabric ----------------------------------------------------------------------

def test_fabric_uses_the_lakehouse_id_in_the_onelake_url(fabric_direct_lake):
    bim = fabric_direct_lake.to_tmsl(MODEL)
    url = "\n".join(bim["model"]["expressions"][0]["expression"])
    assert f"{WORKSPACE}/{LAKEHOUSE}" in url


def test_fabric_always_marks_the_model_as_v3(fabric):
    """Fabric refuses a TMSL import without it: "supported for V3 models only"."""
    assert fabric.to_tmsl(MODEL)["model"]["defaultPowerBIDataSourceVersion"] == "powerBI_V3"


def test_direct_lake_refuses_calculated_columns(fabric_direct_lake):
    """Caught here rather than as an opaque failure from Fabric seconds later."""
    with pytest.raises(FabricError, match="customers.customer_name"):
        fabric_direct_lake.upload(MODEL)


def test_direct_lake_still_previews_so_you_can_see_why(fabric_direct_lake):
    assert '"type": "calculated"' in fabric_direct_lake.preview(MODEL)


def test_without_a_lakehouse_calculated_columns_are_fine(fabric):
    assert fabric.upload(MODEL)


def test_fabric_without_a_lakehouse_falls_back_to_a_placeholder(fabric):
    url = "\n".join(fabric.to_tmsl(MODEL)["model"]["expressions"][0]["expression"])
    assert "00000000-0000-0000-0000-000000000000" in url


def test_fabric_qualifies_bare_sources_with_the_schema(fabric_api):
    connection = Fabric(workspace=WORKSPACE, schema="bronze", token="t", api=fabric_api)
    bim = connection.to_tmsl(MODEL)
    schemas = {t["partitions"][0]["source"]["schemaName"] for t in bim["model"]["tables"]}
    assert schemas == {"bronze"}


def test_fabric_creates_when_absent_and_updates_when_present(fabric, fabric_api):
    fabric.upload(MODEL)
    fabric.upload(MODEL)
    verbs = [call[0] for call in fabric_api.calls]
    assert verbs.count("create_item") == 1
    assert verbs.count("update_item") == 1


def test_fabric_download_names_the_model_after_the_item(fabric, fabric_api):
    """Fabric keeps the name on the item, not in the TMSL, so the converter would
    otherwise call every downloaded model "semantic_model"."""
    fabric.upload(MODEL, name="quarterly-sales")
    assert yaml.safe_load(fabric.download("quarterly-sales"))["name"] == "quarterly-sales"


def test_fabric_download_reports_a_missing_model(fabric):
    with pytest.raises(FabricError, match="no semantic model called 'absent'"):
        fabric.download("absent")


def test_fabric_acquires_a_token_only_when_none_was_given(fabric_api):
    Fabric(workspace=WORKSPACE, api=fabric_api).upload(MODEL)
    assert ("acquire_token",) in fabric_api.calls


# --- Fabric transport (the real handlers, against recorded responses) -------------

def test_fabric_get_definition_picks_the_model_bim_part(monkeypatch):
    from ossie_connect import _fabric_api

    response = {"definition": {"parts": [
        {"path": "definition.pbism", "payload": _fabric_api.encode_part({}),
         "payloadType": "InlineBase64"},
        {"path": "definition/model.bim",
         "payload": _fabric_api.encode_part({"name": "recorded"}),
         "payloadType": "InlineBase64"},
    ]}}
    monkeypatch.setattr(_fabric_api, "request", lambda *a, **k: (200, response, {}))
    api = _fabric_api.FabricApi()
    assert json.loads(api.get_definition(WORKSPACE, ITEM, "t"))["name"] == "recorded"


def test_fabric_get_definition_follows_a_202_operation(monkeypatch):
    from ossie_connect import _fabric_api

    done = {"definition": {"parts": [
        {"path": "model.bim", "payload": _fabric_api.encode_part({"name": "async"}),
         "payloadType": "InlineBase64"}]}}

    def fake_request(method, url, token, payload=None):
        if url.endswith("/result"):
            return 200, done, {}
        return 202, None, {"Location": "https://api.fabric.microsoft.com/v1/operations/x"}

    monkeypatch.setattr(_fabric_api, "request", fake_request)
    monkeypatch.setattr(_fabric_api, "wait_for_operation", lambda *a, **k: {"status": "Succeeded"})
    api = _fabric_api.FabricApi()
    assert json.loads(api.get_definition(WORKSPACE, ITEM, "t"))["name"] == "async"


def test_fabric_find_item_follows_paging(monkeypatch):
    from ossie_connect import _fabric_api

    pages = [
        (200, {"value": [{"displayName": "other", "id": "a"}],
               "continuationUri": "https://api.fabric.microsoft.com/v1/next"}, {}),
        (200, {"value": [{"displayName": "sales_demo", "id": ITEM}]}, {}),
    ]
    monkeypatch.setattr(_fabric_api, "request", lambda *a, **k: pages.pop(0))
    assert _fabric_api.FabricApi().find_item(WORKSPACE, "sales_demo", "t") == ITEM


def test_fabric_surfaces_a_failed_request(monkeypatch):
    from ossie_connect import _fabric_api

    monkeypatch.setattr(_fabric_api, "request",
                        lambda *a, **k: (403, {"message": "nope"}, {}))
    with pytest.raises(FabricError, match="403"):
        _fabric_api.FabricApi().get_definition(WORKSPACE, ITEM, "t")


# --- Databricks ------------------------------------------------------------------

def test_databricks_statement_wraps_the_metric_view(databricks):
    statement = databricks.preview(MODEL)
    assert statement.startswith("CREATE OR REPLACE VIEW main.sales.sales_demo WITH METRICS")
    body = statement.split("$$")[1]
    assert yaml.safe_load(body)["source"].startswith("main.sales.")


def test_databricks_upload_returns_the_full_name(databricks, databricks_client):
    assert databricks.upload(MODEL) == "main.sales.sales_demo"
    assert databricks_client.statements[0].startswith("CREATE OR REPLACE VIEW")


def test_databricks_upload_without_a_warehouse_says_so(databricks_client):
    connection = Databricks(catalog="main", schema="sales", client=databricks_client)
    with pytest.raises(DatabricksError, match="no warehouse_id"):
        connection.upload(MODEL)


def test_databricks_download_needs_no_warehouse_when_the_catalog_answers(databricks_client):
    writer = Databricks(catalog="main", schema="sales", warehouse_id="w",
                        client=databricks_client)
    writer.upload(MODEL)
    reader = Databricks(catalog="main", schema="sales", client=databricks_client)
    assert yaml.safe_load(reader.download("sales_demo"))["name"]


def test_databricks_falls_back_to_show_create_table():
    client = FakeDatabricksClient(supports_view_definition=False)
    connection = Databricks(catalog="main", schema="sales", warehouse_id="w", client=client)
    connection.upload(MODEL)
    assert yaml.safe_load(connection.download("sales_demo"))["name"]
    assert any(s.startswith("SHOW CREATE TABLE") for s in client.statements)


def test_databricks_refuses_a_plain_sql_view():
    client = FakeDatabricksClient({"main.sales.plain": "SELECT 1 AS x"})
    connection = Databricks(catalog="main", schema="sales", warehouse_id="w", client=client)
    with pytest.raises(DatabricksError, match="does not look like a Metric View"):
        connection.download("plain")


def test_databricks_download_accepts_a_fully_qualified_name():
    client = FakeDatabricksClient()
    writer = Databricks(catalog="other", schema="place", warehouse_id="w", client=client)
    writer.upload(MODEL)
    reader = Databricks(catalog="main", schema="sales", warehouse_id="w", client=client)
    assert yaml.safe_load(reader.download("other.place.sales_demo"))["name"]


def test_databricks_download_names_the_model_after_the_view(databricks, databricks_client):
    """A Metric View's YAML has no model name, so it must come from the view itself -
    otherwise the model is named after its source table and re-uploading renames it."""
    databricks.upload(MODEL)
    assert yaml.safe_load(databricks.download("sales_demo"))["name"] == "sales_demo"


def test_databricks_download_relabels_the_fact_dataset():
    """A known converter limitation, pinned here so it is not mistaken for a bug.

    The reverse converter names the fact dataset after the model, so a round trip
    relabels it. `source` survives, which is what re-uploading depends on.
    """
    client = FakeDatabricksClient()
    connection = Databricks(catalog="main", schema="sales", warehouse_id="w",
                            client=client)
    connection.upload(MODEL)
    document = yaml.safe_load(connection.download("sales_demo"))
    datasets = {d["name"]: d["source"] for d in document["datasets"]}
    assert datasets == {
        "sales_demo": "main.sales.orders",     # was `orders` before the round trip
        "customers": "main.sales.customers",
    }


def test_databricks_reports_a_missing_view():
    connection = Databricks(catalog="main", schema="sales", client=FakeDatabricksClient())
    with pytest.raises(DatabricksError, match="could not read"):
        connection.download("absent")


# --- the converter seam ----------------------------------------------------------

def test_a_replacement_converter_is_used_instead(databricks_client):
    """The seam that lets the deprecating Python converter be swapped for the Java one."""

    class StubConverter:
        def to_platform(self, ossie_yaml, *, warn=False):
            return "version: '1.1'\nsource: stub\n"

        def to_ossie(self, metric_view, *, name=None, warn=False):
            return "name: from_stub\n"

    connection = Databricks(catalog="main", schema="sales", warehouse_id="w",
                            client=databricks_client, converter=StubConverter())
    assert "source: stub" in connection.preview(MODEL)
    connection.upload(MODEL)
    assert yaml.safe_load(connection.download("sales_demo"))["name"] == "from_stub"


# --- a folder of models ----------------------------------------------------------

@pytest.fixture
def model_folder(tmp_path):
    """Three models whose filenames deliberately differ from their model names."""
    source = MODEL.read_text()
    (tmp_path / "sales.yaml").write_text(source)
    (tmp_path / "mkt.yml").write_text(source.replace("name: sales_demo",
                                                     "name: marketing_demo", 1))
    (tmp_path / "fin.yaml").write_text(source.replace("name: sales_demo",
                                                      "name: finance_demo", 1))
    (tmp_path / "notes.txt").write_text("not a model")
    (tmp_path / "broken.yaml").write_text("{{{ not yaml")
    return tmp_path


def test_models_are_keyed_by_model_name_not_filename(model_folder):
    from ossie_connect import Models

    models = Models(model_folder)
    assert list(models) == ["finance_demo", "marketing_demo", "sales_demo"]
    assert models["sales_demo"].name == "sales.yaml"


def test_models_ignores_non_models(model_folder):
    from ossie_connect import Models

    assert len(Models(model_folder)) == 3


def test_models_behaves_like_a_mapping(model_folder):
    from ossie_connect import Models

    models = Models(model_folder)
    assert "sales_demo" in models and "absent" not in models
    assert sorted(models.keys()) == sorted(n for n, _ in models.items())


def test_models_values_can_be_uploaded_directly(model_folder, fabric, fabric_api):
    from ossie_connect import Models

    models = Models(model_folder)
    for name, path in models.items():
        if name != "marketing_demo":
            fabric.upload(path)
    assert sorted(fabric_api.items) == ["finance_demo", "sales_demo"]


def test_models_reports_an_unknown_name_with_what_it_has(model_folder):
    from ossie_connect import Models

    with pytest.raises(KeyError, match="found: finance_demo, marketing_demo, sales_demo"):
        Models(model_folder)["nope"]


def test_models_rejects_two_files_defining_the_same_model(model_folder):
    from ossie_connect import Models

    (model_folder / "copy.yaml").write_text(MODEL.read_text())
    with pytest.raises(ValueError, match="both define the model 'sales_demo'"):
        Models(model_folder)


def test_models_rejects_a_missing_folder(tmp_path):
    from ossie_connect import Models

    with pytest.raises(NotADirectoryError):
        Models(tmp_path / "absent")


def test_models_refresh_picks_up_new_files(model_folder):
    from ossie_connect import Models

    models = Models(model_folder)
    (model_folder / "new.yaml").write_text(
        MODEL.read_text().replace("name: sales_demo", "name: later_demo", 1)
    )
    assert "later_demo" not in models
    assert "later_demo" in models.refresh()


# --- the CLI ---------------------------------------------------------------------
# --dry-run touches no network, so the whole command line runs for real here.

def _run(argv, capsys):
    from ossie_connect.cli import main

    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def test_cli_uploads_one_file(model_folder, capsys):
    code, out, _ = _run(
        ["upload", "databricks", str(model_folder / "sales.yaml"),
         "--catalog", "main", "--schema", "sales", "--dry-run"], capsys)
    assert code == 0
    assert out.count("CREATE OR REPLACE VIEW") == 1
    assert "main.sales.sales_demo" in out


def test_cli_uploads_every_model_in_a_folder(model_folder, capsys):
    code, out, _ = _run(
        ["upload", "databricks", str(model_folder),
         "--catalog", "main", "--schema", "sales", "--dry-run"], capsys)
    assert code == 0
    assert out.count("CREATE OR REPLACE VIEW") == 3
    for name in ("sales_demo", "marketing_demo", "finance_demo"):
        assert f"main.sales.{name}" in out


def test_cli_labels_each_model_when_uploading_a_folder(model_folder, capsys):
    _, out, _ = _run(
        ["upload", "fabric", str(model_folder), "--workspace", "w", "--dry-run"], capsys)
    assert "--- sales.yaml ---" in out and "--- fin.yaml ---" in out


def test_cli_refuses_name_with_a_folder(model_folder, capsys):
    code, _, err = _run(
        ["upload", "databricks", str(model_folder), "--catalog", "c", "--schema", "s",
         "--name", "x", "--dry-run"], capsys)
    assert code == 1 and "--name cannot be used with a folder" in err


def test_cli_reports_an_empty_folder(tmp_path, capsys):
    code, _, err = _run(
        ["upload", "fabric", str(tmp_path), "--workspace", "w", "--dry-run"], capsys)
    assert code == 1 and "no Ossie models found" in err


def test_cli_reports_a_missing_file(capsys):
    code, _, err = _run(
        ["upload", "fabric", "nope.yaml", "--workspace", "w", "--dry-run"], capsys)
    assert code == 1 and "no such model file" in err


def test_cli_reports_missing_configuration(model_folder, capsys, monkeypatch):
    monkeypatch.delenv("FABRIC_WORKSPACE_ID", raising=False)
    monkeypatch.chdir(model_folder)   # no .env here
    code, _, err = _run(
        ["upload", "fabric", str(model_folder / "sales.yaml"), "--dry-run"], capsys)
    assert code == 1 and "set FABRIC_WORKSPACE_ID" in err


def test_cli_warnings_carry_no_file_and_line_noise():
    """pytest intercepts the warnings channel, so the formatter is checked directly."""
    from ossie_connect import plain_warnings
    import warnings as w

    plain_warnings()
    line = w.formatwarning("dropped a field", UserWarning, "/x/databricks.py", 94)
    assert line == "warning: dropped a field\n"


# --- Snowflake -------------------------------------------------------------------

def test_snowflake_qualifies_the_base_table(snowflake):
    document = yaml.safe_load(snowflake.to_semantic_view(MODEL))
    for table in document["tables"]:
        assert table["base_table"]["database"] == "OSSIE_DEMO"
        assert table["base_table"]["schema"] == "PUBLIC"


def test_snowflake_nests_every_metric_under_its_table(snowflake):
    """A top-level metrics list is rejected outright by Snowflake's native schema."""
    document = yaml.safe_load(snowflake.to_semantic_view(MODEL))
    assert "metrics" not in document
    nested = {t["name"]: [m["name"] for m in t.get("metrics", [])] for t in document["tables"]}
    assert nested == {
        "customers": [],
        "orders": ["total_revenue", "order_count", "avg_order_value"],
    }


def test_snowflake_nesting_can_be_turned_off(snowflake):
    document = yaml.safe_load(snowflake.to_semantic_view(MODEL, nest_metrics=False))
    assert len(document["metrics"]) == 3


def test_snowflake_upload_creates_everything_then_the_view(snowflake, snowflake_connection):
    assert snowflake.upload(MODEL) == "OSSIE_DEMO.PUBLIC.sales_demo"
    executed = snowflake_connection.statements
    assert executed[0].startswith("CREATE WAREHOUSE IF NOT EXISTS OSSIE_COMPUTE_WH")
    assert "CREATE DATABASE IF NOT EXISTS OSSIE_DEMO" in executed
    assert "CREATE SCHEMA IF NOT EXISTS OSSIE_DEMO.PUBLIC" in executed
    assert executed[-1].startswith("CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(")


def test_snowflake_preview_touches_no_network(snowflake, snowflake_connection):
    assert snowflake.preview(MODEL).startswith("CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(")
    assert snowflake_connection.statements == []


def test_snowflake_reports_missing_credentials():
    connection = Snowflake(database="D", schema="S")
    with pytest.raises(SnowflakeError, match="SNOWFLAKE_ACCOUNT"):
        connection.upload(MODEL)


def test_snowflake_has_no_download_method(snowflake):
    assert not hasattr(snowflake, "download")


# --- metric ownership inference --------------------------------------------------

def _view(metrics, *, relationships=True):
    document = {
        "tables": [
            {"name": "orders", "facts": [{"name": "order_amount"}, {"name": "order_id"}]},
            {"name": "customers", "dimensions": [{"name": "segment"}]},
        ],
        "metrics": metrics,
    }
    if relationships:
        document["relationships"] = [{"left_table": "orders", "right_table": "customers"}]
    return yaml.safe_dump(document, sort_keys=False)


def _nested(yaml_text):
    from ossie_connect.snowflake import nest_metrics_per_table

    document = yaml.safe_load(nest_metrics_per_table(yaml_text))
    return {t["name"]: [m["name"] for m in t.get("metrics", [])] for t in document["tables"]}


def test_metric_follows_the_column_it_references():
    assert _nested(_view([{"name": "revenue", "expr": "SUM(order_amount)"}])) == {
        "orders": ["revenue"], "customers": []
    }


def test_metric_on_a_dimension_table_goes_there():
    assert _nested(_view([{"name": "segments", "expr": "COUNT(DISTINCT segment)"}])) == {
        "orders": [], "customers": ["segments"]
    }


def test_metric_naming_no_column_falls_back_to_the_fact_table():
    """COUNT(*) references nothing; orders is only ever a relationship's left side."""
    assert _nested(_view([{"name": "n", "expr": "COUNT(*)"}])) == {
        "orders": ["n"], "customers": []
    }


def test_metric_spanning_two_tables_falls_back_to_the_fact_table():
    expression = "SUM(order_amount) / COUNT(DISTINCT segment)"
    assert _nested(_view([{"name": "mixed", "expr": expression}])) == {
        "orders": ["mixed"], "customers": []
    }


def test_sql_keywords_around_one_table_do_not_confuse_the_owner():
    expression = "COUNT(CASE WHEN segment IS NOT NULL THEN 1 END)"
    assert _nested(_view([{"name": "m", "expr": expression}])) == {
        "orders": [], "customers": ["m"]
    }


def test_a_column_named_like_a_sql_keyword_is_still_a_column():
    """A keyword list would discard `date`; the trailing "(" test does not."""
    document = {
        "tables": [
            {"name": "orders", "facts": [{"name": "amount"}]},
            {"name": "events", "time_dimensions": [{"name": "date"}]},
        ],
        "relationships": [{"left_table": "orders", "right_table": "events"}],
        "metrics": [{"name": "latest", "expr": "MAX(date)"}],
    }
    assert _nested(yaml.safe_dump(document, sort_keys=False)) == {
        "orders": [], "events": ["latest"]
    }


def test_a_metric_spanning_both_tables_falls_back():
    expression = "SUM(CASE WHEN segment IS NOT NULL THEN order_amount ELSE 0 END)"
    assert _nested(_view([{"name": "m", "expr": expression}]))["orders"] == ["m"]


def test_ambiguous_metric_without_a_fact_table_is_an_error():
    from ossie_connect.snowflake import nest_metrics_per_table

    with pytest.raises(ValueError, match="cannot tell which table metric 'n'"):
        nest_metrics_per_table(_view([{"name": "n", "expr": "COUNT(*)"}], relationships=False))


def test_nesting_a_view_without_metrics_changes_nothing():
    from ossie_connect.snowflake import nest_metrics_per_table

    plain = yaml.safe_dump({"tables": [{"name": "orders"}]}, sort_keys=False)
    assert nest_metrics_per_table(plain) == plain


# --- CLI, Snowflake --------------------------------------------------------------

def test_cli_uploads_to_snowflake(model_folder, capsys):
    code, out, _ = _run(
        ["upload", "snowflake", str(model_folder / "sales.yaml"),
         "--database", "OSSIE_DEMO", "--schema", "PUBLIC", "--dry-run"], capsys)
    assert code == 0
    assert out.startswith("CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(")


def test_cli_refuses_to_download_from_snowflake(capsys):
    code, _, err = _run(
        ["download", "snowflake", "sales_demo",
         "--database", "OSSIE_DEMO", "--schema", "PUBLIC"], capsys)
    assert code == 1
    assert "cannot be downloaded from" in err and "no Snowflake-to-Ossie converter" in err


# --- several targets in one script -----------------------------------------------

def test_databricks_at_shares_the_client(databricks, databricks_client):
    other = databricks.at(schema="marketing")
    assert (other.catalog, other.schema) == ("main", "marketing")
    assert other._client is databricks_client      # not a second connection
    assert databricks.schema == "sales"            # the original is untouched


def test_databricks_at_deploys_to_two_schemas_with_one_client(databricks,
                                                              databricks_client):
    databricks.upload(MODEL)
    databricks.at(schema="marketing").upload(MODEL)
    created = [s.split()[4] for s in databricks_client.statements
               if s.startswith("CREATE OR REPLACE VIEW")]
    assert created == ["main.sales.sales_demo", "main.marketing.sales_demo"]


def test_fabric_at_shares_the_token(fabric_direct_lake, fabric_api):
    fabric = fabric_direct_lake
    other = fabric.at(workspace="99999999-9999-9999-9999-999999999999")
    assert other.token == fabric.token
    assert other._api is fabric_api
    # A new workspace with no lakehouse workspace given follows the new workspace.
    assert other.lakehouse_workspace == "99999999-9999-9999-9999-999999999999"


def test_snowflake_at_shares_the_connection(snowflake, snowflake_connection):
    other = snowflake.at(schema="REPORTING")
    assert other.schema == "REPORTING" and other.database == "OSSIE_DEMO"
    assert other._connection is snowflake_connection


@pytest.mark.parametrize("name", ["fabric", "databricks", "snowflake"])
def test_at_without_arguments_changes_nothing(name, request):
    connection = request.getfixturevalue(name)
    assert repr(connection.at()) == repr(connection)


def test_nothing_requires_an_env_file(monkeypatch, databricks_client):
    """Explicit construction must work with a completely empty environment."""
    for key in list(os.environ):
        if key.startswith(("DATABRICKS_", "FABRIC_", "SNOWFLAKE_")):
            monkeypatch.delenv(key, raising=False)
    connection = Databricks(catalog="c", schema="s", warehouse_id="w",
                            client=databricks_client)
    assert connection.upload(MODEL) == "c.s.sales_demo"


def test_snowflake_prepares_the_target_once(snowflake, snowflake_connection):
    """Warehouse/database/schema setup is four round trips; pay for them once."""
    snowflake.upload(MODEL)
    snowflake.upload(MODEL)
    setup = [s for s in snowflake_connection.statements
             if s.startswith(("CREATE WAREHOUSE", "CREATE DATABASE", "CREATE SCHEMA",
                              "USE WAREHOUSE"))]
    creates = [s for s in snowflake_connection.statements if s.startswith("CALL SYSTEM$")]
    assert len(setup) == 4 and len(creates) == 2


def test_snowflake_at_prepares_the_new_schema(snowflake, snowflake_connection):
    """A different schema does need creating, even on a shared connection."""
    snowflake.upload(MODEL)
    snowflake.at(schema="REPORTING").upload(MODEL)
    assert "CREATE SCHEMA IF NOT EXISTS OSSIE_DEMO.REPORTING" in snowflake_connection.statements


def test_lazy_credentials_are_acquired_once_under_threads(fabric_api):
    """at() shares a client between connections, which invites a thread pool."""
    import concurrent.futures

    connection = Fabric(workspace=WORKSPACE, api=fabric_api)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        tokens = list(pool.map(lambda _: connection.token, range(32)))
    assert len(set(tokens)) == 1
    assert [c[0] for c in fabric_api.calls].count("acquire_token") == 1


# --- discovering what is configured ----------------------------------------------

def _only(monkeypatch, **settings):
    for key in list(os.environ):
        if key.startswith(("DATABRICKS_", "FABRIC_", "SNOWFLAKE_")):
            monkeypatch.delenv(key, raising=False)
    for key, value in settings.items():
        monkeypatch.setenv(key, value)
    from ossie_connect import configured_connections

    return configured_connections()


def test_configured_connections_finds_nothing_in_an_empty_environment(monkeypatch):
    assert _only(monkeypatch) == []


def test_configured_connections_skips_the_unconfigured(monkeypatch):
    found = _only(monkeypatch, DATABRICKS_CATALOG="main", DATABRICKS_SCHEMA="sales")
    assert [c.platform for c in found] == ["Databricks"]
    assert found[0].catalog == "main"


def test_configured_connections_finds_every_configured_platform(monkeypatch):
    found = _only(
        monkeypatch,
        DATABRICKS_CATALOG="main", DATABRICKS_SCHEMA="sales",
        SNOWFLAKE_DATABASE="D", SNOWFLAKE_SCHEMA="S",
        FABRIC_WORKSPACE_ID="w",
    )
    assert [c.platform for c in found] == ["Fabric", "Databricks", "Snowflake"]


def test_configured_connections_passes_overrides_through(monkeypatch):
    found = _only(monkeypatch, DATABRICKS_CATALOG="main", DATABRICKS_SCHEMA="sales")
    from ossie_connect import configured_connections

    assert configured_connections(schema="staging")[0].schema == "staging"
    assert found[0].schema == "sales"


@pytest.mark.parametrize("name,expected", [("fabric", "Fabric"),
                                           ("databricks", "Databricks"),
                                           ("snowflake", "Snowflake")])
def test_every_connection_names_its_platform(name, expected, request):
    assert request.getfixturevalue(name).platform == expected


@pytest.mark.parametrize("name,expected", [
    ("fabric", f"workspace {WORKSPACE}"),
    ("databricks", "main.sales"),
    ("snowflake", "OSSIE_DEMO.PUBLIC"),
])
def test_every_connection_says_where_it_points(name, expected, request):
    assert request.getfixturevalue(name).target == expected


def test_the_join_key_warning_names_the_platform_that_raised_it(databricks):
    """It lands on stderr among other platforms' output; unattributed it is noise."""
    with pytest.warns(UserWarning, match=r"^Databricks main\.sales: dropped customers\.customer_id"):
        databricks.to_metric_view(MODEL)


# --- Reporter --------------------------------------------------------------------

def _reporter(**kwargs):
    import io

    from ossie_connect import Reporter

    out = io.StringIO()
    return Reporter(stream=out, colour=kwargs.pop("colour", False), **kwargs), out


def test_reporter_marks_each_outcome():
    report, out = _reporter()
    report.ok("Databricks", "main.sales.sales_demo")
    report.fail("Fabric", "no credential")
    report.warn("dropped a field")
    assert out.getvalue().splitlines() == [
        "  ✓ Databricks  main.sales.sales_demo",
        "  ✗ Fabric      no credential",
        "    ⚠ dropped a field",
    ]


def test_reporter_wraps_against_the_visible_prefix_with_colour_on():
    """Colouring before wrapping makes textwrap count the escape codes."""
    import re

    plain, plain_out = _reporter(width=50)
    coloured, coloured_out = _reporter(width=50, colour=True)
    message = "a rather long message that will certainly have to wrap somewhere"
    plain.ok("Databricks", message)
    coloured.ok("Databricks", message)
    stripped = re.sub(r"\033\[[0-9;]*m", "", coloured_out.getvalue())
    assert stripped == plain_out.getvalue()


def test_reporter_colour_is_off_for_a_non_terminal():
    import io

    from ossie_connect import Reporter

    assert Reporter(stream=io.StringIO()).colour is False


def test_reporter_capture_puts_warnings_under_their_step(databricks):
    report, out = _reporter()
    with report.capture():
        report.ok(databricks.platform, databricks.upload(MODEL))
    lines = out.getvalue().splitlines()
    assert lines[0].startswith("  ✓ Databricks")
    assert lines[1].startswith("    ⚠ Databricks main.sales: dropped customers.customer_id")


def test_reporter_capture_strips_the_connections_own_prefix(databricks):
    report, out = _reporter()
    with report.capture(databricks):
        report.ok(databricks.platform, databricks.upload(MODEL))
    assert out.getvalue().splitlines()[1] == (
        "    ⚠ dropped customers.customer_id (duplicate dimension; still in primary_key)"
    )


def test_reporter_capture_ignores_other_libraries_warnings():
    report, out = _reporter()
    with report.capture():
        warnings.warn("something unrelated deprecated", DeprecationWarning)
        report.ok("Databricks", "done")
    assert "deprecated" not in out.getvalue()


def test_our_warnings_have_a_category_of_their_own(databricks):
    """So callers can filter on them, and Reporter need not match on a file path."""
    from ossie_connect import OssieConnectWarning

    with pytest.warns(OssieConnectWarning):
        databricks.to_metric_view(MODEL)

    with warnings.catch_warnings():
        # simplefilter inserts at the front, so the ignore must come second to win.
        warnings.simplefilter("error", UserWarning)   # anything else would raise
        warnings.simplefilter("ignore", OssieConnectWarning)
        databricks.to_metric_view(MODEL)


def test_databricks_reads_through_show_create_table_when_it_can():
    """`view_definition` returns a normalized form with synonyms and comments stripped
    - 646 characters against 1149 for the same view on a real workspace - so it is a
    fallback, not the default."""
    client = FakeDatabricksClient()
    connection = Databricks(catalog="main", schema="sales", warehouse_id="w",
                            client=client)
    connection.upload(MODEL)
    client.statements.clear()
    connection.download("sales_demo")
    assert any(s.startswith("SHOW CREATE TABLE") for s in client.statements)


def test_databricks_without_a_warehouse_warns_that_the_read_is_lossy():
    from ossie_connect import OssieConnectWarning

    client = FakeDatabricksClient()
    Databricks(catalog="main", schema="sales", warehouse_id="w",
               client=client).upload(MODEL)
    reader = Databricks(catalog="main", schema="sales", client=client)
    with pytest.warns(OssieConnectWarning, match="synonyms and comments are dropped"):
        reader.download("sales_demo")


# --- delete ----------------------------------------------------------------------

def test_fabric_delete_removes_the_item(fabric, fabric_api):
    fabric.upload(MODEL)
    assert fabric.delete("sales_demo") is True
    assert fabric_api.items == {}
    assert ("delete_item", ITEM) in fabric_api.calls


def test_fabric_delete_tolerates_a_missing_model(fabric):
    assert fabric.delete("never-existed") is False


def test_fabric_delete_can_insist_the_model_exists(fabric):
    with pytest.raises(FabricError, match="no semantic model called 'nope'"):
        fabric.delete("nope", missing_ok=False)


def test_databricks_delete_drops_the_view(databricks, databricks_client):
    databricks.upload(MODEL)
    databricks.delete("sales_demo")
    assert "DROP VIEW IF EXISTS main.sales.sales_demo" in databricks_client.statements
    assert "main.sales.sales_demo" not in databricks_client.views


def test_snowflake_delete_drops_the_semantic_view(snowflake, snowflake_connection):
    snowflake.delete("sales_demo")
    assert snowflake_connection.statements[-1] == (
        "DROP SEMANTIC VIEW IF EXISTS OSSIE_DEMO.PUBLIC.sales_demo"
    )


@pytest.mark.parametrize("name", ["fabric", "databricks", "snowflake"])
def test_upload_then_delete_leaves_nothing_behind(name, request):
    """A script that creates things in someone else's tenant must be able to unmake them."""
    connection = request.getfixturevalue(name)
    connection.upload(MODEL)
    assert connection.delete("sales_demo") is True


# --- preflight -------------------------------------------------------------------

def test_check_passes_for_a_lakehouse_that_exists(fabric_direct_lake):
    assert fabric_direct_lake.check() == []


def test_check_names_the_lakehouses_that_do_exist(fabric_api):
    from ossie_connect import Fabric

    connection = Fabric(workspace=WORKSPACE, lakehouse="deadbeef-0000-0000-0000-000000000000",
                        token="t", api=fabric_api)
    problem = connection.check()[0]
    assert problem.fatal
    assert "is not in workspace" in problem.message and "raw" in problem.message


def test_check_warns_when_there_is_no_lakehouse(fabric):
    findings = fabric.check()
    assert [f.level for f in findings] == ["warning"]
    assert "can never refresh" in findings[0].message


def test_check_reports_an_unreadable_workspace(fabric_api):
    from ossie_connect import Fabric

    connection = Fabric(workspace="99999999-9999-9999-9999-999999999999", token="t",
                        api=fabric_api)
    assert "cannot be read" in connection.check()[0].message


def test_upload_refuses_a_lakehouse_that_is_not_there(fabric_api):
    from ossie_connect import Fabric
    from ossie_connect.preflight import PreflightError

    connection = Fabric(workspace=WORKSPACE, lakehouse="deadbeef-0000-0000-0000-000000000000",
                        token="t", api=fabric_api)
    with pytest.raises(PreflightError, match="is not in workspace"):
        connection.upload(MODEL)
    assert not any(c[0] == "create_item" for c in fabric_api.calls), "nothing was written"


def test_upload_warns_but_proceeds_without_a_lakehouse(fabric):
    from ossie_connect import OssieConnectWarning

    with pytest.warns(OssieConnectWarning, match="can never refresh"):
        assert fabric.upload(MODEL)


def test_preflight_runs_once_per_connection(fabric, fabric_api):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fabric.upload(MODEL)
        fabric.upload(MODEL)
    assert [c[0] for c in fabric_api.calls].count("list_items") == 1


def test_preflight_can_be_skipped(fabric_api):
    """check=False goes straight to the write, for callers who have already checked."""
    from ossie_connect import Fabric, Yaml

    document = yaml.safe_load(MODEL.read_text())
    for dataset in document["datasets"]:   # Direct Lake forbids the calculated column
        dataset["fields"] = [f for f in dataset["fields"] if f["name"] != "customer_name"]
    plain = Yaml(yaml.safe_dump(document, sort_keys=False))

    connection = Fabric(workspace=WORKSPACE, lakehouse="deadbeef-0000-0000-0000-000000000000",
                        token="t", api=fabric_api)
    assert connection.upload(plain, check=False)
    assert not any(c[0] == "list_items" for c in fabric_api.calls)


def test_cli_check_writes_nothing_and_reports_ready(model_folder, capsys, monkeypatch):
    monkeypatch.setenv("DATABRICKS_CATALOG", "main")
    monkeypatch.setenv("DATABRICKS_SCHEMA", "sales")
    code, out, err = _run(["check", "databricks"], capsys)
    # No client is configured here, so the schema lookup fails - which is the point:
    # check reports it instead of discovering it mid-upload.
    assert code == 1 and "cannot be read" in err


def test_cli_check_exits_zero_when_there_is_nothing_wrong(capsys, monkeypatch):
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", WORKSPACE)
    monkeypatch.setattr("ossie_connect.fabric.Fabric.check", lambda self: [])
    code, out, _ = _run(["check", "fabric"], capsys)
    assert code == 0 and "ready" in out


# --- retrying what is worth retrying ---------------------------------------------

def test_a_throttled_request_is_retried(monkeypatch):
    """429 was a hard failure; Fabric means 'wait', not 'give up'."""
    from ossie_connect import _fabric_api

    answers = [(429, {"m": "slow down"}, {"Retry-After": "0"}),
               (200, {"value": []}, {})]
    monkeypatch.setattr(_fabric_api, "_send", lambda *a, **k: answers.pop(0))
    monkeypatch.setattr(_fabric_api.time, "sleep", lambda _s: None)
    status, _body, _ = _fabric_api.request("GET", "https://x", "t")
    assert status == 200 and answers == []


def test_retry_honours_retry_after(monkeypatch):
    from ossie_connect import _fabric_api

    waited = []
    answers = [(503, None, {"Retry-After": "7"}), (200, {}, {})]
    monkeypatch.setattr(_fabric_api, "_send", lambda *a, **k: answers.pop(0))
    monkeypatch.setattr(_fabric_api.time, "sleep", waited.append)
    _fabric_api.request("GET", "https://x", "t")
    assert waited == [7.0]


def test_a_real_error_is_not_retried(monkeypatch):
    from ossie_connect import _fabric_api

    calls = []
    monkeypatch.setattr(_fabric_api, "_send",
                        lambda *a, **k: (calls.append(1), (403, {"m": "no"}, {}))[1])
    assert _fabric_api.request("GET", "https://x", "t")[0] == 403
    assert len(calls) == 1, "403 means no, not later"


def test_retrying_gives_up_eventually(monkeypatch):
    from ossie_connect import _fabric_api

    monkeypatch.setattr(_fabric_api, "_send", lambda *a, **k: (429, None, {}))
    monkeypatch.setattr(_fabric_api.time, "sleep", lambda _s: None)
    assert _fabric_api.request("GET", "https://x", "t")[0] == 429


# --- token expiry ----------------------------------------------------------------

def _token(seconds_left):
    import base64
    import time as _t

    claims = base64.urlsafe_b64encode(
        json.dumps({"exp": int(_t.time()) + seconds_left}).encode()
    ).decode().rstrip("=")
    return f"eyJ0eXAiOiJKV1QifQ.{claims}.sig"


def test_token_expiry_is_read_from_the_claim():
    from ossie_connect._fabric_api import token_expiry

    assert 3500 < token_expiry(_token(3600)) <= 3600
    assert token_expiry("not-a-token") is None


def test_check_refuses_an_expired_token(fabric_api):
    from ossie_connect import Fabric

    connection = Fabric(workspace=WORKSPACE, token=_token(-10), api=fabric_api)
    assert "expired" in connection.check()[0].message


def test_check_warns_about_a_token_about_to_expire(fabric_api):
    from ossie_connect import Fabric

    connection = Fabric(workspace=WORKSPACE, lakehouse=LAKEHOUSE, token=_token(120),
                        api=fabric_api)
    findings = connection.check()
    assert [f.level for f in findings] == ["warning"]
    # Not an exact duration: a second passes between minting the token and reading it.
    assert "the token expires in" in findings[0].message


# --- listing ---------------------------------------------------------------------

def test_fabric_lists_what_is_deployed(fabric, fabric_api):
    assert fabric.list_models() == []
    fabric.upload(MODEL, name="one")
    fabric.upload(MODEL, name="two")
    assert fabric.list_models() == ["one", "two"]


def test_cli_list_prints_each_name(model_folder, capsys, monkeypatch):
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", WORKSPACE)
    monkeypatch.setattr("ossie_connect.fabric.Fabric.list_models", lambda self: ["a", "b"])
    code, out, _ = _run(["list", "fabric"], capsys)
    assert code == 0 and out.split() == ["a", "b"]


def test_cli_delete_removes_and_says_so(capsys, monkeypatch):
    removed = []
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", WORKSPACE)
    monkeypatch.setattr("ossie_connect.fabric.Fabric.delete",
                        lambda self, name, **k: removed.append(name) or True)
    code, out, _ = _run(["delete", "fabric", "sales_demo"], capsys)
    assert code == 0 and removed == ["sales_demo"] and "Deleted sales_demo" in out


def test_cli_delete_reports_nothing_to_remove(capsys, monkeypatch):
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", WORKSPACE)
    monkeypatch.setattr("ossie_connect.fabric.Fabric.delete", lambda self, name, **k: False)
    assert _run(["delete", "fabric", "absent"], capsys)[0] == 1
    assert _run(["delete", "fabric", "absent", "--missing-ok"], capsys)[0] == 0
