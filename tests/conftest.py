"""Fakes standing in for the two platforms."""

import json
from pathlib import Path

import pytest

MODEL = Path(__file__).parent / "data" / "model.yaml"
WORKSPACE = "11111111-1111-1111-1111-111111111111"
LAKEHOUSE = "22222222-2222-2222-2222-222222222222"
ITEM = "33333333-3333-3333-3333-333333333333"


class FakeFabricApi:
    """A Fabric workspace, in memory. Records what it was asked to do."""

    def __init__(self, items=None):
        self.items = dict(items or {})          # name -> model.bim text
        self.ids = {name: ITEM for name in self.items}
        self.calls = []

    lakehouses = [{"id": "22222222-2222-2222-2222-222222222222",
                   "displayName": "raw", "type": "Lakehouse"}]

    def list_items(self, workspace, token, kind=None):
        self.calls.append(("list_items", kind))
        if workspace != WORKSPACE:
            return 404, {"message": "not found"}, {}
        return 200, {"value": self.lakehouses}, {}

    def acquire_token(self):
        self.calls.append(("acquire_token",))
        return "fake-token"

    def find_item(self, workspace, name, token):
        self.calls.append(("find_item", name))
        return self.ids.get(name)

    def create_item(self, workspace, name, bim, token):
        self.calls.append(("create_item", name))
        self.items[name] = json.dumps(bim)
        self.ids[name] = ITEM
        return ITEM

    def update_item(self, workspace, item, bim, token):
        self.calls.append(("update_item", item))
        name = next(n for n, i in self.ids.items() if i == item)
        self.items[name] = json.dumps(bim)

    def delete_item(self, workspace, item, token):
        self.calls.append(("delete_item", item))
        name = next(n for n, i in self.ids.items() if i == item)
        del self.items[name], self.ids[name]

    def get_definition(self, workspace, item, token):
        """As real Fabric does: the stored TMSL has no top-level `name`.

        Fabric keeps the model's name on the item, not in the definition. Observed
        against a live workspace - the returned document has only `compatibilityLevel`
        and `model`.
        """
        self.calls.append(("get_definition", item))
        name = next(n for n, i in self.ids.items() if i == item)
        stored = json.loads(self.items[name])
        stored.pop("name", None)
        return json.dumps(stored)


class FakeDatabricksClient:
    """A Unity Catalog schema, in memory."""

    def __init__(self, view_definitions=None, *, supports_view_definition=True):
        self.views = dict(view_definitions or {})   # full name -> stored CREATE text
        self.supports_view_definition = supports_view_definition
        self.statements = []
        self.tables = self._Tables(self)
        self.statement_execution = self._Statements(self)

    class _Tables:
        def __init__(self, outer):
            self.outer = outer

        def get(self, full_name):
            if full_name not in self.outer.views:
                raise LookupError(f"Table '{full_name}' does not exist.")
            definition = (
                self.outer.views[full_name]
                if self.outer.supports_view_definition
                else ""
            )
            return type("TableInfo", (), {"view_definition": definition})()

    class _Statements:
        def __init__(self, outer):
            self.outer = outer

        def execute_statement(self, statement, warehouse_id, wait_timeout):
            self.outer.statements.append(statement)
            rows = None
            if statement.startswith("SHOW CREATE TABLE"):
                name = statement.split()[-1]
                rows = [[self.outer.views.get(name, "")]]
            elif statement.startswith("CREATE OR REPLACE VIEW"):
                name = statement.split()[4]
                self.outer.views[name] = statement
            elif statement.startswith("DROP VIEW"):
                self.outer.views.pop(statement.split()[-1], None)
            result = type("Result", (), {"data_array": rows})() if rows else None
            return type("Response", (), {"status": None, "result": result})()


class FakeSnowflakeConnection:
    """A Snowflake session, in memory. Records every statement executed."""

    def __init__(self):
        self.statements = []

    def cursor(self):
        return self._Cursor(self)

    class _Cursor:
        def __init__(self, outer):
            self.outer = outer

        def execute(self, statement):
            self.outer.statements.append(statement)
            return self

        def fetchone(self):
            return ("Semantic view created",)


@pytest.fixture
def snowflake_connection():
    return FakeSnowflakeConnection()


@pytest.fixture
def fabric_api():
    return FakeFabricApi()


@pytest.fixture
def databricks_client():
    return FakeDatabricksClient()
