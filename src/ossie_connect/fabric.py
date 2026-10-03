"""Microsoft Fabric connection."""

import json
import os
import threading
import warnings

import yaml

from ._convert import OssieConnectWarning
from ._converters import FabricConverter
from ._fabric_api import FabricApi, FabricError, token_expiry
from ._io import read_model, write_model
from ._model import qualify_sources
from .preflight import Finding, PreflightError


class Fabric:
    """A connection to one Fabric workspace.

        fabric = Fabric(workspace="...", lakehouse="...")
        fabric.upload("model.yaml")
        fabric.download("sales_demo", "model.yaml")

    `lakehouse` is the lakehouse *item id*, and it is what actually identifies the
    lakehouse: it becomes the OneLake URL of the model's Direct Lake partitions. Without
    it the converter writes a zero-GUID placeholder, which uploads but cannot refresh.
    The lakehouse must already hold the tables the model names, under `schema`.

    `token` defaults to the Azure CLI (`az account get-access-token`), so nothing secret
    needs storing; set FABRIC_TOKEN to override that with a token from elsewhere.

    `api` and `converter` exist to be replaced in tests, and to leave room for a
    different converter implementation later.
    """

    platform = "Fabric"

    def __init__(
        self,
        workspace: str,
        *,
        lakehouse: str | None = None,
        lakehouse_workspace: str | None = None,
        schema: str = "dbo",
        mode: str = "directLake",
        token: str | None = None,
        api=None,
        converter=None,
    ):
        if not workspace:
            raise ValueError("workspace is required")
        self.workspace = workspace
        self.lakehouse = lakehouse
        self.lakehouse_workspace = lakehouse_workspace or workspace
        self.schema = schema
        if mode not in ("directLake", "import"):
            raise ValueError("mode must be 'directLake' or 'import'")
        self.mode = mode
        self._token = token
        self._api = api or FabricApi()
        self._lock = threading.Lock()
        self._checked = False
        self._converter = converter or FabricConverter()

    @classmethod
    def from_env(cls, **overrides) -> "Fabric":
        """Build a connection from FABRIC_* environment variables."""
        values = {
            "workspace": os.environ.get("FABRIC_WORKSPACE_ID", ""),
            "lakehouse": os.environ.get("FABRIC_LAKEHOUSE_ID"),
            "lakehouse_workspace": os.environ.get("FABRIC_LAKEHOUSE_WORKSPACE_ID"),
            "schema": os.environ.get("FABRIC_SCHEMA", "dbo"),
            "mode": os.environ.get("FABRIC_MODE", "directLake"),
        }
        values.update({k: v for k, v in overrides.items() if v is not None})
        if not values["workspace"]:
            raise ValueError("no workspace: set FABRIC_WORKSPACE_ID or pass workspace=")
        return cls(**values)

    def at(self, workspace: str | None = None, lakehouse: str | None = None,
           lakehouse_workspace: str | None = None,
           schema: str | None = None) -> "Fabric":
        """The same credential, pointed at a different workspace or lakehouse.

        The token is shared rather than re-acquired, so several targets in one script
        cost one `az` call.
        """
        return Fabric(
            workspace=workspace or self.workspace,
            lakehouse=lakehouse or self.lakehouse,
            lakehouse_workspace=lakehouse_workspace or (
                self.lakehouse_workspace if workspace is None else workspace
            ),
            schema=schema or self.schema,
            token=self._token,
            api=self._api,
            converter=self._converter,
        )

    @property
    def token(self) -> str:
        # Locked so a thread pool over at() connections makes one `az` call, not many.
        with self._lock:
            if self._token is None:
                self._token = self._api.acquire_token()
            return self._token

    def check(self) -> list[Finding]:
        """Verify the workspace and lakehouse exist, without changing anything.

        Worth doing because Fabric validates these at different moments: `createItem`
        never resolves the Direct Lake reference, while `updateDefinition` does. A
        lakehouse id that is not in this workspace therefore gives a first upload that
        succeeds and a second that fails, days later, with an artifact-not-found GUID.
        """
        findings = []
        try:
            token = self.token
        except FabricError as exc:
            return [Finding("error", str(exc))]

        left = token_expiry(token)
        if left is not None and left <= 0:
            return [Finding("error", "the token has expired - fetch a new one")]
        if left is not None and left < 300:
            findings.append(Finding(
                "warning",
                f"the token expires in {left // 60} min {left % 60} s; a long run may "
                "fail partway and leave something behind",
            ))

        status, body, _ = self._api.list_items(self.workspace, token, kind="Lakehouse")
        if status != 200:
            return [Finding(
                "error",
                f"workspace {self.workspace} cannot be read (HTTP {status}) - wrong id, "
                "no permission, or the token is for another tenant",
            )]

        lakehouses = {i["id"]: i.get("displayName") for i in (body or {}).get("value", [])}
        if not self.lakehouse:
            findings.append(Finding(
                "warning",
                "no lakehouse set, so the Direct Lake source is a placeholder: the model "
                "uploads but can never refresh, and a second upload will fail",
            ))
        elif self.lakehouse not in lakehouses:
            known = ", ".join(f"{n} ({i})" for i, n in lakehouses.items()) or "none"
            findings.append(Finding(
                "error",
                f"lakehouse {self.lakehouse} is not in workspace {self.workspace}; "
                f"it has: {known}",
            ))
        return findings

    def _preflight(self):
        """Check once per connection, not once per upload."""
        if self._checked:
            return
        findings = self.check()
        for finding in findings:
            if not finding.fatal:
                warnings.warn(f"{self.platform} {self.target}: {finding.message}",
                              OssieConnectWarning, stacklevel=4)
        if any(f.fatal for f in findings):
            raise PreflightError(findings)
        self._checked = True

    def upload(self, model, *, name: str | None = None, warn: bool = False,
               check: bool = True) -> str:
        """Upload an Ossie model as a semantic model. Returns its Fabric item id.

        `model` is a path, or `Yaml(...)` holding the document itself. An item of the
        same name is updated in place rather than duplicated, so uploading twice is safe.
        """
        if check:
            self._preflight()
        bim = self.to_tmsl(model, warn=warn)
        if self.lakehouse and self.mode == "directLake":
            _refuse_calculated_columns(bim)
        if self.mode == "import":
            warnings.warn(
                f"{self.platform} {self.target}: import mode deploys but cannot refresh "
                "until a cloud connection is bound to the lakehouse SQL endpoint, in the "
                "model's settings in Fabric",
                OssieConnectWarning, stacklevel=2,
            )
        name = name or bim.get("name")
        if not name:
            raise ValueError("the model has no name, and none was given")

        item = self._api.find_item(self.workspace, name, self.token)
        if item:
            self._api.update_item(self.workspace, item, bim, self.token)
            return item
        return self._api.create_item(self.workspace, name, bim, self.token)

    def download(self, name: str, out=None, *, item: str | None = None,
                 warn: bool = False) -> str:
        """Download a semantic model as Ossie YAML, optionally writing it to `out`.

        Pass `item` to address the model by id and skip the name lookup. Nothing here
        assumes the model came from Ossie - any semantic model in the workspace converts.
        """
        item = item or self._api.find_item(self.workspace, name, self.token)
        if not item:
            raise FabricError(
                f"no semantic model called '{name}' in workspace {self.workspace}"
            )
        model_bim = self._api.get_definition(self.workspace, item, self.token)
        # Fabric stores the model's name on the item, not in the TMSL, so what comes
        # back has no `name` and the converter falls back to a generic one. Put the
        # item's name back before converting, or every downloaded model is called
        # "semantic_model" and re-uploading it creates a second item under that name.
        document = json.loads(model_bim)
        document.setdefault("name", name)
        return write_model(self._converter.to_ossie(document, warn=warn), out)

    def list_models(self) -> list[str]:
        """The names of the semantic models in this workspace."""
        return self._api.list_models(self.workspace, self.token)

    def delete(self, name: str, *, item: str | None = None, missing_ok: bool = True) -> bool:
        """Remove a semantic model. Returns whether there was one to remove.

        Uploads create real items in a real workspace; a script that makes them should
        be able to unmake them, especially in someone else's tenant.
        """
        item = item or self._api.find_item(self.workspace, name, self.token)
        if not item:
            if missing_ok:
                return False
            raise FabricError(
                f"no semantic model called '{name}' in workspace {self.workspace}"
            )
        self._api.delete_item(self.workspace, item, self.token)
        return True

    def preview(self, model, *, name: str | None = None, warn: bool = False) -> str:
        """The model.bim `upload` would send. Touches no network."""
        return json.dumps(self.to_tmsl(model, warn=warn), indent=2)

    def to_tmsl(self, model, *, warn: bool = False) -> dict:
        """Convert an Ossie model to TMSL without uploading."""
        ossie_yaml = qualify_sources(read_model(model), self.schema, parts=2)
        source = (
            {"workspaceId": self.lakehouse_workspace, "itemId": self.lakehouse}
            if self.lakehouse
            else None
        )
        bim = self._converter.to_platform(ossie_yaml, source=source, warn=warn)
        # Fabric refuses a TMSL document without this: "Import from JSON supported for
        # V3 models only" (Dataset_Import_FailedToImportDataset). The converter does not
        # set it - it produces a model, not a deployable item - so it is set here, where
        # the document is being prepared to send.
        bim.setdefault("model", {}).setdefault(
            "defaultPowerBIDataSourceVersion", "powerBI_V3"
        )
        if self.mode == "import":
            self._to_import_partitions(bim, yaml.safe_load(ossie_yaml))
        return bim

    def _to_import_partitions(self, bim, document):
        """Replace Direct Lake partitions with import ones reading the SQL endpoint.

        Direct Lake cannot hold a calculated column - any field whose expression is not
        a plain column reference. Import can, but should not: the converter writes such
        a field as a calculated column named after the source column it reads, and in
        import mode that source column is not in the model, so the DAX refers to itself
        and the column fails with "a single value cannot be determined".

        Since the query is ours here, the expression goes in the SQL instead. Each field
        is projected with its ANSI SQL expression aliased to the field name, and the
        column becomes an ordinary imported one with no DAX at all.

        The cost is credentials. The model deploys, but refreshing it fails until a
        cloud connection is bound to the SQL endpoint, which Fabric will not infer:
        "this semantic model uses a default data connection without explicit connection
        credentials". That is a one-off manual step, deliberately not done here since it
        means handling someone's credentials.
        """
        if not self.lakehouse:
            raise FabricError("import mode needs a lakehouse to read from")
        endpoint = (
            self._api.lakehouse(self.lakehouse_workspace, self.lakehouse, self.token)
            .get("properties", {}).get("sqlEndpointProperties", {})
        )
        server, database = endpoint.get("connectionString"), endpoint.get("id")
        if not server or not database:
            raise FabricError(
                f"lakehouse {self.lakehouse} has no SQL endpoint yet "
                f"(provisioning: {endpoint.get('provisioningStatus', 'unknown')})"
            )

        datasets = {d.get("name"): d for d in document.get("datasets") or []}
        model = bim.setdefault("model", {})
        for table in model.get("tables", []):
            source = table["partitions"][0].get("source", {})
            entity = source.get("entityName", table["name"])
            schema = source.get("schemaName", self.schema)
            select = _select_for(datasets.get(table["name"]), table)
            table["partitions"] = [{
                "name": table["name"],
                "mode": "import",
                "source": {"type": "m", "expression": [
                    "let",
                    f'    Source = Sql.Database("{server}", "{database}"),',
                    "    Data = Value.NativeQuery(Source, \"" +
                    f'{select} FROM [{schema}].[{entity}]' + '\")',
                    "in",
                    "    Data",
                ]},
            }]
            # Whatever the SQL now computes is an ordinary column.
            for column in table.get("columns", []):
                if column.pop("type", None) == "calculated":
                    column.pop("expression", None)
                    column["sourceColumn"] = column["name"]

        # DatabaseQuery models the Direct Lake source; leaving it fails the refresh.
        remaining = [e for e in model.get("expressions", [])
                     if e.get("name") != "DatabaseQuery"]
        if remaining:
            model["expressions"] = remaining
        else:
            model.pop("expressions", None)

    @property
    def target(self) -> str:
        """Where this points, short enough for a log line."""
        return f"workspace {self.workspace}"

    def __repr__(self):
        return f"Fabric(workspace={self.workspace!r}, lakehouse={self.lakehouse!r})"


def _select_for(dataset, table):
    """A SELECT projecting each field with its SQL expression, aliased to its name.

    Falls back to the column name for anything the model does not give SQL for, so a
    field is never silently dropped from the query.
    """
    expressions = {}
    for field in (dataset or {}).get("fields") or []:
        dialects = (field.get("expression") or {}).get("dialects") or []
        sql = next((d.get("expression") for d in dialects
                    if str(d.get("dialect", "")).upper() == "ANSI_SQL"), None)
        if sql:
            expressions[field["name"]] = sql

    parts = []
    for column in table.get("columns", []):
        name = column["name"]
        source = expressions.get(name, column.get("sourceColumn") or name)
        parts.append(f"{source} AS [{name}]" if source != name else f"[{name}]")
    return "SELECT " + ", ".join(parts) if parts else "SELECT *"


def _refuse_calculated_columns(bim):
    """Direct Lake tables cannot hold calculated columns; say so before sending.

    A field with an expression that is not a bare column name becomes a calculated
    column, and Fabric rejects the whole import with "Standard expression context may
    not be used for calculated columns in Direct Lake tables". That arrives as an
    opaque long-running-operation failure several seconds later, so it is worth
    catching here and naming the fields responsible.
    """
    offenders = [
        f"{table.get('name')}.{column.get('name')}"
        for table in bim.get("model", {}).get("tables", [])
        for column in table.get("columns", [])
        if column.get("type") == "calculated"
    ]
    if offenders:
        raise FabricError(
            f"Direct Lake cannot hold calculated columns: {', '.join(offenders)}. "
            "Each has an expression that is not a plain column reference. Either give "
            "the field a bare column expression, or deploy without a lakehouse so the "
            "partitions are not Direct Lake."
        )
