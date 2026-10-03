"""Databricks Unity Catalog connection."""

import os
import re
import threading
import warnings

from ._convert import OssieConnectWarning
from ._converters import DatabricksConverter
from ._io import read_model, write_model
from ._model import drop_duplicate_join_keys, model_name, qualify_sources
from .preflight import Finding


class DatabricksError(RuntimeError):
    """A Databricks call failed, or the connection is not usable."""


class Databricks:
    """A connection to one Unity Catalog schema.

        dbx = Databricks(catalog="main", schema="sales", warehouse_id="...")
        dbx.upload("model.yaml")
        dbx.download("sales_demo", "model.yaml")

    An Ossie model becomes a Metric View. `warehouse_id` names the SQL warehouse that
    runs the CREATE statement, and is only needed for `upload` - `download` usually
    reads the definition straight from Unity Catalog.

    Authentication is the Databricks SDK's own: environment variables (DATABRICKS_HOST,
    DATABRICKS_TOKEN) or a profile in ~/.databrickscfg. `client` and `converter` exist
    to be replaced in tests, and to leave room for the Java converter upstream now
    maintains in place of the Python one this uses.
    """

    platform = "Databricks"

    def __init__(
        self,
        catalog: str,
        schema: str,
        *,
        warehouse_id: str | None = None,
        client=None,
        converter=None,
    ):
        if not catalog or not schema:
            raise ValueError("catalog and schema are required")
        self.catalog = catalog
        self.schema = schema
        self.warehouse_id = warehouse_id
        self._client = client
        self._converter = converter or DatabricksConverter()
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls, **overrides) -> "Databricks":
        """Build a connection from DATABRICKS_* environment variables."""
        values = {
            "catalog": os.environ.get("DATABRICKS_CATALOG", ""),
            "schema": os.environ.get("DATABRICKS_SCHEMA", ""),
            "warehouse_id": os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        }
        values.update({k: v for k, v in overrides.items() if v is not None})
        if not values["catalog"] or not values["schema"]:
            raise ValueError(
                "no target schema: set DATABRICKS_CATALOG and DATABRICKS_SCHEMA, or "
                "pass catalog=/schema="
            )
        return cls(**values)

    def at(self, catalog: str | None = None, schema: str | None = None,
           warehouse_id: str | None = None) -> "Databricks":
        """The same workspace, pointed at a different catalog or schema.

            dbx = Databricks.from_env()
            dbx.upload("sales.yaml")
            dbx.at(schema="marketing").upload("marketing.yaml")

        The authenticated client is shared rather than rebuilt, so deploying to several
        schemas in one script opens one connection, not one per target.
        """
        return Databricks(
            catalog=catalog or self.catalog,
            schema=schema or self.schema,
            warehouse_id=warehouse_id or self.warehouse_id,
            client=self._client,
            converter=self._converter,
        )

    @property
    def client(self):
        # Locked because at() hands the same client to several connections, which
        # invites a thread pool over them.
        with self._lock:
            if self._client is None:
                from databricks.sdk import WorkspaceClient

                self._client = WorkspaceClient()  # auth from env / ~/.databrickscfg
            return self._client

    def check(self) -> list[Finding]:
        """Verify the catalog and schema exist, without changing anything."""
        findings = []
        if not self.warehouse_id:
            findings.append(Finding(
                "warning",
                "no warehouse_id: downloading works, uploading needs a SQL warehouse",
            ))
        try:
            self.client.schemas.get(f"{self.catalog}.{self.schema}")
        except Exception as exc:  # the SDK raises NotFound/PermissionDenied of its own
            findings.append(Finding(
                "error",
                f"{self.catalog}.{self.schema} cannot be read - uploading creates tables "
                f"inside it but will not create it: {str(exc)[:160]}",
            ))
        return findings

    def upload(self, model, *, name: str | None = None, warn: bool = False) -> str:
        """Upload an Ossie model as a Metric View. Returns its fully qualified name.

        CREATE OR REPLACE, so uploading twice replaces rather than duplicating.
        """
        ossie_yaml = read_model(model)
        name = name or model_name(ossie_yaml)
        self._execute(self.preview(ossie_yaml, name=name, warn=warn))
        return f"{self.catalog}.{self.schema}.{name}"

    def download(self, name: str, out=None, *, warn: bool = False) -> str:
        """Download a Metric View as Ossie YAML, optionally writing it to `out`."""
        full_name = name if name.count(".") == 2 else f"{self.catalog}.{self.schema}.{name}"
        metric_view = self._read_definition(full_name)
        # The view's own name, or the model comes back named after its source table.
        ossie_yaml = self._converter.to_ossie(
            metric_view, name=full_name.rsplit(".", 1)[-1], warn=warn
        )
        return write_model(ossie_yaml, out)

    def delete(self, name: str, *, missing_ok: bool = True) -> bool:
        """Drop a Metric View. Returns whether there was one to drop."""
        full_name = name if name.count(".") == 2 else f"{self.catalog}.{self.schema}.{name}"
        exists = "IF EXISTS " if missing_ok else ""
        self._execute(f"DROP VIEW {exists}{full_name}")
        return True

    def preview(self, model, *, name: str | None = None, warn: bool = False) -> str:
        """The CREATE OR REPLACE VIEW statement `upload` would run. Touches no network."""
        ossie_yaml = read_model(model)
        name = name or model_name(ossie_yaml)
        metric_view = self.to_metric_view(ossie_yaml, warn=warn)
        if "$$" in metric_view:
            raise DatabricksError(
                "the Metric View YAML contains '$$', which would terminate the SQL "
                "dollar-quoted string early"
            )
        return (
            f"CREATE OR REPLACE VIEW {self.catalog}.{self.schema}.{name} "
            f"WITH METRICS LANGUAGE YAML AS $$\n{metric_view}\n$$"
        )

    def to_metric_view(self, model, *, warn: bool = False,
                       dedupe_join_keys: bool = True) -> str:
        """Convert an Ossie model to Metric View YAML without uploading.

        `dedupe_join_keys` drops join-key fields a Metric View would reject as duplicate
        dimensions - see `_model.drop_duplicate_join_keys`. Turn it off to see the
        converter's own error instead.
        """
        ossie_yaml = qualify_sources(
            read_model(model), f"{self.catalog}.{self.schema}", parts=3
        )
        if dedupe_join_keys:
            ossie_yaml, removed = drop_duplicate_join_keys(ossie_yaml)
            if removed:
                # Named, because this lands on stderr among other platforms' output
                # and is otherwise impossible to attribute.
                warnings.warn(
                    f"{self.platform} {self.target}: dropped {', '.join(removed)} "
                    "(duplicate dimension; still in primary_key)",
                    OssieConnectWarning,
                    stacklevel=2,
                )
        return self._converter.to_platform(ossie_yaml, warn=warn)

    def _execute(self, statement: str):
        if not self.warehouse_id:
            raise DatabricksError(
                "no warehouse_id - set DATABRICKS_WAREHOUSE_ID, or pass warehouse_id="
            )
        result = self.client.statement_execution.execute_statement(
            statement=statement, warehouse_id=self.warehouse_id, wait_timeout="30s"
        )
        error = result.status.error if result.status else None
        if error is not None:
            raise DatabricksError(f"{error.error_code}: {error.message}")
        return result

    def _read_definition(self, full_name: str) -> str:
        """The Metric View's YAML body, from Unity Catalog.

        SHOW CREATE TABLE is preferred because it is the only *complete* answer. The
        `view_definition` column returns a normalized form with `synonyms` and
        `comment` stripped - measured against a real workspace, 646 characters against
        1149 for the same view - so reading it loses exactly the metadata an Ossie
        model cares most about. It is used only when there is no warehouse to run a
        statement on, and says what that costs.
        """
        if self.warehouse_id:
            result = self._execute(f"SHOW CREATE TABLE {full_name}")
            rows = (result.result.data_array or []) if result.result else []
            if not rows or not rows[0]:
                raise DatabricksError(f"SHOW CREATE TABLE returned nothing for {full_name}")
            definition = _yaml_body(rows[0][0])
            if definition is None:
                raise DatabricksError(
                    f"{full_name} does not look like a Metric View - no YAML body found "
                    "in its definition"
                )
            return definition

        warnings.warn(
            f"{self.platform} {self.target}: reading {full_name} without a warehouse; "
            "synonyms and comments are dropped by that path. Set warehouse_id to keep "
            "them",
            OssieConnectWarning,
            stacklevel=3,
        )
        try:
            table = self.client.tables.get(full_name)
        except Exception as exc:  # the SDK raises its own NotFound/PermissionDenied types
            raise DatabricksError(f"could not read {full_name}: {exc}") from exc
        definition = _yaml_body(getattr(table, "view_definition", None))
        if definition is None:
            raise DatabricksError(
                f"{full_name} does not look like a Metric View - no YAML body found in "
                "its definition"
            )
        return definition

    @property
    def target(self) -> str:
        """Where this points, short enough for a log line."""
        return f"{self.catalog}.{self.schema}"

    def __repr__(self):
        return f"Databricks(catalog={self.catalog!r}, schema={self.schema!r})"


def _yaml_body(text):
    """The Metric View YAML inside `... AS $$ <yaml> $$`, or None if this isn't one.

    A plain view's definition is SQL, not YAML, and must not be handed to the converter
    as if it were.
    """
    if not text or not text.strip():
        return None
    match = re.search(r"\$\$(.*?)\$\$", text, re.DOTALL)
    if match:
        return match.group(1).strip("\n")
    return None if text.lstrip().upper().startswith(("CREATE", "SELECT", "WITH")) else text
