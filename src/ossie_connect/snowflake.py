"""Snowflake Semantic View connection.

Upload only. `apache-ossie-snowflake` converts Ossie to Snowflake's native Semantic View
YAML but ships nothing going the other way, so there is no `download` here - see
`connection.SupportsUpload`.
"""

import os
import threading
import re

import yaml

from ._converters import SnowflakeConverter
from ._io import read_model
from ._model import model_name, qualify_sources

# An identifier followed by "(" is a function call, not a column reference. Matching on
# that rather than a keyword list matters: a column genuinely named `date`, `count` or
# `order` would be discarded by any list long enough to be useful.
_IDENTIFIER = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*(\()?")


class Snowflake:
    """A connection to one Snowflake database and schema.

        snow = Snowflake(database="OSSIE_DEMO", schema="PUBLIC")
        snow.upload("model.yaml")

    The model becomes a native, SQL-queryable Semantic View, created through
    `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML`. The warehouse, database and schema are all
    created if missing.

    Authentication is account/user/password, read from SNOWFLAKE_* variables by
    `from_env()`. Pass `connection` to supply a live connector you built yourself, which
    is also how a different auth method (key pair, SSO) is used.
    """

    platform = "Snowflake"

    def __init__(
        self,
        database: str,
        schema: str,
        *,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        role: str | None = None,
        warehouse: str = "OSSIE_COMPUTE_WH",
        connection=None,
        converter=None,
    ):
        if not database or not schema:
            raise ValueError("database and schema are required")
        self.database = database
        self.schema = schema
        self.account = account
        self.user = user
        self.password = password
        self.role = role
        self.warehouse = warehouse
        self._connection = connection
        self._converter = converter or SnowflakeConverter()
        self._prepared = False
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls, **overrides) -> "Snowflake":
        """Build a connection from SNOWFLAKE_* environment variables."""
        values = {
            "database": os.environ.get("SNOWFLAKE_DATABASE", ""),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA", ""),
            "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
            "user": os.environ.get("SNOWFLAKE_USER"),
            "password": os.environ.get("SNOWFLAKE_PASSWORD"),
            "role": os.environ.get("SNOWFLAKE_ROLE"),
        }
        if os.environ.get("SNOWFLAKE_WAREHOUSE"):
            values["warehouse"] = os.environ["SNOWFLAKE_WAREHOUSE"]
        values.update({k: v for k, v in overrides.items() if v is not None})
        if not values["database"] or not values["schema"]:
            raise ValueError(
                "no target schema: set SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA, or pass "
                "database=/schema="
            )
        return cls(**values)

    def at(self, database: str | None = None, schema: str | None = None,
           warehouse: str | None = None) -> "Snowflake":
        """The same session, pointed at a different database or schema.

        The open connection is shared rather than reopened, so several targets in one
        script cost one login.
        """
        return Snowflake(
            database=database or self.database,
            schema=schema or self.schema,
            account=self.account, user=self.user, password=self.password,
            role=self.role, warehouse=warehouse or self.warehouse,
            connection=self._connection, converter=self._converter,
        )

    @property
    def connection(self):
        with self._lock:
            return self._connect()

    def _connect(self):
        if self._connection is None:
            missing = [
                name
                for name, value in (
                    ("SNOWFLAKE_ACCOUNT", self.account),
                    ("SNOWFLAKE_USER", self.user),
                    ("SNOWFLAKE_PASSWORD", self.password),
                )
                if not value
            ]
            if missing:
                raise SnowflakeError(f"no credential - set {', '.join(missing)}")
            import snowflake.connector

            self._connection = snowflake.connector.connect(
                account=self.account, user=self.user,
                password=self.password, role=self.role,
            )
        return self._connection

    def upload(self, model, *, name: str | None = None, warn: bool = False) -> str:
        """Upload an Ossie model as a Semantic View. Returns its fully qualified name.

        The warehouse, database and schema are created if they do not exist. The view
        itself is replaced if it does.
        """
        ossie_yaml = read_model(model)
        name = name or model_name(ossie_yaml)
        semantic_view = self.to_semantic_view(ossie_yaml, warn=warn)

        cursor = self.connection.cursor()
        self._prepare(cursor)
        cursor.execute(self._create_call(semantic_view))
        return f"{self.database}.{self.schema}.{name}"

    def _prepare(self, cursor):
        """Create the warehouse, database and schema - once per connection.

        These were re-run on every upload, four extra round trips each time. Uploading
        several models to one target now pays for them once.
        """
        if self._prepared:
            return
        cursor.execute(
            f"CREATE WAREHOUSE IF NOT EXISTS {self.warehouse} WAREHOUSE_SIZE=XSMALL "
            "AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE"
        )
        cursor.execute(f"USE WAREHOUSE {self.warehouse}")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.database}.{self.schema}")
        self._prepared = True

    def delete(self, name: str, *, missing_ok: bool = True) -> bool:
        """Drop a Semantic View. Returns whether there was one to drop."""
        full_name = name if name.count(".") == 2 else f"{self.database}.{self.schema}.{name}"
        exists = "IF EXISTS " if missing_ok else ""
        self.connection.cursor().execute(f"DROP SEMANTIC VIEW {exists}{full_name}")
        return True

    def preview(self, model, *, name: str | None = None, warn: bool = False) -> str:
        """The CALL statement `upload` would run. Touches no network."""
        return self._create_call(self.to_semantic_view(model, warn=warn))

    def to_semantic_view(self, model, *, warn: bool = False,
                         nest_metrics: bool = True) -> str:
        """Convert an Ossie model to Snowflake Semantic View YAML without uploading.

        `nest_metrics` moves the converter's top-level `metrics:` list under the table
        each metric belongs to, which Snowflake requires - see `nest_metrics_per_table`.
        """
        ossie_yaml = qualify_sources(
            read_model(model), f"{self.database}.{self.schema}", parts=3
        )
        semantic_view = self._converter.to_platform(ossie_yaml, warn=warn)
        return nest_metrics_per_table(semantic_view) if nest_metrics else semantic_view

    def _create_call(self, semantic_view: str) -> str:
        if "$$" in semantic_view:
            raise SnowflakeError(
                "the Semantic View YAML contains '$$', which would terminate the SQL "
                "dollar-quoted string early"
            )
        return (
            f"CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(\n"
            f"  '{self.database}.{self.schema}',\n  $$\n{semantic_view}\n  $$\n)"
        )

    @property
    def target(self) -> str:
        """Where this points, short enough for a log line."""
        return f"{self.database}.{self.schema}"

    def __repr__(self):
        return f"Snowflake(database={self.database!r}, schema={self.schema!r})"


class SnowflakeError(RuntimeError):
    """A Snowflake call failed, or the connection is not usable."""


def nest_metrics_per_table(semantic_view_yaml: str) -> str:
    """Move top-level metrics under the table that owns them.

    `apache-ossie-snowflake` emits `metrics:` as a sibling of `tables:`, which
    Snowflake's native schema rejects outright - every metric fails with "Unsupported
    expression in the definition of derived metric". Snowflake wants each metric nested
    inside its owning table.

    The owning table is read from the metric's own expression: the identifiers it uses
    are matched against each table's dimensions, facts and time dimensions, ignoring any
    followed by "(", which is a function call rather than a column. An expression naming
    no column at all (`COUNT(*)`) or columns from several tables falls back to the fact
    table - the one that is only ever the left side of a relationship.
    """
    document = yaml.safe_load(semantic_view_yaml)
    if not isinstance(document, dict):
        return semantic_view_yaml
    metrics = document.pop("metrics", None)
    if not metrics:
        return semantic_view_yaml

    tables = {t["name"]: t for t in document.get("tables") or [] if isinstance(t, dict)}
    owner_of = _column_owners(tables)
    fallback = _fact_table(document, tables)

    for metric in metrics:
        owners = {
            owner_of[token]
            for token, call in _IDENTIFIER.findall(str(metric.get("expr", "")))
            if not call and token in owner_of
        }
        table = owners.pop() if len(owners) == 1 else fallback
        if table is None:
            raise ValueError(
                f"cannot tell which table metric '{metric.get('name')}' belongs to, and "
                "there is no single fact table to fall back to; Snowflake needs every "
                "metric nested under one table"
            )
        tables[table].setdefault("metrics", []).append(metric)

    return yaml.safe_dump(document, sort_keys=False, allow_unicode=True)


def _column_owners(tables) -> dict:
    """Column name -> the one table that has it, for columns unique across tables."""
    seen: dict[str, set] = {}
    for name, table in tables.items():
        for key in ("dimensions", "facts", "time_dimensions"):
            for column in table.get(key) or []:
                if isinstance(column, dict) and column.get("name"):
                    seen.setdefault(column["name"], set()).add(name)
    return {column: next(iter(owners)) for column, owners in seen.items() if len(owners) == 1}


def _fact_table(document, tables):
    """The table that is only ever the left side of a relationship, if there is one."""
    left, right = set(), set()
    for relationship in document.get("relationships") or []:
        if isinstance(relationship, dict):
            left.add(relationship.get("left_table"))
            right.add(relationship.get("right_table"))
    candidates = {name for name in left - right if name in tables}
    if len(candidates) == 1:
        return candidates.pop()
    return next(iter(tables)) if len(tables) == 1 else None
