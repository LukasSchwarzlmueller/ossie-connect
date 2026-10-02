"""Microsoft Fabric connection."""

import json
import os
import threading

from ._converters import FabricConverter
from ._fabric_api import FabricApi, FabricError
from ._io import read_model, write_model
from ._model import qualify_sources


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
        self._token = token
        self._api = api or FabricApi()
        self._lock = threading.Lock()
        self._converter = converter or FabricConverter()

    @classmethod
    def from_env(cls, **overrides) -> "Fabric":
        """Build a connection from FABRIC_* environment variables."""
        values = {
            "workspace": os.environ.get("FABRIC_WORKSPACE_ID", ""),
            "lakehouse": os.environ.get("FABRIC_LAKEHOUSE_ID"),
            "lakehouse_workspace": os.environ.get("FABRIC_LAKEHOUSE_WORKSPACE_ID"),
            "schema": os.environ.get("FABRIC_SCHEMA", "dbo"),
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

    def upload(self, model, *, name: str | None = None, warn: bool = False) -> str:
        """Upload an Ossie model as a semantic model. Returns its Fabric item id.

        `model` is a path, or `Yaml(...)` holding the document itself. An item of the
        same name is updated in place rather than duplicated, so uploading twice is safe.
        """
        bim = self.to_tmsl(model, warn=warn)
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
        return write_model(self._converter.to_ossie(model_bim, warn=warn), out)

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
        return self._converter.to_platform(ossie_yaml, source=source, warn=warn)

    @property
    def target(self) -> str:
        """Where this points, short enough for a log line."""
        return f"workspace {self.workspace}"

    def __repr__(self):
        return f"Fabric(workspace={self.workspace!r}, lakehouse={self.lakehouse!r})"
