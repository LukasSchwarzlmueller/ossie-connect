"""Fabric REST plumbing: tokens, long-running operations, item definitions.

The HTTP layer is `ossie_microsoft.engine`'s, reused rather than rewritten. `_request`
already handles the Fabric error envelope and transport failures, and `_wait_for_operation`
already polls the operation endpoint that every definition call can return. Both are
private to that module - it exports only `validate_with_engine` - so the import is
quarantined here instead of being spread across the package.
"""

import base64
import json
import os
import shutil
import subprocess

from ossie_microsoft.engine import FABRIC_API, FABRIC_SCOPE
from ossie_microsoft.engine import _request as request
from ossie_microsoft.engine import _request_failure as request_failure
from ossie_microsoft.engine import _wait_for_operation as wait_for_operation


class FabricError(RuntimeError):
    """A Fabric REST call failed, or no credential was available."""


def acquire_token() -> str:
    """A bearer token for the Fabric API, from FABRIC_TOKEN or the Azure CLI."""
    token = os.environ.get("FABRIC_TOKEN")
    if token:
        return token.strip()

    if shutil.which("az") is None:
        raise FabricError(
            "no Fabric credential: install the Azure CLI and run `az login`, or set "
            "FABRIC_TOKEN to a token for https://api.fabric.microsoft.com"
        )
    try:
        result = subprocess.run(
            ["az", "account", "get-access-token", "--resource", FABRIC_SCOPE,
             "--query", "accessToken", "-o", "tsv"],
            capture_output=True, text=True, check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise FabricError(
            f"`az account get-access-token` failed - run `az login` first:\n{exc.stderr.strip()}"
        ) from exc
    return result.stdout.strip()


def encode_part(value) -> str:
    """Base64-encode one definition part, which Fabric takes as InlineBase64."""
    raw = value if isinstance(value, str) else json.dumps(value)
    return base64.b64encode(raw.encode("utf-8")).decode("ascii")


def _resolve(status, body, headers, token, what):
    """Return a completed response body, following a 202 long-running operation."""
    if status in (200, 201):
        return body
    if status == 202:
        operation = headers.get("Location")
        if not operation:
            raise FabricError(f"{what} was accepted but returned no operation location")
        result = wait_for_operation(operation, token)
        if result.get("status") != "Succeeded":
            raise FabricError(f"{what} did not succeed: {json.dumps(result)[:2000]}")
        result_status, created, _headers = request("GET", f"{operation}/result", token)
        if result_status != 200:
            raise FabricError(
                f"{what} succeeded but its result could not be fetched: "
                f"{request_failure(result_status, created)}"
            )
        return created
    raise FabricError(f"{what} failed: {request_failure(status, body)}")


def _find_item(workspace, name, token):
    """The id of the semantic model called `name`, or None. Follows paging."""
    url = f"{FABRIC_API}/workspaces/{workspace}/semanticModels"
    while url:
        status, body, _headers = request("GET", url, token)
        if status != 200:
            raise FabricError(f"listing semantic models failed: {request_failure(status, body)}")
        for item in (body or {}).get("value", []):
            if item.get("displayName") == name:
                return item.get("id")
        url = (body or {}).get("continuationUri")
    return None


def _create_item(workspace, name, bim, token):
    """Create a semantic model from a TMSL document. Returns its id."""
    payload = {
        "displayName": name,
        "type": "SemanticModel",
        "definition": {"format": "TMSL", "parts": _parts(bim)},
    }
    status, body, headers = request(
        "POST", f"{FABRIC_API}/workspaces/{workspace}/items", token, payload
    )
    created = _resolve(status, body, headers, token, "creating the semantic model")
    item = (created or {}).get("id")
    if not item:
        raise FabricError("the semantic model was created but Fabric returned no id")
    return item


def _update_item(workspace, item, bim, token):
    """Replace an existing semantic model's definition, keeping its id."""
    status, body, headers = request(
        "POST",
        f"{FABRIC_API}/workspaces/{workspace}/semanticModels/{item}"
        "/updateDefinition?updateMetadata=True",
        token,
        {"definition": {"parts": _parts(bim)}},
    )
    if status == 204:
        return
    _resolve(status, body, headers, token, "updating the semantic model")


def _get_definition(workspace, item, token):
    """Download a semantic model's TMSL definition. Returns the model.bim text."""
    status, body, headers = request(
        "POST",
        f"{FABRIC_API}/workspaces/{workspace}/semanticModels/{item}/getDefinition?format=TMSL",
        token,
    )
    body = _resolve(status, body, headers, token, "downloading the semantic model")
    parts = ((body or {}).get("definition") or {}).get("parts") or []
    for part in parts:
        # Fabric prefixes parts with `definition/`; match on the basename.
        if part.get("path", "").rsplit("/", 1)[-1] == "model.bim":
            return base64.b64decode(part["payload"]).decode("utf-8")
    raise FabricError(
        "the downloaded definition has no model.bim part; got: "
        + ", ".join(sorted(p.get("path", "?") for p in parts))
    )


def _parts(bim):
    return [
        {"path": "model.bim", "payload": encode_part(bim), "payloadType": "InlineBase64"},
        {
            "path": "definition.pbism",
            "payload": encode_part({"version": "1.0", "settings": {}}),
            "payloadType": "InlineBase64",
        },
    ]


class FabricApi:
    """The Fabric REST calls this package makes, as one replaceable collaborator.

    Keeping them behind an object rather than calling the module functions directly is
    what lets `Fabric` be tested against a fake instead of by patching module globals.
    """

    def find_item(self, workspace, name, token):
        return _find_item(workspace, name, token)

    def create_item(self, workspace, name, bim, token):
        return _create_item(workspace, name, bim, token)

    def update_item(self, workspace, item, bim, token):
        return _update_item(workspace, item, bim, token)

    def get_definition(self, workspace, item, token):
        return _get_definition(workspace, item, token)

    def acquire_token(self):
        return acquire_token()
