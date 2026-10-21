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
import time

from ossie_microsoft.engine import FABRIC_API, FABRIC_SCOPE
from ossie_microsoft.engine import _request as _send
from ossie_microsoft.engine import _request_failure as request_failure
from ossie_microsoft.engine import _wait_for_operation as wait_for_operation


_TRANSIENT = frozenset({408, 425, 429, 500, 502, 503, 504})
_RETRIES = 4


def request(method, url, token, payload=None):
    """Send a request, retrying the failures that are worth retrying.

    Fabric answers 429 with a Retry-After when it throttles, and 5xx transiently. Both
    were previously hard failures: a deploy that needed to wait two seconds instead
    stopped. The header is honoured when present, otherwise the wait doubles.
    """
    delay = 1.0
    for attempt in range(_RETRIES):
        status, body, headers = _send(method, url, token, payload)
        if status is not None and status not in _TRANSIENT:
            return status, body, headers
        if attempt == _RETRIES - 1:
            return status, body, headers
        wait = delay
        after = (headers or {}).get("Retry-After")
        if after:
            try:
                wait = min(float(after), 60.0)
            except (TypeError, ValueError):
                pass
        time.sleep(wait)
        delay *= 2
    raise AssertionError("unreachable")


class FabricError(RuntimeError):
    """A Fabric REST call failed, or no credential was available."""


_AZ_ARGS = ["account", "get-access-token", "--resource", FABRIC_SCOPE,
            "--query", "accessToken", "-o", "tsv"]
_WINDOWS_AZ = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"


def _az_commands():
    """The Azure CLIs to ask for a token, in order, as (label, argv) pairs.

    The Linux `az` comes first. On WSL the Windows one is tried after it, through
    powershell.exe: a tenant with a device-compliance policy refuses the Linux login
    (AADSTS530033) because only the Windows host counts as a managed device. `cd C:\\`
    stops PowerShell warning about a UNC working directory when started from WSL.
    """
    commands = []
    if shutil.which("az"):
        commands.append(("az", ["az", *_AZ_ARGS]))
    if shutil.which("powershell.exe"):
        script = (
            "cd C:\\; "
            "$az = (Get-Command az.cmd -ErrorAction SilentlyContinue).Source; "
            f"if (-not $az) {{ $az = '{_WINDOWS_AZ}' }}; "
            f"& $az {' '.join(_AZ_ARGS)}"
        )
        commands.append(("Windows az", ["powershell.exe", "-NoProfile", "-Command", script]))
    return commands


def acquire_token() -> str:
    """A bearer token for the Fabric API, from FABRIC_TOKEN or the Azure CLI.

    Tries each Azure CLI from `_az_commands` and returns the first token. If none gives
    one, the error names every CLI tried and its last line of output.
    """
    token = os.environ.get("FABRIC_TOKEN")
    if token:
        return token.strip()

    commands = _az_commands()
    if not commands:
        raise FabricError("no credential - run `az login`, or set FABRIC_TOKEN")

    failures = []
    for label, argv in commands:
        result = subprocess.run(argv, capture_output=True, text=True)
        token = result.stdout.strip()
        if result.returncode == 0 and token.startswith("ey"):
            return token
        lines = result.stderr.strip().splitlines()
        failures.append(f"{label}: {lines[-1] if lines else 'returned no token'}")
    raise FabricError(
        "`az login` needed (on WSL, if the tenant requires a managed device, log in "
        "from Windows PowerShell) - " + "; ".join(failures)
    )


def token_expiry(token):
    """Seconds until the token expires, or None if it cannot be read.

    A pasted token is a snapshot: it was valid when copied and may not be now. Reading
    the claim turns a mid-run 401 - which can strand a half-created item - into
    something sayable beforehand.
    """
    parts = token.split(".")
    if len(parts) != 3 or not token.startswith("eyJ"):
        return None
    try:
        claims = json.loads(
            base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4))
        )
        return int(claims["exp"] - time.time())
    except (ValueError, KeyError, TypeError):
        return None


def encode_part(value) -> str:
    """Base64-encode one definition part, which Fabric takes as InlineBase64."""
    raw = value if isinstance(value, str) else json.dumps(value)
    return base64.b64encode(raw.encode("utf-8")).decode("ascii")


def _resolve(status, body, headers, token, what, expect_result=True):
    """Return a completed response body, following a 202 long-running operation.

    `expect_result` is False for operations that finish without producing one. Creating
    an item returns the new item; updating or deleting a definition returns nothing, and
    asking anyway answers HTTP 400 OperationHasNoResult - a success reported as failure.
    """
    if status in (200, 201):
        return body
    if status == 202:
        operation = headers.get("Location")
        if not operation:
            raise FabricError(f"{what} was accepted but returned no operation location")
        result = wait_for_operation(operation, token)
        if result.get("status") != "Succeeded":
            raise FabricError(f"{what} did not succeed: {json.dumps(result)[:2000]}")
        if not expect_result:
            return None
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


def _create_item(workspace, name, bim, token, folder=None):
    """Create a semantic model from a TMSL document, in `folder` if given. Returns its id."""
    payload = {
        "displayName": name,
        "type": "SemanticModel",
        "definition": {"format": "TMSL", "parts": _parts(bim)},
    }
    if folder:
        payload["folderId"] = folder
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
        # No updateMetadata: it requires a .platform part ("UpdateMetadata is true
        # but .platform file was not provided"), and only the definition changes here.
        f"{FABRIC_API}/workspaces/{workspace}/semanticModels/{item}/updateDefinition",
        token,
        {"definition": {"parts": _parts(bim)}},
    )
    if status == 204:
        return
    _resolve(status, body, headers, token, "updating the semantic model",
             expect_result=False)


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


def _delete_item(workspace, item, token):
    status, body, headers = request(
        "DELETE", f"{FABRIC_API}/workspaces/{workspace}/items/{item}", token
    )
    if status in (200, 202, 204):
        return
    _resolve(status, body, headers, token, "deleting the semantic model",
             expect_result=False)


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

    def create_item(self, workspace, name, bim, token, folder=None):
        return _create_item(workspace, name, bim, token, folder)

    def update_item(self, workspace, item, bim, token):
        return _update_item(workspace, item, bim, token)

    def get_definition(self, workspace, item, token):
        return _get_definition(workspace, item, token)

    def delete_item(self, workspace, item, token):
        return _delete_item(workspace, item, token)

    def list_models(self, workspace, token):
        url = f"{FABRIC_API}/workspaces/{workspace}/semanticModels"
        names = []
        while url:
            status, body, _ = request("GET", url, token)
            if status != 200:
                raise FabricError(f"listing failed: {request_failure(status, body)}")
            names += [i["displayName"] for i in (body or {}).get("value", [])]
            url = (body or {}).get("continuationUri")
        return names

    def list_items(self, workspace, token, kind=None):
        suffix = f"?type={kind}" if kind else ""
        return request("GET", f"{FABRIC_API}/workspaces/{workspace}/items{suffix}", token)

    def acquire_token(self):
        return acquire_token()
