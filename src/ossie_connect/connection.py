"""The contracts a connection honours.

`cli.py` drives every platform through the same calls, so these contracts exist whether
or not they are written down. They are split in two because they genuinely differ:
Snowflake can be uploaded to but not read back, since apache-ossie-snowflake ships no
reverse converter. A single fat protocol would force `Snowflake` to declare a `download`
it cannot honour, so callers that only upload depend only on `SupportsUpload`.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class SupportsUpload(Protocol):
    """A platform an Ossie model can be sent to."""

    platform: str
    """What this connects to - "Fabric", "Databricks", "Snowflake"."""

    @property
    def target(self) -> str:
        """Where this points, short enough for a log line."""
        ...

    def upload(self, model, *, name: str | None = None, warn: bool = False) -> str:
        """Upload an Ossie model. Returns where it landed. Idempotent."""
        ...

    def preview(self, model, *, name: str | None = None, warn: bool = False) -> str:
        """What `upload` would send, as text. Touches no network."""
        ...

    def delete(self, name: str, *, missing_ok: bool = True) -> bool:
        """Remove what `upload` created. Returns whether there was anything to remove."""
        ...


@runtime_checkable
class SupportsDownload(Protocol):
    """A platform a model can be read back out of, as Ossie."""

    def download(self, name: str, out=None, *, warn: bool = False) -> str:
        """Download a model as Ossie YAML, writing it to `out` if one is given."""
        ...


@runtime_checkable
class Connection(SupportsUpload, SupportsDownload, Protocol):
    """A platform that works in both directions."""


__all__ = ["Connection", "SupportsDownload", "SupportsUpload"]
