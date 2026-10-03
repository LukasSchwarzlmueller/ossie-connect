"""Checking that a connection describes something real, before it writes anything."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Finding:
    """One thing wrong, or worth knowing, about a connection's settings."""

    level: str      # "error" - uploading will fail; "warning" - it will work but badly
    message: str

    @property
    def fatal(self) -> bool:
        return self.level == "error"

    def __str__(self):
        return f"{self.level}: {self.message}"


class PreflightError(RuntimeError):
    """A connection was asked to write somewhere its settings do not describe."""

    def __init__(self, findings):
        self.findings = list(findings)
        super().__init__("; ".join(f.message for f in self.findings if f.fatal))
