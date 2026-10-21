"""Compare a local Ossie model with what is deployed.

Compared in the platform's own format, not in Ossie: a round trip through Ossie is
lossy (see the README), so a downloaded model never equals the local one even when
nothing changed. Converting the local model and comparing it with the stored
definition asks the useful question instead - would uploading change anything?
"""

import difflib
import json
from dataclasses import dataclass


@dataclass
class Comparison:
    name: str
    status: str          # "new", "changed" or "same"
    diff: str = ""


def compare(name, local, remote):
    """`local` and `remote` are parsed documents; `remote` is None if not deployed."""
    if remote is None:
        return Comparison(name, "new")
    if local == remote:
        return Comparison(name, "same")
    a = json.dumps(remote, indent=2, sort_keys=True).splitlines()
    b = json.dumps(local, indent=2, sort_keys=True).splitlines()
    diff = "\n".join(difflib.unified_diff(a, b, f"deployed/{name}", f"local/{name}",
                                          lineterm=""))
    return Comparison(name, "changed", diff)
