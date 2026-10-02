"""Loading connection settings from a .env file."""

import os
from pathlib import Path


def load_env(path=".env") -> bool:
    """Set variables from a `KEY=value` file, without overriding the real environment.

    `from_env()` reads os.environ, which a shell session has but a script usually does
    not. Call this first and a script needs no credential handling of its own:

        from ossie_connect import Databricks, load_env

        load_env()
        Databricks.from_env().upload("model.yaml")

    Returns whether the file was found. Existing variables always win, so exporting one
    in the shell still overrides the file.
    """
    file = Path(path)
    if not file.is_file():
        return False
    for line in file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        if value:
            os.environ.setdefault(key.strip(), value)
    return True
