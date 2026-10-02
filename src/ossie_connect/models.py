"""A folder of Ossie model files, addressed by model name."""

from collections.abc import Mapping
from pathlib import Path

import yaml


class Models(Mapping):
    """The Ossie models in a folder, keyed by the name inside each file.

        models = Models("models/")

        list(models)                          # ['finance_demo', 'sales_demo', ...]
        fabric.upload(models["sales_demo"])   # values are paths, which upload takes

        for name, path in models.items():
            if name.startswith("sales_"):
                fabric.upload(path)

    A file's name and the model's name are independent - `sales.yaml` can hold
    `name: sales_demo` - so selecting by model name otherwise means parsing every file
    by hand. This is an ordinary Mapping, so `in`, `len`, `.keys()`, `.items()` and
    dict-style iteration all behave as expected.
    """

    def __init__(self, folder, pattern: str = "*.y*ml"):
        self.folder = Path(folder)
        if not self.folder.is_dir():
            raise NotADirectoryError(f"no such model folder: {folder}")
        self.pattern = pattern
        self._paths = self._scan()

    def _scan(self) -> dict[str, Path]:
        found: dict[str, Path] = {}
        for path in sorted(self.folder.glob(self.pattern)):
            name = _name_of(path)
            if name is None:
                continue
            if name in found:
                raise ValueError(
                    f"two files in {self.folder} both define the model '{name}': "
                    f"{found[name].name} and {path.name}"
                )
            found[name] = path
        return found

    def __getitem__(self, name: str) -> Path:
        try:
            return self._paths[name]
        except KeyError:
            known = ", ".join(self._paths) or "none"
            raise KeyError(
                f"no model called '{name}' in {self.folder} (found: {known})"
            ) from None

    def __iter__(self):
        return iter(self._paths)

    def __len__(self) -> int:
        return len(self._paths)

    def refresh(self) -> "Models":
        """Re-scan the folder, picking up files added or renamed since construction."""
        self._paths = self._scan()
        return self

    def __repr__(self):
        return f"Models({str(self.folder)!r}, {len(self)} models)"


def _name_of(path: Path):
    """The `name` of the Ossie model in `path`, or None if it isn't one."""
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (yaml.YAMLError, OSError, UnicodeDecodeError):
        return None
    if not isinstance(document, dict):
        return None
    name = document.get("name")
    return name if isinstance(name, str) and name else None
