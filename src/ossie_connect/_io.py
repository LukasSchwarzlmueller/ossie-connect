"""Reading and writing Ossie model files."""

from pathlib import Path


class Yaml(str):
    """Ossie YAML text, marked as text rather than a path.

    The connections take a file path, which is what nearly every caller has. Wrap a
    string in `Yaml` to pass the document itself instead:

        fabric.upload("model.yaml")
        fabric.upload(Yaml(generated_yaml))

    Guessing between the two from the string's shape would misread a single-line
    document as a filename, so the distinction is explicit.
    """

    __slots__ = ()


def read_model(model) -> Yaml:
    """Return the Ossie YAML for a `Yaml` document, a `Path`, or a path string.

    The result is itself a `Yaml`, so feeding it back in is a no-op rather than a
    second attempt to open it as a file.
    """
    if isinstance(model, Yaml):
        return model
    if isinstance(model, (str, Path)):
        path = Path(model)
        if not path.is_file():
            raise FileNotFoundError(f"no such model file: {model}")
        return Yaml(path.read_text(encoding="utf-8"))
    raise TypeError(
        f"model must be a path or Yaml(...) text, not {type(model).__name__}"
    )


def write_model(ossie_yaml: str, out=None) -> str:
    """Return the YAML, writing it to `out` first if one was given."""
    if out is not None:
        path = Path(out)
        if path.parent != Path(""):
            path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(ossie_yaml, encoding="utf-8")
    return ossie_yaml
