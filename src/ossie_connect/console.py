"""Output helpers for scripts and command lines."""

import warnings


def _plain(message, category, filename, lineno, file=None, line=None):
    return f"warning: {message}\n"


def plain_warnings(prefix: str = "warning: ") -> None:
    """Print warnings as one readable line, without the file, line and source echo.

    The warnings this package raises are for the person running the script - a field
    was dropped, a placeholder was used - not traces for whoever wrote it. Python's
    default format buries that in two lines of noise:

        /…/databricks.py:122: UserWarning: dropped join-key field(s) …
          metric_view = self.to_metric_view(ossie_yaml, warn=warn)

    Calling this once at the top of a script turns that into:

        warning: dropped join-key field(s) …

    It changes global warning formatting, so it is opt-in and belongs to the program,
    never to an import. To attribute warnings to a particular step instead, collect
    them yourself with `warnings.catch_warnings(record=True)`.
    """
    warnings.formatwarning = (
        _plain if prefix == "warning: "
        else lambda message, *a, **k: f"{prefix}{message}\n"
    )
