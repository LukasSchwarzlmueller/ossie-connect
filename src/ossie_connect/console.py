"""Readable output for scripts and command lines."""

import os
import sys
import textwrap
import warnings
from contextlib import contextmanager

from ._convert import OssieConnectWarning

_COLOURS = {"ok": "\033[32m", "fail": "\033[31m", "warn": "\033[33m"}
_RESET = "\033[0m"
_MARKS = {"ok": "✓", "fail": "✗", "warn": "⚠"}


def _plain(message, category, filename, lineno, file=None, line=None):
    return f"warning: {message}\n"


def plain_warnings(prefix: str = "warning: ") -> None:
    """Print warnings as one readable line, without the file, line and source echo.

    The warnings this package raises are for the person running the script - a field
    was dropped, a placeholder was used - not traces for whoever wrote it. Python's
    default format buries that in two lines of noise.

    It changes global warning formatting, so it is opt-in and belongs to the program,
    never to an import. For output you control the order of, use `Reporter` instead.
    """
    warnings.formatwarning = (
        _plain if prefix == "warning: "
        else lambda message, *a, **k: f"{prefix}{message}\n"
    )


class Reporter:
    """One-line-per-outcome output, with warnings kept beside what raised them.

        report = Reporter()
        for connection in configured_connections():
            with report.capture():
                report.ok(connection.platform, connection.upload(model))

    Warnings normally go to stderr while results go to stdout, so the two interleave
    however the OS buffers them - a warning ends up beside whichever line it did not
    come from. `capture()` collects them instead and prints them under the step that
    raised them, through the same stream, in order.

    Colour is used when writing to a terminal, and skipped when piped or when NO_COLOR
    is set, so redirected output stays plain.
    """

    def __init__(self, stream=None, colour: bool | None = None, width: int = 88):
        self.stream = stream or sys.stdout
        self.width = width
        if colour is None:
            colour = self.stream.isatty() and not os.environ.get("NO_COLOR")
        self.colour = colour
        self._held: list[str] = []

    def ok(self, label: str, message: str) -> None:
        """A step that worked."""
        self._line("ok", label, message)

    def fail(self, label: str, message: str) -> None:
        """A step that did not."""
        self._line("fail", label, message)

    def warn(self, message: str) -> None:
        """Something worth knowing about the step just reported."""
        self._line("warn", "", message, indent=4)

    def note(self, message: str) -> None:
        """A line that is not about any one step."""
        print(textwrap.fill(message, self.width, initial_indent="  ",
                            subsequent_indent="  "), file=self.stream)

    def blank(self) -> None:
        print(file=self.stream)

    @contextmanager
    def capture(self, connection=None):
        """Hold warnings raised inside, then print them under the step that raised them.

        Pass the connection and its own "Platform target: " prefix is stripped - the
        message carries it so that it stands alone on stderr, but here the indentation
        already says which step it belongs to.
        """
        prefix = (
            f"{connection.platform} {connection.target}: " if connection is not None
            else None
        )
        with warnings.catch_warnings(record=True) as raised:
            warnings.simplefilter("always")
            try:
                yield self
            finally:
                for warning in raised:
                    # Other libraries' notices are not this script's problem. Matched by
                    # category, not file path: a path check also matches a caller's own
                    # file if it happens to be named after this package.
                    if not issubclass(warning.category, OssieConnectWarning):
                        continue
                    message = str(warning.message)
                    if prefix and message.startswith(prefix):
                        message = message[len(prefix):]
                    self._held.append(message)
        held, self._held = self._held, []
        for message in held:
            self.warn(message)

    def _line(self, kind, label, message, indent=2):
        # Wrap against the *visible* prefix, then colour the mark. Colouring first
        # would make textwrap count the escape codes and over-indent every
        # continuation line by their width.
        plain = f"{' ' * indent}{_MARKS[kind]} " + (f"{label:<11} " if label else "")
        body = textwrap.fill(str(message), self.width, initial_indent=plain,
                             subsequent_indent=" " * len(plain))
        if self.colour:
            body = body.replace(
                _MARKS[kind], f"{_COLOURS[kind]}{_MARKS[kind]}{_RESET}", 1
            )
        print(body, file=self.stream)
