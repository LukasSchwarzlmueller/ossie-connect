"""The Java Databricks converter: finding it, building it, and running it.

Upstream maintains the Databricks converter in Java, in apache/ossie-converters, and
removed the Python one. The Java one ships as a jar with a command-line tool, so it is
run as a subprocess: `export` for Ossie -> Metric View, `import` for the way back.

The jar is not on any package index, so it is built from source at the commit
pyproject.toml pins the Python converters to, keeping all of them at the upstream
version tested together. `ossie-connect build-databricks-converter` does that.
"""

import os
import re
import shutil
import subprocess
import tempfile
import warnings
from pathlib import Path

from ._convert import converting

# Keep equal to the apache/ossie-converters rev in pyproject.toml; a test checks it.
CONVERTERS_REV = "853c52986cd03508fe233fbab8c4213890943095"
CONVERTERS_REPO = "https://github.com/apache/ossie-converters"
JAR_ENV = "OSSIE_DATABRICKS_JAR"

# A notice on stderr: "  [field 'order_date'] datatype has no Metric View counterpart; dropped"
_NOTICE = re.compile(r"^\s+(\[.+?\] .+)$")


def cache_dir() -> Path:
    base = os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache"
    return Path(base) / "ossie-connect"


def built_jar_path() -> Path:
    """Where `build_jar` puts the jar. The rev is in the name, so a re-pin rebuilds."""
    return cache_dir() / f"ossie-databricks-converter-{CONVERTERS_REV[:12]}.jar"


def find_jar() -> Path | None:
    """The converter jar: OSSIE_DATABRICKS_JAR if set, else the one built for this pin."""
    if configured := os.environ.get(JAR_ENV):
        return Path(configured).expanduser()
    built = built_jar_path()
    return built if built.is_file() else None


def build_jar(log=print, *, force: bool = False) -> Path:
    """Clone apache/ossie-converters at the pinned rev and build the Databricks jar.

    Needs git, JDK 21+ and Maven. A jar already built for this rev is kept unless
    `force`. The checkout stays in the cache directory, so a rebuild after a re-pin
    fetches rather than clones.
    """
    if built_jar_path().is_file() and not force:
        log(f"already built for {CONVERTERS_REV[:12]}; pass --force to rebuild")
        return built_jar_path()
    missing = [tool for tool in ("git", "java", "mvn") if shutil.which(tool) is None]
    if missing:
        raise RuntimeError(
            f"building the Java converter needs {', '.join(missing)} on PATH "
            "(git, JDK 21+ and Maven)"
        )
    checkout = cache_dir() / "ossie-converters"
    checkout.parent.mkdir(parents=True, exist_ok=True)

    def run(*argv, cwd=None):
        log("$ " + " ".join(argv))
        subprocess.run(argv, cwd=cwd, check=True)

    if not (checkout / ".git").is_dir():
        run("git", "clone", "--quiet", CONVERTERS_REPO, str(checkout))
    else:
        run("git", "-C", str(checkout), "fetch", "--quiet", "origin")
    run("git", "-C", str(checkout), "checkout", "--quiet", CONVERTERS_REV)
    project = checkout / "databricks" / "java"
    run("mvn", "-q", "-B", "clean", "package", "-DskipTests", cwd=project)

    # The shaded jar holds its dependencies; Maven also leaves an original-*.jar without.
    jars = [j for j in (project / "target").glob("ossie-databricks-converter-*.jar")
            if not j.name.endswith(("-sources.jar", "-javadoc.jar"))]
    if len(jars) != 1:
        raise RuntimeError(f"expected one converter jar, found: {[j.name for j in jars]}")
    target = built_jar_path()
    shutil.copyfile(jars[0], target)
    return target


class JavaDatabricksConverter:
    """Ossie <-> Metric View YAML, via upstream's Java converter.

    Compared with the Python converter it replaces: the Metric View it writes is the
    same, but it reports what it drops rather than dropping it silently, and on import
    it labels an expression with the most portable dialect it fits (`OSSIE_SQL_2026`)
    rather than always `DATABRICKS`, so a model read from Databricks carries over to
    Fabric and Snowflake.
    """

    def __init__(self, jar: str | Path | None = None, *, java: str = "java"):
        jar = Path(jar) if jar else find_jar()
        if jar is None:
            raise FileNotFoundError(
                "no Java converter jar - run `ossie-connect build-databricks-converter`, "
                f"or set {JAR_ENV}"
            )
        self.jar = jar
        self.java = java

    def to_platform(self, ossie_yaml: str, *, warn: bool = False) -> str:
        return self._run("export", ossie_yaml, [], warn)

    def to_ossie(self, metric_view: str, *, name: str | None = None,
                 warn: bool = False) -> str:
        """`name` matters: a Metric View's YAML carries no model name of its own, so
        without one the converter names the model after the source table."""
        return self._run("import", metric_view, ["--name", name] if name else [], warn)

    def _run(self, command, text, extra, warn):
        from ._converters import ConversionError

        if not self.jar.is_file():
            raise ConversionError(f"Databricks: converter jar not found at {self.jar}")
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "input.yaml"
            source.write_text(text, encoding="utf-8")
            try:
                result = subprocess.run(
                    [self.java, "-jar", str(self.jar), command, str(source), *extra],
                    capture_output=True, text=True, encoding="utf-8",
                )
            except FileNotFoundError as exc:
                raise ConversionError(
                    f"Databricks: `{self.java}` not found - the converter needs Java 21+"
                ) from exc
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "no output"
            detail = detail.removeprefix("Conversion failed: ")
            raise ConversionError(f"Databricks: {detail}")
        with converting(warn):
            for line in result.stderr.splitlines():
                if match := _NOTICE.match(line):
                    warnings.warn(match.group(1), UserWarning, stacklevel=4)
        return result.stdout
