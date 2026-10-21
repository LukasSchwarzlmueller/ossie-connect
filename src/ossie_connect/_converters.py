"""Conversion between Ossie and each platform's own format.

Conversion is kept behind these small classes rather than called inline so that the
implementation can be replaced without touching the connections. For Databricks that
has happened: upstream maintains the converter in Java and removed the Python one, so
`default_databricks_converter` picks the Java one (`_java.JavaDatabricksConverter`)
whenever its jar can be found, and falls back to the Python one otherwise.
"""

from contextlib import contextmanager

from ._convert import converting


class ConversionError(RuntimeError):
    """A converter refused the model.

    Each converter raises its own type, all deriving straight from Exception, so a
    caller would have to import three private modules to catch them. They are
    re-raised as this, which the CLI already handles - otherwise a model with, say,
    the wrong `version` reaches the user as a traceback rather than a sentence.
    """


@contextmanager
def _as_conversion_error(platform):
    try:
        yield
    except RuntimeError:
        raise
    except Exception as exc:                     # noqa: BLE001 - three unrelated types
        raise ConversionError(f"{platform}: {exc}") from exc


class FabricConverter:
    """Ossie <-> TMSL, via the apache-ossie-microsoft Python converter."""

    def to_platform(self, ossie_yaml: str, *, source=None, warn: bool = False) -> dict:
        from ossie_microsoft.ossie_to_semantic_model import convert_ossie_to_semantic_model

        with _as_conversion_error("Fabric"), converting(warn):
            return convert_ossie_to_semantic_model(ossie_yaml, source=source)

    def to_ossie(self, model_bim: str, *, warn: bool = False) -> str:
        from ossie_microsoft.semantic_model_to_ossie import convert_semantic_model_to_ossie

        with _as_conversion_error("Fabric"), converting(warn):
            return convert_semantic_model_to_ossie(model_bim)


class DatabricksConverter:
    """Ossie <-> Metric View YAML, via the apache-ossie-databricks Python converter.

    The fallback for machines without the Java converter. Upstream removed it, so it is
    pinned to an older apache/ossie commit and will not receive fixes.
    """

    def to_platform(self, ossie_yaml: str, *, warn: bool = False) -> str:
        from ossie_databricks.ossie_to_metric_view import convert_ossie_to_metric_view

        with _as_conversion_error("Databricks"), converting(warn):
            return convert_ossie_to_metric_view(ossie_yaml)

    def to_ossie(self, metric_view: str, *, name: str | None = None,
                 warn: bool = False) -> str:
        """`name` matters: a Metric View's YAML carries no model name of its own, so
        without one the converter names the model after the source table."""
        from ossie_databricks.metric_view_to_ossie import convert_metric_view_to_ossie

        with _as_conversion_error("Databricks"), converting(warn):
            return convert_metric_view_to_ossie(metric_view, model_name=name)


def default_databricks_converter():
    """The Java converter when its jar can be found, else the deprecated Python one."""
    from ._java import JavaDatabricksConverter, find_jar

    jar = find_jar()
    return JavaDatabricksConverter(jar) if jar else DatabricksConverter()


class SnowflakeConverter:
    """Ossie -> Snowflake Semantic View YAML, via apache-ossie-snowflake.

    One direction only: that package ships no reverse converter, which is why
    `Snowflake` can upload but not download.
    """

    def to_platform(self, ossie_yaml: str, *, warn: bool = False) -> str:
        from ossie_snowflake.ossie_to_snowflake import convert_ossie_to_snowflake

        with _as_conversion_error("Snowflake"), converting(warn):
            return convert_ossie_to_snowflake(ossie_yaml)
