"""Conversion between Ossie and each platform's own format.

Conversion is kept behind these small classes rather than called inline so that the
implementation can be replaced without touching the connections. That is not
hypothetical for Databricks: upstream now maintains the converter in Java and describes
the Python one this package uses as "the original reference implementation, to be
deprecated". When that lands, a Java-backed class implementing the same two methods
drops in via `Databricks(converter=...)`.
"""

from ._convert import converting


class FabricConverter:
    """Ossie <-> TMSL, via the apache-ossie-microsoft Python converter."""

    def to_platform(self, ossie_yaml: str, *, source=None, warn: bool = False) -> dict:
        from ossie_microsoft.ossie_to_semantic_model import convert_ossie_to_semantic_model

        with converting(warn):
            return convert_ossie_to_semantic_model(ossie_yaml, source=source)

    def to_ossie(self, model_bim: str, *, warn: bool = False) -> str:
        from ossie_microsoft.semantic_model_to_ossie import convert_semantic_model_to_ossie

        with converting(warn):
            return convert_semantic_model_to_ossie(model_bim)


class DatabricksConverter:
    """Ossie <-> Metric View YAML, via the apache-ossie-databricks Python converter."""

    def to_platform(self, ossie_yaml: str, *, warn: bool = False) -> str:
        from ossie_databricks.ossie_to_metric_view import convert_ossie_to_metric_view

        with converting(warn):
            return convert_ossie_to_metric_view(ossie_yaml)

    def to_ossie(self, metric_view: str, *, name: str | None = None,
                 warn: bool = False) -> str:
        """`name` matters: a Metric View's YAML carries no model name of its own, so
        without one the converter names the model after the source table."""
        from ossie_databricks.metric_view_to_ossie import convert_metric_view_to_ossie

        with converting(warn):
            return convert_metric_view_to_ossie(metric_view, model_name=name)


class SnowflakeConverter:
    """Ossie -> Snowflake Semantic View YAML, via apache-ossie-snowflake.

    One direction only: that package ships no reverse converter, which is why
    `Snowflake` can upload but not download.
    """

    def to_platform(self, ossie_yaml: str, *, warn: bool = False) -> str:
        from ossie_snowflake.converter import convert_ossie_to_snowflake

        with converting(warn):
            return convert_ossie_to_snowflake(ossie_yaml)
