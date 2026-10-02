"""Finding the platforms the current environment is configured for."""

def configured_connections(**overrides) -> list:
    """The connections this environment has settings for.

        load_env()
        for connection in configured_connections():
            print(connection.platform, connection.upload("model.yaml"))

    Each one carries its own `platform` name, so this is a plain list rather than a
    mapping you would have to call `.items()` on.

    A platform with no settings is left out rather than raising, so a script works
    unchanged on a machine configured for one platform and a machine configured for
    three. Call `load_env()` first if the settings live in a .env file.

    This reports what is *configured*, not what will authenticate - checking that would
    mean acquiring a token from each platform. An upload can still fail; handle it.

    `overrides` are passed to every `from_env()`, so `configured_connections(schema=
    "staging")` re-points whichever platforms were found.
    """
    found = []
    for build in _builders():
        try:
            found.append(build(**overrides))
        except (ValueError, ImportError):
            continue  # not configured, or its optional extra is not installed
    return found


def _builders():
    from .databricks import Databricks
    from .fabric import Fabric

    builders = [Fabric.from_env, Databricks.from_env]
    try:
        from .snowflake import Snowflake
    except ImportError:  # the snowflake extra is not installed
        return builders
    return [*builders, Snowflake.from_env]
