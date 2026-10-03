"""Upload and download Apache Ossie semantic models to Microsoft Fabric and Databricks.

    from ossie_connect import Fabric, Databricks

    fabric = Fabric(workspace="...", lakehouse="...")
    fabric.upload("model.yaml")
    fabric.download("sales_demo", "model.yaml")

The conversion itself belongs to the apache-ossie converters; what this package adds is
the transport those converters deliberately leave out - authenticating, creating or
updating the remote object, and fetching it back.
"""

from ._fabric_api import FabricError
from ._io import Yaml
from .console import Reporter, plain_warnings
from .discover import configured_connections
from ._convert import OssieConnectWarning
from .connection import Connection, SupportsDownload, SupportsUpload
from .databricks import Databricks, DatabricksError
from .env import load_env
from .fabric import Fabric
from .models import Models
from .snowflake import Snowflake, SnowflakeError

__all__ = [
    "Connection",
    "OssieConnectWarning",
    "Reporter",
    "configured_connections",
    "Databricks",
    "DatabricksError",
    "Fabric",
    "FabricError",
    "Models",
    "Snowflake",
    "SnowflakeError",
    "SupportsDownload",
    "SupportsUpload",
    "Yaml",
    "load_env",
    "plain_warnings",
]
__version__ = "0.1.0"
