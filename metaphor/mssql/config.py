from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter


@dataclass(config=ConnectorConfig)
class MssqlConfig(BaseConfig):
    # The database username
    username: str

    # The database password
    password: str

    # SQL Server endpoint. Instead of Azure SQL endpoint if specified
    endpoint: str = ""

    # Azure Directory (tenant) ID
    tenant_id: str = ""

    # SQL Server name
    server_name: str = ""

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())
