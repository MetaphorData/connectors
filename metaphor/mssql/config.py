from dataclasses import field
from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.filter import DatasetFilter


@dataclass
class MssqlConfig(BaseConfig):
    # The database username
    username: str

    # The database password
    password: str

    # SQL Server endpoint. Instead of Azure SQL endpoint if specified
    endpoint: str

    # Include or exclude specific databases/schemas/tables
    filter: Optional[DatasetFilter] = field(default_factory=lambda: DatasetFilter())

    # Azure Directory (tenant) ID
    tenant_id: Optional[str] = ""

    # SQL Server name
    server_name: Optional[str] = ""
