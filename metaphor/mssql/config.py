from typing import Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class MssqlConfig(BaseConfig):
    # The database username
    username: str

    # The database password
    password: str

    # SQL Server endpoint. Instead of Azure SQL endpoint if specified
    endpoint: str

    # Azure Directory (tenant) ID
    tenant_id: Optional[str] = ""

    # SQL Server name
    server_name: Optional[str] = ""
