from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig


@dataclass
class MssqlConfig(BaseConfig):
    # # Azure Directory (tenant) ID
    # tenant_id: str

    # SQL Server name
    server_name: str

    # The database username
    username: str

    # The database password
    password: str
