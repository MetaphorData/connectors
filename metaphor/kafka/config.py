from dataclasses import field as dataclass_field
from typing import Dict, List
from pydantic import BaseModel, field_validator, model_validator

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter

class KafkaBootstrapServer(BaseModel):
    host: str
    port: int

    
@dataclass(config=ConnectorConfig)
class KafkaRunConfig(BaseConfig):

    servers: List[KafkaBootstrapServer] = dataclass_field(default_factory=lambda: [])
    group_id: str
    
    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = dataclass_field(default_factory=lambda: DatasetFilter())

    @field_validator("servers")
    @classmethod
    def _must_have_at_least_one_server(cls, servers: List[KafkaBootstrapServer]):
        if len(servers) < 1:
            raise ValueError("Must specify at least one Kafka server")
        return servers

    @property
    def _bootstrap_servers_str(self) -> str:
        return ",".join(f"{x.host}:{x.port}" for x in self.servers)
    
    def as_config_dict(self) -> Dict:
        return {
            "group.id": self.group_id,
            "bootstrap.servers": self._bootstrap_servers_str,
        }