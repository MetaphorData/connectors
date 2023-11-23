from dataclasses import field as dataclass_field
from typing import Dict, List

from pydantic import BaseModel, field_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import TopicFilter


class KafkaBootstrapServer(BaseModel):
    host: str
    port: int


@dataclass(config=ConnectorConfig)
class KafkaConfig(BaseConfig):
    schema_registry_url: str

    bootstrap_servers: List[KafkaBootstrapServer] = dataclass_field(
        default_factory=lambda: []
    )

    filter: TopicFilter = dataclass_field(default_factory=lambda: TopicFilter())

    @field_validator("bootstrap_servers")
    @classmethod
    def _must_have_at_least_one_server(
        cls, bootstrap_servers: List[KafkaBootstrapServer]
    ):
        if len(bootstrap_servers) < 1:
            raise ValueError("Must specify at least one Kafka server")
        return bootstrap_servers

    @property
    def _bootstrap_servers_str(self) -> str:
        return ",".join(f"{x.host}:{x.port}" for x in self.bootstrap_servers)

    @property
    def admin_conf(self) -> Dict:
        return {
            "bootstrap.servers": self._bootstrap_servers_str,
        }

    @property
    def schema_registry_conf(self) -> Dict:
        return {"url": self.schema_registry_url}
