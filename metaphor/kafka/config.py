from dataclasses import field as dataclass_field
from typing import Dict, List, Literal

from pydantic import BaseModel, field_validator, model_validator
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

    topic_records_map: Dict[str, List[str]] = dataclass_field(default_factory=dict)

    subject_name_strategy: Literal[
        "TOPIC_NAME_STRATEGY", "RECORD_NAME_STRATEGY", "TOPIC_RECORD_NAME_STRATEGY"
    ] = "TOPIC_NAME_STRATEGY"

    @field_validator("bootstrap_servers")
    @classmethod
    def _must_have_at_least_one_bootstrap_server(
        cls, bootstrap_servers: List[KafkaBootstrapServer]
    ):
        if len(bootstrap_servers) < 1:
            raise ValueError("Must specify at least one Kafka server")
        return bootstrap_servers

    @model_validator(mode="after")
    def _record_name_strategy_must_have_topic_records_map(self) -> "KafkaConfig":
        if self.subject_name_strategy == "RECORD_NAME_STRATEGY":
            if not self.topic_records_map:
                raise ValueError(
                    "Cannot have empty topic_records_map when subject name strategy is RECORD_NAME_STRATEGY"
                )
            if all(len(x) == 0 for x in self.topic_records_map.values()):
                raise ValueError(
                    "No topic have record specified. Please fix the topic_records_map value."
                )
        return self

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
