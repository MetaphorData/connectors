from dataclasses import field as dataclass_field
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, field_validator, model_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import TopicFilter


class KafkaBootstrapServer(BaseModel):
    host: str
    port: int


class KafkaSubjectNameStrategy(Enum):
    """
    The naming strategy from topic to subject.
    """

    TOPIC_NAME_STRATEGY = "TOPIC_NAME_STRATEGY"
    """
    The default strategy. Maps a topic to a single subject for its key and a single subject for its value.

    Resulting subject:
    `<TOPIC>-<key|value>`
    """

    RECORD_NAME_STRATEGY = "RECORD_NAME_STRATEGY"
    """
    Maps a topic to multiple fully qualified records, thus allowing multiple schemas for a single topic.

    To use this strategy, user will need to specify the records related to the topic.

    Resulting subject:
    `<RECORD>-<key|value>`
    """

    TOPIC_RECORD_NAME_STRATEGY = "TOPIC_RECORD_NAME_STRATEGY"
    """
    Maps a topic to multiple fully qualified records and prefixes the record with the topic name.

    If user does not specify the records related to the topic, we will use whatever subjects we find
    that starts with `topic` and ends with `key|value` as the related subjects.

    Resulting subject:
    `<TOPIC>-<RECORD>-<key|value>`
    """


@dataclass
class KafkaTopicNamingStrategy:
    override_subject_name_strategy: Optional[KafkaSubjectNameStrategy] = None
    """
    This will be resolved in `SchemaResolver` so that we always know what strategy we're
    using for this topic.
    """

    records: List[str] = dataclass_field(default_factory=list)
    """
    If the subject name strategy is `TopicNameStrategy`, this will be ignored.

    If the subject name strategy is `RecordNameStrategy`, this is mandatory for the topic to be
    correctly ingested.

    If the subject name strategy is `TopicRecordNameStrategy` and `records` is empty / not defined,
    all subjects that starts with topic and ends with `key|value` will be considered as a subject for
    this topic.
    """

    @model_validator(mode="after")
    def _record_name_strategy_must_have_topic_records_map(
        self,
    ) -> "KafkaTopicNamingStrategy":
        if (
            self.override_subject_name_strategy is not None
            and self.override_subject_name_strategy
            is KafkaSubjectNameStrategy.RECORD_NAME_STRATEGY
            and len(self.records) < 1
        ):
            raise ValueError(
                "Cannot have empty records when subject name strategy is RECORD_NAME_STRATEGY"
            )
        return self


@dataclass
class KafkaSASLConfig:
    """
    The most commonly used SASL configuration values. For other configurations, please use `extra_admin_client_config` field.
    """

    username: str
    """
    SASL username for use with the `PLAIN` and `SASL-SCRAM-..` mechanisms.

    Config key: "sasl.username".
    """

    password: str
    """
    SASL password for use with the `PLAIN` and `SASL-SCRAM-..` mechanisms.

    Config key: "sasl.password".
    """

    mechanism: str = "GSSAPI"
    """
    SASL mechanism to use for authentication. Supported: `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`.

    Config key: "sasl.mechanism".
    """

    @property
    def as_dict(self) -> Dict[str, str]:
        return {
            "sasl.username": self.username,
            "sasl.password": self.password,
            "sasl.mechanism": self.mechanism,
        }


@dataclass(config=ConnectorConfig)
class KafkaConfig(BaseConfig):
    schema_registry_url: str
    """
    Schema registry URL.

    Can contain basic HTTP auth username and password, e.g.
    ```
    http://username:password@host:port
    ```
    """

    extra_admin_client_conf: Dict[str, Any] = dataclass_field(default_factory=dict)
    """
    Extra configuration values for the Kafka admin client. See https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    for all available configurations.
    """

    bootstrap_servers: List[KafkaBootstrapServer] = dataclass_field(
        default_factory=lambda: []
    )
    """
    The Kafka bootstrap servers / brokers. Cannot be empty.
    """

    filter: TopicFilter = dataclass_field(default_factory=lambda: TopicFilter())

    topic_naming_strategies: Dict[str, KafkaTopicNamingStrategy] = dataclass_field(
        default_factory=dict
    )
    """
    Mapping from topic name to corresponding naming strategy.
    """

    sasl_config: Optional[KafkaSASLConfig] = None

    default_subject_name_strategy: KafkaSubjectNameStrategy = (
        KafkaSubjectNameStrategy.TOPIC_NAME_STRATEGY
    )

    @field_validator("bootstrap_servers")
    @classmethod
    def _must_have_at_least_one_bootstrap_server(
        cls, bootstrap_servers: List[KafkaBootstrapServer]
    ):
        if len(bootstrap_servers) < 1:
            raise ValueError("Must specify at least one Kafka server")
        return bootstrap_servers

    @model_validator(mode="after")
    def _topic_naming_strategies_must_be_nonempty_when_default_is_record_name_strategy(
        self,
    ) -> "KafkaConfig":
        if (
            self.default_subject_name_strategy
            is KafkaSubjectNameStrategy.RECORD_NAME_STRATEGY
            and not self.topic_naming_strategies
        ):
            raise ValueError(
                "Cannot have RECORD_NAME_STRATEGY as default subject name strategy without specifying any topic's records"
            )
        return self

    @property
    def _bootstrap_servers_str(self) -> str:
        return ",".join(f"{x.host}:{x.port}" for x in self.bootstrap_servers)

    @property
    def admin_conf(self) -> Dict:
        return {
            "bootstrap.servers": self._bootstrap_servers_str,
            **(self.sasl_config.as_dict if self.sasl_config is not None else {}),
            **(self.extra_admin_client_conf),
        }

    @property
    def schema_registry_conf(self) -> Dict:
        return {"url": self.schema_registry_url}
