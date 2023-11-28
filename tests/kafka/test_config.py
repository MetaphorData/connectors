import pytest
from pydantic import ValidationError

from metaphor.common.base_config import OutputConfig
from metaphor.kafka.config import (
    KafkaConfig,
    KafkaSubjectNameStrategy,
    KafkaTopicNamingStrategy,
)
from tests.kafka.test_extractor import dummy_config


def test_config():
    assert dummy_config.admin_conf["bootstrap.servers"] == "localhost:9487"


def test_bad_config_without_bootstrap_server():
    with pytest.raises(ValidationError):
        KafkaConfig(output=OutputConfig(), schema_registry_url="http://localhost:8081")


def test_bad_config_with_incorrect_name_strategy():
    with pytest.raises(ValidationError):
        KafkaConfig(
            output=OutputConfig(),
            schema_registry_url=dummy_config.schema_registry_url,
            bootstrap_servers=dummy_config.bootstrap_servers,
            default_subject_name_strategy=KafkaSubjectNameStrategy.RECORD_NAME_STRATEGY,
        )

    with pytest.raises(ValidationError):
        KafkaConfig(
            output=OutputConfig(),
            schema_registry_url=dummy_config.schema_registry_url,
            bootstrap_servers=dummy_config.bootstrap_servers,
            topic_naming_strategies={
                "bad": KafkaTopicNamingStrategy(
                    override_subject_name_strategy=KafkaSubjectNameStrategy.RECORD_NAME_STRATEGY,
                )
            },
        )
