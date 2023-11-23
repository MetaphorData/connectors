import pytest
from pydantic import ValidationError

from metaphor.common.base_config import OutputConfig
from metaphor.kafka.config import KafkaBootstrapServer, KafkaConfig


def test_config():
    config = KafkaConfig(
        output=OutputConfig(),
        schema_registry_url="http://localhost:8081",
        bootstrap_servers=[
            KafkaBootstrapServer(host="localhost", port=9092),
        ],
    )
    assert config.admin_conf["bootstrap.servers"] == "localhost:9092"

    with pytest.raises(ValidationError):
        KafkaConfig(output=OutputConfig(), schema_registry_url="http://localhost:8081")
