from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.schema_registry import RegisteredSchema, Schema

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.kafka.config import KafkaBootstrapServer, KafkaConfig
from metaphor.kafka.extractor import KafkaExtractor
from tests.test_utils import load_json


@patch("metaphor.kafka.extractor.KafkaExtractor.init_schema_registry_client")
@patch("metaphor.kafka.extractor.KafkaExtractor.init_admin_client")
@pytest.mark.asyncio
async def test_extractor(
    mock_init_admin_client: MagicMock,
    mock_init_schema_registry_client: MagicMock,
    test_root_dir: str,
) -> None:
    config = KafkaConfig(
        output=OutputConfig(),
        schema_registry_url="http://localhost:5566",
        servers=[
            KafkaBootstrapServer(host="localhost", port=9487),
        ],
    )

    mock_admin_client = MagicMock()
    mock_admin_client.list_topics = MagicMock()
    mock_admin_client.list_topics.return_value = SimpleNamespace(
        topics={
            "foo": "",
            "bar": "",
            "no_schema": "",
        }
    )
    mock_init_admin_client.return_value = mock_admin_client

    mock_schema_registry_client = MagicMock()
    mock_schema_registry_client.get_subjects = MagicMock()
    mock_schema_registry_client.get_subjects.return_value = ["foo", "bar"]

    def mock_get_latest_version(topic: str):
        if topic == "foo":
            return RegisteredSchema(0, Schema('"string"', "AVRO"), "foo", 1)
        elif topic == "bar":
            return RegisteredSchema(1, Schema('"double"', "AVRO"), "bar", 1)
        assert False

    mock_schema_registry_client.get_latest_version = mock_get_latest_version
    mock_init_schema_registry_client.return_value = mock_schema_registry_client

    extractor = KafkaExtractor(config=config)
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/kafka/expected.json")
