from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.kafka.config import KafkaBootstrapServer, KafkaConfig
from metaphor.kafka.extractor import KafkaExtractor
from metaphor.models.metadata_change_event import DatasetSchema, SchemaType
from tests.test_utils import load_json


@patch("metaphor.kafka.schema_resolver.SchemaResolver.build_resolver")
@patch("metaphor.kafka.extractor.KafkaExtractor.init_admin_client")
@pytest.mark.asyncio
async def test_extractor(
    mock_init_admin_client: MagicMock,
    mock_build_schema_resolver: MagicMock,
    test_root_dir: str,
) -> None:
    config = KafkaConfig(
        output=OutputConfig(),
        schema_registry_url="http://localhost:5566",
        bootstrap_servers=[
            KafkaBootstrapServer(host="localhost", port=9487),
        ],
    )

    def mock_get_dataset_schemas(topic: str, all_versions: bool):
        if topic == "foo":
            return {
                "1_1": DatasetSchema(
                    schema_type=SchemaType.AVRO,
                    raw_schema='"string"',
                )
            }
        elif topic == "bar":
            return {
                "2_1": DatasetSchema(
                    schema_type=SchemaType.AVRO,
                    raw_schema='"double"',
                )
            }
        return {}

    mock_schema_resolver = MagicMock()
    mock_schema_resolver.get_dataset_schemas = mock_get_dataset_schemas
    mock_build_schema_resolver.side_effect = [mock_schema_resolver]

    mock_admin_client = MagicMock()
    mock_admin_client.list_topics = MagicMock()
    mock_admin_client.list_topics.return_value = SimpleNamespace(
        topics={
            "foo": "",
            "bar": "",
            "no_schema": "",
        }
    )
    mock_init_admin_client.side_effect = [mock_admin_client]

    extractor = KafkaExtractor(config=config)
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/kafka/expected.json")
