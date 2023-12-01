from unittest.mock import MagicMock, patch

from confluent_kafka.schema_registry import RegisteredSchema, Schema

from metaphor.kafka.config import (
    KafkaConfig,
    KafkaSubjectNameStrategy,
    KafkaTopicNamingStrategy,
)
from metaphor.kafka.schema_resolver import SchemaResolver
from metaphor.models.metadata_change_event import DatasetSchema, SchemaField, SchemaType
from tests.kafka.test_extractor import dummy_config


def mock_get_latest_version(subject: str):
    if subject == "foo-key":
        schema = """
        {
            "name": "FooKey",
            "type": "string"
        }
        """
        return RegisteredSchema(1, Schema(schema, "AVRO"), subject, 1)
    if subject == "foo-value":
        schema = """
        {
            "name": "FooValue",
            "type": "double"
        }
        """
        return RegisteredSchema(2, Schema(schema, "AVRO"), subject, 2)
    if subject == "bar-value":
        schema = """
        {
            "name": "BarValue",
            "type": "double"
        }
        """
        return RegisteredSchema(3, Schema(schema, "AVRO"), subject, 1)
    assert False


def mock_get_versions(subject: str):
    if subject == "foo-key":
        return [1]
    if subject == "foo-value":
        return [1, 2]
    if subject == "bar-value":
        return [1]
    assert False


def mock_get_version(subject: str, version: int):
    if subject == "foo-key":
        return mock_get_latest_version(subject)
    if subject == "foo-value":
        if version == 1:
            schema = """
            {
                "name": "FooValue",
                "type": "string"
            }
            """
            return RegisteredSchema(2, Schema(schema, "AVRO"), subject, 1)
        return mock_get_latest_version(subject)
    if subject == "bar-value":
        return mock_get_latest_version(subject)
    assert False


@patch("metaphor.kafka.schema_resolver.SchemaResolver.init_schema_registry_client")
def test_schema_resolver(
    mock_init_schema_registry_client: MagicMock,
) -> None:
    mock_schema_registry_client = MagicMock()
    mock_schema_registry_client.get_subjects = MagicMock()
    mock_schema_registry_client.get_subjects.return_value = [
        "foo-key",
        "foo-value",
        "bar-value",
    ]

    mock_schema_registry_client.get_latest_version = mock_get_latest_version
    mock_schema_registry_client.get_versions = mock_get_versions
    mock_schema_registry_client.get_version = mock_get_version
    mock_init_schema_registry_client.side_effect = [mock_schema_registry_client]

    resolver = SchemaResolver(dummy_config)
    foo_schemas = resolver.get_dataset_schemas("foo")
    assert foo_schemas.get("1_1") is not None
    assert foo_schemas.get("1_1") == DatasetSchema(
        fields=[
            SchemaField(
                field_name="<NA>",
                field_path="<NA>",
                native_type="STRING",
                subfields=[],
            )
        ],
        raw_schema='\n        {\n            "name": "FooKey",\n            "type": "string"\n        }\n        ',
        schema_type=SchemaType.AVRO,
    )
    assert foo_schemas.get("2_2") is not None
    assert foo_schemas.get("2_2") == DatasetSchema(
        fields=[
            SchemaField(
                field_name="<NA>",
                field_path="<NA>",
                native_type="DOUBLE",
                subfields=[],
            )
        ],
        raw_schema='\n        {\n            "name": "FooValue",\n            "type": "double"\n        }\n        ',
        schema_type=SchemaType.AVRO,
    )
    assert "2_1" not in foo_schemas
    assert "3_1" not in foo_schemas

    foo_schemas = resolver.get_dataset_schemas("foo", all_versions=True)
    assert "2_1" in foo_schemas
    assert foo_schemas.get("2_1") == DatasetSchema(
        fields=[
            SchemaField(
                field_name="<NA>",
                field_path="<NA>",
                native_type="STRING",
                subfields=[],
            )
        ],
        raw_schema='\n            {\n                "name": "FooValue",\n                "type": "string"\n            }\n            ',
        schema_type=SchemaType.AVRO,
    )


@patch("metaphor.kafka.schema_resolver.SchemaResolver.init_schema_registry_client")
def test_resolve_overriden_subject_name_strategy(
    mock_init_schema_registry_client: MagicMock,
) -> None:
    mock_schema_registry_client = MagicMock()
    mock_schema_registry_client.get_subjects = MagicMock()
    mock_schema_registry_client.get_subjects.return_value = [
        "foo-value",
        "bar-value",
        "baz-value",
    ]
    mock_init_schema_registry_client.side_effect = [mock_schema_registry_client]

    config = KafkaConfig(
        output=dummy_config.output,
        schema_registry_url=dummy_config.schema_registry_url,
        bootstrap_servers=dummy_config.bootstrap_servers,
        topic_naming_strategies={
            "quax": KafkaTopicNamingStrategy(
                override_subject_name_strategy=KafkaSubjectNameStrategy.RECORD_NAME_STRATEGY,
                records=["bar", "baz"],
            )
        },
    )
    resolver = SchemaResolver(config)
    assert resolver._resolve_topic_to_subjects("quax", is_key_schema=False) == [
        "bar-value",
        "baz-value",
    ]
    assert resolver._resolve_topic_to_subjects("foo", is_key_schema=False) == [
        "foo-value"
    ]


@patch("metaphor.kafka.schema_resolver.SchemaResolver.init_schema_registry_client")
def test_record_name_strategy_as_default(
    mock_init_schema_registry_client: MagicMock,
) -> None:
    mock_schema_registry_client = MagicMock()
    mock_schema_registry_client.get_subjects = MagicMock()
    mock_schema_registry_client.get_subjects.return_value = [
        "foo-really.awesome.fully.quantified.record-value",
        "bar-value",
        "baz-value",
    ]
    mock_init_schema_registry_client.side_effect = [mock_schema_registry_client]

    config = KafkaConfig(
        output=dummy_config.output,
        schema_registry_url=dummy_config.schema_registry_url,
        bootstrap_servers=dummy_config.bootstrap_servers,
        topic_naming_strategies={
            "quax": KafkaTopicNamingStrategy(records=["bar", "baz"]),
            "foo": KafkaTopicNamingStrategy(
                override_subject_name_strategy=KafkaSubjectNameStrategy.TOPIC_RECORD_NAME_STRATEGY
            ),
        },
        default_subject_name_strategy=KafkaSubjectNameStrategy.RECORD_NAME_STRATEGY,
    )
    resolver = SchemaResolver(config)
    assert resolver._resolve_topic_to_subjects("quax", is_key_schema=False) == [
        "bar-value",
        "baz-value",
    ]
    assert resolver._resolve_topic_to_subjects("foo", is_key_schema=False) == [
        "foo-really.awesome.fully.quantified.record-value"
    ]


@patch("metaphor.kafka.schema_resolver.SchemaResolver.init_schema_registry_client")
def test_schema_resolver_ignore_missing_subject_record_name_strategy(
    mock_init_schema_registry_client: MagicMock,
) -> None:
    mock_schema_registry_client = MagicMock()
    mock_schema_registry_client.get_subjects = MagicMock()
    mock_schema_registry_client.get_subjects.return_value = [
        "baz-value",
        "bar-value",
    ]
    mock_init_schema_registry_client.side_effect = [mock_schema_registry_client]
    config = KafkaConfig(
        output=dummy_config.output,
        schema_registry_url=dummy_config.schema_registry_url,
        bootstrap_servers=dummy_config.bootstrap_servers,
        topic_naming_strategies={
            "foo": KafkaTopicNamingStrategy(records=["bar"]),
        },
        default_subject_name_strategy=KafkaSubjectNameStrategy.RECORD_NAME_STRATEGY,
    )
    resolver = SchemaResolver(config)
    assert resolver._resolve_topic_to_subjects("foo", False) == ["bar-value"]

    mock_init_schema_registry_client.side_effect = [mock_schema_registry_client]
    config = KafkaConfig(
        output=dummy_config.output,
        schema_registry_url=dummy_config.schema_registry_url,
        bootstrap_servers=dummy_config.bootstrap_servers,
        topic_naming_strategies={
            "foo": KafkaTopicNamingStrategy(records=["quax"]),
        },
        default_subject_name_strategy=KafkaSubjectNameStrategy.RECORD_NAME_STRATEGY,
    )
    resolver = SchemaResolver(config)
    assert len(resolver._resolve_topic_to_subjects("foo", False)) == 0


@patch("metaphor.kafka.schema_resolver.SchemaResolver.init_schema_registry_client")
def test_schema_resolver_ignore_missing_subject_topic_record_name_strategy(
    mock_init_schema_registry_client: MagicMock,
) -> None:
    mock_schema_registry_client = MagicMock()
    mock_schema_registry_client.get_subjects = MagicMock()
    mock_schema_registry_client.get_subjects.return_value = [
        "foo-baz-value",
        "foo-bar-value",
    ]
    mock_init_schema_registry_client.side_effect = [mock_schema_registry_client]
    config = KafkaConfig(
        output=dummy_config.output,
        schema_registry_url=dummy_config.schema_registry_url,
        bootstrap_servers=dummy_config.bootstrap_servers,
        topic_naming_strategies={
            "foo": KafkaTopicNamingStrategy(records=["bar"]),
        },
        default_subject_name_strategy=KafkaSubjectNameStrategy.TOPIC_RECORD_NAME_STRATEGY,
    )
    resolver = SchemaResolver(config)
    assert resolver._resolve_topic_to_subjects("foo", False) == ["foo-bar-value"]

    mock_init_schema_registry_client.side_effect = [mock_schema_registry_client]
    config = KafkaConfig(
        output=dummy_config.output,
        schema_registry_url=dummy_config.schema_registry_url,
        bootstrap_servers=dummy_config.bootstrap_servers,
        topic_naming_strategies={
            "foo": KafkaTopicNamingStrategy(records=["quax"]),
        },
        default_subject_name_strategy=KafkaSubjectNameStrategy.TOPIC_RECORD_NAME_STRATEGY,
    )
    resolver = SchemaResolver(config)
    assert len(resolver._resolve_topic_to_subjects("foo", False)) == 0
