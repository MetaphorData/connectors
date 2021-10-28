from metaphor.models.metadata_change_event import DataPlatform

from metaphor.looker.config import LookerConnectionConfig, LookerRunConfig


def test_json_config(test_root_dir):
    config = LookerRunConfig.from_json_file(f"{test_root_dir}/looker/config.json")

    assert config == LookerRunConfig(
        base_url="base_url",
        client_id="client_id",
        client_secret="client_secret",
        lookml_dir="lookml_dir",
        connections={
            "conn1": LookerConnectionConfig(
                database="db1",
                default_schema="schema1",
                platform=DataPlatform.SNOWFLAKE,
                account="account1",
            )
        },
        verify_ssl=True,
        timeout=1,
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = LookerRunConfig.from_yaml_file(f"{test_root_dir}/looker/config.yml")

    assert config == LookerRunConfig(
        base_url="base_url",
        client_id="client_id",
        client_secret="client_secret",
        lookml_dir="lookml_dir",
        connections={
            "conn1": LookerConnectionConfig(
                database="db1",
                default_schema="schema1",
                platform=DataPlatform.SNOWFLAKE,
                account="account1",
            )
        },
        verify_ssl=True,
        timeout=1,
        output=None,
    )
