from metaphor.common.api_sink import ApiSinkConfig
from metaphor.common.extractor import OutputConfig, RunConfig
from metaphor.common.file_sink import FileSinkConfig


def test_json_config(test_root_dir):
    config = RunConfig.from_json_file(f"{test_root_dir}/common/config.json")

    assert config == RunConfig(
        output=OutputConfig(
            api=ApiSinkConfig(url="url", api_key="api_key", batch_size=1, timeout=2),
            file=FileSinkConfig(directory="path", assume_role_arn="arn"),
        )
    )


def test_yaml_config(test_root_dir):
    config = RunConfig.from_yaml_file(f"{test_root_dir}/common/config.yml")

    assert config == RunConfig(
        output=OutputConfig(
            api=ApiSinkConfig(url="url", api_key="api_key", batch_size=1, timeout=2),
            file=FileSinkConfig(directory="path", assume_role_arn="arn"),
        )
    )
