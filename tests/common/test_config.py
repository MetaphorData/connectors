import pytest
from pydantic.dataclasses import dataclass

from metaphor.common.api_sink import ApiSinkConfig
from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.file_sink import FileSinkConfig


def test_yaml_config(test_root_dir):
    config = BaseConfig.from_yaml_file(f"{test_root_dir}/common/configs/config.yml")

    assert config == BaseConfig(
        output=OutputConfig(
            api=ApiSinkConfig(url="url", api_key="api_key", batch_size=1, timeout=2),
            file=FileSinkConfig(directory="path", assume_role_arn="arn"),
        )
    )


def test_yaml_config_with_missing_config(test_root_dir):
    with pytest.raises(TypeError):
        BaseConfig.from_yaml_file(f"{test_root_dir}/common/configs/missing.yml")


def test_yaml_config_with_extra_config(test_root_dir):
    with pytest.raises(TypeError):
        BaseConfig.from_yaml_file(f"{test_root_dir}/common/configs/extend.yml")


@dataclass
class ExtendConfig(BaseConfig):
    foo: str


def test_extend_config(test_root_dir):
    config = ExtendConfig.from_yaml_file(f"{test_root_dir}/common/configs/extend.yml")
    assert config == ExtendConfig(foo="bar", output={})
