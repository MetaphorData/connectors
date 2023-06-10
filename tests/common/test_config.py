import pytest
from pydantic import ValidationError
from pydantic.dataclasses import dataclass

from metaphor.common.api_sink import ApiSinkConfig
from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.file_sink import FileSinkConfig


def test_yaml_config(test_root_dir):
    config = BaseConfig.from_yaml_file(f"{test_root_dir}/common/configs/config.yml")

    assert config == BaseConfig(
        output=OutputConfig(
            api=ApiSinkConfig(url="url", api_key="api_key", batch_size=1, timeout=2),
            file=FileSinkConfig(
                directory="path",
                assume_role_arn="arn",
                write_logs=False,
                batch_size_count=2,
                batch_size_bytes=1000,
            ),
        )
    )


def test_yaml_config_with_missing_config(test_root_dir):
    with pytest.raises(ValidationError):
        BaseConfig.from_yaml_file(f"{test_root_dir}/common/configs/missing.yml")


def test_yaml_config_with_extra_config(test_root_dir):
    with pytest.raises(ValidationError):
        BaseConfig.from_yaml_file(f"{test_root_dir}/common/configs/extend.yml")


@dataclass
class ExtendConfig(BaseConfig):
    foo: str


def test_extend_config(test_root_dir):
    config = ExtendConfig.from_yaml_file(f"{test_root_dir}/common/configs/extend.yml")
    assert config == ExtendConfig(foo="bar", output={})
