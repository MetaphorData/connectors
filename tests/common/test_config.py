import pytest
from pydantic import ValidationError
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.sink import SinkConfig


def test_yaml_config(test_root_dir):
    config = BaseConfig.from_yaml_file(f"{test_root_dir}/common/configs/config.yml")

    assert config == BaseConfig(
        output=OutputConfig(
            file=SinkConfig(
                directory="path",
                assume_role_arn="arn",
                write_logs=False,
                batch_size_count=2,
                batch_size_bytes=1000,
            ),
        )
    )


def test_missing_output_config(test_root_dir):
    missing_output = BaseConfig.from_yaml_file(
        f"{test_root_dir}/common/configs/missing_output.yml"
    )
    assert missing_output.output.file and missing_output.output.file.directory


@dataclass(config=ConnectorConfig)
class AnotherBaseConfig(BaseConfig):
    pass


def test_yaml_config_with_extra_config(test_root_dir):
    # BaseConfig allows extras
    config = BaseConfig.from_yaml_file(f"{test_root_dir}/common/configs/extend.yml")
    assert config == BaseConfig(output=OutputConfig())
    assert not config.output.file

    # AnotherBaseConfig does not allow extras
    with pytest.raises(ValidationError):
        AnotherBaseConfig.from_yaml_file(f"{test_root_dir}/common/configs/extend.yml")


@dataclass(config=ConnectorConfig)
class ExtendConfig(BaseConfig):
    foo: str


def test_extend_config(test_root_dir):
    config = ExtendConfig.from_yaml_file(f"{test_root_dir}/common/configs/extend.yml")
    assert config == ExtendConfig(foo="bar", output=OutputConfig())
