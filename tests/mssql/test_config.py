import pytest
from pydantic import ValidationError

from metaphor.common.base_config import OutputConfig
from metaphor.mssql.config import MssqlConfig


def test_yaml_config(test_root_dir):
    config = MssqlConfig.from_yaml_file(f"{test_root_dir}/mssql/config.yml")

    assert config == MssqlConfig(
        tenant_id="tenant_id",
        server_name="workspace_name",
        username="username",
        password="password",
        output=OutputConfig(file={"directory": "./mssql_result"}),
    )


def test_yaml_config_lack_require_parameter(test_root_dir):
    with pytest.raises(ValidationError):
        MssqlConfig.from_yaml_file(
            f"{test_root_dir}/mssql/config_lack_required_parameter.yml"
        )
