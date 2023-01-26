import pytest
from pydantic import ValidationError

from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.synapse.config import SynapseConfig, SynapseQueryLogConfig


def test_yaml_config(test_root_dir):
    config = SynapseConfig.from_yaml_file(f"{test_root_dir}/synapse/config.yml")

    assert config == SynapseConfig(
        tenant_id="tenant_id",
        server_name="workspace_name",
        username="username",
        password="password",
        output=OutputConfig(file={"directory": "./synapse_result"}),
    )


def test_yaml_config_lack_require_parameter(test_root_dir):
    with pytest.raises(ValidationError):
        SynapseConfig.from_yaml_file(
            f"{test_root_dir}/synapse/config_lack_required_parameter.yml"
        )


def test_yaml_config_with_query_log(test_root_dir):
    config = SynapseConfig.from_yaml_file(
        f"{test_root_dir}/synapse/config_with_query_log.yml"
    )

    assert config == SynapseConfig(
        tenant_id="tenant_id",
        server_name="workspace_name",
        username="username",
        password="password",
        query_log=SynapseQueryLogConfig(15),
        output=OutputConfig(file={"directory": "./synapse_result"}),
    )


def test_yaml_config_with_filter(test_root_dir):
    config = SynapseConfig.from_yaml_file(
        f"{test_root_dir}/synapse/config_with_filter.yml"
    )
    datasetFilter = DatasetFilter()
    datasetFilter.includes = {
        "db1": {"schemas1": set(["table1", "table2"])},
        "db2": None,
    }
    datasetFilter.excludes = {"db1": {"schemas1": set(["table3"])}}
    assert config == SynapseConfig(
        tenant_id="tenant_id",
        server_name="workspace_name",
        username="username",
        password="password",
        filter=datasetFilter,
        query_log=SynapseQueryLogConfig(15),
        output=OutputConfig(file={"directory": "./synapse_result"}),
    )
