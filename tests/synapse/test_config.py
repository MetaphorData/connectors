import pytest
from pydantic import ValidationError

from metaphor.common.base_config import OutputConfig
from metaphor.synapse.config import SynapseConfig


def test_yaml_config(test_root_dir):
    config = SynapseConfig.from_yaml_file(f"{test_root_dir}/synapse/config.yml")

    assert config == SynapseConfig(
        tenant_id="tenant_id",
        client_id="client_id",
        secret="client_secret",
        subscription_id="client_subscription_id",
        output=OutputConfig(file={"directory": "./synapse_result"}),
    )


def test_yaml_config_with_optional(test_root_dir):
    config = SynapseConfig.from_yaml_file(
        f"{test_root_dir}/synapse/config_with_optional.yml"
    )

    assert config == SynapseConfig(
        tenant_id="tenant_id",
        client_id="client_id",
        secret="client_secret",
        subscription_id="client_subscription_id",
        workspaces=["workspace1", "workspace2"],
        resource_group_name="resource_group_name",
        output=OutputConfig(file={"directory": "./synapse_result"}),
    )


def test_yaml_config_lack_require_parameter(test_root_dir):
    with pytest.raises(ValidationError):
        SynapseConfig.from_yaml_file(
            f"{test_root_dir}/synapse/config_lack_required_parameter.yml"
        )
