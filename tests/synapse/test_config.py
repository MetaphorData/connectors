from metaphor.common.base_config import OutputConfig
from metaphor.synapse.config import SynapseConfig


def test_yaml_config_with_key_path(test_root_dir):
    config = SynapseConfig.from_yaml_file(f"{test_root_dir}/synapse/config.yml")

    assert config == SynapseConfig(
        tenant_id="tenant_id",
        client_id="client_id",
        secret="client_secret",
        subscription_id="client_subscription_id",
        output=OutputConfig(file={"directory": "./synapse_result"}),
    )
