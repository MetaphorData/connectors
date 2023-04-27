from metaphor.common.base_config import OutputConfig
from metaphor.fivetran.config import FivetranRunConfig


def test_yaml_config(test_root_dir):
    config = FivetranRunConfig.from_yaml_file(f"{test_root_dir}/fivetran/config.yml")

    assert config == FivetranRunConfig(
        api_key="key",
        api_secret="secret",
        output=OutputConfig(),
    )
