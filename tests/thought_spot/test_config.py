from metaphor.common.base_config import OutputConfig
from metaphor.thought_spot.config import ThoughtSpotRunConfig


def test_yaml_config_password(test_root_dir):
    config = ThoughtSpotRunConfig.from_yaml_file(
        f"{test_root_dir}/thought_spot/config_password.yml"
    )

    assert config == ThoughtSpotRunConfig(
        user="user",
        password="password",
        base_url="https://base.url",
        output=OutputConfig(),
    )


def test_yaml_config_secret_key(test_root_dir):
    config = ThoughtSpotRunConfig.from_yaml_file(
        f"{test_root_dir}/thought_spot/config_secret_key.yml"
    )

    assert config == ThoughtSpotRunConfig(
        user="user",
        secret_key="key",
        base_url="https://base.url",
        output=OutputConfig(),
    )
