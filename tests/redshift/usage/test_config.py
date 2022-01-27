from metaphor.common.base_config import OutputConfig
from metaphor.redshift.usage.config import RedshiftUsageRunConfig


def test_yaml_config(test_root_dir):
    config = RedshiftUsageRunConfig.from_yaml_file(
        f"{test_root_dir}/redshift/usage/config.yml"
    )

    assert config == RedshiftUsageRunConfig(
        host="host",
        database="database",
        user="user",
        password="password",
        lookback_days=365,
        output=OutputConfig(),
    )
