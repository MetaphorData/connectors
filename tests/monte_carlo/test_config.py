from metaphor.common.base_config import OutputConfig
from metaphor.monte_carlo.config import MonteCarloRunConfig


def test_yaml_config(test_root_dir):
    config = MonteCarloRunConfig.from_yaml_file(
        f"{test_root_dir}/monte_carlo/config.yml"
    )

    assert config == MonteCarloRunConfig(
        api_key_id="key_id",
        api_key_secret="key_secret",
        treat_unhandled_anomalies_as_errors=True,
        output=OutputConfig(),
    )
