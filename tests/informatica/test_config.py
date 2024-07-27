from metaphor.common.base_config import OutputConfig
from metaphor.informatica.config import InformaticaRunConfig


def test_yaml_config(test_root_dir):
    config = InformaticaRunConfig.from_yaml_file(
        f"{test_root_dir}/informatica/config.yml"
    )

    assert config == InformaticaRunConfig(
        base_url="https://test",
        user="user",
        password="password",
        output=OutputConfig(),
    )
