from metaphor.common.base_config import OutputConfig
from metaphor.google_directory.extractor import GoogleDirectoryRunConfig


def test_yaml_config(test_root_dir):
    config = GoogleDirectoryRunConfig.from_yaml_file(
        f"{test_root_dir}/google_directory/config.yml"
    )

    assert config == GoogleDirectoryRunConfig(
        token_file="token_file",
        output=OutputConfig(),
    )
