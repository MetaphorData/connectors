from metaphor.common.base_config import OutputConfig
from metaphor.metabase.config import MetabaseRunConfig


def test_yaml_config(test_root_dir):
    config = MetabaseRunConfig.from_yaml_file(f"{test_root_dir}/metabase/config.yml")

    assert config == MetabaseRunConfig(
        server_url="https://metaphor.metabaseapp.com",
        username="foo",
        password="bar",
        output=OutputConfig(),
    )
