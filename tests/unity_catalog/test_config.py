from metaphor.common.base_config import OutputConfig
from metaphor.unity_catalog.config import UnityCatalogRunConfig


def test_yaml_config_password(test_root_dir):
    config = UnityCatalogRunConfig.from_yaml_file(
        f"{test_root_dir}/unity_catalog/config.yml"
    )

    assert config == UnityCatalogRunConfig(
        hostname="hostname",
        http_path="path",
        token="token",
        source_url="http://foo.bar/{catalog}/{schema}/{table}",
        describe_history_limit=30,
        output=OutputConfig(),
    )
