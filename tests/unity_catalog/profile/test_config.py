from metaphor.common.base_config import OutputConfig
from metaphor.unity_catalog.profile.config import UnityCatalogProfileRunConfig


def test_yaml_config(test_root_dir):
    config = UnityCatalogProfileRunConfig.from_yaml_file(
        f"{test_root_dir}/unity_catalog/profile/config.yml"
    )

    assert config == UnityCatalogProfileRunConfig(
        host="host",
        token="token",
        output=OutputConfig(),
        warehouse_id=None,
    )
