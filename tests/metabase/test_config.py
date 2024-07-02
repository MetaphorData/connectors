from metaphor.common.base_config import OutputConfig
from metaphor.metabase.config import MetabaseDatabaseDefaults, MetabaseRunConfig


def test_yaml_config(test_root_dir):
    config = MetabaseRunConfig.from_yaml_file(f"{test_root_dir}/metabase/config.yml")

    assert config == MetabaseRunConfig(
        server_url="https://metaphor.metabaseapp.com",
        username="foo",
        password="bar",
        database_defaults=[
            MetabaseDatabaseDefaults(
                id=1,
                default_schema="SCH",
            ),
            MetabaseDatabaseDefaults(
                id=2,
                default_schema="SCH2",
            ),
        ],
        output=OutputConfig(),
    )
    assert config.asset_filter.include_path([])
    assert config.asset_filter.include_path(["a"])


def test_excluded_paths(test_root_dir) -> None:
    config = MetabaseRunConfig.from_yaml_file(
        f"{test_root_dir}/metabase/config_exclude_paths.yml"
    )
    assert not config.asset_filter.include_path(["1"])
    assert not config.asset_filter.include_path(["a", "1"])
    assert config.asset_filter.include_path(["a", "2"])
    assert config.asset_filter.include_path(["b", "c"])
    assert config.asset_filter.include_path(["b", "c", "1"])
    assert not config.asset_filter.include_path(["b", "c", "d"])
