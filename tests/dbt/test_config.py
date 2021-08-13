from metaphor.dbt.extractor import DbtRunConfig


def test_json_config(test_root_dir):
    config = DbtRunConfig.from_json_file(f"{test_root_dir}/dbt/config.json")

    assert config == DbtRunConfig(
        manifest="manifest",
        catalog="catalog",
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = DbtRunConfig.from_yaml_file(f"{test_root_dir}/dbt/config.yml")

    assert config == DbtRunConfig(
        manifest="manifest",
        catalog="catalog",
        output=None,
    )
