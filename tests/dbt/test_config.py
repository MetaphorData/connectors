from metaphor.dbt.extractor import DbtRunConfig


def test_yaml_config(test_root_dir):
    config = DbtRunConfig.from_yaml_file(f"{test_root_dir}/dbt/config.yml")

    assert config == DbtRunConfig(
        manifest="manifest",
        catalog="catalog",
        docs_base_url="http://localhost",
        project_source_url="http://foo.bar",
        output=None,
    )
