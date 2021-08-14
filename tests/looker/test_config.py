from metaphor.looker.extractor import LookerRunConfig


def test_json_config(test_root_dir):
    config = LookerRunConfig.from_json_file(f"{test_root_dir}/looker/config.json")

    assert config == LookerRunConfig(
        base_url="base_url",
        client_id="client_id",
        client_secret="client_secret",
        lookml_dir="lookml_dir",
        verify_ssl=True,
        timeout=1,
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = LookerRunConfig.from_yaml_file(f"{test_root_dir}/looker/config.yml")

    assert config == LookerRunConfig(
        base_url="base_url",
        client_id="client_id",
        client_secret="client_secret",
        lookml_dir="lookml_dir",
        verify_ssl=True,
        timeout=1,
        output=None,
    )
