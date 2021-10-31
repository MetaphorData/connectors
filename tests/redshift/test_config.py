from metaphor.redshift.config import RedshiftRunConfig


def test_json_config(test_root_dir):
    config = RedshiftRunConfig.from_json_file(f"{test_root_dir}/redshift/config.json")

    assert config == RedshiftRunConfig(
        host="host",
        database="database",
        user="user",
        password="password",
        port=5439,
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = RedshiftRunConfig.from_yaml_file(f"{test_root_dir}/redshift/config.yml")

    assert config == RedshiftRunConfig(
        host="host",
        database="database",
        user="user",
        password="password",
        port=1234,
        output=None,
    )
