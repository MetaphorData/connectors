from metaphor.postgresql.extractor import PostgresqlRunConfig


def test_json_config(test_root_dir):
    config = PostgresqlRunConfig.from_json_file(
        f"{test_root_dir}/postgresql/config.json"
    )

    assert config == PostgresqlRunConfig(
        host="host",
        database="database",
        user="user",
        password="password",
        port=5432,
        redshift=True,
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = PostgresqlRunConfig.from_yaml_file(
        f"{test_root_dir}/postgresql/config.yml"
    )

    assert config == PostgresqlRunConfig(
        host="host",
        database="database",
        user="user",
        password="password",
        port=5432,
        redshift=True,
        output=None,
    )
