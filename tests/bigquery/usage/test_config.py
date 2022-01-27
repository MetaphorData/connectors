from metaphor.bigquery.usage.config import BigQueryUsageRunConfig


def test_yaml_config(test_root_dir):
    config = BigQueryUsageRunConfig.from_yaml_file(
        f"{test_root_dir}/bigquery/usage/config.yml"
    )

    assert config == BigQueryUsageRunConfig(
        key_path="key_path",
        project_id="project_id",
        lookback_days=365,
        batch_size=1000,
        output=None,
    )
