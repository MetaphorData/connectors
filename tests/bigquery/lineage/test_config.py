from metaphor.bigquery.lineage.config import BigQueryLineageRunConfig


def test_yaml_config(test_root_dir):
    config = BigQueryLineageRunConfig.from_yaml_file(
        f"{test_root_dir}/bigquery/lineage/config.yml"
    )

    assert config == BigQueryLineageRunConfig(
        key_path="key_path",
        project_id="project_id",
        lookback_days=10,
        batch_size=1000,
        output=None,
    )
