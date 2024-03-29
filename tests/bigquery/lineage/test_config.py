from metaphor.bigquery.lineage.config import BigQueryLineageRunConfig
from metaphor.common.base_config import OutputConfig


def test_yaml_config(test_root_dir):
    config = BigQueryLineageRunConfig.from_yaml_file(
        f"{test_root_dir}/bigquery/lineage/config.yml"
    )

    assert config == BigQueryLineageRunConfig(
        key_path="key_path",
        project_ids=["project_id"],
        enable_view_lineage=True,
        enable_lineage_from_log=False,
        lookback_days=10,
        include_self_lineage=False,
        batch_size=1000,
        output=OutputConfig(),
    )
