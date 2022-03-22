from metaphor.bigquery.query.config import BigQueryQueryRunConfig
from metaphor.common.base_config import OutputConfig


def test_yaml_config(test_root_dir):
    config = BigQueryQueryRunConfig.from_yaml_file(
        f"{test_root_dir}/bigquery/query/config.yml"
    )

    assert config == BigQueryQueryRunConfig(
        key_path="key_path",
        project_id="project_id",
        batch_size=100,
        max_queries_per_table=200,
        exclude_service_accounts=False,
        excluded_usernames=set(["foo@foo.com", "bar@bar.com"]),
        lookback_days=10,
        output=OutputConfig(),
    )
