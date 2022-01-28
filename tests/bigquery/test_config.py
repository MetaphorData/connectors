from metaphor.bigquery.extractor import BigQueryRunConfig
from metaphor.common.base_config import OutputConfig


def test_yaml_config(test_root_dir):
    config = BigQueryRunConfig.from_yaml_file(f"{test_root_dir}/bigquery/config.yml")

    assert config == BigQueryRunConfig(
        key_path="key_path",
        project_id="project_id",
        output=OutputConfig(),
    )
