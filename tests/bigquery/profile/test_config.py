from metaphor.bigquery.profile.config import SamplingConfig
from metaphor.bigquery.profile.extractor import BigQueryProfileRunConfig
from metaphor.common.filter import DatasetFilter


def test_json_config(test_root_dir):
    config = BigQueryProfileRunConfig.from_json_file(
        f"{test_root_dir}/bigquery/profile/config.json"
    )

    assert config == BigQueryProfileRunConfig(
        key_path="key_path",
        project_id="project_id",
        output=None,
    )


def test_yaml_config(test_root_dir):
    config = BigQueryProfileRunConfig.from_yaml_file(
        f"{test_root_dir}/bigquery/profile/config.yml"
    )

    assert config == BigQueryProfileRunConfig(
        key_path="key_path",
        project_id="project_id",
        filter=DatasetFilter(
            excludes={
                "project_id": {
                    "dataset_foo": {"table_bar"},
                },
            },
            includes=None,
        ),
        sampling=SamplingConfig(percentage=20),
        output=None,
    )
