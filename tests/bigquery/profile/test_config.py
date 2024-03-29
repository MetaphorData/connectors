from metaphor.bigquery.profile.extractor import BigQueryProfileRunConfig
from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.common.sampling import SamplingConfig


def test_yaml_config(test_root_dir):
    config = BigQueryProfileRunConfig.from_yaml_file(
        f"{test_root_dir}/bigquery/profile/config.yml"
    )

    assert config == BigQueryProfileRunConfig(
        key_path="key_path",
        project_ids=["project_id"],
        filter=DatasetFilter(
            excludes={
                "project_id": {
                    "dataset_foo": {"table_bar"},
                },
            },
            includes=None,
        ),
        sampling=SamplingConfig(percentage=20, threshold=100000),
        output=OutputConfig(),
    )
