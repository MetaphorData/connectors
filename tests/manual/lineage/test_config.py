from metaphor.common.base_config import OutputConfig
from metaphor.manual.lineage.config import (
    DeserializableDatasetLogicalID,
    Lineage,
    ManualLineageConfig,
)


def test_yaml_config(test_root_dir):
    config = ManualLineageConfig.from_yaml_file(
        f"{test_root_dir}/manual/lineage/config.yml"
    )

    assert config == ManualLineageConfig(
        lineages=[
            Lineage(
                dataset=DeserializableDatasetLogicalID(name="foo", platform="BIGQUERY"),
                upstreams=[
                    DeserializableDatasetLogicalID(name="foo", platform="BIGQUERY"),
                    DeserializableDatasetLogicalID(
                        name="bar", platform="SNOWFLAKE", account="account"
                    ),
                ],
            )
        ],
        output=OutputConfig(),
    )
