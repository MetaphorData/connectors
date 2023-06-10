from metaphor.common.base_config import OutputConfig
from metaphor.custom.lineage.config import (
    CustomLineageConfig,
    DeserializableDatasetLogicalID,
    Lineage,
)


def test_yaml_config(test_root_dir):
    config = CustomLineageConfig.from_yaml_file(
        f"{test_root_dir}/custom/lineage/config.yml"
    )

    assert config == CustomLineageConfig(
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
