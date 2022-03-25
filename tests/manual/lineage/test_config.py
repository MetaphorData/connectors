from metaphor.common.base_config import OutputConfig
from metaphor.manual.lineage.config import (
    DeserializableDatasetLogicalID,
    Lineage,
    ManualLienageConfig,
)


def test_yaml_config(test_root_dir):
    config = ManualLienageConfig.from_yaml_file(
        f"{test_root_dir}/manual/lineage/config.yml"
    )

    assert config == ManualLienageConfig(
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
