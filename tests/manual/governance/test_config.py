from metaphor.common.base_config import OutputConfig
from metaphor.manual.governance.config import (
    ColumnTags,
    DatasetGovernance,
    Description,
    DeserializableDatasetLogicalID,
    ManualGovernanceConfig,
    Ownership,
)


def test_yaml_config(test_root_dir):
    config = ManualGovernanceConfig.from_yaml_file(
        f"{test_root_dir}/manual/governance/config.yml"
    )

    assert config == ManualGovernanceConfig(
        datasets=[
            DatasetGovernance(
                id=DeserializableDatasetLogicalID(name="foo", platform="BIGQUERY"),
                ownerships=[
                    Ownership(type="Data Steward", email="foo1@bar.com"),
                    Ownership(type="Maintainer", email="foo2@bar.com"),
                ],
                tags=["pii", "golden"],
                column_tags=[
                    ColumnTags(column="col1", tags=["phi"]),
                    ColumnTags(column="col2", tags=["deprecated", "do_not_use"]),
                ],
                descriptions=[
                    Description(description="Quick brown fox", email="foo3@bar.com")
                ],
            )
        ],
        output=OutputConfig(),
    )
