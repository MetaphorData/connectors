from metaphor.common.base_config import OutputConfig
from metaphor.custom.governance.config import (
    ColumnDescriptions,
    ColumnTags,
    CustomGovernanceConfig,
    DatasetGovernance,
    Description,
    DeserializableDatasetLogicalID,
    Ownership,
)


def test_yaml_config(test_root_dir):
    config = CustomGovernanceConfig.from_yaml_file(
        f"{test_root_dir}/custom/governance/config.yml"
    )

    assert config == CustomGovernanceConfig(
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
                column_descriptions=[
                    ColumnDescriptions(
                        column_name="col1",
                        descriptions=[
                            Description(description="foo", email="foo@test.com"),
                            Description(description="bar", email="barbar@test.com"),
                        ],
                    ),
                    ColumnDescriptions(
                        column_name="col3",
                        descriptions=[
                            Description(description="baz", email="bazbazbaz@test.com"),
                        ],
                    ),
                ],
            )
        ],
        output=OutputConfig(),
    )
