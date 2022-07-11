from metaphor.common.base_config import OutputConfig
from metaphor.manual.governance.config import (
    DatasetGovernance,
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
            )
        ],
        output=OutputConfig(),
    )
