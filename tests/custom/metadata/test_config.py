from metaphor.common.base_config import OutputConfig
from metaphor.custom.governance.config import DeserializableDatasetLogicalID
from metaphor.custom.metadata.config import CustomMetadataConfig, DatasetMetadata


def test_yaml_config(test_root_dir):
    config = CustomMetadataConfig.from_yaml_file(
        f"{test_root_dir}/custom/metadata/config.yml"
    )

    assert config == CustomMetadataConfig(
        datasets=[
            DatasetMetadata(
                id=DeserializableDatasetLogicalID(name="foo", platform="BIGQUERY"),
                metadata={
                    "string": "v1",
                    "number": 1,
                    "list": ["v1", "v2"],
                    "dict": {
                        "k1": "v1",
                        "k2": "v2",
                    },
                },
            )
        ],
        output=OutputConfig(),
    )
