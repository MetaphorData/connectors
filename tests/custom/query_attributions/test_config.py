from metaphor.common.base_config import OutputConfig
from metaphor.custom.query_attributions.config import (
    CustomQueryAttributionsConfig,
    PlatformQueryAttributions,
)
from metaphor.models.metadata_change_event import DataPlatform


def test_yaml_config(test_root_dir):
    config = CustomQueryAttributionsConfig.from_yaml_file(
        f"{test_root_dir}/custom/query_attributions/config.yml"
    )

    assert config == CustomQueryAttributionsConfig(
        attributions=[
            PlatformQueryAttributions(
                platform=DataPlatform.BIGQUERY,
                queries={
                    "projects/metaphor-data/jobs/b6e4ca01-6a80-4c6e-8be0-68be727deffd": "andy@metaphor.io",
                    "projects/metaphor-data/jobs/955ec125-357a-4b32-a672-49741f0c1864": "andy@metaphor.io",
                    "projects/metaphor-data/jobs/3e5afc6e-e8c2-4d75-b89d-e3131aaa2371": "andy@metaphor.io",
                    "projects/metaphor-data/jobs/96748ee6-de19-4e65-b5c2-c6c4dd50a63c": "yolo@metaphor.io",
                    "projects/metaphor-data/jobs/de50ea24-b6ef-4cd4-bb00-b390f0dbbf7f": "yolo@metaphor.io",
                },
            )
        ],
        output=OutputConfig(),
    )
