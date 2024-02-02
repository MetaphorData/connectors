from typing import Collection, Optional

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.runner import metaphor_sink_config, run_connector
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetDataQuality,
    DatasetLogicalID,
)


@dataclass(config=ConnectorConfig)
class CustomDQRunConfig(BaseConfig):
    pass


class CustomDQConnector(BaseExtractor):
    """
    The connector class that produces a list of entities
    """

    @staticmethod
    def from_config_file(config_file: str) -> "CustomDQConnector":
        return CustomDQConnector(CustomDQRunConfig.from_yaml_file(config_file))

    def __init__(self, config: CustomDQRunConfig) -> None:
        super().__init__(config)

    # Run actual DQ tests and fill out DatasetDataQuality
    def run_dq_tests(self) -> Optional[DatasetDataQuality]:
        pass

    async def extract(self) -> Collection[ENTITY_TYPES]:
        # Set the upstream aspect
        dataset = Dataset(
            logical_id=DatasetLogicalID(
                name=dataset_normalized_name("db", "schema", "dest"),
                platform=DataPlatform.BIGQUERY,
            ),
            data_quality=self.run_dq_tests(),
        )

        # Return a list of datasets
        return [dataset]


# Use the runner to run the connector and output events to the tenant's S3 bucket
connector_name = "custom_dq_connector"
tenant_name = "tenant"
run_connector(
    connector=CustomDQConnector.from_config_file(""),
    name=connector_name,
    platform=Platform.BIGQUERY,
    description="This is a custom connector made by Acme, Inc.",
    sink_config=metaphor_sink_config(tenant_name, connector_name),
)
