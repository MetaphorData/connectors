from typing import Collection

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.runner import metaphor_file_sink_config, run_connector
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    EntityUpstream,
)


@dataclass(config=ConnectorConfig)
class CustomLineageRunConfig(BaseConfig):
    pass


class CustomLineageConnector(BaseExtractor):
    """
    The connector class that produces a list of entities
    """

    @staticmethod
    def from_config_file(config_file: str) -> "CustomLineageConnector":
        return CustomLineageConnector(
            CustomLineageRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: CustomLineageRunConfig) -> None:
        super().__init__(config)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        # Create string-based entity IDs for src1 & src2
        src1_id = str(
            to_dataset_entity_id(
                dataset_normalized_name("db", "schema", "src1"), DataPlatform.BIGQUERY
            )
        )
        src2_id = str(
            to_dataset_entity_id(
                dataset_normalized_name("db", "schema", "src2"), DataPlatform.BIGQUERY
            )
        )

        # Set the upstream aspect
        dataset = Dataset(
            logical_id=DatasetLogicalID(
                platform=DataPlatform.BIGQUERY,
                name=dataset_normalized_name("db", "schema", "dest"),
            ),
            entity_upstream=EntityUpstream(source_entities=[src1_id, src2_id]),
        )

        # Return a list of datasets
        return [dataset]


# Use the runner to run the connector and output events to the tenant's S3 bucket
connector_name = "custom_lineage_connector"
tenant_name = "tenant"
run_connector(
    connector=CustomLineageConnector.from_config_file(""),
    name=connector_name,
    platform=Platform.BIGQUERY,
    description="This is a custom connector made by Acme, Inc.",
    file_sink_config=metaphor_file_sink_config(tenant_name, connector_name),
)
