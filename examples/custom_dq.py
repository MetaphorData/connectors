from typing import Collection

from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.runner import metaphor_file_sink_config, run_connector
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetDataQuality,
    DatasetLogicalID,
)


# Run actual DQ tests and fill out DatasetDataQuality
def run_dq_tests() -> DatasetDataQuality:
    pass


# The connector function that produces a list of entities
def custom_dq_connector() -> Collection[ENTITY_TYPES]:
    # Set the upstream aspect
    dataset = Dataset(
        logical_id=DatasetLogicalID(
            name=dataset_normalized_name("db", "schema", "dest"),
            platform=DataPlatform.BIGQUERY,
        ),
        data_quality=run_dq_tests(),
    )

    # Return a list of datasets
    return [dataset]


# Use the runner to run the connector and output events to the tenant's S3 bucket
connector_name = "custom_dq_connector"
tenant_name = "tenant"
run_connector(
    connector_func=custom_dq_connector,
    name=connector_name,
    platform=Platform.BIGQUERY,
    description="This is a custom connector made by Acme, Inc.",
    file_sink_config=metaphor_file_sink_config(tenant_name, connector_name),
)
