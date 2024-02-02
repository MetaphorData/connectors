from typing import Collection

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.runner import metaphor_sink_config, run_connector
from metaphor.common.sink import S3StorageConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AssetStructure,
    Chart,
    ChartType,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DataPlatform,
    EntityUpstream,
)


@dataclass(config=ConnectorConfig)
class CustomDashboardRunConfig(BaseConfig):
    pass


class CustomDashboardConnector(BaseExtractor):
    """
    The connector class that produces a list of entities
    """

    @staticmethod
    def from_config_file(config_file: str) -> "CustomDashboardConnector":
        return CustomDashboardConnector(
            CustomDashboardRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: CustomDashboardRunConfig) -> None:
        super().__init__(config)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        # Extract metadata from custom source and fill out the applicable fields
        dashboard = Dashboard(
            logical_id=DashboardLogicalID(
                dashboard_id="dashboard1",
                platform=DashboardPlatform.CUSTOM_DASHBOARD,
            ),
            structure=AssetStructure(
                name="dashboard1", directories=["level1", "level2"]
            ),
            dashboard_info=DashboardInfo(
                title="dashboard1",
                charts=[
                    Chart(chart_type=ChartType.BAR, title="chart1"),
                    Chart(chart_type=ChartType.LINE, title="chart2"),
                ],
            ),
            entity_upstream=EntityUpstream(
                transformation="select * from table1",
                source_entities=[
                    str(
                        to_dataset_entity_id(
                            dataset_normalized_name("db", "schema", "table1"),
                            DataPlatform.SNOWFLAKE,
                            "snowflake_account",
                        )
                    )
                ],
            ),
        )

        # Return a list of dashboards
        return [dashboard]


# Use the runner to run the connector and output events to the tenant's S3 bucket
connector_name = "custom_dashboard_connector"
tenant_name = "<tenant>"
run_connector(
    connector=CustomDashboardConnector.from_config_file(""),
    name=connector_name,
    platform=Platform.CUSTOM_DASHBOARD,
    description="This is a custom connector made by Acme, Inc.",
    sink_config=metaphor_sink_config(
        tenant_name,
        connector_name,
        is_metaphor_cloud=False,
        s3_auth_config=S3StorageConfig(
            aws_access_key_id="<access_key_id>",
            aws_secret_access_key="<secret_access_key>",
            region_name="<region>",
        ),
    ),
)
