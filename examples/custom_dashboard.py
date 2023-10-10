from typing import Collection

from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.file_sink import S3StorageConfig
from metaphor.common.runner import metaphor_file_sink_config, run_connector
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


# The connector function that produces a list of entities
def custom_dashboard_connector() -> Collection[ENTITY_TYPES]:
    # Extract metadata from custom source and fill out the applicable fields
    dashboard = Dashboard(
        logical_id=DashboardLogicalID(
            dashboard_id="dashboard1",
            platform=DashboardPlatform.CUSTOM_DASHBOARD,
        ),
        structure=AssetStructure(name="dashboard1", directories=["level1", "level2"]),
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
    connector_func=custom_dashboard_connector,
    name=connector_name,
    platform=Platform.CUSTOM_DASHBOARD,
    description="This is a custom connector made by Acme, Inc.",
    file_sink_config=metaphor_file_sink_config(
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
