from typing import Collection

from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.runner import metaphor_file_sink_config, run_connector
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
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
            name="dashboard1",
            platform=DashboardPlatform.CUSTOM_DASHBOARD,
        ),
        dashboard_info=DashboardInfo(
            chars=[
                Chart(chart_type=ChartType.BAR, title="chart1"),
                Chart(chart_type=ChartType.LINE, title="chart2"),
            ]
        ),
        entity_upstream=[
            EntityUpstream(
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
            )
        ],
    )

    # Return a list of dashboards
    return [dashboard]


# Use the runner to run the connector and output events to the tenant's S3 bucket
connector_name = "custom_dashboard_connector"
tenant_name = "tenant"
run_connector(
    connector_func=custom_dashboard_connector,
    name=connector_name,
    platform=Platform.CUSTOM_DASHBOARD,
    description="This is a custom connector made by Acme, Inc.",
    file_sink_config=metaphor_file_sink_config(
        tenant_name, connector_name, is_metaphor_cloud=False
    ),
)
