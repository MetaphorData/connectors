from tableauserverclient import ViewItem, WorkbookItem

from metaphor.common.base_config import OutputConfig
from metaphor.common.entity_id import to_dataset_entity_id, to_virtual_view_entity_id
from metaphor.models.metadata_change_event import (
    Chart,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardUpstream,
    DataPlatform,
    SourceInfo,
    VirtualViewType,
)
from metaphor.tableau.config import TableauRunConfig, TableauTokenAuthConfig
from metaphor.tableau.extractor import TableauExtractor
from metaphor.tableau.query import Database, DatabaseTable, WorkbookQueryResponse


def dummy_config():
    return TableauRunConfig(
        server_url="url",
        site_name="name",
        access_token=TableauTokenAuthConfig(token_name="name", token_value="value"),
        output=OutputConfig(),
    )


def test_view_url():
    view_name = "Regional/sheets/Obesity"

    extractor = TableauExtractor(dummy_config())
    extractor._base_url = "https://10ax.online.tableau.com/#/site/abc"

    view_url = extractor._build_view_url(view_name)

    assert (
        view_url == "https://10ax.online.tableau.com/#/site/abc/views/Regional/Obesity"
    )


def test_parse_workbook_url():
    extractor = TableauExtractor(dummy_config())

    # Tableau Online
    base_url, workbook_id = extractor._parse_workbook_url(
        "https://10ax.online.tableau.com/#/site/abc/workbooks/123"
    )
    assert base_url == "https://10ax.online.tableau.com/#/site/abc"
    assert workbook_id == "123"

    # Tableau Server with Default Site
    base_url, workbook_id = extractor._parse_workbook_url(
        "https://tableau01/#/workbooks/123"
    )
    assert base_url == "https://tableau01/#"
    assert workbook_id == "123"


def test_parse_dashboard():
    view = ViewItem()
    view._id = "vid"
    view._name = "name"
    view._content_url = "workbook/sheets/view"
    view._total_views = 100
    view._preview_image = lambda: bytes(1)

    workbook = WorkbookItem("project1")
    workbook._set_values(
        id="abc",
        name="wb",
        content_url="wb",
        webpage_url="https://10ax.online.tableau.com/#/site/abc/workbooks/123",
        created_at=None,
        description="d",
        updated_at=None,
        size=1,
        show_tabs=True,
        project_id=456,
        project_name="project1",
        owner_id=None,
        tags=None,
        views=[],
        data_acceleration_config=None,
    )
    workbook._set_views([view])

    extractor = TableauExtractor(dummy_config())
    extractor._views = {"vid": view}
    extractor._parse_dashboard(workbook)

    assert len(extractor._dashboards) == 1
    assert extractor._dashboards["123"] == Dashboard(
        logical_id=DashboardLogicalID(
            dashboard_id="123", platform=DashboardPlatform.TABLEAU
        ),
        dashboard_info=DashboardInfo(
            charts=[
                Chart(
                    title="name",
                    url="https://10ax.online.tableau.com/#/site/abc/views/workbook/view",
                    preview="data:image/png;base64,AA==",
                )
            ],
            description="d",
            title="project1.wb",
            view_count=100.0,
        ),
        source_info=SourceInfo(
            main_url="https://10ax.online.tableau.com/#/site/abc/workbooks/123",
        ),
    )

    workbook_graphql_response = {
        "luid": "abc",
        "name": "Snowflake test1",
        "projectName": "default",
        "vizportalUrlId": "123",
        "upstreamDatasources": [
            {
                "id": "sourceId1",
                "luid": "sourceId1",
                "name": "source1",
                "vizportalUrlId": "777",
                "fields": [],
                "upstreamTables": [
                    {
                        "luid": "4ba4462e",
                        "name": "CYCLE",
                        "fullName": "[LONDON].[CYCLE]",
                        "schema": "LONDON",
                        "database": {
                            "name": "DEV_DB",
                            "connectionType": "snowflake",
                        },
                    }
                ],
            }
        ],
        "embeddedDatasources": [
            {
                "id": "sourceId2",
                "name": "source2",
                "fields": [],
                "upstreamTables": [
                    {
                        "luid": "5dca51d8",
                        "name": "CYCLE_HIRE",
                        "fullName": "[BERLIN_BICYCLES].[CYCLE_HIRE]",
                        "schema": "BERLIN_BICYCLES",
                        "description": "",
                        "database": {"name": "ACME", "connectionType": "redshift"},
                    }
                ],
            }
        ],
    }

    extractor._snowflake_account = "snow"
    extractor._parse_workbook_query_response(
        WorkbookQueryResponse.parse_obj(workbook_graphql_response)
    )

    assert extractor._dashboards["123"].upstream == DashboardUpstream(
        source_virtual_views=[
            "VIRTUAL_VIEW~B5446C160574546FB913DB1D78989BAE",
            "VIRTUAL_VIEW~3ADEC7EE47E751141F716D29158E63A3",
        ],
    )
    assert extractor._dashboards["123"].upstream.source_virtual_views[0] == str(
        to_virtual_view_entity_id("sourceId1", VirtualViewType.TABLEAU_DATASOURCE)
    )
    assert extractor._dashboards["123"].upstream.source_virtual_views[1] == str(
        to_virtual_view_entity_id("sourceId2", VirtualViewType.TABLEAU_DATASOURCE)
    )
    assert extractor._virtual_views["sourceId1"].tableau_datasource.source_datasets[
        0
    ] == str(
        to_dataset_entity_id("dev_db.london.cycle", DataPlatform.SNOWFLAKE, "snow")
    )
    assert extractor._virtual_views["sourceId2"].tableau_datasource.source_datasets[
        0
    ] == str(
        to_dataset_entity_id("acme.berlin_bicycles.cycle_hire", DataPlatform.REDSHIFT)
    )


def test_parse_dataset_id():
    extractor = TableauExtractor(dummy_config())

    assert (
        extractor._parse_dataset_id(
            DatabaseTable(
                luid="uuid",
                name="name",
                fullName=None,
                database=None,
                schema_=None,
            )
        )
        is None
    )

    assert (
        extractor._parse_dataset_id(
            DatabaseTable(
                luid="uuid",
                name="name",
                fullName=None,
                database=Database(name="db", connectionType="invalid_type"),
                schema_=None,
            )
        )
        is None
    )

    # Use three-segment "fullName"
    assert extractor._parse_dataset_id(
        DatabaseTable(
            luid="uuid",
            name="name",
            fullName="db.schema.table",
            database=Database(name="db", connectionType="redshift"),
            schema_=None,
        )
    ) == to_dataset_entity_id("db.schema.table", DataPlatform.REDSHIFT)

    # Full back to two-segment "name"
    assert extractor._parse_dataset_id(
        DatabaseTable(
            luid="uuid",
            name="schema.table",
            fullName="random_name",
            database=Database(name="db", connectionType="redshift"),
            schema_=None,
        )
    ) == to_dataset_entity_id("db.schema.table", DataPlatform.REDSHIFT)

    # Test BigQuery project name => ID mapping
    extractor = TableauExtractor(
        TableauRunConfig(
            server_url="url",
            site_name="name",
            access_token=TableauTokenAuthConfig(token_name="name", token_value="value"),
            bigquery_project_name_to_id_map={"bq_name": "bq_id"},
            output=OutputConfig(),
        )
    )
    assert extractor._parse_dataset_id(
        DatabaseTable(
            luid="uuid",
            name="schema.table",
            fullName="random_name",
            database=Database(name="bq_name", connectionType="bigquery"),
            schema_=None,
        )
    ) == to_dataset_entity_id("bq_id.schema.table", DataPlatform.BIGQUERY)
