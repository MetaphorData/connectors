from metaphor.models.metadata_change_event import (
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardUpstream,
    DataPlatform,
    SourceInfo,
)
from tableauserverclient import WorkbookItem

from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.tableau.extractor import TableauExtractor


def test_view_url():
    view_name = "Regional/sheets/Obesity"

    extractor = TableauExtractor()
    extractor._base_url = "https://10ax.online.tableau.com/#/site/abc"

    view_url = extractor._build_view_url(view_name)

    assert (
        view_url == "https://10ax.online.tableau.com/#/site/abc/views/Regional/Obesity"
    )


def test_parse_workbook_url():

    extractor = TableauExtractor()

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
    workbook._set_views([])

    extractor = TableauExtractor()
    extractor._parse_dashboard(workbook)

    assert len(extractor._dashboards) == 1
    assert extractor._dashboards["123"] == Dashboard(
        logical_id=DashboardLogicalID(
            dashboard_id="123", platform=DashboardPlatform.TABLEAU
        ),
        dashboard_info=DashboardInfo(
            charts=[],
            description="d",
            title="project1.wb",
            url="https://10ax.online.tableau.com/#/site/abc/workbooks/123",
            view_count=0.0,
        ),
        source_info=SourceInfo(
            main_url="https://10ax.online.tableau.com/#/site/abc/workbooks/123",
        ),
    )

    workbook_graphql = {
        "name": "Snowflake test1",
        "projectName": "default",
        "vizportalUrlId": "123",
        "upstreamTables": [
            {
                "name": "CYCLE",
                "fullName": "[LONDON].[CYCLE]",
                "schema": "LONDON",
                "database": {
                    "name": "DEV_DB",
                    "connectionType": "snowflake",
                },
            },
            {
                "name": "CYCLE",
                "fullName": "[DEV_DB.LONDON].[CYCLE]",
                "schema": "LONDON",
                "database": {
                    "name": "foo",
                    "connectionType": "snowflake",
                },
            },
        ],
    }

    extractor._snowflake_account = "snow"
    extractor._parse_dashboard_upstream(workbook_graphql)

    assert extractor._dashboards["123"].upstream == DashboardUpstream(
        source_datasets=["DATASET~44EFB0A109CA87DB8792256D25D030C9"],
    )
    assert extractor._dashboards["123"].upstream.source_datasets[0] == str(
        to_dataset_entity_id("dev_db.london.cycle", DataPlatform.SNOWFLAKE, "snow")
    )
