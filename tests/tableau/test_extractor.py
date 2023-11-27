from typing import List
from unittest.mock import MagicMock, patch

import pytest
from tableauserverclient import ProjectItem, ViewItem, WorkbookItem

from metaphor.common.base_config import OutputConfig
from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.common.event_util import EventUtil
from metaphor.models.metadata_change_event import (
    AssetStructure,
    Chart,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DataPlatform,
    SourceInfo,
)
from metaphor.tableau.config import TableauRunConfig, TableauTokenAuthConfig
from metaphor.tableau.extractor import TableauExtractor
from metaphor.tableau.query import Database, DatabaseTable
from tests.test_utils import load_json


class ProjectsWrapper:
    def __init__(self, projects: List[ProjectItem]):
        self._projects = projects

    def __iter__(self):
        for w in self._projects:
            yield w


class ViewsWrapper:
    def __init__(self, views: List[ViewItem]):
        self._views = views

    def populate_preview_image(self, *args, **kwargs):
        pass

    def __iter__(self):
        for v in self._views:
            yield v


class WorkbooksWrapper:
    def __init__(self, workbooks: List[WorkbookItem]):
        self._workbooks = workbooks

    def populate_views(self, *args, **kwargs):
        pass

    def __iter__(self):
        for w in self._workbooks:
            yield w


class MockPager:
    def __init__(self, input, **argv):
        self._input = input

    def __iter__(self):
        return self._input.__iter__()


def dummy_config():
    return TableauRunConfig(
        server_url="https://10ax.online.tableau.com",
        site_name="abc",
        access_token=TableauTokenAuthConfig(token_name="name", token_value="value"),
        output=OutputConfig(),
    )


def dummy_config_with_alternative_url():
    return TableauRunConfig(
        server_url="https://10ax.online.tableau.com",
        site_name="",
        alternative_base_url="https://tableau.my_company.com",
        access_token=TableauTokenAuthConfig(token_name="name", token_value="value"),
        output=OutputConfig(),
    )


def test_build_base_url():
    # Tableau Online
    assert (
        TableauExtractor._build_base_url("https://10ax.online.tableau.com", "abc")
        == "https://10ax.online.tableau.com/#/site/abc"
    )

    # Tableau Server with Default Site
    assert (
        TableauExtractor._build_base_url("https://tableau01", "")
        == "https://tableau01/#"
    )


def test_view_url():
    view_name = "Regional/sheets/Obesity"

    extractor = TableauExtractor(dummy_config())

    view_url2 = extractor._build_view_url(view_name)

    assert (
        view_url2 == "https://10ax.online.tableau.com/#/site/abc/views/Regional/Obesity"
    )

    extractor2 = TableauExtractor(dummy_config_with_alternative_url())

    view_url2 = extractor2._build_view_url(view_name)

    assert view_url2 == "https://tableau.my_company.com/#/views/Regional/Obesity"


def test_asset_full_name():
    extractor = TableauExtractor(dummy_config())

    assert extractor._build_asset_full_name_and_structure(
        "asset_name", "project_id", "project_name"
    ) == (
        "project_name.asset_name",
        AssetStructure(directories=["project_name"], name="asset_name"),
    )
    assert extractor._build_asset_full_name_and_structure(
        "asset_name", "project_id", None
    ) == ("asset_name", AssetStructure(name="asset_name"))

    extractor._projects = {"project_id": ["parent", "project"]}

    assert extractor._build_asset_full_name_and_structure(
        "asset_name", "project_id", "project_name"
    ) == (
        "parent.project.asset_name",
        AssetStructure(directories=["parent", "project"], name="asset_name"),
    )


def test_extract_workbook_id():
    # Tableau Online
    assert (
        TableauExtractor._extract_workbook_id(
            "https://10ax.online.tableau.com/#/site/abc/workbooks/123"
        )
        == "123"
    )

    # Tableau Server with Default Site
    assert (
        TableauExtractor._extract_workbook_id("https://tableau01/#/workbooks/123")
        == "123"
    )


def test_parse_database_table():
    extractor = TableauExtractor(dummy_config())

    assert (
        extractor._parse_dataset_id(
            DatabaseTable(
                luid="uuid",
                name="name",
                fullName=None,
                database=None,
                schema=None,
            )
        )
        is None
    )

    assert (
        extractor._parse_dataset_id(
            DatabaseTable(
                luid="uuid",
                name="name",
                fullName="fullname",
                database=Database(name="db", connectionType="invalid_type"),
                schema=None,
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
            schema="foo",
        )
    ) == to_dataset_entity_id("db.schema.table", DataPlatform.REDSHIFT)

    # Full back to two-segment "name"
    assert extractor._parse_dataset_id(
        DatabaseTable(
            luid="uuid",
            name="schema.table",
            fullName="random_name",
            database=Database(name="db", connectionType="redshift"),
            schema="foo",
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
            schema="foo",
        )
    ) == to_dataset_entity_id("bq_id.schema.table", DataPlatform.BIGQUERY)


@patch("tableauserverclient.Server")
@patch("metaphor.tableau.extractor.paginate_connection")
@patch("tableauserverclient.Pager")
@pytest.mark.asyncio
async def test_extractor(
    mock_pager_cls: MagicMock,
    mock_paginate_connection: MagicMock,
    mock_server_cls: MagicMock,
    test_root_dir: str,
):
    extractor = TableauExtractor(dummy_config())

    project1 = ProjectItem("parent")
    project1._set_values("parent_id", "parent", "", None, None, None)
    project2 = ProjectItem("child")
    project2._set_values("child_project_id", "child", "", None, "parent_id", None)

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
        webpage_url="https://hostname/#/site/abc/workbooks/123",
        created_at=None,
        description="d",
        updated_at=None,
        size=1,
        show_tabs=True,
        project_id="child_project_id",
        project_name="project1",
        owner_id=None,
        tags={"tag1", "tag2"},
        views=[],
        data_acceleration_config=None,
    )
    workbook._set_views(lambda: [view])

    extractor._views = {"vid": view}
    extractor._parse_dashboard(workbook)

    assert len(extractor._dashboards) == 1
    assert extractor._dashboards["123"] == Dashboard(
        logical_id=DashboardLogicalID(
            dashboard_id="123", platform=DashboardPlatform.TABLEAU
        ),
        structure=AssetStructure(directories=["project1"], name="wb"),
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

    graphql_custom_sql_tables_response = [
        {
            "id": "custom_sql_1",
            "connectionType": "bigquery",
            "query": "select * from db.schema.table",
            "columnsConnection": {
                "nodes": [{"referencedByFields": [{"datasource": {"id": "sourceId3"}}]}]
            },
        }
    ]

    graphql_workbooks_response = [
        {
            "luid": "abc",
            "name": "Snowflake test1",
            "projectLuid": "child_project_id",
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
                            "database": {
                                "name": "ACME",
                                "connectionType": "redshift",
                            },
                        }
                    ],
                },
                {
                    "id": "sourceId3",
                    "name": "source3",
                    "fields": [],
                    "upstreamTables": [],
                },
            ],
        }
    ]

    extractor._snowflake_account = "snow"

    mock_auth = MagicMock()
    mock_auth.sign_in = MagicMock()

    mock_server = MagicMock()
    mock_server.auth = mock_auth
    mock_server.projects = ProjectsWrapper([project1, project2])
    mock_server.views = ViewsWrapper([view])
    mock_server.workbooks = WorkbooksWrapper([workbook])
    mock_server_cls.return_value = mock_server

    mock_pager_cls.side_effect = MockPager

    mock_paginate_connection.side_effect = [
        graphql_custom_sql_tables_response,
        graphql_workbooks_response,
    ]

    extractor._hierarchies = []  # Reset this!

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/tableau/expected.json")
