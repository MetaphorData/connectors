import json
from unittest.mock import MagicMock, patch

from tableauserverclient import WorkbookItem

from metaphor.tableau.workbook import get_all_workbooks
from tests.tableau.test_extractor import MockPager, WorkbooksWrapper


@patch("tableauserverclient.Server")
@patch("metaphor.tableau.graphql_utils._paginate_connection")
@patch("tableauserverclient.Pager")
def test_workbook(
    mock_pager_cls: MagicMock,
    mock_paginate_connection: MagicMock,
    mock_server_cls: MagicMock,
    test_root_dir: str,
):
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

    graphql_workbooks_response = [
        {
            "luid": "abc",
            "name": "Snowflake test1",
            "projectName": "default",
            "projectVizportalUrlId": "123456",
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
                    "owner": {
                        "luid": "12345678",
                    },
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
            "tags": [
                {
                    "name": "foo",
                },
                {
                    "name": "bar",
                },
            ],
        },
    ]
    mock_server = MagicMock()
    mock_server.workbooks = WorkbooksWrapper([workbook])
    mock_server_cls.return_value = mock_server
    mock_pager_cls.side_effect = MockPager

    mock_paginate_connection.side_effect = [
        graphql_workbooks_response,
    ]

    workbooks = get_all_workbooks(mock_server, 20)
    assert len(workbooks) == 1
    with open(f"{test_root_dir}/tableau/workbook/expected_graphql_item.json") as f:
        assert workbooks[0].graphql_item.model_dump(exclude_none=True) == json.loads(
            f.read()
        )
