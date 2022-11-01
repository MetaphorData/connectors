from typing import List
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.power_bi.config import PowerBIRunConfig
from metaphor.power_bi.extractor import PowerBIExtractor
from metaphor.power_bi.power_bi_client import (
    PowerBIApp,
    PowerBIDashboard,
    PowerBIDataset,
    PowerBIPage,
    PowerBIRefresh,
    PowerBIRefreshStatus,
    PowerBIReport,
    PowerBITable,
    PowerBITableColumn,
    PowerBITableMeasure,
    PowerBITile,
    WorkspaceInfo,
    WorkspaceInfoDashboard,
    WorkspaceInfoDataset,
    WorkspaceInfoReport,
)
from tests.test_utils import load_json


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor(test_root_dir):
    mock_instance = MagicMock()

    workspace1_id = "workspace-1"

    app1 = PowerBIApp(
        id="00000000-0000-0000-0000-000000000000",
        name="foo app",
        workspaceId=workspace1_id,
    )

    app2 = PowerBIApp(
        id="00000000-0000-0000-0000-000000000001",
        name="bar app",
        workspaceId=workspace1_id,
    )

    dataset1_id = "00000000-0000-0000-0000-000000000002"
    dataset1 = PowerBIDataset(
        id=dataset1_id,
        name="Foo Dataset",
        isRefreshable=True,
        webUrl=f"https://powerbi.com/{dataset1_id}",
    )
    dataset2_id = "00000000-0000-0000-0000-000000000003"
    dataset2 = PowerBIDataset(
        id=dataset2_id,
        name="Bar Dataset",
        isRefreshable=False,
        webUrl=f"https://powerbi.com/{dataset2_id}",
    )

    report1_id = "00000000-0000-0000-0000-000000000004"
    report1 = PowerBIReport(
        id=report1_id,
        name="Foo Report",
        datasetId=dataset1_id,
        reportType="",
        webUrl=f"https://powerbi.com/report/{report1_id}",
    )

    report1_app_id = "00000000-0000-0000-0000-000000000005"
    report1_app = PowerBIReport(
        id=report1_app_id,
        name="Foo Report",
        datasetId=dataset1_id,
        reportType="",
        webUrl=f"https://powerbi.com/groups/me/apps/{app1.id}/reports/{report1_id}",
    )

    report2_id = "00000000-0000-0000-0000-000000000006"
    report2 = PowerBIReport(
        id=report2_id,
        name="Bar Report",
        datasetId=dataset2_id,
        reportType="",
        webUrl=f"https://powerbi.com/report/{report2_id}",
    )

    dashboard1_id = "00000000-0000-0000-0000-000000000007"
    dashboard1 = PowerBIDashboard(
        id=dashboard1_id,
        displayName="Dashboard A",
        webUrl=f"https://powerbi.com/dashboard/{dashboard1_id}",
    )

    dashboard1_app_id = "00000000-0000-0000-0000-000000000008"
    dashboard1_app = PowerBIDashboard(
        id=dashboard1_app_id,
        displayName="Dashboard A",
        webUrl=f"https://powerbi.com/groups/me/apps/{app2.id}/dashboards/{dashboard1_id}",
    )

    dashboard2_id = "00000000-0000-0000-0000-000000000009"
    dashboard2 = PowerBIDashboard(
        id=dashboard2_id,
        displayName="Dashboard B",
        webUrl=f"https://powerbi.com/dashboard/{dashboard2_id}",
    )

    tile1 = PowerBITile(
        id="00000000-0000-0000-0000-00000000000A",
        title="First Chart",
        datasetId=dataset1_id,
        reportId=report1_id,
        embedUrl="",
    )

    tile2 = PowerBITile(
        id="00000000-0000-0000-0000-00000000000B",
        title="Second Chart",
        datasetId=dataset1_id,
        reportId=report1_id,
        embedUrl="",
    )

    tile3 = PowerBITile(
        id="00000000-0000-0000-0000-00000000000C",
        title="Third Chart",
        datasetId=dataset2_id,
        reportId=report2_id,
        embedUrl="",
    )

    tiles = {
        dashboard1_id: [tile1, tile3],
        dashboard2_id: [tile2, tile3],
        dashboard1_app_id: [],
    }

    page1 = PowerBIPage(name="name-1", displayName="First Page", order=1)

    page2 = PowerBIPage(name="name-2", displayName="Second Page", order=2)

    pages = {
        workspace1_id: {report1_id: [], report2_id: [page1, page2], report1_app_id: []}
    }

    refreshes = {
        workspace1_id: {
            dataset1_id: [
                PowerBIRefresh(status=PowerBIRefreshStatus.FAILED, endTime=""),
                PowerBIRefresh(
                    status=PowerBIRefreshStatus.COMPLETED,
                    endTime="2022-01-01T01:02:03.456Z",
                ),
            ]
        }
    }

    mock_instance.get_workspace_info = MagicMock(
        return_value=[
            WorkspaceInfo(
                id=workspace1_id,
                name="Workspace",
                type="normal",
                state="",
                reports=[
                    WorkspaceInfoReport(
                        id=report1.id,
                        name=report1.name,
                        datasetId=report1.datasetId,
                        description="This is a report about foo",
                    ),
                    WorkspaceInfoReport(
                        id=report1_app.id,
                        appId=app1.id,
                        name=report1_app.name,
                        datasetId=report1_app.datasetId,
                        description="[app] This is a report about foo",
                    ),
                    WorkspaceInfoReport(
                        id=report2.id,
                        name=report2.name,
                        datasetId=report2.datasetId,
                        description="This is a report about bar",
                    ),
                ],
                datasets=[
                    WorkspaceInfoDataset(
                        configuredBy="alice@foo.com",
                        id=dataset1.id,
                        name=dataset1.name,
                        description="This is a dataset",
                        tables=[
                            PowerBITable(
                                name="table1",
                                columns=[
                                    PowerBITableColumn(name="col1", dataType="string"),
                                    PowerBITableColumn(name="col2", dataType="int"),
                                ],
                                measures=[
                                    PowerBITableMeasure(
                                        name="exp1",
                                        expression="avg(col1)",
                                        description="this is exp1",
                                    ),
                                    PowerBITableMeasure(
                                        name="exp2", expression="max(col1)"
                                    ),
                                ],
                                source=[
                                    {
                                        "expression": 'let\n    Source = AmazonRedshift.Database("url:5439","db"),\n    public = Source{[Name="public"]}[Data],\n    table1 = public{[Name="table"]}[Data]\nin\n    table1'
                                    }
                                ],
                            )
                        ],
                    ),
                    WorkspaceInfoDataset(
                        configuredBy="bob@foo.com",
                        id=dataset2.id,
                        name=dataset2.name,
                        description="This is another dataset",
                        tables=[
                            PowerBITable(
                                name="table2",
                                columns=[
                                    PowerBITableColumn(name="col1", dataType="string"),
                                    PowerBITableColumn(name="col2", dataType="int"),
                                ],
                                measures=[],
                                source=[
                                    {
                                        "expression": 'let\n    Source = Snowflake.Databases("some-account.snowflakecomputing.com","COMPUTE_WH"),\n    DB_Database = Source{[Name="DB",Kind="Database"]}[Data],\n    PUBLIC_Schema = DB_Database{[Name="PUBLIC",Kind="Schema"]}[Data],\n    TEST_Table = PUBLIC_Schema{[Name="TEST",Kind="Table"]}[Data]\nin\n    TEST_Table'
                                    }
                                ],
                            ),
                            PowerBITable(
                                name="table2",
                                columns=[
                                    PowerBITableColumn(name="col1", dataType="string"),
                                    PowerBITableColumn(name="col2", dataType="int"),
                                ],
                                measures=[],
                                source=[
                                    {
                                        "expression": 'let\n    Source = GoogleBigQuery.Database(),\n    #"test-project" = Source{[Name="test-project"]}[Data],\n    test_Schema = #"test-project"{[Name="test",Kind="Schema"]}[Data],\n    example_Table = test_Schema{[Name="example",Kind="Table"]}[Data]\nin\n    example_Table'
                                    }
                                ],
                            ),
                        ],
                    ),
                ],
                dashboards=[
                    WorkspaceInfoDashboard(
                        displayName="Dashboard A",
                        id=dashboard1_id,
                    ),
                    WorkspaceInfoDashboard(
                        displayName="Dashboard A",
                        id=dashboard1_app_id,
                        appId=app2.id,
                    ),
                    WorkspaceInfoDashboard(displayName="Dashboard B", id=dashboard2_id),
                ],
            )
        ]
    )

    def fake_get_datasets(workspace_id) -> List[PowerBIDataset]:
        return [dataset1, dataset2]

    def fake_get_reports(workspace_id) -> List[PowerBIReport]:
        return [report1, report2, report1_app]

    def fake_get_dashboards(workspace_id) -> List[PowerBIReport]:
        return [dashboard1, dashboard2, dashboard1_app]

    def fake_get_tiles(dashboard_id) -> List[PowerBITile]:
        return tiles.get(dashboard_id)

    def fake_get_pages(workspace_id, report_id) -> List[PowerBIPage]:
        return pages.get(workspace_id).get(report_id)

    def fake_get_refreshes(workspace_id, dataset_id) -> List[PowerBIRefresh]:
        return refreshes.get(workspace_id).get(dataset_id)

    def fake_get_apps() -> List[PowerBIApp]:
        return [app1, app2]

    mock_instance.get_datasets.side_effect = fake_get_datasets
    mock_instance.get_reports.side_effect = fake_get_reports
    mock_instance.get_dashboards.side_effect = fake_get_dashboards
    mock_instance.get_tiles.side_effect = fake_get_tiles
    mock_instance.get_pages.side_effect = fake_get_pages
    mock_instance.get_refreshes.side_effect = fake_get_refreshes
    mock_instance.get_apps.side_effect = fake_get_apps

    with patch("metaphor.power_bi.extractor.PowerBIClient") as mock_client:
        mock_client.return_value = mock_instance

        config = PowerBIRunConfig(
            output=OutputConfig(),
            tenant_id="fake",
            client_id="fake_client_id",
            secret="fake_secret",
            workspaces=["bar"],
        )
        extractor = PowerBIExtractor(config)

        events = [EventUtil.trim_event(e) for e in await extractor.extract()]

        import json

        print(json.dumps(events))

    assert events == load_json(f"{test_root_dir}/power_bi/expected.json")
