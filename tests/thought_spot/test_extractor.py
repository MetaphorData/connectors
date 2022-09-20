from unittest.mock import patch

import pytest
from freezegun import freeze_time
from restapisdk.models.search_object_header_type_enum import SearchObjectHeaderTypeEnum

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.thought_spot.config import ThoughtSpotRunConfig
from metaphor.thought_spot.extractor import ThoughtSpotExtractor
from metaphor.thought_spot.models import (
    AnswerMetadata,
    ConnectionMetadata,
    ConnectionType,
    DataSourceConfiguration,
    DataSourceContent,
    DataSourceTypeEnum,
    Header,
    LiveBoardHeader,
    LiveBoardMetadate,
    LogicalTableContent,
    Reference,
    ReportContent,
    Sheet,
    SheetContent,
    SourceMetadata,
    SourceType,
    TableMappingInfo,
    Tag,
    Visualization,
    VizColumn,
    VizContent,
)
from tests.test_utils import load_json


def dummy_config():
    return ThoughtSpotRunConfig(
        user="user",
        password="password",
        base_url="http://base.url",
        output=OutputConfig(),
    )


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor(test_root_dir):

    connections = [
        ConnectionMetadata(
            header=Header(id="conn1", name="Connection 1"),
            type=ConnectionType.BIGQUERY,
            dataSourceContent=DataSourceContent(
                configuration=DataSourceConfiguration(project_id="project")
            ),
        )
    ]

    # TODO(SC-12448): Improve test coverage for other ThoughtSpot data objects & lineage
    tables = [
        SourceMetadata(
            header=Header(
                id="table1",
                name="Table 1",
                description="This is table1",
                tags=[
                    Tag(
                        name="tag1", isDeleted=False, isHidden=False, isDeprecated=False
                    )
                ],
            ),
            type=SourceType.WORKSHEET,
            columns=[],
            dataSourceId="conn1",
            dataSourceTypeEnum=DataSourceTypeEnum.DEFAULT,
            logicalTableContent=LogicalTableContent(
                physicalTableName="__unused__",
                worksheetType="__unused__",
                joinType="__unused__",
                tableMappingInfo=TableMappingInfo(
                    databaseName="db",
                    schemaName="schema",
                    tableName="table",
                    tableType="__unused__",
                ),
            ),
        )
    ]

    answers = [
        AnswerMetadata(
            header=Header(
                id="answer1",
                name="Answer 1",
                description="This is answer1",
                tags=[
                    Tag(
                        name="tag2", isDeleted=False, isHidden=False, isDeprecated=False
                    )
                ],
            ),
            type="type",
            reportContent=ReportContent(
                sheets=[
                    Sheet(
                        header=Header(
                            id="sheet1", name="Sheet 1", description="This is sheet1"
                        ),
                        sheetContent=SheetContent(
                            visualizations=[
                                Visualization(
                                    header=Header(
                                        id="viz1",
                                        name="__unused__",
                                        description="__unused__",
                                    ),
                                    vizContent=VizContent(
                                        vizType="CHART",
                                        chartType="LINE",
                                        columns=[
                                            VizColumn(
                                                referencedTableHeaders=[
                                                    Reference(
                                                        id="data1", name="__unused__"
                                                    )
                                                ]
                                            )
                                        ],
                                    ),
                                )
                            ]
                        ),
                    )
                ]
            ),
        )
    ]

    liveboards = [
        LiveBoardMetadate(
            header=LiveBoardHeader(
                id="board1",
                name="Board 1",
                description="This is board1",
                tags=[
                    Tag(
                        name="tag3", isDeleted=False, isHidden=False, isDeprecated=False
                    )
                ],
                resolvedObjects={
                    "answer1": answers[0],
                },
            ),
            type="type",
            reportContent=ReportContent(
                sheets=[
                    Sheet(
                        header=Header(
                            id="sheet2", name="Sheet 2", description="This is sheet2"
                        ),
                        sheetContent=SheetContent(
                            visualizations=[
                                Visualization(
                                    header=Header(
                                        id="viz2",
                                        name="Viz 2",
                                        description="This is viz2",
                                    ),
                                    vizContent=VizContent(
                                        vizType="CHART",
                                        chartType="LINE",
                                        refVizId="answer1",
                                    ),
                                )
                            ],
                        ),
                    )
                ]
            ),
        )
    ]

    def mock_fetch_connections(client):
        return connections

    def mock_fetch_objects(client, type):
        return {
            SearchObjectHeaderTypeEnum.DATAOBJECT_TABLE: tables,
            SearchObjectHeaderTypeEnum.ANSWER: answers,
            SearchObjectHeaderTypeEnum.LIVEBOARD: liveboards,
        }.get(type, [])

    with patch("metaphor.thought_spot.extractor.ThoughtSpot") as mock_thought_spot:
        mock_thought_spot.fetch_connections = mock_fetch_connections
        mock_thought_spot.fetch_objects = mock_fetch_objects

        extractor = ThoughtSpotExtractor(dummy_config())
        events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/thought_spot/expected.json")
