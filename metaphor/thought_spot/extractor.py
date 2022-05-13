from itertools import chain
from typing import Collection, Dict, List, Tuple

from metaphor.models.metadata_change_event import (
    Chart,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardUpstream,
    SourceInfo,
    ThoughtSpotColumn,
    ThoughtSpotDataObject,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from restapisdk.models.type_10_enum import Type10Enum
from restapisdk.restapisdk_client import RestapisdkClient

from metaphor.common.entity_id import (
    dataset_fullname,
    to_dataset_entity_id,
    to_virtual_view_entity_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.common.utils import unique_list
from metaphor.thought_spot.config import ThoughtspotRunConfig
from metaphor.thought_spot.models import (
    AnswerMetadata,
    ConnectionMetadata,
    DataSourceTypeEnum,
    Header,
    LiveBoardMetadate,
    SourceMetadata,
    Visualization,
)
from metaphor.thought_spot.utils import (
    ThoughtSpot,
    from_list,
    mapping_chart_type,
    mapping_data_object_type,
    mapping_data_platform,
)

logger = get_logger(__name__)


class ThoughtspotExtractor(BaseExtractor):
    """Thoughtspot metadata extractor"""

    def __init__(self):
        self._virtual_views: Dict[str, VirtualView] = {}
        self._dashboards: Dict[str, Dashboard] = {}

    @staticmethod
    def config_class():
        return ThoughtspotRunConfig

    async def extract(self, config: ThoughtspotRunConfig) -> Collection[ENTITY_TYPES]:
        self.base_url = config.base_url

        client = ThoughtSpot.create_client(config)

        self.fetch_virtual_views(client)
        self.fetch_dashboards(client)

        return list(chain(self._virtual_views.values(), self._dashboards.values()))

    def fetch_virtual_views(self, client: RestapisdkClient):
        connections = from_list(ThoughtSpot.fetch_connections(client))

        tables = ThoughtSpot.fetch_objects(client, Type10Enum.DATAOBJECT_TABLE)
        sheets = ThoughtSpot.fetch_objects(client, Type10Enum.DATAOBJECT_WORKSHEET)
        views = ThoughtSpot.fetch_objects(client, Type10Enum.DATAOBJECT_VIEW)

        def is_source_valid(table: SourceMetadata):
            """
            Table should source from a connection
            """
            return table.dataSourceId in connections

        valid_tables = filter(is_source_valid, tables)

        # In ThoushtSpot, Tables, Worksheets, and Views can be treated as a kind of Table.
        tables = from_list(chain(valid_tables, sheets, views))

        self.populate_virtual_views(tables)
        self.populate_lineage(connections, tables)

    def populate_virtual_views(self, tables: Dict[str, SourceMetadata]):
        for table in tables.values():
            table_id = table.header.id
            view = VirtualView(
                logical_id=VirtualViewLogicalID(
                    name=table_id, type=VirtualViewType.THOUGHT_SPOT_DATA_OBJECT
                ),
                thought_spot=ThoughtSpotDataObject(
                    columns=[
                        ThoughtSpotColumn(
                            description=column.header.description,
                            name=column.header.name,
                            type=column.dataType if column.dataType else column.type,
                        )
                        for column in table.columns
                    ],
                    name=table.header.name,
                    description=table.header.description,
                    type=mapping_data_object_type(table.type),
                    url=f"{self.base_url}/#/data/tables/{table_id}",
                ),
            )
            self._virtual_views[table_id] = view

    def populate_lineage(
        self,
        connections: Dict[str, ConnectionMetadata],
        tables: Dict[str, SourceMetadata],
    ):
        for view in self._virtual_views.values():
            table = tables[view.logical_id.name]

            if table.dataSourceTypeEnum != DataSourceTypeEnum.DEFAULT:
                source_id = table.dataSourceId
                mapping = table.logicalTableContent.tableMappingInfo
                connection = connections[source_id]
                view.thought_spot.source_datasets = [
                    str(
                        to_dataset_entity_id(
                            dataset_fullname(
                                db=mapping.databaseName,
                                schema=mapping.schemaName,
                                table=mapping.tableName,
                            ),
                            mapping_data_platform(connection.type),
                            account=connection.dataSourceContent.configuration.accountName,
                        )
                    )
                ]
            else:
                # use unique_list later to make order of sources stable
                source_virtual_views = [
                    str(
                        to_virtual_view_entity_id(
                            name=source.tableId,
                            virtualViewType=VirtualViewType.THOUGHT_SPOT_DATA_OBJECT,
                        )
                    )
                    for column in table.columns
                    for source in column.sources
                    if source.tableId in tables
                ]
                view.thought_spot.source_virtual_views = unique_list(
                    source_virtual_views
                )

    def fetch_dashboards(self, client: RestapisdkClient):
        answers = ThoughtSpot.fetch_objects(client, Type10Enum.ANSWER)
        self.populate_answers(answers)

        boards = ThoughtSpot.fetch_objects(client, Type10Enum.LIVEBOARD)
        self.populate_liveboards(boards)

    def populate_answers(self, answers: List[AnswerMetadata]):
        for answer in answers:
            answer_id = answer.header.id

            visualizations = [
                (viz, answer.header, "")
                for sheet in answer.reportContent.sheets
                for viz in sheet.sheetContent.visualizations
                if viz.vizContent.vizType == "CHART"
            ]

            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=answer_id,
                    platform=DashboardPlatform.THOUGHT_SPOT,
                ),
                dashboard_info=DashboardInfo(
                    description=answer.header.description,
                    title=answer.header.name,
                    charts=self._populate_charts(
                        visualizations, self.base_url, answer_id
                    ),
                ),
                source_info=SourceInfo(
                    main_url=f"{self.base_url}/#/saved-answer/{answer_id}",
                ),
                upstream=DashboardUpstream(
                    source_virtual_views=self._populate_source_virtual_views(
                        visualizations
                    )
                ),
            )

            self._dashboards[answer_id] = dashboard

    def populate_liveboards(self, liveboards: List[LiveBoardMetadate]):
        for board in liveboards:
            board_id = board.header.id

            resolvedObjects = board.header.resolvedObjects
            answers = {
                viz.header.id: resolvedObjects[viz.vizContent.refVizId]
                for sheet in board.reportContent.sheets
                for viz in sheet.sheetContent.visualizations
            }
            visualizations = [
                (viz, answer.header, chart_id)
                for chart_id, answer in answers.items()
                for sheet in answer.reportContent.sheets
                for viz in sheet.sheetContent.visualizations
                if viz.vizContent.vizType == "CHART"
            ]

            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=board_id,
                    platform=DashboardPlatform.THOUGHT_SPOT,
                ),
                dashboard_info=DashboardInfo(
                    description=board.header.description,
                    title=board.header.name,
                    charts=self._populate_charts(
                        visualizations, self.base_url, board_id
                    ),
                ),
                source_info=SourceInfo(
                    main_url=f"{self.base_url}/#/pinboard/{board_id}",
                ),
                upstream=DashboardUpstream(
                    source_virtual_views=self._populate_source_virtual_views(
                        visualizations
                    )
                ),
            )

            self._dashboards[board_id] = dashboard

    @staticmethod
    def _populate_charts(
        charts: List[Tuple[Visualization, Header, str]], base_url: str, board_id: str
    ) -> List[Chart]:
        return [
            Chart(
                description=header.description,
                title=header.name,
                chart_type=mapping_chart_type(viz.vizContent.chartType),
                url=f"{base_url}#/embed/viz/{board_id}/{chart_id}"
                if chart_id
                else None,
            )
            for viz, header, chart_id in charts
        ]

    @staticmethod
    def _populate_source_virtual_views(
        charts: List[Tuple[Visualization, Header, str]],
    ) -> List[str]:
        return unique_list(
            [
                str(
                    to_virtual_view_entity_id(
                        name=reference.id,
                        virtualViewType=VirtualViewType.THOUGHT_SPOT_DATA_OBJECT,
                    )
                )
                for viz, *_ in charts
                for column in viz.vizContent.columns
                for reference in column.referencedTableHeaders
            ]
        )
