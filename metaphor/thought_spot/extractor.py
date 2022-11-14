from dataclasses import dataclass
from itertools import chain
from typing import Collection, Dict, List, Tuple

from pydantic import parse_raw_as
from restapisdk.models.search_object_header_type_enum import SearchObjectHeaderTypeEnum

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    EntityId,
    dataset_normalized_name,
    to_dataset_entity_id,
    to_virtual_view_entity_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.utils import unique_list
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    Chart,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardUpstream,
    EntityType,
    EntityUpstream,
    FieldMapping,
    SourceField,
    SourceInfo,
    ThoughtSpotColumn,
    ThoughtSpotDashboardType,
    ThoughtSpotDataObject,
    ThoughtSpotInfo,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from metaphor.thought_spot.config import ThoughtSpotRunConfig
from metaphor.thought_spot.models import (
    AnswerMetadata,
    ConnectionMetadata,
    DataSourceTypeEnum,
    Header,
    LiveBoardMetadata,
    SourceMetadata,
    Tag,
    TMLObject,
    Visualization,
)
from metaphor.thought_spot.utils import (
    ThoughtSpot,
    from_list,
    mapping_chart_type,
    mapping_data_object_type,
    mapping_data_platform,
)

logger = get_logger()


@dataclass
class ColumnReference:
    entity_id: str
    field: str


class ThoughtSpotExtractor(BaseExtractor):
    """ThoughtSpot metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "ThoughtSpotExtractor":
        return ThoughtSpotExtractor(ThoughtSpotRunConfig.from_yaml_file(config_file))

    def __init__(self, config: ThoughtSpotRunConfig):
        super().__init__(config, "ThoughtSpot metadata crawler", Platform.THOUGHT_SPOT)
        self._base_url = config.base_url

        self._client = ThoughtSpot.create_client(config)
        self._dashboards: Dict[str, Dashboard] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._column_references: Dict[str, ColumnReference] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:

        self.fetch_virtual_views()
        self.fetch_dashboards()

        return list(chain(self._virtual_views.values(), self._dashboards.values()))

    def fetch_virtual_views(self):
        connections = from_list(ThoughtSpot.fetch_connections(self._client))

        self.populate_physical_column_mapping(connections)

        tables = ThoughtSpot.fetch_objects(
            self._client, SearchObjectHeaderTypeEnum.DATAOBJECT_TABLE
        )
        sheets = ThoughtSpot.fetch_objects(
            self._client, SearchObjectHeaderTypeEnum.DATAOBJECT_WORKSHEET
        )
        views = ThoughtSpot.fetch_objects(
            self._client, SearchObjectHeaderTypeEnum.DATAOBJECT_VIEW
        )

        def is_source_valid(table: SourceMetadata):
            """
            Table should source from a connection
            """
            return table.dataSourceId in connections

        valid_tables = filter(is_source_valid, tables)

        # In ThoughtSpot, Tables, Worksheets, and Views can be treated as a kind of Table.
        tables = from_list(chain(valid_tables, sheets, views))

        self.populate_logical_column_mapping(tables)

        self.populate_virtual_views(tables)
        self.populate_lineage(connections, tables)
        self.populate_formula()

    def populate_physical_column_mapping(
        self, connections: Dict[str, ConnectionMetadata]
    ):
        for connection in connections.values():
            for logical_table in connection.logicalTableList:
                mapping = logical_table.logicalTableContent.tableMappingInfo
                source_entity_id = str(
                    to_dataset_entity_id(
                        dataset_normalized_name(
                            db=mapping.databaseName,
                            schema=mapping.schemaName,
                            table=mapping.tableName,
                        ),
                        mapping_data_platform(connection.type),
                        account=connection.dataSourceContent.configuration.accountName,
                    )
                )
                for column in logical_table.columns:
                    physical_column_id = column.physicalColumnGUID
                    physical_column_name = column.physicalColumnName
                    if not physical_column_id or not physical_column_name:
                        logger.warning(f"Cannot map column: ${column.header.name}")
                        continue
                    self._column_references[physical_column_id] = ColumnReference(
                        entity_id=source_entity_id,
                        field=physical_column_name,
                    )

    def populate_logical_column_mapping(self, tables: Dict[str, SourceMetadata]):
        for table in tables.values():
            table_id = table.header.id
            view_id = VirtualViewLogicalID(
                name=table_id, type=VirtualViewType.THOUGHT_SPOT_DATA_OBJECT
            )
            for column in table.columns:
                self._column_references[column.header.id] = ColumnReference(
                    entity_id=str(EntityId(EntityType.VIRTUAL_VIEW, view_id)),
                    field=column.header.name,
                )

    def populate_virtual_views(self, tables: Dict[str, SourceMetadata]):
        for table in tables.values():
            logger.debug(f"table: {table}")

            table_id = table.header.id

            fieldMappings = []
            for column in table.columns:
                fieldMapping = FieldMapping(destination=column.header.name, sources=[])
                column_id = column.header.id

                if table.dataSourceTypeEnum != DataSourceTypeEnum.DEFAULT:
                    # the table upstream is external source, i.e. BigQuery
                    if column.physicalColumnGUID is None:
                        logger.warning(
                            f"Missing physical column GUID, column_id: {column_id}"
                        )
                        continue
                    column_reference = self._column_references.get(
                        column.physicalColumnGUID
                    )
                    if column_reference is None:
                        logger.warning(
                            f"Missing physical column reference: physical column GUID: {column.physicalColumnGUID}"
                        )
                        continue
                    fieldMapping.sources.append(
                        SourceField(
                            field=column_reference.field,
                            source_entity_id=column_reference.entity_id,
                        )
                    )
                else:
                    fieldMapping.sources += [
                        SourceField(
                            source_entity_id=self._column_references[
                                source.columnId
                            ].entity_id,
                            field=self._column_references[source.columnId].field,
                        )
                        for source in column.sources
                        if source.columnId in self._column_references
                    ]
                fieldMappings.append(fieldMapping)

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
                            optional_type=column.optionalType,
                        )
                        for column in table.columns
                    ],
                    name=table.header.name,
                    description=table.header.description,
                    type=mapping_data_object_type(table.type),
                    url=f"{self._base_url}/#/data/tables/{table_id}",
                    tags=self._tag_names(table.header.tags),
                ),
                entity_upstream=EntityUpstream(
                    field_mappings=fieldMappings if fieldMappings else None
                ),
            )
            self._virtual_views[table_id] = view

    @staticmethod
    def build_column_expr_map(tml: TMLObject):
        def build_formula_map(tml_table):
            formula_map = {}
            for f in tml_table.formulas:
                if f.id:
                    formula_map[f.id] = f.expr
                formula_map[f.name] = f.expr
            return formula_map

        if tml.worksheet:
            formula_map = build_formula_map(tml.worksheet)
            columns = tml.worksheet.worksheet_columns
        elif tml.view:
            formula_map = build_formula_map(tml.view)
            columns = tml.view.view_columns
        else:
            return

        expr_map = {}
        for column in columns:
            lookup_id = column.formula_id if column.formula_id else column.name
            if lookup_id in formula_map:
                expr_map[column.name] = formula_map[lookup_id]
        return expr_map

    def populate_formula(self):
        ids = []
        for guid, virtual_view in self._virtual_views.items():
            if "FORMULA" in [
                c.optional_type for c in virtual_view.thought_spot.columns
            ]:
                ids.append(guid)
        for tml_result in ThoughtSpot.fetch_tml(self._client, ids):
            if not tml_result.edoc:
                continue
            tml = parse_raw_as(TMLObject, tml_result.edoc)

            column_expr_map = self.build_column_expr_map(tml)

            for column in self._virtual_views[tml.guid].thought_spot.columns:
                if column.name in column_expr_map:
                    column.formula = column_expr_map[column.name]

    def populate_lineage(
        self,
        connections: Dict[str, ConnectionMetadata],
        tables: Dict[str, SourceMetadata],
    ):
        """
        Populate lineage between tables/worksheets/views
        """
        for view in self._virtual_views.values():
            table = tables[view.logical_id.name]

            if table.dataSourceTypeEnum != DataSourceTypeEnum.DEFAULT:
                source_id = table.dataSourceId
                mapping = table.logicalTableContent.tableMappingInfo
                connection = connections[source_id]
                view.thought_spot.source_datasets = [
                    str(
                        to_dataset_entity_id(
                            dataset_normalized_name(
                                db=mapping.databaseName,
                                schema=mapping.schemaName,
                                table=mapping.tableName,
                            ),
                            mapping_data_platform(connection.type),
                            account=connection.dataSourceContent.configuration.accountName,
                        )
                    )
                ]
                view.entity_upstream.source_entities = view.thought_spot.source_datasets
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
                view.entity_upstream.source_entities = (
                    view.thought_spot.source_virtual_views
                )

    def fetch_dashboards(self):
        answers = ThoughtSpot.fetch_objects(
            self._client, SearchObjectHeaderTypeEnum.ANSWER
        )
        self.populate_answers(answers)

        boards = ThoughtSpot.fetch_objects(
            self._client, SearchObjectHeaderTypeEnum.LIVEBOARD
        )
        self.populate_liveboards(boards)

    def populate_answers(self, answers: List[AnswerMetadata]):
        for answer in answers:
            logger.debug(f"answer: {answer}")
            answer_id = answer.header.id

            visualizations = [
                # Use answer.header instead as viz.header contain only dummy values
                (viz, answer.header, "")
                for sheet in answer.reportContent.sheets
                for viz in sheet.sheetContent.visualizations
                if viz.vizContent.vizType == "CHART"
            ]

            source_entities = self._populate_source_virtual_views(visualizations)

            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=answer_id,
                    platform=DashboardPlatform.THOUGHT_SPOT,
                ),
                dashboard_info=DashboardInfo(
                    description=answer.header.description,
                    title=answer.header.name,
                    charts=self._populate_charts(
                        visualizations, self._base_url, answer_id
                    ),
                    thought_spot=ThoughtSpotInfo(
                        type=ThoughtSpotDashboardType.ANSWER,
                        tags=self._tag_names(answer.header.tags),
                    ),
                ),
                source_info=SourceInfo(
                    main_url=f"{self._base_url}/#/saved-answer/{answer_id}",
                ),
                upstream=DashboardUpstream(source_virtual_views=source_entities),
                entity_upstream=EntityUpstream(
                    source_entities=source_entities,
                    field_mappings=self._get_field_mapping_from_visualizations(
                        visualizations
                    ),
                ),
            )

            self._dashboards[answer_id] = dashboard

    def populate_liveboards(self, liveboards: List[LiveBoardMetadata]):
        for board in liveboards:
            logger.debug(f"board: {board}")
            board_id = board.header.id

            resolvedObjects = board.header.resolvedObjects
            answers = {
                viz.header.id: resolvedObjects[viz.vizContent.refVizId]
                for sheet in board.reportContent.sheets
                for viz in sheet.sheetContent.visualizations
            }
            visualizations = [
                # Use answer.header instead as viz.header contain only dummy values
                (viz, answer.header, chart_id)
                for chart_id, answer in answers.items()
                for sheet in answer.reportContent.sheets
                for viz in sheet.sheetContent.visualizations
                if viz.vizContent.vizType == "CHART"
            ]

            source_entities = self._populate_source_virtual_views(visualizations)

            dashboard = Dashboard(
                logical_id=DashboardLogicalID(
                    dashboard_id=board_id,
                    platform=DashboardPlatform.THOUGHT_SPOT,
                ),
                dashboard_info=DashboardInfo(
                    description=board.header.description,
                    title=board.header.name,
                    charts=self._populate_charts(
                        visualizations, self._base_url, board_id
                    ),
                    thought_spot=ThoughtSpotInfo(
                        type=ThoughtSpotDashboardType.LIVEBOARD,
                        tags=self._tag_names(board.header.tags),
                        embed_url=f"{self._base_url}/#/embed/viz/{board_id}",
                    ),
                ),
                source_info=SourceInfo(
                    main_url=f"{self._base_url}/#/pinboard/{board_id}",
                ),
                upstream=DashboardUpstream(source_virtual_views=source_entities),
                entity_upstream=EntityUpstream(
                    source_entities=source_entities,
                    field_mappings=self._get_field_mapping_from_visualizations(
                        visualizations
                    ),
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

    def _get_field_mapping_from_visualizations(
        self,
        charts: List[Tuple[Visualization, Header, str]],
    ) -> List[FieldMapping]:
        return [
            FieldMapping(
                destination=header.name,
                sources=[
                    SourceField(
                        source_entity_id=self._column_references[
                            reference.id
                        ].entity_id,
                        field=self._column_references[reference.id].field,
                    )
                    for column in viz.vizContent.columns
                    for reference in column.referencedColumnHeaders
                    if reference is not None and reference.id in self._column_references
                ],
            )
            for viz, header, *_ in charts
        ]

    @staticmethod
    def _tag_names(tags: List[Tag]) -> List[str]:
        return [
            tag.name
            for tag in tags
            if not (tag.isDeleted or tag.isHidden or tag.isDeprecated)
        ]
