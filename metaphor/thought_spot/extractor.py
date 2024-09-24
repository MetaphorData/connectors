import json
from itertools import chain
from typing import Collection, Dict, List, Optional, Tuple

from pydantic.dataclasses import dataclass
from sqllineage.core.models import Column
from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.entity_id import (
    EntityId,
    dataset_normalized_name,
    to_dataset_entity_id,
    to_virtual_view_entity_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.hierarchy import create_hierarchy
from metaphor.common.logger import get_logger
from metaphor.common.utils import unique_list
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AssetPlatform,
    AssetStructure,
    Chart,
    Dashboard,
    DashboardInfo,
    DashboardLogicalID,
    DashboardPlatform,
    DashboardType,
    EntityType,
    EntityUpstream,
    FieldMapping,
    HierarchyType,
    SourceField,
    SourceInfo,
    SystemTag,
    SystemTags,
    SystemTagSource,
    ThoughtSpotColumn,
    ThoughtSpotDashboardType,
    ThoughtSpotDataObject,
    ThoughtSpotDataObjectType,
    ThoughtSpotInfo,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from metaphor.thought_spot.config import ThoughtSpotRunConfig
from metaphor.thought_spot.models import (
    AnswerMetadataDetail,
    ConnectionDetail,
    DataSourceTypeEnum,
    Header,
    LiveBoardMetadataDetail,
    LogicalTableMetadataDetail,
    TableMappingInfo,
    Tag,
    TMLObject,
    Visualization,
)
from metaphor.thought_spot.utils import (
    ThoughtSpot,
    from_list,
    getColumnTransformation,
    mapping_chart_type,
    mapping_data_object_type,
    mapping_data_platform,
)

logger = get_logger()


@dataclass(config=ConnectorConfig)
class ColumnReference:
    entity_id: str
    field: str


class ThoughtSpotExtractor(BaseExtractor):
    """ThoughtSpot metadata extractor"""

    _description = "ThoughtSpot metadata crawler"
    _platform = Platform.THOUGHT_SPOT

    @staticmethod
    def from_config_file(config_file: str) -> "ThoughtSpotExtractor":
        return ThoughtSpotExtractor(ThoughtSpotRunConfig.from_yaml_file(config_file))

    def __init__(self, config: ThoughtSpotRunConfig):
        super().__init__(config)
        self._base_url = config.base_url
        self._config = config

        self._client = ThoughtSpot.create_client(self._config)

        self._dashboards: Dict[str, Dashboard] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._column_references: Dict[str, ColumnReference] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from ThoughtSpot")

        self.fetch_virtual_views()
        self.fetch_dashboards()
        virtual_hierarchies = self._create_virtual_hierarchies()

        return list(
            chain(
                self._virtual_views.values(),
                self._dashboards.values(),
                virtual_hierarchies,
            ),
        )

    def fetch_virtual_views(self):
        connections = from_list(ThoughtSpot.fetch_connections(self._client))

        data_objects = ThoughtSpot.fetch_tables(self._client)

        def is_source_valid(table: LogicalTableMetadataDetail):
            """
            Table should source from a connection
            """
            return table.dataSourceId in connections

        tables = filter(is_source_valid, data_objects)

        # In ThoughtSpot, Tables, Worksheets, and Views can be treated as a kind of Table.
        tables = from_list(tables)

        self.populate_logical_column_mapping(tables)

        self.populate_virtual_views(connections, tables)
        self.populate_lineage(connections, tables)
        self.populate_formula()

    def populate_logical_column_mapping(
        self, tables: Dict[str, LogicalTableMetadataDetail]
    ):
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

    def populate_virtual_views(
        self,
        connections: Dict[str, ConnectionDetail],
        tables: Dict[str, LogicalTableMetadataDetail],
    ):
        for table in tables.values():
            table_id = table.header.id
            table_type = mapping_data_object_type(table.type)

            field_mappings = []
            for column in table.columns:
                field_mapping = FieldMapping(destination=column.header.name, sources=[])

                assert field_mapping.sources is not None
                if table.dataSourceTypeEnum != DataSourceTypeEnum.DEFAULT:
                    # the table upstream is external source, i.e. BigQuery
                    table_mapping_info = table.logicalTableContent.tableMappingInfo
                    if table_mapping_info is None:
                        logger.warning(
                            f"tableMappingInfo is missing, skip for column: {column.header.name}"
                        )
                        continue

                    source_entity_id = self.find_entity_id_from_connection(
                        connections,
                        table_mapping_info,
                        table.dataSourceId,
                    )
                    field_mapping.sources.append(
                        SourceField(
                            field=(
                                column.columnMappingInfo.columnName
                                if column.columnMappingInfo
                                else None
                            ),
                            source_entity_id=source_entity_id,
                        )
                    )
                else:
                    field_mapping.sources += [
                        SourceField(
                            source_entity_id=self._column_references[
                                source.columnId
                            ].entity_id,
                            field=self._column_references[source.columnId].field,
                        )
                        for source in column.sources
                        if source.columnId in self._column_references
                    ]
                field_mappings.append(field_mapping)

            view = VirtualView(
                logical_id=VirtualViewLogicalID(
                    name=table_id, type=VirtualViewType.THOUGHT_SPOT_DATA_OBJECT
                ),
                structure=AssetStructure(
                    directories=[table_type.name],
                    name=table.header.name,
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
                    type=table_type,
                    url=f"{self._base_url}/#/data/tables/{table_id}",
                ),
                entity_upstream=EntityUpstream(
                    field_mappings=field_mappings if field_mappings else None
                ),
                system_tags=self._get_system_tags(table.header.tags),
            )
            self._virtual_views[table_id] = view

    @staticmethod
    def _create_virtual_hierarchies():
        return [
            create_hierarchy(
                name=name,
                path=[enum_value.value],
                platform=AssetPlatform.THOUGHT_SPOT,
                hierarchy_type=HierarchyType.THOUGHT_SPOT_VIRTUAL_HIERARCHY,
            )
            for name, enum_value in [
                ("Answer", ThoughtSpotDashboardType.ANSWER),
                ("Liveboard", ThoughtSpotDashboardType.LIVEBOARD),
                ("Table", ThoughtSpotDataObjectType.TABLE),
                ("View", ThoughtSpotDataObjectType.VIEW),
                ("Worksheet", ThoughtSpotDataObjectType.WORKSHEET),
            ]
        ]

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
            tml = TMLObject.model_validate_json(tml_result.edoc)

            column_expr_map = self.build_column_expr_map(tml)

            for column in self._virtual_views[tml.guid].thought_spot.columns:
                if column.name in column_expr_map:
                    column.formula = column_expr_map[column.name]

    def populate_lineage(
        self,
        connections: Dict[str, ConnectionDetail],
        tables: Dict[str, LogicalTableMetadataDetail],
    ):
        """
        Populate lineage between tables/worksheets/views
        """
        for view in self._virtual_views.values():
            table = tables[view.logical_id.name]

            if table.dataSourceTypeEnum != DataSourceTypeEnum.DEFAULT:
                # SQL_VIEW case
                if table.logicalTableContent.sqlQuery:
                    view.entity_upstream.transformation = (
                        table.logicalTableContent.sqlQuery
                    )
                    source_entities = ThoughtSpotExtractor.get_source_entities_from_sql(
                        connections,
                        table.logicalTableContent.sqlQuery,
                        table.dataSourceId,
                    )
                    view.entity_upstream.source_entities = source_entities
                    view.thought_spot.source_datasets = source_entities
                    view.entity_upstream.field_mappings = (
                        ThoughtSpotExtractor.get_field_mappings_from_sql(
                            connections,
                            table.logicalTableContent.sqlQuery,
                            table.dataSourceId,
                        )
                    )

                if table.logicalTableContent.tableMappingInfo is None:
                    logger.warning(
                        f"Skip lineage for {view.logical_id.name} because the mapping info is missing"
                    )
                    continue

                view.thought_spot.source_datasets = [
                    self.find_entity_id_from_connection(
                        connections,
                        table.logicalTableContent.tableMappingInfo,
                        table.dataSourceId,
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

    @staticmethod
    def get_source_entity_id_from_connection(
        connections: Dict[str, ConnectionDetail],
        normalized_name: str,
        source_id: str,
    ) -> str:
        connection = connections[source_id]

        try:
            accountName = json.loads(connection.configuration).get("accountName")
        except json.decoder.JSONDecodeError:
            accountName = None

        return str(
            to_dataset_entity_id(
                normalized_name,
                mapping_data_platform(connection.type),
                account=accountName,
            )
        )

    @staticmethod
    def find_entity_id_from_connection(
        connections: Dict[str, ConnectionDetail],
        mapping: TableMappingInfo,
        source_id: str,
    ) -> str:
        if mapping is None:
            logger.warning("Table mapping info is missing")

        return ThoughtSpotExtractor.get_source_entity_id_from_connection(
            connections,
            dataset_normalized_name(
                db=mapping.databaseName,
                schema=mapping.schemaName,
                table=mapping.tableName,
            ),
            source_id,
        )

    @staticmethod
    def get_source_entities_from_sql(
        connections, sql: str, source_id: str
    ) -> List[str]:
        try:
            parser = LineageRunner(sql)
        except SQLLineageException as e:
            logger.warning(f"Cannot parse SQL. Query: {sql}, Error: {e}")
            return []

        if len(parser.source_tables) < 1:
            return []

        sources = parser.source_tables

        source_ids = [
            str(
                ThoughtSpotExtractor.get_source_entity_id_from_connection(
                    connections,
                    str(source).lower(),
                    source_id,
                )
            )
            for source in sources
        ]

        return source_ids

    @staticmethod
    def get_mapping_from_sql(
        sql: Optional[str],
    ) -> Dict[str, Tuple[Column, List[Column]]]:
        if not sql:
            return {}

        try:
            # prepend insert before sql to get column level lineage
            parser = LineageRunner(f"insert into db.table {sql}")

            column_lineages = parser.get_column_lineage()
        except SQLLineageException as e:
            logger.warning(f"Cannot parse SQL. Query: {sql}, Error: {e}")
            return {}

        mapping: Dict[str, Tuple[Column, List[Column]]] = {}

        for cll_tuple in column_lineages:
            if len(cll_tuple) < 2:
                continue

            target_col = cll_tuple[-1]
            source_col = cll_tuple[0]
            mapping.setdefault(target_col.raw_name, (target_col, []))[1].append(
                source_col
            )

        return mapping

    @staticmethod
    def get_field_mappings_from_sql(
        connections, sql: str, source_id: str
    ) -> List[FieldMapping]:
        mapping = ThoughtSpotExtractor.get_mapping_from_sql(sql)

        field_mappings: List[FieldMapping] = []

        for target_col, source_cols in mapping.values():
            sources: List[SourceField] = []
            destination = target_col.raw_name

            for source_col in source_cols:
                sources.append(
                    SourceField(
                        source_entity_id=str(
                            ThoughtSpotExtractor.get_source_entity_id_from_connection(
                                connections,
                                str(source_col.parent).lower(),
                                source_id,
                            )
                        ),
                        field=source_col.raw_name,
                    )
                )

            field_mappings.append(
                FieldMapping(
                    destination=destination,
                    sources=sources,
                    transformation=getColumnTransformation(target_col),
                )
            )

        return field_mappings

    def fetch_dashboards(self):
        answers = ThoughtSpot.fetch_answers(self._client)
        self.populate_answers(answers)
        self.populate_answers_lineage(answers)

        liveboards = ThoughtSpot.fetch_liveboards(self._client)
        self.populate_liveboards(liveboards)

    def populate_answers(self, answers: List[AnswerMetadataDetail]):
        for answer in answers:
            answer_id = answer.header.id

            visualizations = [
                # Use answer.header instead as viz.header contain only dummy values
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
                structure=AssetStructure(
                    directories=[ThoughtSpotDashboardType.ANSWER.name],
                    name=answer.header.name,
                ),
                dashboard_info=DashboardInfo(
                    description=answer.header.description,
                    title=answer.header.name,
                    charts=self._populate_charts(
                        visualizations, self._base_url, answer_id
                    ),
                    thought_spot=ThoughtSpotInfo(
                        type=ThoughtSpotDashboardType.ANSWER,
                    ),
                    dashboard_type=DashboardType.THOUGHT_SPOT_ANSWER,
                ),
                source_info=SourceInfo(
                    main_url=f"{self._base_url}/#/saved-answer/{answer_id}",
                ),
                system_tags=self._get_system_tags(answer.header.tags),
            )

            self._dashboards[answer_id] = dashboard

    def populate_answers_lineage(self, answers: List[AnswerMetadataDetail]):
        ids = [answer.header.id for answer in answers]
        for tml_result in ThoughtSpot.fetch_tml(self._client, ids):
            if not tml_result.edoc:
                continue
            tml = TMLObject.model_validate_json(tml_result.edoc)

            answer_id = tml.guid
            dashboard = self._dashboards.get(answer_id)

            if not dashboard:
                continue

            source_ids = (
                [tml_table.fqn for tml_table in tml.answer.tables if tml_table.fqn]
                if tml.answer and tml.answer.tables
                else []
            )

            source_entities = [
                str(
                    to_virtual_view_entity_id(
                        source_id, VirtualViewType.THOUGHT_SPOT_DATA_OBJECT
                    )
                )
                for source_id in source_ids
            ]

            # assume answer only have one source table
            if len(source_entities) == 1 and tml.answer:
                field_mappings = self.get_field_mappings_from_answer_sql(
                    tml.guid, source_entities[0], tml.answer.table.ordered_column_ids
                )

            dashboard.entity_upstream = EntityUpstream(
                source_entities=source_entities, field_mappings=field_mappings
            )

    def get_field_mappings_from_answer_sql(
        self, answer_id, source_id, target_columns: Optional[List[str]]
    ) -> List[FieldMapping]:
        if not target_columns:
            return []

        answer_sql = ThoughtSpot.fetch_answer_sql(self._client, answer_id)
        mapping = ThoughtSpotExtractor.get_mapping_from_sql(answer_sql)

        field_mappings: List[FieldMapping] = []

        try:
            for target_col, source_cols in mapping.values():
                sources: List[SourceField] = []

                # Assume the target column name align the format ca_{index+1}
                index = int(target_col.raw_name.split("_")[1]) - 1

                destination = target_columns[index]
                for source_col in source_cols:
                    sources.append(
                        SourceField(
                            source_entity_id=source_id, field=source_col.raw_name
                        )
                    )
                field_mappings.append(
                    FieldMapping(
                        destination=destination,
                        sources=sources,
                        transformation=getColumnTransformation(target_col),
                    )
                )
        except (ValueError, IndexError) as error:
            # if the target name is not matched the assumption
            logger.warning(
                f"{error} The target column name in answer sql didn't match the assumption, sql: {answer_sql}"
            )
            return []

        return field_mappings

    def populate_liveboards(self, liveboards: List[LiveBoardMetadataDetail]):
        for board in liveboards:
            board_id = board.header.id

            resolvedObjects = board.header.resolvedObjects
            answers = {
                viz.header.id: resolvedObjects[viz.vizContent.refVizId]
                for sheet in board.reportContent.sheets
                for viz in sheet.sheetContent.visualizations
                if viz.vizContent.refVizId
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
                structure=AssetStructure(
                    directories=[ThoughtSpotDashboardType.LIVEBOARD.name],
                    name=board.header.name,
                ),
                dashboard_info=DashboardInfo(
                    description=board.header.description,
                    title=board.header.name,
                    charts=self._populate_charts(
                        visualizations, self._base_url, board_id
                    ),
                    thought_spot=ThoughtSpotInfo(
                        type=ThoughtSpotDashboardType.LIVEBOARD,
                        embed_url=f"{self._base_url}/#/embed/viz/{board_id}",
                    ),
                    dashboard_type=DashboardType.THOUGHT_SPOT_LIVEBOARD,
                ),
                source_info=SourceInfo(
                    main_url=f"{self._base_url}/#/pinboard/{board_id}",
                ),
                entity_upstream=EntityUpstream(
                    source_entities=source_entities,
                    field_mappings=self._get_field_mapping_from_visualizations(
                        visualizations
                    ),
                ),
                system_tags=self._get_system_tags(board.header.tags),
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
                chart_type=(
                    mapping_chart_type(viz.vizContent.chartType)
                    if viz.vizContent.chartType
                    else None
                ),
                url=(
                    f"{base_url}#/embed/viz/{board_id}/{chart_id}" if chart_id else None
                ),
            )
            for viz, header, chart_id in charts
        ]

    @staticmethod
    def _populate_source_virtual_views(
        charts: List[Tuple[Visualization, Header, str]],
    ) -> Optional[List[str]]:
        return (
            unique_list(
                [
                    str(
                        to_virtual_view_entity_id(
                            name=reference.id,
                            virtualViewType=VirtualViewType.THOUGHT_SPOT_DATA_OBJECT,
                        )
                    )
                    for viz, *_ in charts
                    for column in viz.vizContent.columns
                    if column.referencedTableHeaders
                    for reference in column.referencedTableHeaders
                ]
            )
            or None
        )

    def _get_field_mapping_from_visualizations(
        self,
        charts: List[Tuple[Visualization, Header, str]],
    ) -> Optional[List[FieldMapping]]:
        field_mappings = [
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
                    if column.referencedColumnHeaders
                    for reference in column.referencedColumnHeaders
                    if reference.id in self._column_references
                ],
            )
            for viz, header, *_ in charts
        ]

        return [f for f in field_mappings if f.sources] or None

    @staticmethod
    def _get_system_tags(tags: List[Tag]) -> Optional[SystemTags]:
        system_tags = [
            SystemTag(
                key=None,
                system_tag_source=SystemTagSource.THOUGHT_SPOT,
                value=tag.name,
            )
            for tag in tags
            if not (tag.isDeleted or tag.isHidden or tag.isDeprecated)
        ]

        return SystemTags(tags=system_tags)
