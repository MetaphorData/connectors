from typing import Callable, Dict, Iterable, List, Optional, TypeVar

from pydantic import TypeAdapter
from sqllineage.core.models import Column
from thoughtspot_rest_api_v1 import TSRestApiV2

from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.common.utils import chunks
from metaphor.models.metadata_change_event import (
    ChartType,
    DashboardPlatform,
    DataPlatform,
    Hierarchy,
    HierarchyInfo,
    HierarchyLogicalID,
    HierarchyType,
    ThoughtSpotDataObjectType,
)
from metaphor.thought_spot.config import ThoughtSpotRunConfig
from metaphor.thought_spot.models import (
    AnswerMetadata,
    AnswerMetadataDetail,
    Connection,
    ConnectionDetail,
    ConnectionType,
    LiveBoardMetadata,
    LiveBoardMetadataDetail,
    LogicalTableMetadata,
    LogicalTableMetadataDetail,
    SourceType,
    TMLResult,
)

logger = get_logger()


def mapping_data_object_type(type_: str) -> ThoughtSpotDataObjectType:
    mapping = {
        SourceType.WORKSHEET.value: ThoughtSpotDataObjectType.WORKSHEET,
        SourceType.ONE_TO_ONE_LOGICAL.value: ThoughtSpotDataObjectType.TABLE,
        SourceType.AGGR_WORKSHEET.value: ThoughtSpotDataObjectType.VIEW,
        SourceType.SQL_VIEW.value: ThoughtSpotDataObjectType.VIEW,
    }
    return mapping.get(type_)


# https://docs.thoughtspot.com/software/latest/chart-types
def mapping_chart_type(type_: str) -> Optional[ChartType]:
    mapping = {
        "COLUMN": ChartType.COLUMN,
        "STACKED_COLUMN": ChartType.COLUMN,
        "LINE": ChartType.LINE,
        "KPI": ChartType.OTHER,
        "PIVOT_TABLE": ChartType.TABLE,
        "PIE": ChartType.PIE,
        "BAR": ChartType.BAR,
        "STACKED_BAR": ChartType.BAR,
        "LINE_COLUMN": ChartType.OTHER,
        "AREA": ChartType.AREA,
        "STACKED_AREA": ChartType.AREA,
        "LINE_STACKED_COLUMN": ChartType.OTHER,
        "SCATTER": ChartType.SCATTER,
        "BUBBLE": ChartType.OTHER,
        "WATERFALL": ChartType.WATERFALL,
        "HEATMAP": ChartType.OTHER,
        "TREEMAP": ChartType.OTHER,
        "FUNNEL": ChartType.FUNNEL,
        "GEO_BUBBLE": ChartType.OTHER,
        "GEO_HEATMAP": ChartType.OTHER,
        "GEO_AREA": ChartType.OTHER,
        "SANKEY": ChartType.OTHER,
        "SPIDER_WEB": ChartType.OTHER,
        "CANDLESTICK": ChartType.OTHER,
        "PARETO": ChartType.OTHER,
    }
    return mapping.get(type_)


def mapping_data_platform(type_: ConnectionType) -> DataPlatform:
    mapping = {
        ConnectionType.SNOWFLAKE: DataPlatform.SNOWFLAKE,
        ConnectionType.BIGQUERY: DataPlatform.BIGQUERY,
        ConnectionType.REDSHIFT: DataPlatform.REDSHIFT,
    }
    return mapping.get(type_)


T = TypeVar("T")


def from_list(list_: Iterable[T], key: Callable[[T], str] = repr) -> Dict[str, T]:
    dict_: Dict[str, T] = {}
    for item in list_:
        dict_.setdefault(key(item), item)
    return dict_


class ThoughtSpot:
    @staticmethod
    def create_client(config: ThoughtSpotRunConfig) -> TSRestApiV2:
        client: TSRestApiV2 = TSRestApiV2(server_url=config.base_url)

        auth_token_response = client.auth_token_full(
            username=config.user,
            password=config.password,
            secret_key=config.secret_key,
            validity_time_in_sec=3600,
        )

        client.bearer_token = auth_token_response["token"]

        return client

    @staticmethod
    def fetch_connections(client: TSRestApiV2) -> List[ConnectionDetail]:
        supported_platform = set(ConnectionType)

        connection_details: List[ConnectionDetail] = []

        batch_count = 0
        batch_size = 100

        while True:
            search_response = client.connection_search(
                {
                    "include_details": True,
                    "record_size": batch_size,
                    "record_offset": batch_count * batch_size,
                }
            )
            json_dump_to_debug_file(
                search_response, f"connection_search__{batch_count}.json"
            )

            batch_count += 1

            connections = TypeAdapter(List[Connection]).validate_python(search_response)

            for connection in connections:
                if connection.details.type in supported_platform:
                    connection_details.append(connection.details)

            if len(connections) < batch_size:
                break

        logger.info(f"Extract #{len(connection_details)} connections")

        return connection_details

    @classmethod
    def fetch_tables(cls, client: TSRestApiV2) -> List[LogicalTableMetadataDetail]:
        table_details: List[LogicalTableMetadataDetail] = []

        batch_count = 0
        batch_size = 100

        while len(table_details) == batch_count * batch_size:
            response = client.metadata_search(
                {
                    "metadata": [{"type": "LOGICAL_TABLE"}],
                    "include_details": True,
                    "record_size": batch_size,
                    "record_offset": batch_count * batch_size,
                }
            )
            json_dump_to_debug_file(
                response, f"metadata_search__logical_table_{batch_count}.json"
            )

            batch_count += 1

            for table in TypeAdapter(List[LogicalTableMetadata]).validate_python(
                response
            ):
                table_details.append(table.metadata_detail)

        logger.info(f"Extract #{len(table_details)} tables")

        return table_details

    @classmethod
    def fetch_answers(cls, client: TSRestApiV2) -> List[AnswerMetadataDetail]:
        answer_details: List[AnswerMetadataDetail] = []

        batch_count = 0
        batch_size = 100

        while len(answer_details) == batch_count * batch_size:
            response = client.metadata_search(
                {
                    "metadata": [{"type": "ANSWER"}],
                    "include_details": True,
                    "record_size": batch_size,
                    "record_offset": batch_count * batch_size,
                }
            )
            json_dump_to_debug_file(
                response, f"metadata_search__answer_{batch_count}.json"
            )

            batch_count += 1

            for answer in TypeAdapter(List[AnswerMetadata]).validate_python(response):
                answer_details.append(answer.metadata_detail)

        logger.info(f"Extract #{len(answer_details)} liveboards")

        return answer_details

    @classmethod
    def fetch_liveboards(cls, client: TSRestApiV2) -> List[LiveBoardMetadataDetail]:
        liveboard_details: List[LiveBoardMetadataDetail] = []

        batch_count = 0
        batch_size = 100

        while len(liveboard_details) == batch_count * batch_size:
            response = client.metadata_search(
                {
                    "metadata": [{"type": "LIVEBOARD"}],
                    "include_details": True,
                    "record_size": batch_size,
                    "record_offset": batch_count * batch_size,
                }
            )
            json_dump_to_debug_file(
                response, f"metadata_search__liveboard_{batch_count}.json"
            )

            batch_count += 1

            for liveboard in TypeAdapter(List[LiveBoardMetadata]).validate_python(
                response
            ):
                liveboard_details.append(liveboard.metadata_detail)

        logger.info(f"Extract #{len(liveboard_details)} liveboards")

        return liveboard_details

    @classmethod
    def fetch_tml(cls, client: TSRestApiV2, ids: List[str]) -> List[TMLResult]:
        logger.info(f"Fetching tml for ids: {ids}")

        if not ids:
            return []

        result: List[TMLResult] = []

        for chunk_ids in chunks(ids, 50):
            response = client.metadata_tml_export(chunk_ids, export_fqn=True)
            json_dump_to_debug_file(response, f"tml_{chunk_ids[0]}.json")

            result.extend(TypeAdapter(List[TMLResult]).validate_python(response))

        return result

    @classmethod
    def fetch_answer_sql(cls, client: TSRestApiV2, answer_id: str) -> Optional[str]:
        logger.info(f"Fetching answer sql for id: {answer_id}")

        response = client.metadata_answer_sql(answer_id)
        json_dump_to_debug_file(response, f"answer_sql__{answer_id}.json")

        if "sql_queries" in response:
            sql_queries = response["sql_queries"]
            if isinstance(sql_queries, list) and len(sql_queries) == 1:
                sql_query: Dict[str, str] = sql_queries[0]
                if "sql_query" in sql_query:
                    return sql_query.get("sql_query")

        return None


def getColumnTransformation(target_column: Column) -> Optional[str]:
    if not hasattr(target_column, "expression"):
        return None
    if target_column.expression is None or target_column.expression.token is None:
        return None
    return str(target_column.expression.token)


def create_virtual_hierarchy(name: str, path: List[str]) -> Hierarchy:
    return Hierarchy(
        logical_id=HierarchyLogicalID(
            path=[DashboardPlatform.THOUGHT_SPOT.name] + path
        ),
        hierarchy_info=HierarchyInfo(
            name=name, type=HierarchyType.THOUGHT_SPOT_VIRTUAL_HIERARCHY
        ),
    )
