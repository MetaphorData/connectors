from typing import Callable, Dict, Iterable, List, Optional, TypeVar

from pydantic import parse_obj_as
from thoughtspot_rest_api_v1 import TSRestApiV2

from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.models.metadata_change_event import (
    ChartType,
    DataPlatform,
    ThoughtSpotDataObjectType,
)
from metaphor.thought_spot.config import ThoughtSpotRunConfig
from metaphor.thought_spot.models import (
    AnswerMetadata,
    AnswerMetadataDetail,
    ConnectionMetadata,
    ConnectionMetadataDetail,
    ConnectionType,
    LiveBoardMetadata,
    LiveBoardMetadataDetail,
    LogicalTableMetadata,
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
    def fetch_connections(client: TSRestApiV2) -> List[ConnectionMetadataDetail]:
        supported_platform = set(ConnectionType)

        search_response = client.metadata_search(
            {
                "metadata": [{"type": "CONNECTION"}],
                "include_details": True,
                "record_size": 100,
            }
        )
        json_dump_to_debug_file(search_response, "metadata_search__connection.json")

        connections = parse_obj_as(List[ConnectionMetadata], search_response)
        connection_details = [
            connection.metadata_detail
            for connection in connections
            if connection.metadata_detail.type in supported_platform
        ]

        logger.info(f"CONNECTION ids: {[c.header.id for c in connection_details]}")

        return connection_details

    @classmethod
    def fetch_tables(cls, client: TSRestApiV2) -> List[LogicalTableMetadata]:
        response = client.metadata_search(
            {
                "metadata": [{"type": "LOGICAL_TABLE"}],
                "include_details": True,
                "include_dependent_objects": True,
                "record_size": 100,
            }
        )
        json_dump_to_debug_file(response, "metadata_search__logical_table.json")

        tables = parse_obj_as(List[LogicalTableMetadata], response)

        return tables

    @classmethod
    def fetch_answers(cls, client: TSRestApiV2) -> List[AnswerMetadataDetail]:
        response = client.metadata_search(
            {
                "metadata": [{"type": "ANSWER"}],
                "include_details": True,
                "record_size": 100,
            }
        )
        json_dump_to_debug_file(response, "metadata_search__answer.json")

        answer_details = [
            answer.metadata_detail
            for answer in parse_obj_as(List[AnswerMetadata], response)
        ]

        logger.info(f"ANSWER ids: {[c.header.id for c in answer_details]}")

        return answer_details

    @classmethod
    def fetch_liveboards(cls, client: TSRestApiV2) -> List[LiveBoardMetadataDetail]:
        response = client.metadata_search(
            {
                "metadata": [{"type": "LIVEBOARD"}],
                "include_details": True,
                "record_size": 100,
            }
        )
        json_dump_to_debug_file(response, "metadata_search__liveboard.json")

        liveboard_details = [
            liveboard.metadata_detail
            for liveboard in parse_obj_as(List[LiveBoardMetadata], response)
        ]

        logger.info(f"LIVEBOARD Ids: {[c.header.id for c in liveboard_details]}")

        return liveboard_details

    @classmethod
    def fetch_tml(cls, client: TSRestApiV2, ids: List[str]) -> List[TMLResult]:
        logger.info(f"Fetching tml for ids: {ids}")

        response = client.metadata_tml_export(ids)
        json_dump_to_debug_file(response, "tml.json")

        return parse_obj_as(List[TMLResult], response)
