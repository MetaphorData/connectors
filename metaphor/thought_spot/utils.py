import json
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from metaphor.models.metadata_change_event import (
    ChartType,
    DataPlatform,
    ThoughtSpotDataObjectType,
)
from pydantic import parse_obj_as
from restapisdk.configuration import Environment
from restapisdk.controllers.metadata_controller import MetadataController
from restapisdk.controllers.session_controller import SessionController
from restapisdk.models.session_login_response import SessionLoginResponse
from restapisdk.models.tspublic_rest_v_2_metadata_header_search_request import (
    TspublicRestV2MetadataHeaderSearchRequest,
)
from restapisdk.models.tspublic_rest_v_2_session_gettoken_request import (
    TspublicRestV2SessionGettokenRequest,
)
from restapisdk.models.type_9_enum import Type9Enum
from restapisdk.models.type_10_enum import Type10Enum
from restapisdk.restapisdk_client import RestapisdkClient

from metaphor.common.logger import get_logger
from metaphor.common.utils import chunks
from metaphor.thought_spot.config import ThoughtspotRunConfig
from metaphor.thought_spot.models import (
    AnswerMetadata,
    ConnectionHeader,
    ConnectionMetadata,
    ConnectionType,
    Header,
    LiveBoardMetadate,
    Metadata,
    SourceMetadata,
    SourceType,
)

logger = get_logger(__name__)


def mapping_data_object_type(type_: SourceType) -> ThoughtSpotDataObjectType:
    mapping = {
        SourceType.WORKSHEET: ThoughtSpotDataObjectType.WORKSHEET,
        SourceType.ONE_TO_ONE_LOGICAL: ThoughtSpotDataObjectType.TABLE,
        SourceType.AGGR_WORKSHEET: ThoughtSpotDataObjectType.VIEW,
    }
    return mapping.get(type_)


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
    mapping: ClassVar[Dict[Type10Enum, Tuple[Type9Enum, Type[Metadata]]]] = {
        Type10Enum.DATAOBJECT_TABLE: (Type9Enum.DATAOBJECT, SourceMetadata),
        Type10Enum.DATAOBJECT_WORKSHEET: (Type9Enum.DATAOBJECT, SourceMetadata),
        Type10Enum.DATAOBJECT_VIEW: (Type9Enum.DATAOBJECT, SourceMetadata),
        Type10Enum.ANSWER: (Type9Enum.ANSWER, AnswerMetadata),
        Type10Enum.LIVEBOARD: (Type9Enum.LIVEBOARD, LiveBoardMetadate),
    }

    @staticmethod
    def create_client(config: ThoughtspotRunConfig) -> RestapisdkClient:
        def _client(token: str):
            return RestapisdkClient(
                content_type="application/json",
                accept_language="application/json",
                environment=Environment.PRODUCTION,
                access_token=token,
                base_url=config.base_url,
            )

        client = _client("")
        session_controller: SessionController = client.session

        result: SessionLoginResponse = session_controller.get_token(
            TspublicRestV2SessionGettokenRequest(
                user_name=config.user,
                password=config.password,
                secret_key=config.secret_key,
                token_expiry_duration=900,
            )
        )

        logger.info("Login successfully")

        return _client(result.token)

    @staticmethod
    def fetch_connections(client: RestapisdkClient) -> List[Metadata]:
        supported_platform = set(c.value for c in ConnectionType)

        headers: List[ConnectionHeader] = ThoughtSpot._fetch_headers(
            client.metadata, Type10Enum.CONNECTION, ConnectionHeader
        )
        ids = [h.id for h in headers if h.type in supported_platform]

        logger.info(f"CONNECTION ids: {ids}")

        obj = ThoughtSpot._fetch_object_detail(
            client.metadata, ids, Type9Enum.CONNECTION
        )
        return parse_obj_as(List[ConnectionMetadata], obj)

    @classmethod
    def fetch_objects(
        cls, client: RestapisdkClient, mtype: Type10Enum
    ) -> List[Metadata]:
        if mtype not in cls.mapping:
            return []
        detail_type, target_type = cls.mapping[mtype]

        ids = [h.id for h in ThoughtSpot._fetch_headers(client.metadata, mtype)]
        logger.info(f"{mtype} ids: {ids}")

        obj = ThoughtSpot._fetch_object_detail(client.metadata, ids, detail_type)

        # Because mypy can't handle dynamic type variables properly, we skip type-checking here.
        return parse_obj_as(List[target_type], obj)  # type: ignore

    @staticmethod
    def _fetch_headers(
        metadata_controller: MetadataController,
        mtype: Type10Enum,
        header_type: Type[Header] = Header,
    ) -> List[Header]:
        body = TspublicRestV2MetadataHeaderSearchRequest(mtype=mtype)
        response_json = metadata_controller.search_object_header(body)
        response = json.loads(response_json)
        if "headers" not in response:
            return []

        # Because mypy can't handle dynamic type variables properly, we skip type-checking here.
        return parse_obj_as(List[header_type], response["headers"])  # type: ignore

    @staticmethod
    def _fetch_object_detail(
        metadata_controller: MetadataController, ids: List[str], object_type: Type9Enum
    ) -> List[Any]:
        res: List[Any] = []
        for chunk_ids in chunks(ids, 10):
            response_json = metadata_controller.get_object_detail(
                object_type, chunk_ids
            )
            response = json.loads(response_json)
            if "storables" in response:
                res.extend(response["storables"])
        return res
