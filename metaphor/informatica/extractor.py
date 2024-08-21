from typing import Callable, Collection, Dict, List
from urllib.parse import urljoin

from metaphor.common.api_request import ApiError, make_request
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    to_dataset_entity_id_from_logical_id,
    to_pipeline_entity_id_from_logical_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.common.utils import unique_list
from metaphor.informatica.config import InformaticaRunConfig
from metaphor.informatica.models import (
    AuthResponse,
    ConnectionDetail,
    MappingDetailResponse,
    MappingParameter,
    ObjectDetail,
    ObjectDetailResponse,
    ObjectReferenceResponse,
    ReferenceObjectDetail,
)
from metaphor.informatica.utils import (
    get_account,
    get_platform,
    init_dataset_logical_id,
    parse_error,
)
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    AssetStructure,
    Dataset,
    EntityUpstream,
    InformaticaMapping,
    Pipeline,
    PipelineInfo,
    PipelineLogicalID,
    PipelineMapping,
    PipelineType,
    SourceInfo,
)

logger = get_logger()


V2_SESSION_HEADER = "icSessionId"
V3_SESSION_HEADER = "INFA-SESSION-ID"
AUTH_ERROR_CODE = "AUTH_01"
PARAMETER_SOURCE_TYPES = {"EXTENDED_SOURCE", "SOURCE"}
PARAMETER_TARGET_TYPES = {"TARGET"}


class InformaticaExtractor(BaseExtractor):
    """Informatica metadata extractor"""

    _description = "Fivetran metadata crawler"
    _platform = Platform.INFORMATICA

    @staticmethod
    def from_config_file(config_file: str) -> "InformaticaExtractor":
        return InformaticaExtractor(InformaticaRunConfig.from_yaml_file(config_file))

    def __init__(self, config: InformaticaRunConfig) -> None:
        super().__init__(config)
        self._base_url = config.base_url
        self._user = config.user
        self._password = config.password

        self._session_id: str = ""
        self._api_base_url: str = ""

        self._pipelines: Dict[str, Pipeline] = {}
        self._datasets: Dict[str, Dataset] = {}

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Informatica")

        connections = self.extract_connection_detail()
        mappings = self.extract_mapping()
        self.extract_table_lineage(connections, mappings)

        entities: List[ENTITY_TYPES] = []
        entities.extend(self._pipelines.values())
        entities.extend(self._datasets.values())
        return entities

    def extract_connection_detail(self) -> Dict[str, ConnectionDetail]:
        connection_map: Dict[str, ConnectionDetail] = {}
        for connection_id in self._list_connection():
            connection_map[connection_id] = self._get_connection_detail(connection_id)
        return connection_map

    def extract_mapping(self) -> Dict[str, MappingDetailResponse]:
        # v3 id -> mapping object
        v3_mapping_objects = self._get_v3_object(type="MAPPING")

        # v2 id -> connection ref object
        v3_connections_ref_type: Dict[str, ReferenceObjectDetail] = {}

        # v2 id -> mapping object
        mapping_v3_v2_id_map: Dict[str, ReferenceObjectDetail] = {}

        for mapping_v3_object in v3_mapping_objects.values():
            reference = self._get_object_reference(mapping_v3_object.id, "Uses")
            for ref in reference.references:
                if ref.documentType == "SAAS_CONNECTION":
                    v3_connections_ref_type[ref.appContextId] = ref

        for connection_ref in v3_connections_ref_type.values():
            reference = self._get_object_reference(connection_ref.id, "usedBy")
            for ref in reference.references:
                if ref.documentType == "MAPPING":
                    mapping_v3_v2_id_map[ref.appContextId] = ref

        mappings: Dict[str, MappingDetailResponse] = {}

        for mapping_ref in mapping_v3_v2_id_map.values():
            mapping_detail = self._get_mapping_detail(mapping_ref.appContextId)

            v3_id = mapping_ref.id
            v3_mapping_object = v3_mapping_objects.get(v3_id)

            pipeline = Pipeline(
                logical_id=PipelineLogicalID(
                    name=mapping_ref.id,  # we should use v3 id
                    type=PipelineType.INFORMATICA_MAPPING,
                ),
                source_info=SourceInfo(
                    main_url=urljoin(
                        self._api_base_url,
                        f"/diUI/products/integrationDesign/main/mapping/{v3_id}",
                    ),
                    created_at_source=mapping_detail.createTime,
                    created_by=mapping_detail.createdBy,
                    last_updated=mapping_detail.updateTime,
                    updated_by=mapping_detail.updatedBy,
                ),
                informatica_mapping=InformaticaMapping(
                    name=mapping_detail.name,
                    description=mapping_detail.description,
                ),
                structure=AssetStructure(
                    name=mapping_detail.name,
                    directories=(
                        v3_mapping_object.path.split("/")[:-1]
                        if v3_mapping_object
                        else []
                    ),
                ),
            )
            self._pipelines[v3_id] = pipeline

            mappings[v3_id] = mapping_detail
        return mappings

    def extract_table_lineage(
        self,
        connections: Dict[str, ConnectionDetail],
        mappings: Dict[str, MappingDetailResponse],
    ):
        for v3_id, mapping in mappings.items():
            pipeline = self._pipelines.get(v3_id)
            assert pipeline is not None
            pipeline_entity_id = str(
                to_pipeline_entity_id_from_logical_id(pipeline.logical_id)
            )

            sources = [
                p for p in mapping.parameters if p.type in PARAMETER_SOURCE_TYPES
            ]

            def trans_source(source: MappingParameter) -> List[str]:
                connection = connections.get(source.sourceConnectionId or "")
                if connection is None:
                    logger.warning(
                        f"Invalid source connection id for mapping '{mapping.name}'"
                    )
                    return []

                if source.customQuery:
                    result = extract_table_level_lineage(
                        sql=source.customQuery,
                        platform=get_platform(connection),
                        account=get_account(connection),
                        default_database=connection.database,
                    )
                    return [dataset.id for dataset in result.sources]

                if (
                    source.extendedObject is None
                    or source.extendedObject.object is None
                ):
                    return []

                logical_id = init_dataset_logical_id(
                    source.extendedObject.object.name, connection
                )

                if logical_id is None:
                    return []

                return [str(to_dataset_entity_id_from_logical_id(logical_id))]

            source_entities = unique_list(
                source_id for ids in map(trans_source, sources) for source_id in ids
            )

            targets = [
                p for p in mapping.parameters if p.type in PARAMETER_TARGET_TYPES
            ]

            for target in targets:
                connection = connections.get(target.targetConnectionId or "")

                if connection is None or target.targetObject is None:
                    logger.warning(
                        f"Invalid target connection id for mapping '{mapping.name}'"
                    )
                    continue

                logical_id = init_dataset_logical_id(target.targetObject, connection)

                if logical_id is None:
                    logger.warning(
                        f"Failed to construct target object for mapping '{mapping.name}'"
                    )
                    continue

                dataset = Dataset(
                    logical_id=logical_id,
                    entity_upstream=EntityUpstream(source_entities=source_entities),
                    pipeline_info=PipelineInfo(
                        pipeline_mapping=[
                            PipelineMapping(
                                is_virtual=False,
                                pipeline_entity_id=pipeline_entity_id,
                            )
                        ]
                    ),
                )

                self._datasets[logical_id.name] = dataset

    def with_retry(func: Callable):  # type: ignore
        def retry_once(self, *args, **kwargs):
            for _ in range(2):
                try:
                    self._ensure_session_id()
                    return func(self, *args, **kwargs)
                except ApiError as api_error:
                    response = parse_error(api_error.body)
                    if (
                        api_error.status_code == 401
                        and response
                        and response.get("code") == AUTH_ERROR_CODE
                    ):
                        logger.warning("Session is expired")
                        self._ensure_session_id(new_session=True)
                        continue
                    raise api_error
            raise RuntimeError("Invalid Informatica login credential")

        return retry_once

    @with_retry
    def _list_connection(self) -> List[str]:
        connections = make_request(
            url=urljoin(self._api_base_url, "saas/api/v2/connection/"),
            headers={V2_SESSION_HEADER: self._session_id},
            type_=List[ConnectionDetail],
            timeout=10,
        )
        return [c.id for c in connections]

    @with_retry
    def _get_connection_detail(self, connection_id: str) -> ConnectionDetail:
        return make_request(
            url=urljoin(self._api_base_url, f"saas/api/v2/connection/{connection_id}"),
            headers={V2_SESSION_HEADER: self._session_id},
            type_=ConnectionDetail,
            timeout=10,
        )

    @with_retry
    def _get_v3_object(self, type: str) -> Dict[str, ObjectDetail]:
        v3_objects: Dict[str, ObjectDetail] = {}

        page_size = 20
        while True:
            page = make_request(
                url=urljoin(self._api_base_url, "saas/public/core/v3/objects"),
                headers={V3_SESSION_HEADER: self._session_id},
                params={
                    "limit": page_size,
                    "skip": len(v3_objects),
                    "q": f"type=='{type}'",
                },
                type_=ObjectDetailResponse,
                timeout=30,
            )
            if len(page.objects) == 0:
                break
            for object in page.objects:
                v3_objects[object.id] = object

        return v3_objects

    @with_retry
    def _get_mapping_detail(self, id: str) -> MappingDetailResponse:
        return make_request(
            url=urljoin(self._api_base_url, f"saas/api/v2/mapping/{id}"),
            headers={V2_SESSION_HEADER: self._session_id},
            type_=MappingDetailResponse,
            timeout=10,
        )

    @with_retry
    def _get_object_reference(
        self, v3_id: str, ref_type: str
    ) -> ObjectReferenceResponse:
        return make_request(
            url=urljoin(
                self._api_base_url, f"saas/public/core/v3/objects/{v3_id}/references"
            ),
            headers={V3_SESSION_HEADER: self._session_id},
            params={
                "refType": ref_type,
            },
            type_=ObjectReferenceResponse,
            timeout=10,
        )

    def _ensure_session_id(self, new_session=False):
        if new_session or not self._session_id:
            self._get_session_id()

    def _get_session_id(self):
        auth_response = make_request(
            url=urljoin(self._base_url, "saas/public/core/v3/login"),
            headers={},
            type_=AuthResponse,
            timeout=10,
            method="post",
            json={
                "username": self._user,
                "password": self._password,
            },
        )
        assert len(auth_response.products) >= 1
        self._api_base_url = auth_response.products[0].baseApiUrl
        self._session_id = auth_response.userInfo.sessionId
