import json
from typing import Collection, List, Optional
from urllib.parse import urljoin

import requests

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    API,
    APILogicalID,
    APIPlatform,
    AssetPlatform,
    AssetStructure,
    Hierarchy,
    HierarchyInfo,
    HierarchyLogicalID,
    HierarchyType,
    OpenAPI,
    OpenAPIMethod,
    OpenAPISpecification,
    OperationType,
)
from metaphor.openapi.config import OpenAPIRunConfig

logger = get_logger()


class OpenAPIExtractor(BaseExtractor):
    """OpenAPI metadata extractor"""

    _description = "OpenAPI metadata crawler"
    _platform = Platform.OPEN_API

    @staticmethod
    def from_config_file(config_file: str) -> "OpenAPIExtractor":
        return OpenAPIExtractor(OpenAPIRunConfig.from_yaml_file(config_file))

    def __init__(self, config: OpenAPIRunConfig):
        super().__init__(config)

        self._base_url = config.base_url
        self._api_name = config.api_name or config.base_url
        self._openapi_json_path = config.openapi_json_path
        self._auth = config.auth
        self._requests_session = requests.sessions.Session()

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(
            f"Fetching metadata from {self._base_url + self._openapi_json_path}"
        )

        self._initial_session()
        openapi_json = self._get_openapi_json()
        endpoints = self._extract_paths(openapi_json)
        hierarchies = self._build_hierarchies(openapi_json)

        return hierarchies + endpoints

    def _initial_session(self):
        if not self._auth:
            return

        if self._auth.basic_auth:
            basic_auth = self._auth.basic_auth
            self._requests_session.auth = (basic_auth.user, basic_auth.password)

    def _get_openapi_json(self) -> dict:
        url = self._base_url + self._openapi_json_path
        return self._requests_session.get(url).json()

    def _extract_paths(self, openapi: dict) -> List[API]:
        endpoints: List[API] = []
        servers = openapi.get("servers")

        for path, path_item in openapi["paths"].items():
            path_servers = path_item.get("servers")
            base_path = (
                path_servers[0]["url"]
                if path_servers
                else servers[0]["url"] if servers else ""
            )

            if not base_path.startswith("http"):
                endpoint_url = urljoin(self._base_url, base_path + path)
            else:
                endpoint_url = urljoin(base_path, path)

            endpoint = API(
                logical_id=APILogicalID(
                    name=endpoint_url, platform=APIPlatform.OPEN_API
                ),
                open_api=OpenAPI(path=path, methods=self._extract_methods(path_item)),
                structure=AssetStructure(directories=[self._api_name]),
            )
            endpoints.append(endpoint)
        return endpoints

    def _extract_methods(self, path_item: dict) -> List[OpenAPIMethod]:
        def to_operation_type(method: str) -> Optional[OperationType]:
            try:
                operation_type = OperationType(method.upper())
                return operation_type
            except ValueError:
                return None

        methods: List[OpenAPIMethod] = []
        for method, item in path_item.items():
            operation_type = to_operation_type(method)

            if not operation_type:
                continue

            methods.append(
                OpenAPIMethod(
                    summary=item.get("summary"),
                    description=item.get("description"),
                    type=operation_type,
                )
            )
        return methods

    def _build_hierarchies(self, openapi: dict) -> List[Hierarchy]:
        title = openapi["info"]["title"]
        hierarchy = Hierarchy(
            logical_id=HierarchyLogicalID(
                path=[AssetPlatform.OPEN_API.value] + [self._api_name],
            ),
            hierarchy_info=HierarchyInfo(
                name=title,
                open_api=OpenAPISpecification(definition=json.dumps(openapi)),
                type=HierarchyType.OPEN_API,
            ),
        )

        return [hierarchy]
