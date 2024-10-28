import json
from collections import OrderedDict
from typing import Collection, List, Optional
from urllib.parse import urljoin

import requests

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.utils import md5_digest
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

        self._base_url = str(config.base_url)
        self._api_id = md5_digest(self._base_url.encode("utf-8"))
        self._openapi_json_path = config.openapi_json_path
        self._openapi_json_url = str(config.openapi_json_url)
        self._auth = config.auth
        self._init_session()

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(
            f"Fetching metadata from {self._openapi_json_path or self._openapi_json_url}"
        )

        openapi_json = self._get_openapi_json()

        if not openapi_json:
            logger.error("Unable to get OAS json")
            return []

        apis = self._extract_apis(openapi_json)
        hierarchies = self._extract_hierarchies(openapi_json)

        return hierarchies + apis

    def _init_session(self):
        self._requests_session = requests.sessions.Session()

        if not self._auth:
            return

        if self._auth.basic_auth:
            basic_auth = self._auth.basic_auth
            self._requests_session.auth = (basic_auth.user, basic_auth.password)

    def _get_openapi_json(self) -> Optional[dict]:
        if self._openapi_json_path:
            with open(self._openapi_json_path, "r") as f:
                return json.load(f)

        # to have full control of HTTP header
        headers = OrderedDict(
            {
                "User-Agent": None,
                "Accept": "application/json",
                "Connection": None,
                "Accept-Encoding": None,
            }
        )
        resp = self._requests_session.get(self._openapi_json_url, headers=headers)

        if resp.status_code != 200:
            return None

        return resp.json()

    def _extract_apis(self, openapi: dict) -> List[API]:
        apis: List[API] = []
        servers = openapi.get("servers")

        for path, path_item in openapi["paths"].items():
            path_servers = path_item.get("servers")
            server = (
                path_servers[0]["url"]
                if path_servers
                else servers[0]["url"] if servers else ""
            )

            if not server.startswith("http"):
                endpoint_url = urljoin(self._base_url, server + path)
            else:
                endpoint_url = urljoin(server + "/", f"./{path}")

            first_tag = self._get_first_tag(path_item)

            endpoint = API(
                logical_id=APILogicalID(
                    name=endpoint_url, platform=APIPlatform.OPEN_API
                ),
                open_api=OpenAPI(path=path, methods=self._extract_methods(path_item)),
                structure=AssetStructure(
                    directories=[self._api_id] + [first_tag] if first_tag else [],
                    name=path,
                ),
            )
            apis.append(endpoint)
        return apis

    def _get_first_tag(self, path_item: dict) -> Optional[str]:
        for item in path_item.values():
            if "tags" in item and len(item["tags"]) > 0:
                return item["tags"][0]
        return None

    def _extract_methods(self, path_item: dict) -> List[OpenAPIMethod]:
        def to_operation_type(method: str) -> Optional[OperationType]:
            try:
                return OperationType(method.upper())
            except ValueError:
                return None

        methods: List[OpenAPIMethod] = []
        for method, item in path_item.items():
            if operation_type := to_operation_type(method):
                methods.append(
                    OpenAPIMethod(
                        summary=item.get("summary") or None,
                        description=item.get("description") or None,
                        type=operation_type,
                    )
                )
        return methods

    def _extract_hierarchies(self, openapi: dict) -> List[Hierarchy]:
        hierarchies: List[Hierarchy] = []

        title = openapi["info"]["title"]
        hierarchies.append(
            Hierarchy(
                logical_id=HierarchyLogicalID(
                    path=[AssetPlatform.OPEN_API.value, self._api_id],
                ),
                hierarchy_info=HierarchyInfo(
                    name=title,
                    open_api=OpenAPISpecification(definition=json.dumps(openapi)),
                    type=HierarchyType.OPEN_API,
                ),
            )
        )

        for tag in openapi.get("tags") or []:
            name = tag.get("name")
            description = tag.get("description")

            if not name:
                continue

            hierarchies.append(
                Hierarchy(
                    logical_id=HierarchyLogicalID(
                        path=[AssetPlatform.OPEN_API.value, self._api_id, name],
                    ),
                    hierarchy_info=HierarchyInfo(
                        name=name,
                        description=description,
                        type=HierarchyType.OPEN_API,
                    ),
                )
            )

        return hierarchies
