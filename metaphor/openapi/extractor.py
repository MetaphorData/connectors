import json
from collections import OrderedDict
from typing import Collection, List, Optional
from urllib.parse import urljoin

import requests

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import EntityId
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
    EntityType,
    Hierarchy,
    HierarchyInfo,
    HierarchyLogicalID,
    HierarchyType,
    OpenAPI,
    OpenAPIMethod,
    OpenAPISpecification,
    OperationType,
)
from metaphor.openapi.config import OpenAPIJsonConfig, OpenAPIRunConfig

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
        self._specs: List[OpenAPIJsonConfig]
        self._get_spec_configs(config)

    async def extract(self) -> Collection[ENTITY_TYPES]:

        apis = []
        hierarchies = []

        for spec in self._specs:
            base_url = str(spec.base_url)
            api_id = md5_digest(base_url.encode("utf-8"))

            logger.info(
                f"Fetching metadata from {spec.openapi_json_path or str(spec.openapi_json_url)}"
            )

            openapi_json = self._get_openapi_json(spec)

            if not openapi_json:
                logger.error("Unable to get OAS json")
                return []

            apis.extend(
                self._extract_apis(
                    openapi_json,
                    base_url=base_url,
                    api_id=api_id,
                )
            )
            hierarchies.extend(self._extract_hierarchies(openapi_json, api_id))

        return hierarchies + apis

    def _get_spec_configs(self, config: OpenAPIRunConfig):
        self._specs = (
            [
                OpenAPIJsonConfig(
                    base_url=config.base_url,
                    openapi_json_path=config.openapi_json_path,
                    openapi_json_url=config.openapi_json_url,
                )
            ]
            if config.base_url
            else []
        ) + (config.specs or [])

    def _init_session(self, spec: OpenAPIJsonConfig):
        session = requests.sessions.Session()

        if spec.auth and spec.auth.basic_auth:
            basic_auth = spec.auth.basic_auth
            session.auth = (basic_auth.user, basic_auth.password)

        return session

    def _get_openapi_json(
        self,
        spec: OpenAPIJsonConfig,
    ) -> Optional[dict]:
        if spec.openapi_json_path:
            with open(spec.openapi_json_path, "r") as f:
                return json.load(f)

        session = self._init_session(spec)

        # to have full control of HTTP header
        headers = OrderedDict(
            {
                "User-Agent": None,
                "Accept": "application/json",
                "Connection": None,
                "Accept-Encoding": None,
            }
        )
        resp = session.get(str(spec.openapi_json_url), headers=headers)

        if resp.status_code != 200:
            return None

        return resp.json()

    def _extract_apis(self, openapi: dict, base_url: str, api_id: str) -> List[API]:
        oas_hierarchy_id = str(
            EntityId(
                EntityType.HIERARCHY,
                HierarchyLogicalID(path=[AssetPlatform.OPEN_API.value, api_id]),
            )
        )

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
                endpoint_url = urljoin(base_url, server + path)
            else:
                endpoint_url = urljoin(server + "/", f"./{path}")

            first_tag = self._get_first_tag(path_item)

            endpoint = API(
                logical_id=APILogicalID(
                    name=endpoint_url, platform=APIPlatform.OPEN_API
                ),
                open_api=OpenAPI(
                    path=path,
                    methods=self._extract_methods(path_item, path, oas_hierarchy_id),
                    oas_hierarchy_id=oas_hierarchy_id,
                ),
                structure=AssetStructure(
                    directories=[api_id] + ([first_tag] if first_tag else []),
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

    def _extract_methods(
        self, path_item: dict, path: str, oas_hierarchy_id: str
    ) -> List[OpenAPIMethod]:
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
                        path=path,
                        oas_hierarchy_id=oas_hierarchy_id,
                    )
                )
        return methods

    def _extract_hierarchies(self, openapi: dict, api_id: str) -> List[Hierarchy]:
        hierarchies: List[Hierarchy] = []

        title = openapi["info"]["title"]
        hierarchies.append(
            Hierarchy(
                logical_id=HierarchyLogicalID(
                    path=[AssetPlatform.OPEN_API.value, api_id],
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
                        path=[AssetPlatform.OPEN_API.value, api_id, name],
                    ),
                    hierarchy_info=HierarchyInfo(
                        name=name,
                        description=description,
                        type=HierarchyType.OPEN_API,
                    ),
                )
            )

        return hierarchies
