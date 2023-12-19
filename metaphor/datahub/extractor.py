from typing import Any, Collection, Dict, List, Optional

import requests
from gql import Client
from gql.transport.requests import RequestsHTTPTransport

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.datahub.config import DatahubConfig
from metaphor.datahub.gql_parser import get_dataset

logger = get_logger()


class DatahubExtractor(BaseExtractor):
    """Datahub metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "DatahubExtractor":
        return DatahubExtractor(DatahubConfig.from_yaml_file(config_file))

    _description = "Datahub metadata connector"
    _platform = None

    def __init__(self, config: DatahubConfig) -> None:
        super().__init__(config)
        self._config = config
        self._endpoint = f"http://{config.host}:{config.port}/openapi/v2"
        self._headers = {
            "Authorization": f"Bearer {self._config.token}",
            "Content-Type": "application/json",
        }
        self._gql_client = Client(
            transport=RequestsHTTPTransport(
                f"http://{config.host}:{config.port}/api/graphql", headers=self._headers
            ),
            fetch_schema_from_transport=True,
        )

        self._entities: List[ENTITY_TYPES] = []

    def _init_dataset(self, urn: str) -> None:
        dataset_info = get_dataset(self._gql_client, urn)
        if dataset_info.has_additional_information():
            self._entities.append(dataset_info.as_meta_dataset())

    def _get_dataset_page(self, scroll_id: Optional[str]) -> Dict[str, Any]:
        params = {}
        if scroll_id:
            params["scrollId"] = scroll_id

        resp = requests.get(
            f"{self._endpoint}/entity/dataset",
            headers=self._headers,
            params=params,
            timeout=10,
        )
        return resp.json()

    async def extract(self) -> Collection[ENTITY_TYPES]:
        scroll_id = None
        while True:
            body = self._get_dataset_page(scroll_id)
            for entity in body.get("entities", []):
                self._init_dataset(entity["urn"])
            scroll_id = body.get("scrollId", None)
            if not scroll_id:
                break

        return self._entities
