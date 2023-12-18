from typing import Collection, List

import requests
from gql import Client
from gql.transport.requests import RequestsHTTPTransport

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.datahub.config import DatahubConfig
from metaphor.datahub.gql_parser import get_dataset
from metaphor.models.metadata_change_event import Dataset

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

    def _init_dataset(self, urn: str) -> Dataset:
        return get_dataset(self._gql_client, urn).as_meta_dataset()

    async def extract(self) -> Collection[ENTITY_TYPES]:
        resp = requests.get(
            f"{self._endpoint}/entity/dataset",
            headers=self._headers,
            params={"aspects": "ownership"},
            timeout=10,
        )
        entities: List[ENTITY_TYPES] = []
        for entity in resp.json().get("entities", []):
            urn = entity["urn"]
            entities.append(self._init_dataset(urn))

        return entities
