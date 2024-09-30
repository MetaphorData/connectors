from typing import Collection
from httpx import delete
from metaphor.models.crawler_run_metadata import Platform
from metaphor.common import infer_schema
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.mongodb.config import MongoDBConfig


class MongoDBExtractor(BaseExtractor):
    """MongoDB metadata extractor"""

    _description = "MongoDB metadata crawler"
    _platform = Platform.S3

    def __init__(self, config: MongoDBConfig) -> None:
        super().__init__(config)
        self._sample_size = config.documents_to_infer_schema
        self._excluded_collections = config.excluded_collections
        self._excluded_databases = config.excluded_databases
        self.client = config.get_client()

    @staticmethod
    def from_config_file(config_file: str) -> "MongoDBExtractor":
        return MongoDBExtractor(MongoDBConfig.from_yaml_file(config_file))


    async def extract(self) -> Collection[ENTITY_TYPES]:
        self.client.list_databases()
        for database_name in self.client.list_database_names():
            if database_name in self._excluded_databases:
                continue

            database = self.client.get_database(database_name)
            for collection_name in database.list_collection_names():
                if collection_name in self._excluded_collections:
                    continue

                collection = database.get_collection(collection_name)
                docs = collection.aggregate([
                    {
                        "$sample": {
                            "size": self._sample_size,
                        }
                    }
                ]).to_list()
                schema = infer_schema.construct_schema(docs, delimiter=".")
                print("")
        return []
