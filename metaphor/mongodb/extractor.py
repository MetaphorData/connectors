from datetime import date, datetime, time
from typing import Any, Collection, Dict

import bson
from pymongo.collection import Collection as MongoCollection

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.infer_schema import SchemaTypeNameMapping, infer_schema
from metaphor.common.utils import safe_float
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStatistics,
    SchemaType,
)
from metaphor.mongodb.config import MongoDBConfig


class MongoDBExtractor(BaseExtractor):
    """MongoDB metadata extractor"""

    _description = "MongoDB metadata crawler"
    _platform = Platform.S3  # FIXME

    _type_mapping: SchemaTypeNameMapping = {
        list: "Array",
        int: "Int",
        float: "Float",
        bool: "Bool",
        str: "String",
        None: "Null",
        datetime: "Datetime",
        date: "Date",
        time: "Time",
        dict: "Object",
        bson.ObjectId: "ObjectId",
    }

    def __init__(self, config: MongoDBConfig) -> None:
        super().__init__(config)
        self._sample_size = config.documents_to_infer_schema
        self._excluded_collections = config.excluded_collections
        self._excluded_databases = config.excluded_databases
        self.client = config.get_client()
        self._datasets: Dict[str, Dataset] = {}

    @staticmethod
    def from_config_file(config_file: str) -> "MongoDBExtractor":
        return MongoDBExtractor(MongoDBConfig.from_yaml_file(config_file))

    def _get_collection_schema(self, collection: MongoCollection):
        docs = collection.aggregate(
            [
                {
                    "$sample": {
                        "size": self._sample_size,
                    }
                }
            ]
        ).to_list()
        return infer_schema(docs, self._type_mapping)

    def _get_collection_statistics(
        self, raw_stats: Dict[str, Any]
    ) -> DatasetStatistics:
        size = raw_stats.get("size")
        record_count = raw_stats.get("count")
        return DatasetStatistics(
            record_count=safe_float(record_count),
            data_size_bytes=safe_float(size),
        )

    def _init_dataset(
        self, collection: MongoCollection, raw_coll_stats: Dict[str, Any]
    ) -> None:
        fields = self._get_collection_schema(collection)
        name = dataset_normalized_name(
            schema=collection.database.name, table=collection.name
        )
        self._datasets[name] = Dataset(
            logical_id=DatasetLogicalID(
                name=name,
                platform=DataPlatform.S3,  # FIXME
            ),
            schema=DatasetSchema(
                fields=fields, schema_type=SchemaType.SCHEMALESS  # FIXME
            ),
            statistics=self._get_collection_statistics(raw_coll_stats),
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        for database_name in self.client.list_database_names():
            if database_name in self._excluded_databases:
                continue

            database = self.client.get_database(database_name)
            for collection_name in database.list_collection_names():
                if collection_name in self._excluded_collections:
                    continue

                collection = database.get_collection(collection_name)
                raw_collection_stats = database.command("collstats", collection_name)
                self._init_dataset(collection, raw_collection_stats)
        return self._datasets.values()
