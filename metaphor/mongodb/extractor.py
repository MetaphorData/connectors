from datetime import date, datetime, time
from typing import Any, Collection, Dict

import bson
from pymongo.collection import Collection as MongoCollection

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.infer_schema import SchemaTypeNameMapping, infer_schema
from metaphor.common.logger import get_logger
from metaphor.common.utils import safe_float
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStatistics,
    DatasetStructure,
    SchemaType,
)
from metaphor.mongodb.config import MongoDBConfig

logger = get_logger()


class MongoDBExtractor(BaseExtractor):
    """MongoDB metadata extractor"""

    _description = "MongoDB metadata crawler"
    _platform = Platform.MONGODB

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
        bson.Int64: "Int64",
        bson.Decimal128: "Decimal128",
    }

    def __init__(self, config: MongoDBConfig) -> None:
        super().__init__(config)
        self._sample_size = config.infer_schema_sample_size
        self._excluded_collections = config.excluded_collections
        self._excluded_databases = config.excluded_databases
        self.client = config.get_client()
        self._datasets: Dict[str, Dataset] = {}
        if self._sample_size is None:
            logger.info(
                "Not sampling, all objects in a collection will be used to infer the collection's schema"
            )
        if self._sample_size == 0:
            logger.info("Infer sample size set to 0, not inferring collection schema")

    @staticmethod
    def from_config_file(config_file: str) -> "MongoDBExtractor":
        return MongoDBExtractor(MongoDBConfig.from_yaml_file(config_file))

    def _get_collection_schema(self, collection: MongoCollection):
        if self._sample_size == 0:
            return []
        pipeline = []
        if self._sample_size:
            pipeline.append(
                {
                    "$sample": {
                        "size": self._sample_size,
                    }
                }
            )
        docs = collection.aggregate(pipeline).to_list()
        fields = infer_schema(docs, self._type_mapping)
        return fields

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
        database = None
        schema = collection.database.name
        table = collection.name
        name = dataset_normalized_name(database, schema, table)
        self._datasets[name] = Dataset(
            logical_id=DatasetLogicalID(
                name=name,
                platform=DataPlatform.MONGODB,
            ),
            schema=DatasetSchema(
                fields=fields,
                schema_type=SchemaType.BSON,
            ),
            statistics=self._get_collection_statistics(raw_coll_stats),
            structure=DatasetStructure(
                database=database,
                schema=schema,
                table=table,
            ),
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
