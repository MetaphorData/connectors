from glob import glob
from pathlib import Path
from typing import List
import pytest


from testcontainers.mongodb import MongoDbContainer

from metaphor.common.base_config import OutputConfig
from metaphor.models.metadata_change_event import Dataset, SchemaField
from metaphor.common.event_util import EventUtil
from metaphor.common.logger import get_logger
from metaphor.mongodb.config import MongoDBConfig
from metaphor.mongodb.extractor import MongoDBExtractor
from tests.test_utils import load_json

user = "test"
password = "test"

logger = get_logger()


def sort_fields(fields: List[SchemaField]) -> List[SchemaField]:
    for field in fields:
        if isinstance(field.subfields, list):
            sort_fields(field.subfields)

    return list(
        dict(
            sorted({field.field_name: field for field in fields}.items())
        ).values()
    )


@pytest.fixture(scope="module")
def mongodb_container(test_root_dir: str):
    with MongoDbContainer().with_volume_mapping(
        host=f"{test_root_dir}/mongodb/mongodb-sample-dataset",
        container="/samples",
        mode="ro",
    ) as container:
        for path_str in glob(
            f"{test_root_dir}/mongodb/mongodb-sample-dataset/*/*.json"
        ):
            path = Path(path_str)
            db = path.parent.name
            collection = path.name.split(".", maxsplit=1)[0]
            container.exec(
                f"mongoimport --drop --db {db} --collection {collection} --file /samples/{db}/{collection}.json --authenticationDatabase admin -u {user} -p {password}"
            )
            logger.info(f"Imported {db}.{collection}")

        yield container


@pytest.mark.asyncio
async def test_extractor(test_root_dir: str, mongodb_container: MongoDbContainer):
    extractor = MongoDBExtractor(
        MongoDBConfig(
            output=OutputConfig(),
            uri=f"mongodb://{user}:{password}@{mongodb_container.get_container_host_ip()}:{mongodb_container.get_exposed_port(27017)}",
            infer_schema_sample_size=None,
        )
    )
    entities = await extractor.extract()
    datasets = [ent for ent in entities if isinstance(ent, Dataset)]
    # Sort the schema fields recursively
    for dataset in datasets:
        if dataset.schema and dataset.schema.fields:
            dataset.schema.fields = sort_fields(dataset.schema.fields)

    # Then sort the datasets
    datasets = list(
        dict(
            sorted(
                {
                    (dataset.logical_id.name if dataset.logical_id and dataset.logical_id.name else ""): dataset
                    for dataset in datasets
                }.items()
            )
        ).values()
    )

    trimmed_datasets = [EventUtil.trim_event(ds) for ds in datasets]
    expected = f"{test_root_dir}/mongodb/expected_datasets.json"
    assert trimmed_datasets == load_json(expected, trimmed_datasets)
