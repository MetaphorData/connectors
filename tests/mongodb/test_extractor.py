import time
import typing
from glob import glob
from pathlib import Path

import pytest
from testcontainers.mongodb import MongoDbContainer

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import Dataset
from metaphor.mongodb.config import MongoDBConfig
from metaphor.mongodb.extractor import MongoDBExtractor
from tests.test_utils import load_json

user = "test"
password = "test"  # nosec B105

logger = get_logger()


@pytest.fixture(scope="module")
def mongodb_container(test_root_dir: str):
    with MongoDbContainer().with_volume_mapping(
        host=f"{test_root_dir}/mongodb/mongodb-sample-dataset",
        container="/samples",
        mode="ro",
    ) as container:
        container = typing.cast(MongoDbContainer, container)
        # Make sure it's up and running before importing stuff
        container.get_connection_client()
        logger.info("MongoDB container is up")
        # Wait 3 seconds for MongoDB to actually go up
        time.sleep(3)

        for path_str in glob(
            f"{test_root_dir}/mongodb/mongodb-sample-dataset/*/*.json"
        ):
            path = Path(path_str)
            db = path.parent.name
            collection = path.name.split(".", maxsplit=1)[0]
            retries = 3
            attempt = 1
            while attempt <= retries:
                logger.info(f"Importing {db}.{collection}, attempt #{attempt}")
                res = container.exec(
                    f"mongoimport --drop --db {db} --collection {collection} --file /samples/{db}/{collection}.json --authenticationDatabase admin -u {user} -p {password}"
                )
                if res.exit_code == 0:
                    break
                attempt += 1

            if attempt > retries:
                assert False, "Failed to initialize mongodb container"

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

    def get_dataset_name(dataset: Dataset):
        return (
            dataset.logical_id.name
            if dataset.logical_id and dataset.logical_id.name
            else ""
        )

    # Sort the datasets
    datasets = sorted(
        [ent for ent in entities if isinstance(ent, Dataset)], key=get_dataset_name
    )

    trimmed_datasets = [EventUtil.trim_event(ds) for ds in datasets]
    expected = f"{test_root_dir}/mongodb/expected_datasets.json"
    assert trimmed_datasets == load_json(expected)
