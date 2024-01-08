import re
import time

import pytest
from testcontainers.general import DockerContainer

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.logger import get_logger
from metaphor.hive.config import HiveRunConfig
from metaphor.hive.extractor import HiveExtractor
from metaphor.models.metadata_change_event import Dataset, DatasetSchema
from tests.test_utils import load_json

logger = get_logger()


def remove_transient_ddl_time_from_table_schema(schema: DatasetSchema) -> DatasetSchema:
    """
    Hive returns the creation time of the table, and we don't want to match that!
    """
    if schema.sql_schema and schema.sql_schema.table_schema:
        table_schema = schema.sql_schema.table_schema
        matched = re.search(r",\s*\n\s*\'transient_lastDdlTime\'=\'\d+\'", table_schema)
        if matched is not None:
            schema.sql_schema.table_schema = table_schema.replace(matched.group(), "")
    return schema


@pytest.mark.asyncio
async def test_extractor(test_root_dir: str) -> None:
    with (
        DockerContainer("apache/hive:4.0.0-beta-1")
        .with_exposed_ports(10000)
        .with_env("SERVICE_NAME", "hiveserver2")
        .with_volume_mapping(
            f"{test_root_dir}/hive/data/init.sql", "/opt/hive/init.sql"
        )
        .with_volume_mapping(
            f"{test_root_dir}/hive/data/u.data", "/opt/hive/examples/files/u.data"
        )
    ) as container:
        port = container.get_exposed_port(10000)
        host = container.get_container_host_ip()

        config = HiveRunConfig(output=OutputConfig(), host=host, port=int(port))

        while True:
            try:
                HiveExtractor.get_connection(**config.connect_kwargs)
                break
            except Exception:
                # Wait till it's up
                logger.info("Waiting for hiveserver2 to start")
                time.sleep(1)

        container.exec("beeline -u jdbc:hive2://localhost:10000/default -f init.sql")

        extractor = HiveExtractor(config)

        events = []
        for entity in await extractor.extract():
            if isinstance(entity, Dataset) and entity.schema is not None:
                entity.schema = remove_transient_ddl_time_from_table_schema(
                    entity.schema
                )
            events.append(EventUtil.trim_event(entity))

        expected = f"{test_root_dir}/hive/expected.json"

        assert events == load_json(expected)
