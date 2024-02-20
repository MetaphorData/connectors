import tempfile
from typing import Collection, Iterator, List

from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.file_sink import FileSinkConfig
from metaphor.common.runner import run_connector
from metaphor.common.utils import md5_digest
from metaphor.models.crawler_run_metadata import RunStatus
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    QueryLog,
)


def test_run_connector() -> None:
    class DummyConnector(BaseExtractor):
        @staticmethod
        def from_config_file(config_file: str) -> "DummyConnector":
            return DummyConnector(BaseConfig.from_yaml_file(config_file))

        def __init__(self, config: BaseConfig) -> None:
            super().__init__(config)

        async def extract(self) -> Collection[ENTITY_TYPES]:
            entities: List[ENTITY_TYPES] = []
            for i in range(4):
                try:
                    if i not in [1, 2]:
                        logical_id = DatasetLogicalID(
                            name=str(i), platform=DataPlatform.BIGQUERY
                        )
                        entities.append(Dataset(logical_id=logical_id))
                    else:
                        raise ValueError(str(i))
                except Exception as e:
                    self.extend_errors(e)
            return entities

        def collect_query_logs(self) -> Iterator[QueryLog]:
            query_logs = [
                QueryLog(
                    id=f"{DataPlatform.SNOWFLAKE.name}:{query_id}",
                    query_id=str(query_id),
                    platform=DataPlatform.SNOWFLAKE,
                    account="account",
                    sql=query_text,
                    sql_hash=md5_digest(query_text.encode("utf-8")),
                )
                for query_id, query_text in enumerate(
                    f"this is query no. {x}" for x in range(5)
                )
            ]
            for query_log in query_logs:
                yield query_log

    directory = tempfile.mkdtemp()
    file_sink_config = FileSinkConfig(directory=directory, batch_size_bytes=1000000)
    dummy_connector = DummyConnector(BaseConfig(output=OutputConfig()))
    events, run_metadata = run_connector(
        dummy_connector,
        "dummy_connector",
        "dummy connector",
        file_sink_config=file_sink_config,
    )
    assert run_metadata.status is RunStatus.FAILURE
    assert sorted(
        event.dataset.logical_id.name
        for event in events
        if event.dataset and event.dataset.logical_id and event.dataset.logical_id.name
    ) == ["0", "3"]
    assert dummy_connector.stacktrace
    assert "ValueError: 1" in dummy_connector.stacktrace
    assert "ValueError: 2" in dummy_connector.stacktrace
