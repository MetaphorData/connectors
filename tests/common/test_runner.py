from typing import Collection, List

from metaphor.common.base_config import BaseConfig, OutputConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.runner import run_connector
from metaphor.models.crawler_run_metadata import RunStatus
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
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
                    if i != 2:
                        logical_id = DatasetLogicalID(
                            name=str(i), platform=DataPlatform.BIGQUERY
                        )
                        entities.append(Dataset(logical_id=logical_id))
                    else:
                        raise ValueError
                except Exception as e:
                    self.extend_errors(e)
            return entities

    dummy_connector = DummyConnector(BaseConfig(output=OutputConfig()))
    events, run_metadata = run_connector(
        dummy_connector, "dummy_connector", "dummy connector"
    )
    assert run_metadata.status is RunStatus.FAILURE
    assert sorted(
        event.dataset.logical_id.name
        for event in events
        if event.dataset and event.dataset.logical_id and event.dataset.logical_id.name
    ) == ["0", "1", "3"]
