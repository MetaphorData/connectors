from datetime import datetime, timezone
from typing import List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.manual.data_quality.config import ManualDataQualityConfig
from metaphor.models.metadata_change_event import (
    DataMonitor,
    DataMonitorSeverity,
    DataMonitorStatus,
    DataMonitorTarget,
    DataQualityProvider,
    Dataset,
    DatasetDataQuality,
    MetadataChangeEvent,
)

logger = get_logger()


class ManualDataQualityExtractor(BaseExtractor):
    """Manual data quality extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "ManualDataQualityExtractor":
        return ManualDataQualityExtractor(
            ManualDataQualityConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: ManualDataQualityConfig) -> None:
        super().__init__(config, "Manual data quality connector", None)
        self._datasets = config.datasets

    async def extract(self) -> List[MetadataChangeEvent]:
        logger.info("Fetching manual data quality from config")

        datasets = []
        for dataset_data_quality in self._datasets:
            dataset = Dataset(logical_id=dataset_data_quality.id.to_logical_id())
            datasets.append(dataset)

            data_quality = dataset_data_quality.data_quality

            dataset.data_quality = DatasetDataQuality(
                provider=None
                if data_quality.provider is None
                else DataQualityProvider[data_quality.provider],
                url=data_quality.url,
                monitors=[],
            )
            for monitor in data_quality.monitors:
                targets = [
                    DataMonitorTarget(
                        dataset=target.dataset,
                        column=target.column,
                    )
                    for target in monitor.targets
                ]

                dataset.data_quality.monitors.append(
                    DataMonitor(
                        title=monitor.title,
                        description=monitor.description,
                        url=monitor.url,
                        owner=monitor.owner,
                        status=DataMonitorStatus[monitor.status],
                        severity=None
                        if monitor.severity is None
                        else DataMonitorSeverity[monitor.severity],
                        last_run=datetime.fromtimestamp(
                            monitor.last_run.timestamp()
                        ).replace(tzinfo=timezone.utc),
                        value=None if monitor.value is None else float(monitor.value),
                        targets=targets,
                    )
                )

        return datasets
