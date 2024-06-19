from typing import List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.common.utils import safe_float, to_utc_time
from metaphor.custom.data_quality.config import CustomDataQualityConfig
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


class CustomDataQualityExtractor(BaseExtractor):
    """Custom data quality extractor"""

    _description = "Custom data quality connector"
    _platform = None

    @staticmethod
    def from_config_file(config_file: str) -> "CustomDataQualityExtractor":
        return CustomDataQualityExtractor(
            CustomDataQualityConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: CustomDataQualityConfig) -> None:
        super().__init__(config)
        self._datasets = config.datasets

    async def extract(self) -> List[MetadataChangeEvent]:
        logger.info("Fetching custom data quality from config")

        datasets = []
        for dataset_data_quality in self._datasets:
            dataset = Dataset(logical_id=dataset_data_quality.id.to_logical_id())
            datasets.append(dataset)

            data_quality = dataset_data_quality.data_quality

            dataset.data_quality = DatasetDataQuality(
                provider=(
                    DataQualityProvider[data_quality.provider]
                    if data_quality.provider
                    else None
                ),
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
                        severity=(
                            DataMonitorSeverity[monitor.severity]
                            if monitor.severity
                            else None
                        ),
                        last_run=to_utc_time(monitor.last_run),
                        value=safe_float(monitor.value),
                        targets=targets,
                    )
                )

        return datasets
