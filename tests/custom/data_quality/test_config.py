from datetime import datetime

from metaphor.common.base_config import OutputConfig
from metaphor.custom.data_quality.config import (
    CustomDataQualityConfig,
    DataQuality,
    DataQualityMonitor,
    DataQualityMonitorTarget,
    DatasetDataQuality,
)
from metaphor.custom.governance.config import DeserializableDatasetLogicalID


def test_yaml_config(test_root_dir):
    config = CustomDataQualityConfig.from_yaml_file(
        f"{test_root_dir}/custom/data_quality/config.yml"
    )

    assert config == CustomDataQualityConfig(
        datasets=[
            DatasetDataQuality(
                id=DeserializableDatasetLogicalID(name="foo", platform="BIGQUERY"),
                data_quality=DataQuality(
                    provider="SODA",
                    url="https://soda.io",
                    monitors=[
                        DataQualityMonitor(
                            title="monitor_title",
                            description="monitor_description",
                            url="https://soda.io/monitor1",
                            owner="foo@bar.com",
                            status="PASSED",
                            severity="LOW",
                            value=100,
                            last_run=datetime(2000, 1, 1, 0, 0, 0),
                            targets=[
                                DataQualityMonitorTarget(
                                    dataset="dataset1", column="col1"
                                )
                            ],
                        )
                    ],
                ),
            )
        ],
        output=OutputConfig(),
    )
