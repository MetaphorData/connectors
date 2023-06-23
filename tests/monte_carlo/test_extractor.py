from datetime import datetime

from dateutil.tz import tzutc

from metaphor.common.base_config import OutputConfig
from metaphor.models.metadata_change_event import (
    DataMonitor,
    DataMonitorSeverity,
    DataMonitorStatus,
    DataMonitorTarget,
    DataPlatform,
    DataQualityProvider,
    Dataset,
    DatasetDataQuality,
    DatasetLogicalID,
)
from metaphor.monte_carlo.config import MonteCarloRunConfig
from metaphor.monte_carlo.extractor import MonteCarloExtractor


def dummy_config():
    return MonteCarloRunConfig(
        api_key_id="key_id",
        api_key_secret="key_secret",
        data_platform=DataPlatform.SNOWFLAKE,
        output=OutputConfig(),
    )


def test_parse_monitors():
    extractor = MonteCarloExtractor(dummy_config())

    response = {
        "get_monitors": [
            {
                "uuid": "e0dc143e-dd8a-4cb9-b4cc-dedec715d955",
                "name": "auto_monitor_name_cd5b69bd-e465-4545-b3f9-a5d507ea766c",
                "description": "Field Health for all fields in db:metaphor.test1",
                "entities": ["db:metaphor.test1"],
                "severity": None,
                "monitorStatus": "MISCONFIGURED",
                "monitorFields": None,
                "creatorId": "yi@metaphor.io",
                "prevExecutionTime": "2023-06-23T03:54:35.817000+00:00",
            },
            {
                "uuid": "ce4c4568-35f4-4365-a6fe-95f233fcf6c3",
                "name": "auto_monitor_name_53c985e6-8f49-4af7-8ef1-7b402a27538b",
                "description": "Field Health for all fields in db:metaphor.test2",
                "entities": ["db:metaphor.test2"],
                "severity": "LOW",
                "monitorStatus": "SUCCESS",
                "monitorFields": ["foo", "bar"],
                "creatorId": "yi@metaphor.io",
                "prevExecutionTime": "2023-06-23T03:54:35.817000+00:00",
            },
        ]
    }

    extractor._parse_monitors(response)

    assert [v for v in extractor._datasets.values()] == [
        Dataset(
            dataset_id="DATASET~C7049C940D285091076C957891D54BC6",
            logical_id=DatasetLogicalID(
                name="db.metaphor.test1", platform=DataPlatform.SNOWFLAKE
            ),
            data_quality=DatasetDataQuality(
                provider=DataQualityProvider.MONTE_CARLO,
                monitors=[
                    DataMonitor(
                        description="Field Health for all fields in db:metaphor.test1",
                        last_run=datetime(
                            2023, 6, 23, 3, 54, 35, 817000, tzinfo=tzutc()
                        ),
                        owner="yi@metaphor.io",
                        severity=DataMonitorSeverity.UNKNOWN,
                        status=DataMonitorStatus.ERROR,
                        targets=[],
                        title="auto_monitor_name_cd5b69bd-e465-4545-b3f9-a5d507ea766c",
                        url="https://getmontecarlo.com/monitors/e0dc143e-dd8a-4cb9-b4cc-dedec715d955",
                    )
                ],
            ),
        ),
        Dataset(
            dataset_id="DATASET~4CCEB16A5904DAA5CDC936B28CFFCD19",
            logical_id=DatasetLogicalID(
                name="db.metaphor.test2", platform=DataPlatform.SNOWFLAKE
            ),
            data_quality=DatasetDataQuality(
                monitors=[
                    DataMonitor(
                        description="Field Health for all fields in db:metaphor.test2",
                        last_run=datetime(
                            2023, 6, 23, 3, 54, 35, 817000, tzinfo=tzutc()
                        ),
                        owner="yi@metaphor.io",
                        severity=DataMonitorSeverity.UNKNOWN,
                        status=DataMonitorStatus.PASSED,
                        targets=[
                            DataMonitorTarget(column="foo"),
                            DataMonitorTarget(column="bar"),
                        ],
                        title="auto_monitor_name_53c985e6-8f49-4af7-8ef1-7b402a27538b",
                        url="https://getmontecarlo.com/monitors/ce4c4568-35f4-4365-a6fe-95f233fcf6c3",
                    )
                ],
                provider=DataQualityProvider.MONTE_CARLO,
            ),
        ),
    ]
