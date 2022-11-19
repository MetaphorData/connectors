import pytest

from metaphor.common.event_util import EventUtil
from metaphor.manual.data_quality.config import ManualDataQualityConfig
from metaphor.manual.data_quality.extractor import ManualDataQualityExtractor
from tests.test_utils import load_json


@pytest.mark.asyncio
async def test_extractor(test_root_dir):
    config = ManualDataQualityConfig.from_yaml_file(
        f"{test_root_dir}/manual/data_quality/config.yml"
    )
    extractor = ManualDataQualityExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/manual/data_quality/expected.json")
