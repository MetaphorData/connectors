import pytest

from metaphor.common.event_util import EventUtil
from metaphor.custom.data_quality.config import CustomDataQualityConfig
from metaphor.custom.data_quality.extractor import CustomDataQualityExtractor
from tests.test_utils import load_json


@pytest.mark.asyncio
async def test_extractor(test_root_dir):
    config = CustomDataQualityConfig.from_yaml_file(
        f"{test_root_dir}/custom/data_quality/config.yml"
    )
    extractor = CustomDataQualityExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/custom/data_quality/expected.json")
