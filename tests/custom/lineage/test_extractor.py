import pytest

from metaphor.common.event_util import EventUtil
from metaphor.custom.lineage.config import CustomLineageConfig
from metaphor.custom.lineage.extractor import CustomLineageExtractor
from tests.test_utils import load_json


@pytest.mark.asyncio
async def test_extractor(test_root_dir):
    config = CustomLineageConfig.from_yaml_file(
        f"{test_root_dir}/custom/lineage/config.yml"
    )
    extractor = CustomLineageExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/custom/lineage/expected.json")
