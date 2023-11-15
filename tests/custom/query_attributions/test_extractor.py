import pytest

from metaphor.common.event_util import EventUtil
from metaphor.custom.query_attributions.config import CustomQueryAttributionsConfig
from metaphor.custom.query_attributions.extractor import (
    CustomQueryAttributionsExtractor,
)
from tests.test_utils import load_json


@pytest.mark.asyncio
async def test_extractor(test_root_dir):
    config = CustomQueryAttributionsConfig.from_yaml_file(
        f"{test_root_dir}/custom/query_attributions/config.yml"
    )
    extractor = CustomQueryAttributionsExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(
        f"{test_root_dir}/custom/query_attributions/expected.json"
    )
