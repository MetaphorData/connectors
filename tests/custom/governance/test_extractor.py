import pytest

from metaphor.common.event_util import EventUtil
from metaphor.custom.governance.config import CustomGovernanceConfig
from metaphor.custom.governance.extractor import CustomGovernanceExtractor
from tests.test_utils import load_json


@pytest.mark.asyncio
async def test_extractor(test_root_dir):
    config = CustomGovernanceConfig.from_yaml_file(
        f"{test_root_dir}/custom/governance/config.yml"
    )
    extractor = CustomGovernanceExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/custom/governance/expected.json")
