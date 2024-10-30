import importlib
from pathlib import Path

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.great_expectations.config import GreatExpectationConfig
from metaphor.great_expectations.extractor import GreatExpectationsExtractor
from tests.test_utils import load_json


@pytest.mark.parametrize("directory", ["basic_sql", "snowflake"])
@pytest.mark.asyncio
async def test_extractor(test_root_dir: str, directory: str) -> None:
    root = Path(test_root_dir)
    # Imports the module that runs the GX stuff
    module = importlib.import_module(f"tests.great_expectations.{directory}.run")
    # Runs GX stuff
    module.run()

    config = GreatExpectationConfig(
        output=OutputConfig(),
        project_root_dir=root / "great_expectations" / directory,
        snowflake_account="john.doe@metaphor.io",
    )

    extractor = GreatExpectationsExtractor(config)
    entities = await extractor.extract()
    events = [EventUtil.trim_event(e) for e in entities]
    expected = root / "great_expectations" / f"expected_{directory}.json"
    assert events == load_json(expected, events)
