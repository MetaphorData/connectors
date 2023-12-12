from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.redshift.profile.config import RedshiftProfileRunConfig
from metaphor.redshift.profile.extractor import RedshiftProfileExtractor


def mock_databases(mock: MagicMock, databases: List[str]):
    mock.return_value = databases


def dummy_config(**args):
    return RedshiftProfileRunConfig(
        host="", database="", user="", password="", output=OutputConfig(), **args
    )


@patch(
    "metaphor.postgresql.extractor.PostgreSQLExtractor._fetch_databases",
    new_callable=AsyncMock,
)
@pytest.mark.asyncio
async def test_extractor(
    mock_fetch_databases: MagicMock,
):
    mock_databases(mock_fetch_databases, [])

    extractor = RedshiftProfileExtractor(dummy_config())

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == []
