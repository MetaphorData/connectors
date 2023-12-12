from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.redshift.config import RedshiftRunConfig
from metaphor.redshift.extractor import RedshiftExtractor


def mock_databases(mock: MagicMock, databases: List[str]):
    mock.return_value = databases


def dummy_config(**args):
    return RedshiftRunConfig(
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
    extractor = RedshiftExtractor(dummy_config())

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    assert events == []
