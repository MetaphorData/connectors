from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from metaphor.athena.config import AthenaRunConfig, AwsCredentials, QueryLogConfig
from metaphor.athena.extractor import AthenaExtractor
from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.filter import DatasetFilter
from metaphor.common.sql.process_query.config import (
    ProcessQueryConfig,
    RedactPIILiteralsConfig,
)
from tests.test_utils import load_json


def dummy_config():
    return AthenaRunConfig(
        aws=AwsCredentials(
            access_key_id="key", secret_access_key="secret", region_name="region"
        ),
        filter=DatasetFilter(
            excludes={
                "test": None,
                "awsdatacatalog": {"foo": None, "spectrum_db2": set(["table"])},
            }
        ),
        query_log=QueryLogConfig(
            process_query=ProcessQueryConfig(
                redact_literals=RedactPIILiteralsConfig(enabled=True)
            )
        ),
        output=OutputConfig(),
    )


@patch("metaphor.athena.extractor.create_athena_client")
@pytest.mark.asyncio
async def test_extractor(mock_create_client: MagicMock, test_root_dir: str):
    def mock_list_data_catalogs():
        yield load_json(
            f"{test_root_dir}/athena/data/list_data_catalogs_f8346904-fad9-4b57-a4ca-4a5df6392d62.json"
        )

    def mock_list_databases(**_):
        yield load_json(
            f"{test_root_dir}/athena/data/list_databases_3039188f-6f71-4c41-b79c-cac7bcb905bc.json"
        )

    def mock_list_table_metadata(**_):
        yield load_json(
            f"{test_root_dir}/athena/data/list_table_metadata_e6d7f820-7a9d-46ff-b479-df064d0c8c65.json"
        )

    def mock_get_paginator(method: str):
        if method == "list_data_catalogs":
            mock_paginator = MagicMock()
            mock_paginator.paginate = mock_list_data_catalogs
            return mock_paginator
        elif method == "list_databases":
            mock_paginator = MagicMock()
            mock_paginator.paginate = mock_list_databases
            return mock_paginator
        elif method == "list_table_metadata":
            mock_paginator = MagicMock()
            mock_paginator.paginate = mock_list_table_metadata
            return mock_paginator

    mock_client = MagicMock()
    mock_client.get_paginator = mock_get_paginator
    mock_create_client.return_value = mock_client

    extractor = AthenaExtractor(dummy_config())
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/athena/expected.json")


@patch("metaphor.athena.extractor.create_athena_client")
@pytest.mark.asyncio
@freeze_time("2024-10-03")
async def test_collect_query_logs(mock_create_client: MagicMock, test_root_dir: str):
    def mock_list_query_executions(**_):
        yield load_json(
            f"{test_root_dir}/athena/data/list_query_executions_3ec07c89-d2ee-414a-8490-c0a63471bd47.json"
        )

    def mock_get_paginator(method: str):
        if method == "list_query_executions":
            mock_paginator = MagicMock()
            mock_paginator.paginate = mock_list_query_executions
            return mock_paginator
        if method == "list_data_catalogs":
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = []
            return mock_paginator

    mock_client = MagicMock()
    mock_client.get_paginator = mock_get_paginator
    mock_client.batch_get_query_execution.return_value = load_json(
        f"{test_root_dir}/athena/data/batch_get_query_executions_ef77e890-ca4f-4e46-b4d3-ee90915237ae.json"
    )
    mock_create_client.return_value = mock_client

    extractor = AthenaExtractor(dummy_config())
    assert list(await extractor.extract()) == []
    events = [EventUtil.trim_event(e) for e in extractor.collect_query_logs()]

    assert events == load_json(f"{test_root_dir}/athena/expected_query_logs.json")
