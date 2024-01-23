from typing import Any, Dict, Optional

import pytest

from metaphor.alation.config import AlationConfig
from metaphor.alation.extractor import AlationExtractor
from metaphor.alation.schema import Column, Datasource, Schema, Table
from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from tests.test_utils import load_json


class MockClient:
    def __init__(self) -> None:
        pass

    def get(self, path, params: Optional[Dict[str, Any]] = None):
        if path == "integration/v2/table/":
            yield Table(
                id=1,
                name="foo",
                description="some description",
                ds_id=2,
                table_type="TABLE",
                schema_id=3,
            ).model_dump()
            return

        if path == "integration/v2/schema/":
            yield Schema(
                ds_id=2,
                key="2.bar",
            ).model_dump()
            return

        if "integration/v1/datasource/" in path:
            yield Datasource(
                dbtype="snowflake",
                dbname="baz",
            ).model_dump()
            return

        if path == "integration/v2/column/":
            for col in [
                Column(
                    id=4,
                    name="col1",
                    description="col1 description",
                ),
                Column(
                    id=5,
                    name="col2",
                    description="col2 description",
                ),
            ]:
                yield col.model_dump()
            return

        if path == "integration/tag/":
            assert params
            if params["otype"] == "attribute":
                if params["oid"] == 4:
                    yield {"name": "col1 tag1"}
                    return

                for tag in [{"name": "col2 tag 1"}, {"name": "col2 tag 2"}]:
                    yield tag
                return

            for tag in [
                {
                    "name": "table tag1",
                },
                {
                    "name": "table tag2",
                },
                {
                    "name": "table tag3",
                },
            ]:
                yield tag
            return


@pytest.mark.asyncio
async def test_extractor(
    test_root_dir: str,
) -> None:
    config = AlationConfig(
        output=OutputConfig(),
        url="http://alation-instance",
        token="token",
    )
    extractor = AlationExtractor(config)
    extractor._client = MockClient()  # type: ignore
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]
    expected = f"{test_root_dir}/alation/expected.json"
    assert events == load_json(expected)
