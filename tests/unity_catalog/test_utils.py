from datetime import datetime
from typing import Optional, Tuple
from unittest.mock import MagicMock

from databricks.sdk.service.iam import ServicePrincipal
from pytest_snapshot.plugin import Snapshot

from metaphor.common.entity_id import dataset_normalized_name, to_dataset_entity_id
from metaphor.common.event_util import EventUtil
from metaphor.common.sql.process_query.config import ProcessQueryConfig
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetStructure,
    QueriedDataset,
    QueryLogs,
    SystemTag,
    SystemTags,
    SystemTagSource,
)
from metaphor.unity_catalog.models import Tag
from metaphor.unity_catalog.utils import (
    batch_get_last_refreshed_time,
    batch_get_table_properties,
    escape_special_characters,
    find_qualified_dataset,
    get_last_refreshed_time,
    get_query_logs,
    list_service_principals,
    to_system_tags,
)
from tests.test_utils import load_json
from tests.unity_catalog.mocks import mock_connection_pool, mock_sql_connection


def test_get_query_logs(test_root_dir):

    mock_cursor = MagicMock()
    mock_cursor.fetchall = MagicMock()
    mock_cursor.fetchall.return_value = [
        {
            "query_id": "query1",
            "query_type": None,
            "query_text": "create table db.sch.tgt as select * from db.sch.src",
            "start_time": datetime.fromisoformat("2024-01-01T12:00:00"),
            "duration": 123,
            "rows_read": 456,
            "rows_written": 456,
            "bytes_read": 789,
            "bytes_written": 789,
            "email": "john.doe@metaphor.io",
        },
        {
            "query_id": "query2",
            "query_type": "CREATE",
            "query_text": """
CREATE TABLE monthly_sales_summary AS
SELECT
    DATE_TRUNC('month', o.order_date) AS month,
    SUM(oi.quantity * oi.price) AS total_sales,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM
    orders o
JOIN
    order_items oi ON o.order_id = oi.order_id
WHERE
    o.order_status = 'completed'
GROUP BY
    DATE_TRUNC('month', o.order_date);
            """,
            "start_time": datetime.fromisoformat("2024-02-01T12:00:00"),
            "duration": 12345,
            "rows_read": 45678,
            "rows_written": 45691,
            "bytes_read": 78922,
            "bytes_written": 78911,
            "email": "sp1",
        },
    ]

    mock_cursor_ctx = MagicMock()
    mock_cursor_ctx.__enter__ = MagicMock()
    mock_cursor_ctx.__enter__.return_value = mock_cursor

    mock_connection = MagicMock()
    mock_connection.cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor_ctx

    logs = list(
        get_query_logs(
            mock_connection,
            1,
            set(),
            {"sp1": ServicePrincipal(display_name="service principal 1")},
            {
                "mydb.myschema.orders": Dataset(
                    structure=DatasetStructure(
                        database="myDb",
                        schema="mySchema",
                        table="orders",
                    ),
                ),
                "mydb.myschema.order_items": Dataset(
                    structure=DatasetStructure(
                        database="myDb",
                        schema="mySchema",
                        table="order_items",
                    ),
                ),
                "mydb.myschema.monthly_sales_summary": Dataset(
                    structure=DatasetStructure(
                        database="myDb",
                        schema="mySchema",
                        table="monthly_sales_summary",
                    ),
                ),
            },
            ProcessQueryConfig(),
        )
    )
    for log in logs:
        if log.sources:
            log.sources = sorted(log.sources, key=lambda source: source.id or "")
    logs = sorted((log for log in logs), key=lambda log: log.id or "")
    query_logs = EventUtil.build_then_trim(QueryLogs(logs))
    expected_query_logs = f"{test_root_dir}/unity_catalog/expected_query_logs.json"
    assert query_logs == load_json(expected_query_logs)


def test_find_qualified_dataset():
    def make_queried_dataset(
        parts: Tuple[Optional[str], Optional[str], str]
    ) -> QueriedDataset:
        return QueriedDataset(
            id=str(
                to_dataset_entity_id(
                    dataset_normalized_name(
                        parts[0],
                        parts[1],
                        parts[2],
                    ),
                    DataPlatform.UNITY_CATALOG,
                )
            ),
            database=parts[0],
            schema=parts[1],
            table=parts[2],
        )

    def make_dataset(parts: Tuple[str, str, str]) -> Dataset:
        return Dataset(
            structure=DatasetStructure(
                database=parts[0],
                schema=parts[1],
                table=parts[2],
            ),
        )

    dataset = make_queried_dataset(("db", "sch", "tab"))
    found = find_qualified_dataset(dataset, {})
    assert found and found.id == dataset.id

    dataset = make_queried_dataset((None, "sch", "tab"))
    found = find_qualified_dataset(
        dataset,
        {
            "db.sch.tab": make_dataset(("db", "sch", "tab")),
        },
    )
    assert found and found.id == make_queried_dataset(("db", "sch", "tab")).id

    dataset = make_queried_dataset((None, "sch", "tab"))
    found = find_qualified_dataset(
        dataset,
        {
            "db1.sch.tab": make_dataset(("db1", "sch", "tab")),
            "db2.sch.tab": make_dataset(("db2", "sch", "tab")),
        },
    )
    assert not found


def test_get_last_refreshed_time(
    test_root_dir: str,
    snapshot: Snapshot,
):

    mock_cursor = MagicMock()

    mock_connection = mock_sql_connection(
        [
            [
                {
                    "operation": "SET TBLPROPERTIES",
                    "timestamp": datetime(2020, 1, 4),
                },
                {
                    "operation": "ADD CONSTRAINT",
                    "timestamp": datetime(2020, 1, 3),
                },
                {
                    "operation": "CHANGE COLUMN",
                    "timestamp": datetime(2020, 1, 2),
                },
                {
                    "operation": "WRITE",
                    "timestamp": datetime(2020, 1, 1),
                },
            ]
        ],
        None,
        mock_cursor,
    )

    result = get_last_refreshed_time(mock_connection, "db.schema.table", 50)

    assert result == ("db.schema.table", datetime(2020, 1, 1))

    args = mock_cursor.execute.call_args_list[0].args
    snapshot.assert_match(args[0], "describe_history.sql")


def test_batch_get_last_refreshed_time():

    connection_pool = mock_connection_pool(
        [
            [
                {
                    "operation": "WRITE",
                    "timestamp": datetime(2020, 1, 1),
                },
            ],
            [
                {
                    "operation": "WRITE",
                    "timestamp": datetime(2020, 1, 1),
                },
            ],
        ],
    )

    result_map = batch_get_last_refreshed_time(connection_pool, ["a.b.c", "d.e.f"], 10)

    assert result_map == {"a.b.c": datetime(2020, 1, 1), "d.e.f": datetime(2020, 1, 1)}


def test_batch_get_table_properties():

    connection_pool = mock_connection_pool(
        [
            [
                {
                    "key": "prop1",
                    "value": "value1",
                },
            ],
            [
                {
                    "key": "prop2",
                    "value": "value2",
                },
            ],
        ],
    )

    result_map = batch_get_table_properties(connection_pool, ["a.b.c", "d.e.f"])

    assert result_map == {"a.b.c": {"prop1": "value1"}, "d.e.f": {"prop2": "value2"}}


def test_list_service_principals():

    sp1 = ServicePrincipal(application_id="sp1", display_name="SP1")
    sp2 = ServicePrincipal(application_id="sp2", display_name="SP2")

    mock_api = MagicMock()
    mock_api.service_principals = MagicMock()
    mock_api.service_principals.list.return_value = [sp1, sp2]

    principals = list_service_principals(mock_api)

    assert principals == {"sp1": sp1, "sp2": sp2}


def test_escape_special_characters():
    assert escape_special_characters("this.is.a_table") == "this.is.a_table"
    assert escape_special_characters("this.is.also-a-table") == "`this.is.also-a-table`"


def test_to_system_tags():
    assert to_system_tags(
        [Tag(key="tag", value="value"), Tag(key="tag2", value="")]
    ) == SystemTags(
        tags=[
            SystemTag(
                key="tag",
                value="value",
                system_tag_source=SystemTagSource.UNITY_CATALOG,
            ),
            SystemTag(
                key=None,
                value="tag2",
                system_tag_source=SystemTagSource.UNITY_CATALOG,
            ),
        ]
    )
