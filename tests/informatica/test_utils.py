from metaphor.informatica.models import ConnectionDetail, ConnectorParams
from metaphor.informatica.utils import init_dataset_logical_id, parse_error
from metaphor.models.metadata_change_event import DataPlatform, DatasetLogicalID


def test_parse_error():
    assert parse_error('{"code": 1}') == {"code": 1}
    assert parse_error("[}]") is None


def test_init_dataset_logical_id():
    assert init_dataset_logical_id(
        "DB/SCHEMA/TABLE",
        ConnectionDetail(
            id="id",
            type="TOOLKIT_CCI",
            connectorGuid="com.infa.adapter.snowflake",
            connParams=ConnectorParams(account="account"),
        ),
    ) == DatasetLogicalID(
        name="db.schema.table",
        platform=DataPlatform.SNOWFLAKE,
        account="account",
    )

    assert (
        init_dataset_logical_id(
            "DB.SCHEMA.TABLE",
            ConnectionDetail(
                id="id",
                type="TOOLKIT_CCI",
                connectorGuid="com.infa.adapter.snowflake",
                connParams=ConnectorParams(account="account"),
            ),
        )
        is None
    )

    assert (
        init_dataset_logical_id(
            "DB/SCHEMA/TABLE",
            ConnectionDetail(
                id="id",
                type="MYSQL",
                connectorGuid="com.infa.adapter.snowflake",
                connParams=ConnectorParams(account="account"),
            ),
        )
        is None
    )

    assert (
        init_dataset_logical_id(
            "DB/SCHEMA/TABLE",
            ConnectionDetail(
                id="id",
                type="TOOLKIT_CCI",
                connectorGuid="com.infa.adapter.bigQuery",
                connParams=ConnectorParams(account="account"),
            ),
        )
        is None
    )
