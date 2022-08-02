from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.metabase.extractor import DatabaseInfo, MetabaseExtractor
from metaphor.models.metadata_change_event import Chart, ChartType, DataPlatform


def test_parse_database_and_card():
    extractor = MetabaseExtractor()

    database_json = {
        "name": "metaphor_bigquery",
        "updated_at": "2022-01-19T06:11:29.507426Z",
        "native_permissions": "write",
        "details": {
            "project-id": "metaphor",
            "service-account-json": "**MetabasePass**",
            "dataset-id": "foo",
            "use-jvm-timezone": False,
            "include-user-id-and-hash": True,
            "project-id-from-credentials": "metaphor",
        },
        "is_sample": False,
        "id": 1,
        "is_on_demand": False,
        "options": None,
        "engine": "bigquery-cloud-sdk",
        "created_at": "2022-01-19T06:09:07.180228Z",
    }

    extractor._parse_database(database_json)

    assert len(extractor._databases) == 1
    assert extractor._databases[1] == DatabaseInfo(
        platform=DataPlatform.BIGQUERY, database="metaphor", schema="foo", account=None
    )

    card_json = {
        "description": None,
        "archived": False,
        "table_id": 200,
        "creator": {
            "email": "abc@metaphor.io",
            "first_name": "Metaphor",
            "last_login": "2022-02-04T21:37:40.260905Z",
            "is_qbnewb": False,
            "is_superuser": True,
            "id": 1,
            "last_name": "Data",
            "date_joined": "2022-01-14T23:13:15.600349Z",
            "common_name": "Metaphor Data",
        },
        "database_id": 1,
        "enable_embedding": False,
        "collection_id": 34,
        "query_type": "query",
        "name": "Sample Sales Records by Total Revenue",
        "creator_id": 1,
        "updated_at": "2022-01-31T07:20:41.511367Z",
        "made_public_by_id": False,
        "dataset_query": {
            "type": "native",
            "database": 1,
            "native": {
                "query": "select a, b, c from table1 join table2 on table1.id = table2.id"
            },
        },
        "id": 3,
        "display": "bar",
        "last-edit-info": {
            "id": 1,
            "email": "abc@metaphor.io",
            "first_name": "Metaphor",
            "last_name": "Data",
            "timestamp": "2022-01-31T07:19:03.389714Z",
        },
        "favorite": False,
        "created_at": "2022-01-31T07:19:03.374289Z",
    }

    extractor._parse_chart(card_json)

    assert len(extractor._charts) == 1
    assert extractor._charts[3].chart == Chart(
        chart_type=ChartType.BAR,
        title="Sample Sales Records by Total Revenue",
        url="/card/3",
    )

    assert set(extractor._charts[3].upstream) == {
        str(to_dataset_entity_id("metaphor.foo.table1", DataPlatform.BIGQUERY)),
        str(to_dataset_entity_id("metaphor.foo.table2", DataPlatform.BIGQUERY)),
    }
