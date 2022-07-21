from metaphor.models.metadata_change_event import DataPlatform

from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.power_bi.power_query_parser import PowerQueryParser


def test_parse_dataset_redshift():
    exp = 'let\n    Source = AmazonRedshift.Database("url:5439","db"),\n    public = Source{[Name="public"]}[Data],\n    table1 = public{[Name="table"]}[Data]\nin\n    table1'
    assert PowerQueryParser(exp).source_datasets() == [
        to_dataset_entity_id("db.public.table", platform=DataPlatform.REDSHIFT)
    ]


def test_parse_dataset_snowflake():
    exp = 'let\n    Source = Snowflake.Databases("some-account.snowflakecomputing.com","COMPUTE_WH"),\n    DB_Database = Source{[Name="DB",Kind="Database"]}[Data],\n    PUBLIC_Schema = DB_Database{[Name="PUBLIC",Kind="Schema"]}[Data],\n    TEST_Table = PUBLIC_Schema{[Name="TEST",Kind="Table"]}[Data]\nin\n    TEST_Table'
    assert PowerQueryParser(exp).source_datasets() == [
        to_dataset_entity_id(
            "db.public.test",
            platform=DataPlatform.SNOWFLAKE,
            account="some-account",
        )
    ]


def test_parse_dataset_bigquery():
    exp = 'let\n    Source = GoogleBigQuery.Database(),\n    #"test-project" = Source{[Name="test-project"]}[Data],\n    test_Schema = #"test-project"{[Name="test",Kind="Schema"]}[Data],\n    example_Table = test_Schema{[Name="example",Kind="Table"]}[Data]\nin\n    example_Table'
    assert PowerQueryParser(exp).source_datasets() == [
        to_dataset_entity_id(
            "test-project.test.example", platform=DataPlatform.BIGQUERY
        )
    ]


def test_parse_native_query_parameters():
    exp = '''let\n    Source = Value.NativeQuery(Snowflake.Databases("some-account.dev.snowflakecomputing.com","WH_CA_USERS"){[Name="DB_PRD_CONFORMED"]}[Data], "SELECT table1.id, table3.name#(lf)FROM db.foo.table1 AS table1#(lf)INNER JOIN db.bar.account AS account ON account.id = table1.account_id#(lf)INNER JOIN db.foo.table3 AS table3 ON table3.id = table1.product_id#(lf)WHERE table1.name in ('A','B')#(lf)ORDER BY 3", null, [EnableFolding=true]),\nin\n    #"Source"'''
    assert PowerQueryParser(exp).source_datasets() == [
        to_dataset_entity_id(
            "db.foo.table1",
            platform=DataPlatform.SNOWFLAKE,
            account="some-account.dev",
        ),
        to_dataset_entity_id(
            "db.bar.account",
            platform=DataPlatform.SNOWFLAKE,
            account="some-account.dev",
        ),
        to_dataset_entity_id(
            "db.foo.table3",
            platform=DataPlatform.SNOWFLAKE,
            account="some-account.dev",
        ),
    ]
