from metaphor.models.metadata_change_event import DataPlatform

from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.power_bi.extractor import PowerBIExtractor


def test_parse_dataset_redshift():
    assert PowerBIExtractor.parse_power_query(
        'let\n    Source = AmazonRedshift.Database("url:5439","db"),\n    public = Source{[Name="public"]}[Data],\n    table1 = public{[Name="table"]}[Data]\nin\n    table1'
    ) == to_dataset_entity_id("db.public.table", platform=DataPlatform.REDSHIFT)


def test_parse_dataset_snowflake():
    assert PowerBIExtractor.parse_power_query(
        'let\n    Source = Snowflake.Databases("some-account.snowflakecomputing.com","COMPUTE_WH"),\n    DB_Database = Source{[Name="DB",Kind="Database"]}[Data],\n    PUBLIC_Schema = DB_Database{[Name="PUBLIC",Kind="Schema"]}[Data],\n    TEST_Table = PUBLIC_Schema{[Name="TEST",Kind="Table"]}[Data]\nin\n    TEST_Table'
    ) == to_dataset_entity_id(
        "db.public.test",
        platform=DataPlatform.SNOWFLAKE,
        account="some-account",
    )


def test_parse_dataset_bigquery():
    assert PowerBIExtractor.parse_power_query(
        'let\n    Source = GoogleBigQuery.Database(),\n    #"test-project" = Source{[Name="test-project"]}[Data],\n    test_Schema = #"test-project"{[Name="test",Kind="Schema"]}[Data],\n    example_Table = test_Schema{[Name="example",Kind="Table"]}[Data]\nin\n    example_Table'
    ) == to_dataset_entity_id(
        "test-project.test.example", platform=DataPlatform.BIGQUERY
    )
