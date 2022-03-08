from metaphor.models.metadata_change_event import DataPlatform

from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.power_bi.extractor import PowerBIExtractor


def test_parse_dataset_redshift():
    assert PowerBIExtractor.parse_power_query(
        'let\n    Source = AmazonRedshift.Database("url:5439","metaphor"),\n    public = Source{[Name="public"]}[Data],\n    sample_sales_records1 = public{[Name="sample_sales_records"]}[Data]\nin\n    sample_sales_records1'
    ) == to_dataset_entity_id(
        "metaphor.public.sample_sales_records", platform=DataPlatform.REDSHIFT
    )

    assert PowerBIExtractor.parse_power_query(
        'let\n    Source = AmazonRedshift.Database("url:5439","metaphor"),\n    private = Source{[Name="private"]}[Data],\n    test_scott_2 = private{[Name="test_scott_1"]}[Data]\nin\n    test_scott_2'
    ) == to_dataset_entity_id(
        "metaphor.private.test_scott_1", platform=DataPlatform.REDSHIFT
    )

    assert PowerBIExtractor.parse_power_query(
        'let\n    Source = AmazonRedshift.Database("url:5439","metaphor"),\n    private = Source{[Name="private"]}[Data],\n    test_scott_3 = private{[Name="test_scott_2"]}[Data]\nin\n    test_scott_3'
    ) == to_dataset_entity_id(
        "metaphor.private.test_scott_2", platform=DataPlatform.REDSHIFT
    )


def test_parse_dataset_snowflake():
    assert PowerBIExtractor.parse_power_query(
        'let\n    Source = Snowflake.Databases("metaphor-dev.snowflakecomputing.com","COMPUTE_WH"),\n    ACME_Database = Source{[Name="ACME",Kind="Database"]}[Data],\n    SHOPIFY_Schema = ACME_Database{[Name="SHOPIFY",Kind="Schema"]}[Data],\n    TRANSACTION_Table = SHOPIFY_Schema{[Name="TRANSACTION",Kind="Table"]}[Data]\nin\n    TRANSACTION_Table'
    ) == to_dataset_entity_id(
        "acme.shopify.transaction",
        platform=DataPlatform.SNOWFLAKE,
        account="metaphor-dev",
    )


def test_parse_dataset_bigquery():
    assert PowerBIExtractor.parse_power_query(
        'let\n    Source = GoogleBigQuery.Database(),\n    #"metaphor-data" = Source{[Name="metaphor-data"]}[Data],\n    test_Schema = #"metaphor-data"{[Name="test",Kind="Schema"]}[Data],\n    yi_test3_Table = test_Schema{[Name="yi_test3",Kind="Table"]}[Data]\nin\n    yi_test3_Table'
    ) == to_dataset_entity_id(
        "metaphor-data.test.yi_test3", platform=DataPlatform.BIGQUERY
    )
