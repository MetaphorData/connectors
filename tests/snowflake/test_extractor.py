from metaphor.snowflake.extractor import SnowflakeExtractor


def test_table_url():
    account = "foo"
    full_name = "this.is.TesT"
    assert (
        SnowflakeExtractor.build_table_url(account, full_name)
        == "https://foo.snowflakecomputing.com/console#/data/tables/detail?databaseName=THIS&schemaName=IS&tableName=TEST"
    )
