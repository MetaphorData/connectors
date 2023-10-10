from metaphor.common.entity_id import to_dataset_entity_id
from metaphor.models.metadata_change_event import (
    DataPlatform,
    DatasetLogicalID,
    FieldMapping,
    SourceField,
)
from metaphor.power_bi.power_query_parser import PowerQueryParser


def test_parse_dataset_redshift():
    exp = 'let\n    Source = AmazonRedshift.Database("url:5439", "db" ),\n    public = Source{[Name="public"]}[Data],\n    table1 = public{[Name="table"]}[Data]\nin\n    table1'
    assert PowerQueryParser.parse_query_expression("table", ["col1", "col2"], exp) == (
        [to_dataset_entity_id("db.public.table", platform=DataPlatform.REDSHIFT)],
        [
            FieldMapping(
                destination="table.col1",
                sources=[
                    SourceField(
                        dataset=DatasetLogicalID(
                            name="db.public.table", platform=DataPlatform.REDSHIFT
                        ),
                        field="col1",
                        source_entity_id="DATASET~C640B5FA073F8E7910DDF6C8DCB2BB85",
                    )
                ],
            ),
            FieldMapping(
                destination="table.col2",
                sources=[
                    SourceField(
                        dataset=DatasetLogicalID(
                            name="db.public.table", platform=DataPlatform.REDSHIFT
                        ),
                        field="col2",
                        source_entity_id="DATASET~C640B5FA073F8E7910DDF6C8DCB2BB85",
                    )
                ],
            ),
        ],
    )


def test_parse_dataset_snowflake():
    exp = 'let\n    Source = Snowflake.Databases("some-account.snowflakecomputing.com","COMPUTE_WH"),\n    DB_Database = Source{[Name="DB",Kind="Database"]}[Data],\n    PUBLIC_Schema = DB_Database{[Name="PUBLIC",Kind="Schema"]}[Data],\n    TEST_Table = PUBLIC_Schema{[Name="TEST",Kind="Table"]}[Data]\nin\n    TEST_Table'
    assert PowerQueryParser.parse_query_expression("table", [], exp) == (
        [
            to_dataset_entity_id(
                "db.public.test",
                platform=DataPlatform.SNOWFLAKE,
                account="some-account",
            )
        ],
        [],
    )

    exp = 'let\n    Source = Snowflake.Databases("some-account.snowflakecomputing.com","COMPUTE_WH"),\n    DB_NAME_Database = Source{[Name=Database,Kind="Database"]}[Data],\n    PUBLIC_Schema = DB_Database{[Name="PUBLIC",Kind="Schema"]}[Data],\n    TEST_Table = PUBLIC_Schema{[Name="TEST",Kind="Table"]}[Data]\nin\n    TEST_Table'
    assert PowerQueryParser.parse_query_expression("table", [], exp) == (
        [
            to_dataset_entity_id(
                "db_name.public.test",
                platform=DataPlatform.SNOWFLAKE,
                account="some-account",
            )
        ],
        [],
    )


def test_parse_dataset_bigquery():
    exp = 'let\n    Source = GoogleBigQuery.Database(),\n    #"test-project" = Source{[Name="test-project"]}[Data],\n    test_Schema = #"test-project"{[Name="test",Kind="Schema"]}[Data],\n    example_Table = test_Schema{[Name="example",Kind="Table"]}[Data]\nin\n    example_Table'
    assert PowerQueryParser.parse_query_expression("table", ["foo"], exp) == (
        [
            to_dataset_entity_id(
                "test-project.test.example", platform=DataPlatform.BIGQUERY
            )
        ],
        [
            FieldMapping(
                destination="table.foo",
                sources=[
                    SourceField(
                        dataset=DatasetLogicalID(
                            name="test-project.test.example",
                            platform=DataPlatform.BIGQUERY,
                        ),
                        field="foo",
                        source_entity_id="DATASET~16C07C11C73EF917CC2C728B240ABDDA",
                    )
                ],
            ),
        ],
    )


def test_parse_native_query_parameters():
    exp = '''let\n    Source = Value.NativeQuery(Snowflake.Databases("some-account.dev.snowflakecomputing.com","WH_CA_USERS"){[Name="DB"]}[Data], "SELECT table1.id, table3.name#(lf)FROM db.foo.table1 AS table1#(lf)INNER JOIN db.bar.account AS account ON account.id = table1.account_id#(lf)INNER JOIN db.foo.table3 AS table3 ON table3.id = table1.product_id#(lf)WHERE table1.name in ('A','B')#(lf)ORDER BY 3", null, [EnableFolding=true]),\nin\n    #"Source"'''
    assert PowerQueryParser.parse_query_expression("table", [], exp) == (
        [
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
        ],
        [],
    )

    exp = 'let\n    Source = Value.NativeQuery(AmazonRedshift.Database("redshift-cluster-1.account.us-east-1.redshift.amazonaws.com","db"), "SELECT *#(lf)FROM schema1.table1#(lf)WHERE is_active = 1", null, [EnableFolding=true]),)'
    assert PowerQueryParser.parse_query_expression("table", [], exp) == (
        [
            to_dataset_entity_id(
                "db.schema1.table1",
                platform=DataPlatform.REDSHIFT,
            )
        ],
        [],
    )


def test_native_query_with_provided_account():
    exp = 'let\n    Source = Value.NativeQuery(Snowflake.Databases(Server,Warehouse){[Name=Database]}[Data], "/*#(lf)comments:#(lf)blah...#blah...#(lf)--SQL comments#(lf)    --COMMENT#(lf)#(lf)text#(lf)FOO#(lf)BAR#(lf)BAZ#(lf)*/#(lf)#(lf)SELECT #(lf)T.COL1 AS ""COL1"",#(lf)T.COL2 AS ""COL2"",#(lf)TT.COL2 AS ""COL3"",#(lf)--TT.COL4 AS ""COL4""#(lf)FROM  db.schema.TABLE AS ""T""#(lf)JOIN db.schema.TABLE2 AS ""TT"" ON T.A = TT.A", null, [EnableFolding=true]),\n    #"Changed Type" = Table.TransformColumnTypes(Source,{{"COL1", Int64.Type}, {"COL2", Int64.Type}, {"COL3", Int64.Type}})\nin\n    #"Changed Type"'
    assert PowerQueryParser.parse_query_expression("table", [], exp, "account") == (
        [
            to_dataset_entity_id(
                "db.schema.table",
                platform=DataPlatform.SNOWFLAKE,
                account="account",
            ),
            to_dataset_entity_id(
                "db.schema.table2", platform=DataPlatform.SNOWFLAKE, account="account"
            ),
        ],
        [],
    )


def test_extract_function_parameter():
    assert PowerQueryParser.extract_function_parameter(
        'int a = 1;\n printf("%d", a);', "printf"
    ) == ['"%d"', "a"]

    assert PowerQueryParser.extract_function_parameter(
        '  abc.foo(20, (1+2), "SELECT foo from TABLE")', "foo"
    ) == ["20", "(1+2)", '"SELECT foo from TABLE"']

    assert PowerQueryParser.extract_function_parameter(
        '  abc.foo(20, \n    \t(1+2), \n   "SELECT foo from TABLE  ", "())), asd")',
        "foo",
    ) == ["20", "(1+2)", '"SELECT foo from TABLE  "', '"())), asd"']


def test_parse_dataflow_snowflake():
    exp = 'section Section1;\r\nshared ENTITY_NAME = let\n    Source = Snowflake.Databases("some-account.snowflakecomputing.com","COMPUTE_WH"),\n    DB_Database = Source{[Name = "DB",Kind="Database"]}[Data],\n    PUBLIC_Schema = DB_Database{[Name="PUBLIC",Kind="Schema"]}[Data],\n    TEST_Table = PUBLIC_Schema{[Name="TEST",Kind="Table"]}[Data]\nin\n    TEST_Table'
    assert PowerQueryParser.parse_query_expression("table", [], exp) == (
        [
            to_dataset_entity_id(
                "db.public.test",
                platform=DataPlatform.SNOWFLAKE,
                account="some-account",
            )
        ],
        [],
    )
