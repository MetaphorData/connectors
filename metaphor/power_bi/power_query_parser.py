import re
from typing import List, Optional, Tuple

from sql_metadata import Parser

from metaphor.common.entity_id import (
    EntityId,
    dataset_normalized_name,
    to_dataset_entity_id,
    to_dataset_entity_id_from_logical_id,
)
from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import (
    DataPlatform,
    DatasetLogicalID,
    FieldMapping,
    SourceField,
)

logger = get_logger()


class PowerQueryParser:
    """
    A simple PowerQuery parser, doc: https://docs.microsoft.com/en-us/powerquery-m/power-query-m-language-specification
    """

    @staticmethod
    def _parse_power_query(
        table_name: str, columns: List[str], power_query: str
    ) -> Tuple[List[EntityId], List[FieldMapping]]:
        lines = power_query.split("\n")

        platform_pattern = re.compile(r"Source = (\w+).Database")
        for idx, line in enumerate(lines):
            match = platform_pattern.search(line)
            if match:
                break
        assert match, "Can't parse platform from power query expression."
        platform_str = match.group(1)

        field_pattern_param = re.compile(r'{\[Name\s*=\s*"([\w\-]+)".*\]}')
        field_pattern_var = re.compile(r"^\s+(\w+)_\w+ = .+")

        def get_field(text: str) -> str:
            match = field_pattern_param.search(text)
            if match is None:
                match = field_pattern_var.search(text)

            assert match, f"Can't parse field from power query expression: {text}"
            return match.group(1)

        def is_direct_import(line: str) -> bool:
            """Check if the expression is a direct import of the source dataset"""
            return line.strip() == "in"

        account = None
        direct_import = False

        if platform_str == "AmazonRedshift":
            platform = DataPlatform.REDSHIFT
            db_pattern = re.compile(r"Source = (\w+).Database\((.*)\),$")
            match = db_pattern.search(lines[idx])
            assert (
                match
            ), "Can't parse AmazonRedshift database from power query expression"

            db = match.group(2).split(",")[1].strip().replace('"', "")
            schema = get_field(lines[idx + 1])
            table = get_field(lines[idx + 2])
            direct_import = is_direct_import(lines[idx + 3])

        elif platform_str == "Snowflake":
            platform = DataPlatform.SNOWFLAKE
            account_pattern = re.compile(r'Snowflake.Databases\("([\w\-\.]+)"')

            match = account_pattern.search(lines[idx])
            assert match, "Can't parse Snowflake account from power query expression"

            # remove trailing snowflakecomputing.com
            account = ".".join(match.group(1).split(".")[:-2])

            db = get_field(lines[idx + 1])
            schema = get_field(lines[idx + 2])
            table = get_field(lines[idx + 3])
            direct_import = is_direct_import(lines[idx + 4])

        elif platform_str == "GoogleBigQuery":
            platform = DataPlatform.BIGQUERY
            db = get_field(lines[idx + 1])
            schema = get_field(lines[idx + 2])
            table = get_field(lines[idx + 3])
            direct_import = is_direct_import(lines[idx + 4])

        else:
            raise AssertionError(f"Unknown platform ${platform_str}")

        dataset_logical_id = DatasetLogicalID(
            name=dataset_normalized_name(db, schema, table),
            platform=platform,
            account=account,
        )
        dataset_id = to_dataset_entity_id_from_logical_id(dataset_logical_id)

        # if the expression is direct import of source dataset, generate 1 to 1 field mapping based on table columns
        field_mappings = (
            [
                FieldMapping(
                    destination=f"{table_name}.{col}",  # prepend table name to the column name to distinguish tables in the same PowerBI dataset
                    sources=[
                        SourceField(
                            dataset=dataset_logical_id,
                            source_entity_id=str(dataset_id),
                            field=col,
                        )
                    ],
                )
                for col in columns
            ]
            if direct_import
            else []
        )

        return [dataset_id], field_mappings

    @staticmethod
    def extract_function_parameter(text: str, function_name: str) -> List[str]:
        """
        Return parameters for a given function name
        Input: 'int a = 1;\n printf("%d", a);'
        Output: ["\"%d\"", "a"]
        """

        left_parentheses_count, in_quote = 0, False
        start = index = text.find(function_name) + len(function_name)
        parameters = []

        # Skip to the first char of first parameter
        while start < len(text) and (text[start] == "(" or text[start].isspace()):
            start += 1

        def slice_with_ltrim(text: str, start: int, end: int):
            # Trim spaces and the left parentheses
            while start < end and text[start].isspace():
                start += 1
            return text[start:end]

        while True:
            ch = text[index]

            if ch == '"':
                in_quote = not in_quote
            elif not in_quote:
                if ch == "(":
                    left_parentheses_count += 1
                elif ch == ")":
                    left_parentheses_count -= 1
                elif left_parentheses_count == 1 and in_quote is False and ch == ",":
                    # Parameter should be in the first level, and not in quoted-string
                    parameters.append(slice_with_ltrim(text, start, index))
                    start = index + 1

            if index >= len(text) or (not ch.isspace() and left_parentheses_count == 0):
                break

            index += 1

        parameters.append(slice_with_ltrim(text, start, index))

        return parameters

    @staticmethod
    def _parse_platform(
        exp: str, snowflake_account: Optional[str]
    ) -> Tuple[Optional[DataPlatform], Optional[str], Optional[str]]:
        """
        Parse platform information from native query expression.

        :param exp: the native query expression
        :returns: a tuple of (platform_type, account, default_db)
        """

        lower_exp = exp.lower()
        account = None
        default_db = None

        if "snowflake" in lower_exp:
            url = PowerQueryParser.extract_function_parameter(
                lower_exp, "snowflake.databases"
            )[0]
            platform = DataPlatform.SNOWFLAKE
            account = (
                ".".join(url.split(".")[:-2]).replace('"', "") or snowflake_account
            )
        elif "bigquery" in lower_exp:
            platform = DataPlatform.BIGQUERY
        elif "redshift" in lower_exp:
            default_db = PowerQueryParser.extract_function_parameter(
                exp, "AmazonRedshift.Database"
            )[1].replace('"', "")
            platform = DataPlatform.REDSHIFT
        else:
            raise AssertionError(f"Unknown platform for a native query: ${exp}")

        return platform, account, default_db

    @staticmethod
    def _parse_tables(
        sql: str, default_db: Optional[str]
    ) -> List[Tuple[str, str, str]]:
        """
        Parse source tables from a SQL statement.

        :param sql: the sql query to parse
        :returns: a list of (database_name, schema_name, table_name)
        """

        parser = Parser(sql.replace('"', ""))
        tables = []
        for table_name in parser.tables:
            fields = table_name.split(".")

            # Fallback to default database name if not specified in query
            if len(fields) == 2 and default_db is not None:
                fields.insert(0, default_db)

            assert (
                len(fields) == 3
            ), f"Expecting a fully-qualified table name, {table_name}"
            tables.append((fields[0], fields[1], fields[2]))

        return tables

    @staticmethod
    def _parse_native_query(
        power_query: str, snowflake_account: Optional[str]
    ) -> List[EntityId]:
        parameters = PowerQueryParser.extract_function_parameter(
            power_query, "NativeQuery"
        )
        assert len(parameters) >= 2, "expecting at least two parameter for NativeQuery"

        platform, account, default_db = PowerQueryParser._parse_platform(
            parameters[0], snowflake_account
        )
        tables = PowerQueryParser._parse_tables(parameters[1], default_db)

        return [
            to_dataset_entity_id(
                dataset_normalized_name(db, schema, table), platform, account
            )
            for db, schema, table in tables
        ]

    @staticmethod
    def parse_query_expression(
        table_name: str,
        columns: List[str],
        expression: str,
        snowflake_account: Optional[str] = None,
    ) -> Tuple[List[EntityId], List[FieldMapping]]:
        """
        Parse the power query and return source entity IDs and field mapping
        """

        def replacer(match: re.Match) -> str:
            controls = {
                "lf": "\n",
                "cr": "\r",
                "tab": "\t",
            }
            return "".join(list(map(lambda m: controls.get(m, ""), match.groups())))

        # Replace the control character escape sequence for power query
        # Doc: https://docs.microsoft.com/en-us/powerquery-m/m-spec-lexical-structure#character-escape-sequences
        expression = re.sub(r"#\((cr|lf|tab)(,(cr|lf|tab))*\)", replacer, expression)

        if "Value.NativeQuery(" in expression:
            return (
                PowerQueryParser._parse_native_query(expression, snowflake_account),
                [],
            )  # don't support CLL yet
        else:
            return PowerQueryParser._parse_power_query(table_name, columns, expression)
