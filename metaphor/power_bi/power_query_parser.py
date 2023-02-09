import re
from typing import List, Optional, Tuple

from sql_metadata import Parser

from metaphor.common.entity_id import (
    EntityId,
    dataset_normalized_name,
    to_dataset_entity_id,
)
from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import DataPlatform

logger = get_logger()


class PowerQueryParser:
    """
    A simple PowerQuery parser, doc: https://docs.microsoft.com/en-us/powerquery-m/power-query-m-language-specification
    """

    @staticmethod
    def parse_power_query(power_query: str) -> EntityId:
        lines = power_query.split("\n")
        platform_pattern = re.compile(r"Source = (\w+).")
        match = platform_pattern.search(lines[1])
        assert match, "Can't parse platform from power query expression."
        platform_str = match.group(1)

        field_pattern_param = re.compile(r'{\[Name="([\w\-]+)".*\]}')
        field_pattern_var = re.compile(r"^\s+(\w+)_\w+ = .+")

        def get_field(text: str) -> str:
            match = field_pattern_param.search(text)
            if match is None:
                match = field_pattern_var.search(text)

            assert match, f"Can't parse field from power query expression: {text}"
            return match.group(1)

        account = None
        if platform_str == "AmazonRedshift":
            platform = DataPlatform.REDSHIFT
            db_pattern = re.compile(r"Source = (\w+).Database\((.*)\),$")
            match = db_pattern.search(lines[1])
            assert (
                match
            ), "Can't parse AmazonRedshift database from power query expression"

            db = match.group(2).split(",")[1].strip().replace('"', "")
            schema = get_field(lines[2])
            table = get_field(lines[3])
        elif platform_str == "Snowflake":
            platform = DataPlatform.SNOWFLAKE
            account_pattern = re.compile(r'Snowflake.Databases\("([\w\-\.]+)"')

            # remove trailing snowflakecomputing.com
            match = account_pattern.search(lines[1])
            assert match, "Can't parse Snowflake account from power query expression"

            account = ".".join(match.group(1).split(".")[:-2])
            db = get_field(lines[2])
            schema = get_field(lines[3])
            table = get_field(lines[4])
        elif platform_str == "GoogleBigQuery":
            platform = DataPlatform.BIGQUERY
            db = get_field(lines[2])
            schema = get_field(lines[3])
            table = get_field(lines[4])
        else:
            raise AssertionError(f"Unknown platform ${platform_str}")

        return to_dataset_entity_id(
            dataset_normalized_name(db, schema, table), platform, account
        )

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
    def parse_platform(
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
    def parse_tables(sql: str, default_db: Optional[str]) -> List[Tuple[str, str, str]]:
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
    def parse_native_query(
        power_query: str, snowflake_account: Optional[str]
    ) -> List[EntityId]:
        parameters = PowerQueryParser.extract_function_parameter(
            power_query, "NativeQuery"
        )
        assert len(parameters) >= 2, "expecting at least two parameter for NativeQuery"

        platform, account, default_db = PowerQueryParser.parse_platform(
            parameters[0], snowflake_account
        )
        tables = PowerQueryParser.parse_tables(parameters[1], default_db)

        return [
            to_dataset_entity_id(
                dataset_normalized_name(db, schema, table), platform, account
            )
            for db, schema, table in tables
        ]

    @staticmethod
    def parse_source_datasets(
        power_query: str, snowflake_account: Optional[str]
    ) -> List[EntityId]:
        def replacer(match: re.Match) -> str:
            controls = {
                "lf": "\n",
                "cr": "\r",
                "tab": "\t",
            }
            return "".join(list(map(lambda m: controls.get(m, ""), match.groups())))

        # Replace the the control character escape sequence for power query
        # Doc: https://docs.microsoft.com/en-us/powerquery-m/m-spec-lexical-structure#character-escape-sequences
        power_query = re.sub(r"#\((cr|lf|tab)(,(cr|lf|tab))*\)", replacer, power_query)

        if "Value.NativeQuery(" in power_query:
            return PowerQueryParser.parse_native_query(power_query, snowflake_account)

        try:
            return [PowerQueryParser.parse_power_query(power_query)]
        except (AssertionError, IndexError) as error:
            logger.warning(f"Parsing upstream fail, exp: {power_query}, error: {error}")

        return []
