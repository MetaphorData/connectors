import re
from typing import List, Optional, Tuple

from metaphor.models.metadata_change_event import DataPlatform
from sql_metadata import Parser

from metaphor.common.entity_id import EntityId, dataset_fullname, to_dataset_entity_id
from metaphor.common.logger import get_logger

logger = get_logger(__name__)


class PowerQueryParser:
    """
    A simple PowerQuery parser, doc: https://docs.microsoft.com/en-us/powerquery-m/power-query-m-language-specification
    """

    @staticmethod
    def parse_power_query(power_query) -> EntityId:
        lines = power_query.split("\n")
        platform_pattern = re.compile(r"Source = (\w+).")
        match = platform_pattern.search(lines[1])
        assert match, "Can't parse platform from power query expression."
        platform_str = match.group(1)

        field_pattern = re.compile(r'{\[Name="([\w\-]+)"(.*)\]}')

        def get_field(text: str) -> str:
            match = field_pattern.search(text)
            assert match, "Can't parse field from power query expression"
            return match.group(1)

        account = None
        if platform_str == "AmazonRedshift":
            platform = DataPlatform.REDSHIFT
            db_pattern = re.compile(r"Source = (\w+).Database\((.*)\),$")
            match = db_pattern.search(lines[1])
            assert (
                match
            ), "Can't parse AmazonRedshift database from power query expression"

            db = match.group(2).split(",")[1].replace('"', "")
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
            dataset_fullname(db, schema, table), platform, account
        )

    @staticmethod
    def extract_function_parameter(text: str, function_name: str) -> List[str]:
        """
        Return parameters for a given function name
        Input: 'int a = 1;\n printf("%d", a);'
        Output: ["\"%d\"", "a"]
        """

        start = index = text.find(function_name) + len(function_name)
        left_parentheses_count, in_quote = 0, False
        parameters = []

        def slice_with_ltrim(text: str, start: int, end: int):
            while text[start].isspace() or text[start] == "(":
                start += 1
            return text[start:end]

        while True:

            ch = text[index]

            if ch == "(":
                left_parentheses_count += 1
            elif ch == ")":
                left_parentheses_count -= 1
            elif ch == '"':
                in_quote = not in_quote
            elif left_parentheses_count == 1 and in_quote is False and ch == ",":
                # Parameter should be in the first level, and not in quoted-string
                parameters.append(slice_with_ltrim(text, start, index))
                start = index + 1

            if index >= len(text) or (not ch.isspace() and left_parentheses_count == 0):
                break

            index += 1
        parameters.append(slice_with_ltrim(text, start, index))
        assert len(parameters) >= 2, "expecting at least two parameter"
        return parameters

    @staticmethod
    def parse_platform(exp: str) -> Tuple[Optional[DataPlatform], Optional[str]]:
        lower_exp = exp.lower()
        account = None

        if "snowflake" in lower_exp:
            url = PowerQueryParser.extract_function_parameter(
                lower_exp, "snowflake.databases"
            )[0]
            platform = DataPlatform.SNOWFLAKE
            account = ".".join(url.split(".")[:-2]).replace('"', "")
        elif "bigquery" in lower_exp:
            platform = DataPlatform.BIGQUERY
        elif "redshift" in lower_exp:
            platform = DataPlatform.REDSHIFT

        return platform, account

    @staticmethod
    def parse_tables(sql: str) -> List[Tuple[str, str, str]]:
        parser = Parser(sql.replace('"', ""))
        tables = []
        for table_name in parser.tables:
            fields = table_name.split(".")

            assert (
                len(fields) == 3
            ), f"Expecting a fully-qualified table name, {table_name}"
            tables.append((fields[0], fields[1], fields[2]))

        return tables

    @staticmethod
    def parse_native_query(power_query: str) -> List[EntityId]:
        parameters = PowerQueryParser.extract_function_parameter(
            power_query, "NativeQuery"
        )
        platform, account = PowerQueryParser.parse_platform(parameters[0])
        tables = PowerQueryParser.parse_tables(parameters[1])

        return [
            to_dataset_entity_id(dataset_fullname(db, schema, table), platform, account)
            for db, schema, table in tables
        ]

    @staticmethod
    def parse_source_datasets(power_query: str) -> List[EntityId]:
        # Replace the the control character escape sequence for power query
        # Doc: https://docs.microsoft.com/en-us/powerquery-m/m-spec-lexical-structure#character-escape-sequences
        power_query = re.sub(r"#\((cr|lf|tab)(,(cr|lf|tab))*\)", " ", power_query)

        if "Value.NativeQuery(" in power_query:
            return PowerQueryParser.parse_native_query(power_query)

        try:
            return [PowerQueryParser.parse_power_query(power_query)]
        except AssertionError:
            logger.warning(f"Parsing upstream fail, exp: {power_query}")
        except IndexError:
            logger.warning(f"Parsing upstream fail, exp: {power_query}")

        return []
