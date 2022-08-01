from metaphor.common.cli import cli_main
from metaphor.postgresql.usage.extractor import PostgreSQLUsageExtractor


def main(config_file: str):
    cli_main(PostgreSQLUsageExtractor, config_file)
