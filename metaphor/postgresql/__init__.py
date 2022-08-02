from metaphor.common.cli import cli_main
from metaphor.postgresql.extractor import PostgreSQLExtractor


def main(config_file: str):
    cli_main(PostgreSQLExtractor, config_file)
