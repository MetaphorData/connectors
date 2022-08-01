from metaphor.common.cli import cli_main
from metaphor.postgresql.profile.extractor import PostgreSQLProfileExtractor


def main(config_file: str):
    cli_main(PostgreSQLProfileExtractor, config_file)
