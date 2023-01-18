from metaphor.common.cli import cli_main
from metaphor.mssql.extractor import MssqlExtractor


def main(config_file: str):
    cli_main(MssqlExtractor, config_file)
