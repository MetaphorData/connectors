from metaphor.common.cli import cli_main
from metaphor.mysql.extractor import MySQLExtractor


def main(config_file: str):
    cli_main(MySQLExtractor, config_file)
