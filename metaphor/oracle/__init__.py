from metaphor.common.cli import cli_main
from metaphor.oracle.extractor import OracleExtractor


def main(config_file: str):
    cli_main(OracleExtractor, config_file)
