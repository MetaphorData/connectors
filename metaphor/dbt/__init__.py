from metaphor.common.cli import cli_main
from metaphor.dbt.extractor import DbtExtractor


def main(config_file: str):
    cli_main(DbtExtractor, config_file)
