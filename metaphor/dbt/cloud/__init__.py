from metaphor.common.cli import cli_main
from metaphor.dbt.cloud.extractor import DbtCloudExtractor


def main(config_file: str):
    cli_main(DbtCloudExtractor, config_file)
