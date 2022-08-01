from metaphor.bigquery.extractor import BigQueryExtractor
from metaphor.common.cli import cli_main


def main(config_file: str):
    cli_main(BigQueryExtractor, config_file)
