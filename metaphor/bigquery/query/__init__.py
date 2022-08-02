from metaphor.bigquery.query.extractor import BigQueryQueryExtractor
from metaphor.common.cli import cli_main


def main(config_file: str):
    cli_main(BigQueryQueryExtractor, config_file)
