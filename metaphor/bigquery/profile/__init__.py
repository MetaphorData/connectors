from metaphor.bigquery.profile.extractor import BigQueryProfileExtractor
from metaphor.common.cli import cli_main


def main(config_file: str):
    cli_main(BigQueryProfileExtractor, config_file)
